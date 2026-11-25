"""存储分层领域服务：热/冷层判定、迁移窗口、冷文件交接包与接收回执。

冷层迁移的分层语义：
- 迁移清单按快照导出，导出瞬间冻结准备迁移的分区与文件身份，只收录冷层文件；
- 冷端副本校验通过后，只有接收回执被确认，读取位置才从热端原位置切到冷端副本；
- 回执迟到或重复投递时只确认一次；归属其它湖表或内容无法校验的回执保持未确认，
  时间旅行仍按旧快照回读；
- 旧位置释放申请在确认后由冷端接手，原位置在接手端结清前继续保留；
- 分层对账对同一份迁移清单只核销一次已迁字节。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import (
    MigrationManifest,
    ReleaseRequest,
    lake_store,
)


class StorageTierService(BaseModuleService):
    MODULE = "storage_tier"
    ENTITY = "分层规则"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["分层编号", "所属湖表", "热层判定", "冷层判定", "迁移窗口", "压缩算法", "迁移字节数", "最近迁移"]
    REQUIRED_FIELDS = ["分层编号", "所属湖表", "热层判定"]
    STATUS_ORDER = ["草稿", "已生效", "迁移中", "已暂停"]
    ACTION_RULES = {"启效分层": "已生效", "启动迁移": "迁移中", "暂停分层": "已暂停"}
    NEGATIVE_ACTIONS = []

    def classify_to_tier(self, lake_table: str, hot_days: int = 7) -> dict[str, Any]:
        """按时间窗口区分热层/冷层：创建时间超过 hot_days 的分区为冷层。"""
        files = lake_store.list_files(lake_table)
        now = time.time()
        hot_files = [f for f in files if lake_store.is_hot(f, hot_days=hot_days, now=now)]
        cold_files = [f for f in files if not lake_store.is_hot(f, hot_days=hot_days, now=now)]
        return {
            "lake_table": lake_table,
            "hot_days": hot_days,
            "hot_file_count": len(hot_files),
            "cold_file_count": len(cold_files),
            "hot_bytes": sum(f.file_bytes for f in hot_files),
            "cold_bytes": sum(f.file_bytes for f in cold_files),
        }

    def migrate_cold_files(self, lake_table: str, compression: str = "SNAPPY",
                           hot_days: int = 7) -> dict[str, Any]:
        """迁移冷层文件到压缩存储；热层文件不参与迁移。"""
        files = lake_store.list_files(lake_table)
        migrated_fids: list[str] = []
        migrated_bytes = 0
        for f in files:
            if lake_store.is_hot(f, hot_days=hot_days):
                continue
            original_bytes = f.file_bytes
            # 冷层压缩体积降 60%
            f.file_bytes = max(1, int(original_bytes * 0.4))
            migrated_bytes += original_bytes - f.file_bytes
            migrated_fids.append(f.file_id)
        return {
            "lake_table": lake_table,
            "compression": compression,
            "migrated_file_count": len(migrated_fids),
            "migrated_file_ids": migrated_fids,
            "bytes_saved": migrated_bytes,
        }

    # ---- 按快照导出迁移清单 ----

    def export_migration_manifest(
        self, lake_table: str, snapshot_id: str, compression: str = "SNAPPY",
        manifest_id: str | None = None, hot_days: int = 7,
    ) -> dict[str, Any]:
        """按快照导出冷文件交接包清单。

        只把该快照里属于冷层的文件列为迁移对象；热层文件 H 即使在同一分区、
        同一快照里也不会进入清单。清单条目冻结导出时的文件身份。
        """
        mid = manifest_id or f"MM-{lake_table}-{uuid.uuid4().hex[:8]}"
        try:
            manifest = lake_store.export_migration_manifest(
                mid, lake_table, snapshot_id, compression=compression, hot_days=hot_days)
        except KeyError as exc:
            return {"ok": False, "error": str(exc)}
        except PermissionError as exc:
            return {"ok": False, "error": str(exc)}
        return {
            "ok": True,
            "manifest_id": manifest.manifest_id,
            "lake_table": manifest.lake_table,
            "snapshot_id": manifest.snapshot_id,
            "compression": manifest.compression,
            "entry_count": len(manifest.entries),
            "entries": [entry.to_dict() for entry in manifest.entries],
            "exported_at": manifest.created_at,
        }

    def list_manifests(self, lake_table: str | None = None) -> list[dict[str, Any]]:
        return [
            self._manifest_view(manifest)
            for manifest in lake_store.list_migration_manifests(lake_table)
        ]

    def get_manifest(self, manifest_id: str) -> dict[str, Any] | None:
        manifest = lake_store.get_migration_manifest(manifest_id)
        return self._manifest_view(manifest) if manifest else None

    @staticmethod
    def _manifest_view(manifest: MigrationManifest) -> dict[str, Any]:
        return {
            "manifest_id": manifest.manifest_id,
            "lake_table": manifest.lake_table,
            "snapshot_id": manifest.snapshot_id,
            "compression": manifest.compression,
            "entry_count": len(manifest.entries),
            "entries": [entry.to_dict() for entry in manifest.entries],
            "created_at": manifest.created_at,
        }

    # ---- 冷端副本校验 ----

    def verify_cold_copy(
        self, lake_table: str, file_id: str, copy_checksum: str | None = None,
        rows: int | None = None, file_bytes: int | None = None,
        source_snapshot: str | None = None,
    ) -> dict[str, Any]:
        """登记并校验冷端副本内容：摘要必须与热端原文件一致。"""
        original = lake_store.get_file(lake_table, file_id)
        if original is None:
            return {"ok": False, "verified": False, "reason": "原文件不存在，无法比对内容"}
        checksum = copy_checksum or original.file_manifest
        verified = checksum == original.file_manifest
        copy = lake_store.register_cold_copy(
            lake_table=lake_table,
            file_id=file_id,
            partition_key=original.partition_key,
            rows=rows if rows is not None else original.file_rows,
            bytes_=file_bytes if file_bytes is not None else original.file_bytes,
            copy_checksum=checksum,
            source_snapshot=source_snapshot,
            verified=verified,
        )
        return {
            "ok": verified,
            "verified": verified,
            "file_id": copy.file_id,
            "partition_key": copy.partition_key,
            "copy_checksum": copy.copy_checksum,
            "file_rows": copy.file_rows,
            "verified_at": copy.verified_at,
            "reason": "" if verified else "冷端副本内容摘要与热端原文件不一致",
        }

    def list_cold_copies(self, lake_table: str) -> list[dict[str, Any]]:
        return [
            {
                "file_id": c.file_id,
                "partition_key": c.partition_key,
                "file_rows": c.file_rows,
                "file_bytes": c.file_bytes,
                "copy_checksum": c.copy_checksum,
                "verified": c.verified,
                "verified_at": c.verified_at,
            }
            for c in lake_store.list_cold_copies(lake_table)
        ]

    # ---- 接收回执 ----

    def submit_receipt(
        self, manifest_id: str, receipt_id: str, file_id: str,
        lake_table: str, content_checksum: str,
        release_request_id: str | None = None, defer: bool = False,
    ) -> dict[str, Any]:
        """投递迁移接收回执：重复投递同一回执只确认一次。

        defer=True 表示回执已收到但确认处理迟到：先登记为 pending，读取位置
        仍是原位置；再次投递同一 receipt_id 时才做内容对账并确认。
        确认条件：回执文件在清单内、归属同一张湖表、冷端副本已校验且摘要一致。
        归属其它湖表或内容无法校验时回执被拒，文件仍在未确认列表，
        时间旅行继续按旧快照回读。
        """
        manifest = lake_store.get_migration_manifest(manifest_id)
        receipt = lake_store.submit_migration_receipt(
            receipt_id, manifest_id, lake_table, file_id, content_checksum)
        if receipt.status != "pending":
            # 迟到的重复投递：状态保持不变，读取位置与释放申请也不被改写。
            return {
                "ok": receipt.status == "confirmed",
                "receipt_id": receipt.receipt_id,
                "status": receipt.status,
                "duplicate": True,
                "submission_count": receipt.submission_count,
                "reason": receipt.reason,
                "read_location": self._read_location(lake_table, file_id),
            }
        if defer:
            return {
                "ok": False,
                "receipt_id": receipt.receipt_id,
                "status": "pending",
                "duplicate": receipt.submission_count > 1,
                "submission_count": receipt.submission_count,
                "reason": "回执已收到，确认处理迟到，读取位置仍为原位置",
                "read_location": self._read_location(lake_table, file_id),
            }
        return self._evaluate_receipt(
            receipt, manifest, lake_table, file_id, content_checksum,
            release_request_id=release_request_id)

    def _evaluate_receipt(
        self, receipt, manifest, lake_table: str, file_id: str,
        content_checksum: str, release_request_id: str | None = None,
    ) -> dict[str, Any]:
        receipt_id = receipt.receipt_id
        if manifest is None:
            lake_store.confirm_migration_receipt(
                receipt_id, False, "迁移清单不存在，回执无法校验")
            return self._receipt_result(receipt, lake_table, file_id)
        if manifest.lake_table != lake_table:
            lake_store.confirm_migration_receipt(
                receipt_id, False,
                f"回执文件归属湖表 {lake_table} 与清单湖表 {manifest.lake_table} 不一致")
            return self._receipt_result(receipt, lake_table, file_id)
        entry = next((e for e in manifest.entries if e.file_id == file_id), None)
        if entry is None:
            lake_store.confirm_migration_receipt(
                receipt_id, False, "回执文件不在迁移清单内")
            return self._receipt_result(receipt, lake_table, file_id)
        if entry.lake_table != lake_table:
            lake_store.confirm_migration_receipt(
                receipt_id, False,
                f"回执文件归属湖表 {entry.lake_table}，不属于本表迁移清单")
            return self._receipt_result(receipt, lake_table, file_id)
        if lake_store.file_belongs_to_other_table(lake_table, file_id):
            lake_store.confirm_migration_receipt(
                receipt_id, False, "回执文件已归属另一张湖表，不能纳入本表冷迁移")
            return self._receipt_result(receipt, lake_table, file_id)

        copy = lake_store.get_cold_copy(lake_table, file_id)
        content_ok = (
            content_checksum == entry.file_manifest
            and copy is not None
            and copy.verified
            and copy.copy_checksum == content_checksum
        )
        if not content_ok:
            lake_store.confirm_migration_receipt(
                receipt_id, False, "回执内容无法与已校验的冷端副本对账")
            return self._receipt_result(receipt, lake_table, file_id)

        lake_store.confirm_migration_receipt(receipt_id, True, "回执与冷端校验副本一致")
        # 确认后旧位置释放申请交给冷端接手；原位置在冷端结清前继续保留。
        if release_request_id:
            lake_store.hand_over_release_request(
                release_request_id, "cold", "回执确认后由冷端接手旧位置释放")
        return self._receipt_result(receipt, lake_table, file_id)

    def _receipt_result(self, receipt, lake_table: str, file_id: str) -> dict[str, Any]:
        return {
            "ok": receipt.status == "confirmed",
            "receipt_id": receipt.receipt_id,
            "manifest_id": receipt.manifest_id,
            "file_id": receipt.file_id,
            "status": receipt.status,
            "duplicate": receipt.submission_count > 1,
            "submission_count": receipt.submission_count,
            "reason": receipt.reason,
            "read_location": self._read_location(lake_table, file_id),
        }

    def list_receipts(self, manifest_id: str | None = None) -> list[dict[str, Any]]:
        return [
            {
                "receipt_id": r.receipt_id,
                "manifest_id": r.manifest_id,
                "lake_table": r.lake_table,
                "file_id": r.file_id,
                "status": r.status,
                "reason": r.reason,
                "submission_count": r.submission_count,
                "submitted_at": r.submitted_at,
                "confirmed_at": r.confirmed_at,
            }
            for r in lake_store.list_migration_receipts(manifest_id)
        ]

    def unconfirmed_files(self, lake_table: str) -> list[dict[str, Any]]:
        """未确认列表：清单里没有已确认回执的文件，附最近一次回执结论。"""
        result: list[dict[str, Any]] = []
        confirmed_ids = {
            r.file_id for r in lake_store.list_migration_receipts()
            if r.status == "confirmed" and r.lake_table == lake_table
        }
        for manifest in lake_store.list_migration_manifests(lake_table):
            for entry in manifest.entries:
                if entry.file_id in confirmed_ids:
                    continue
                receipts = lake_store.list_migration_receipts(manifest.manifest_id)
                latest = next(
                    (r for r in receipts if r.file_id == entry.file_id), None)
                result.append({
                    "manifest_id": manifest.manifest_id,
                    "snapshot_id": manifest.snapshot_id,
                    "file_id": entry.file_id,
                    "partition_key": entry.partition_key,
                    "file_rows": entry.file_rows,
                    "file_bytes": entry.file_bytes,
                    "status": latest.status if latest else "pending",
                    "reason": latest.reason if latest else "尚未收到接收回执",
                    "read_location": self._read_location(lake_table, entry.file_id),
                })
        return result

    # ---- 读取位置 ----

    def _read_location(self, lake_table: str, file_id: str) -> dict[str, Any]:
        location = lake_store.resolve_read_location(lake_table, file_id)
        copy = lake_store.get_cold_copy(lake_table, file_id)
        if copy is not None:
            location["cold_copy_rows"] = copy.file_rows
            location["cold_copy_partition"] = copy.partition_key
        return location

    def read_location(self, lake_table: str, file_id: str) -> dict[str, Any]:
        """核对单个文件当前由原位置还是校验过的冷副本提供读取。"""
        return {"ok": True, **self._read_location(lake_table, file_id)}

    # ---- 旧位置释放申请 ----

    def open_release_request(
        self, lake_table: str, file_id: str, request_id: str | None = None,
        requested_by: str = "hot", reason: str = "热层发起旧位置释放申请",
    ) -> dict[str, Any]:
        rid = request_id or f"RR-{lake_table}-{uuid.uuid4().hex[:8]}"
        request = lake_store.open_release_request(rid, lake_table, file_id, requested_by, reason)
        return self._release_view(request)

    def hand_over_release_request(self, request_id: str) -> dict[str, Any]:
        """确认归属后把释放申请移交给实际持有方（冷端）。"""
        request = lake_store.hand_over_release_request(
            request_id, "cold", "回执确认，冷端接手旧位置释放")
        if request is None:
            return {"ok": False, "error": f"释放申请 {request_id} 不存在"}
        return self._release_view(request)

    def settle_release_request(self, request_id: str, completed: bool = True) -> dict[str, Any]:
        """接手端结清释放；只有接手方为冷端时才允许释放原位置。"""
        request = lake_store.get_release_request(request_id)
        if request is None:
            return {"ok": False, "error": f"释放申请 {request_id} 不存在"}
        if completed and request.owner != "cold":
            return {"ok": False, "error": "释放申请尚未由冷端接手，原位置继续保留"}
        if completed:
            copy = lake_store.get_cold_copy(request.lake_table, request.file_id)
            receipt_confirmed = any(
                r.status == "confirmed" and r.file_id == request.file_id
                and r.lake_table == request.lake_table
                for r in lake_store.list_migration_receipts()
            )
            if copy is None or not copy.verified or not receipt_confirmed:
                return {"ok": False, "error": "冷端副本未校验或回执未确认，原位置不得释放"}
            lake_store.remove_files(request.lake_table, [request.file_id])
        request = lake_store.settle_release_request(
            request_id, completed, "冷端接手并结清后释放原位置"
            if completed else "释放申请被驳回，原位置保留")
        return self._release_view(request)

    @staticmethod
    def _release_view(request: ReleaseRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "request_id": request.request_id,
            "lake_table": request.lake_table,
            "file_id": request.file_id,
            "requested_by": request.requested_by,
            "owner": request.owner,
            "status": request.status,
            "original_released": request.original_released,
            "reason": request.reason,
        }

    def list_release_requests(self, lake_table: str, file_id: str | None = None) -> list[dict[str, Any]]:
        return [self._release_view(r) for r in lake_store.list_release_requests(lake_table, file_id)]

    # ---- 分层对账 ----

    def reconcile(self, manifest_id: str) -> dict[str, Any]:
        """分层对账：按清单核销已迁字节，同一份清单只核销一次。"""
        manifest = lake_store.get_migration_manifest(manifest_id)
        if manifest is None:
            return {"ok": False, "error": f"迁移清单 {manifest_id} 不存在"}
        total_bytes = sum(entry.file_bytes for entry in manifest.entries)
        reconciled = lake_store.record_manifest_reconciliation(manifest_id, total_bytes)
        return {
            "ok": True,
            "manifest_id": manifest_id,
            "entry_count": len(manifest.entries),
            "migrated_bytes": total_bytes,
            "reconciled_now": reconciled,
            "already_reconciled": reconciled == 0,
            "total_reconciled": lake_store.get_manifest_reconciliation(manifest_id),
        }
