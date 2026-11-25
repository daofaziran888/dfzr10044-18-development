"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float
    # hot | cold | auto；auto 时按创建时间与热层天数判定（兼容历史调用）
    tier: str = "auto"


# ----------------------------- 冷层迁移交接 -----------------------------

@dataclass
class MigrationManifestEntry:
    """迁移清单条目：导出瞬间冻结的分区与文件身份，后续不得跟随文件变化。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str
    tier: str = "cold"
    read_location: str = "hot"  # 导出时该文件的读取位置

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_id": self.file_id,
            "lake_table": self.lake_table,
            "partition_key": self.partition_key,
            "file_rows": self.file_rows,
            "file_bytes": self.file_bytes,
            "file_manifest": self.file_manifest,
            "tier": self.tier,
            "read_location": self.read_location,
        }


@dataclass
class MigrationManifest:
    """按快照导出的冷文件交接包清单。"""
    manifest_id: str
    lake_table: str
    snapshot_id: str
    entries: list[MigrationManifestEntry]
    compression: str
    created_at: float


@dataclass
class ColdCopy:
    """冷端副本：记录从冷端校验过的内容摘要与可回读的行/分区信息。"""
    lake_table: str
    file_id: str
    partition_key: str
    file_rows: int
    file_bytes: int
    copy_checksum: str
    source_snapshot: str | None
    verified: bool
    verified_at: float | None


@dataclass
class MigrationReceipt:
    """迁移接收回执：同一份回执可重复投递，但只确认/核销一次。"""
    receipt_id: str
    manifest_id: str
    lake_table: str
    file_id: str
    content_checksum: str
    status: str  # confirmed | rejected
    reason: str
    submitted_at: float
    confirmed_at: float | None
    submission_count: int = 1


@dataclass
class ReleaseRequest:
    """旧位置释放申请：由发起端记录，确认归属后由接手端执行。"""
    request_id: str
    lake_table: str
    file_id: str
    requested_by: str  # hot
    owner: str  # hot | cold
    status: str  # pending | taken_over_by_cold | completed | rejected
    created_at: float
    updated_at: float
    original_released: bool
    reason: str


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 冷层迁移：交接包清单 / 冷端副本 / 接收回执 / 释放申请
        self._migration_manifests: dict[str, MigrationManifest] = {}
        self._cold_copies: dict[tuple[str, str], ColdCopy] = {}
        self._migration_receipts: dict[str, MigrationReceipt] = {}
        self._release_requests: dict[str, ReleaseRequest] = {}
        # 已核销的迁移字节：manifest_id -> 已核销字节，保证同一份清单只核销一次
        self._reconciled_manifests: dict[str, int] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int,
        manifest: str | None = None, tier: str = "auto",
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
            tier=tier,
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def is_hot(self, pf: PartitionFile, hot_days: int = 7, now: float | None = None) -> bool:
        """判定文件属于热层还是冷层：显式 tier 优先，否则按创建时间。"""
        if pf.tier == "hot":
            return True
        if pf.tier == "cold":
            return False
        current = time.time() if now is None else now
        return (current - pf.created_at) / 86400 <= hot_days

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 冷层迁移交接 ----

    def export_migration_manifest(
        self, manifest_id: str, lake_table: str, snapshot_id: str,
        compression: str = "SNAPPY", hot_days: int = 7,
    ) -> MigrationManifest:
        """按快照导出冷文件迁移清单：只收录该快照中属于冷层的文件。

        清单在导出瞬间冻结文件身份（file_id/分区/行数/字节/摘要），
        之后即使文件体积、位置发生变化，清单内容仍可核对当时的准备对象。
        """
        snap = self.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            raise KeyError(f"快照不存在：{snapshot_id}")
        if snap.lake_table != lake_table:
            raise PermissionError(
                f"快照 {snapshot_id} 不属于湖表 {lake_table}，不能作为该湖表的迁移来源")
        entries: list[MigrationManifestEntry] = []
        for fid in snap.file_ids:
            pf = self.get_file(lake_table, fid)
            if pf is None:
                continue
            if self.is_hot(pf, hot_days=hot_days):
                continue
            entries.append(MigrationManifestEntry(
                file_id=pf.file_id,
                lake_table=pf.lake_table,
                partition_key=pf.partition_key,
                file_rows=pf.file_rows,
                file_bytes=pf.file_bytes,
                file_manifest=pf.file_manifest,
                tier="cold",
                read_location="hot",
            ))
        manifest = MigrationManifest(
            manifest_id=manifest_id,
            lake_table=lake_table,
            snapshot_id=snapshot_id,
            entries=entries,
            compression=compression,
            created_at=time.time(),
        )
        self._migration_manifests[manifest_id] = manifest
        return manifest

    def get_migration_manifest(self, manifest_id: str) -> MigrationManifest | None:
        return self._migration_manifests.get(manifest_id)

    def list_migration_manifests(self, lake_table: str | None = None) -> list[MigrationManifest]:
        manifests = list(self._migration_manifests.values())
        if lake_table is not None:
            manifests = [m for m in manifests if m.lake_table == lake_table]
        return manifests

    def register_cold_copy(
        self, lake_table: str, file_id: str, partition_key: str,
        rows: int, bytes_: int, copy_checksum: str,
        source_snapshot: str | None = None, verified: bool = False,
    ) -> ColdCopy:
        """登记冷端副本；校验通过后 verified=True，可作为回读来源。"""
        now = time.time()
        copy = ColdCopy(
            lake_table=lake_table,
            file_id=file_id,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            copy_checksum=copy_checksum,
            source_snapshot=source_snapshot,
            verified=verified,
            verified_at=now if verified else None,
        )
        self._cold_copies[(lake_table, file_id)] = copy
        return copy

    def get_cold_copy(self, lake_table: str, file_id: str) -> ColdCopy | None:
        return self._cold_copies.get((lake_table, file_id))

    def list_cold_copies(self, lake_table: str) -> list[ColdCopy]:
        return [c for (lk, _fid), c in self._cold_copies.items() if lk == lake_table]

    def find_cold_copy_by_checksum(
        self, lake_table: str, partition_key: str, copy_checksum: str,
    ) -> ColdCopy | None:
        """按分区与内容摘要定位冷端副本，用于校验回执内容是否可信。"""
        for copy in self.list_cold_copies(lake_table):
            if copy.partition_key == partition_key and copy.copy_checksum == copy_checksum:
                return copy
        return None

    def submit_migration_receipt(
        self, receipt_id: str, manifest_id: str, lake_table: str, file_id: str,
        content_checksum: str,
    ) -> MigrationReceipt:
        """投递接收回执：同一 receipt_id 重复投递时只累加次数，状态以首次确认为准。"""
        existing = self._migration_receipts.get(receipt_id)
        if existing is not None:
            existing.submission_count += 1
            return existing
        receipt = MigrationReceipt(
            receipt_id=receipt_id,
            manifest_id=manifest_id,
            lake_table=lake_table,
            file_id=file_id,
            content_checksum=content_checksum,
            status="pending",
            reason="",
            submitted_at=time.time(),
            confirmed_at=None,
        )
        self._migration_receipts[receipt_id] = receipt
        return receipt

    def get_migration_receipt(self, receipt_id: str) -> MigrationReceipt | None:
        return self._migration_receipts.get(receipt_id)

    def list_migration_receipts(self, manifest_id: str | None = None) -> list[MigrationReceipt]:
        receipts = list(self._migration_receipts.values())
        if manifest_id is not None:
            receipts = [r for r in receipts if r.manifest_id == manifest_id]
        return receipts

    def confirm_migration_receipt(
        self, receipt_id: str, accepted: bool, reason: str = "",
    ) -> MigrationReceipt | None:
        """确认回执结果；已确认的回执不会被二次确认改写。"""
        receipt = self._migration_receipts.get(receipt_id)
        if receipt is None:
            return None
        if receipt.status != "pending":
            return receipt
        receipt.status = "confirmed" if accepted else "rejected"
        receipt.reason = reason
        receipt.confirmed_at = time.time()
        return receipt

    def file_belongs_to_other_table(self, lake_table: str, file_id: str) -> bool:
        """回执里的文件标识是否实际归属另一张湖表。"""
        for (lk, fid), _copy in self._cold_copies.items():
            if fid == file_id and lk != lake_table:
                return True
        for other_table, files in self._files.items():
            if other_table != lake_table and file_id in files:
                return True
        return False

    def open_release_request(
        self, request_id: str, lake_table: str, file_id: str,
        requested_by: str = "hot", reason: str = "",
    ) -> ReleaseRequest:
        """发起旧位置释放申请；归属在确认回执后由核验结果决定。"""
        now = time.time()
        request = ReleaseRequest(
            request_id=request_id,
            lake_table=lake_table,
            file_id=file_id,
            requested_by=requested_by,
            owner=requested_by,
            status="pending",
            created_at=now,
            updated_at=now,
            original_released=False,
            reason=reason,
        )
        self._release_requests[request_id] = request
        return request

    def get_release_request(self, request_id: str) -> ReleaseRequest | None:
        return self._release_requests.get(request_id)

    def list_release_requests(self, lake_table: str, file_id: str | None = None) -> list[ReleaseRequest]:
        requests = [r for r in self._release_requests.values() if r.lake_table == lake_table]
        if file_id is not None:
            requests = [r for r in requests if r.file_id == file_id]
        return requests

    def hand_over_release_request(self, request_id: str, owner: str, reason: str = "") -> ReleaseRequest | None:
        """把释放申请移交给实际持有文件的一端；旧位置不因申请而立即释放。"""
        request = self._release_requests.get(request_id)
        if request is None:
            return None
        request.owner = owner
        request.status = "taken_over_by_cold" if owner == "cold" else "pending"
        request.updated_at = time.time()
        if reason:
            request.reason = reason
        return request

    def settle_release_request(self, request_id: str, completed: bool, reason: str = "") -> ReleaseRequest | None:
        """结清释放申请：由接手端执行后才释放原位置。"""
        request = self._release_requests.get(request_id)
        if request is None:
            return None
        request.status = "completed" if completed else "rejected"
        request.original_released = completed
        request.updated_at = time.time()
        if reason:
            request.reason = reason
        return request

    def record_manifest_reconciliation(self, manifest_id: str, migrated_bytes: int) -> int:
        """登记清单已核销的迁移字节；同一份清单只核销一次，返回本次实际核销字节。"""
        if manifest_id in self._reconciled_manifests:
            return 0
        self._reconciled_manifests[manifest_id] = migrated_bytes
        return migrated_bytes

    def get_manifest_reconciliation(self, manifest_id: str) -> int | None:
        return self._reconciled_manifests.get(manifest_id)

    def resolve_read_location(self, lake_table: str, file_id: str) -> dict[str, Any]:
        """解析文件当前的实际读取位置，供时间旅行核对迁移前后读取位置。

        优先级：已确认迁移且冷副本校验通过 -> 冷端；
        否则回退热端原位置（只要文件仍在注册表里）。
        """
        copy = self.get_cold_copy(lake_table, file_id)
        file = self.get_file(lake_table, file_id)
        confirmed = any(
            r.status == "confirmed" and r.file_id == file_id and r.lake_table == lake_table
            for r in self._migration_receipts.values()
        )
        if copy is not None and copy.verified and confirmed:
            return {
                "file_id": file_id,
                "read_location": "cold",
                "cold_copy_verified": True,
                "original_present": file is not None,
                "original_retained": file is not None,
                "original_released": file is None,
            }
        return {
            "file_id": file_id,
            "read_location": "hot" if file is not None else "missing",
            "cold_copy_verified": bool(copy and copy.verified),
            "original_present": file is not None,
            "original_retained": file is not None,
            "original_released": file is None and copy is not None,
        }

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()