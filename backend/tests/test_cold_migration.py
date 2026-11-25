"""冷层迁移交接：按快照导出迁移清单、冷端校验、接收回执与旧位置释放。

覆盖场景时间线：
导出清单(只含冷文件 C) → 冷端校验副本 → 接收确认迟到 → 热层发起释放申请
→ 回溯旧快照核对 H/C 读取位置 → 重投同一回执 → 冷端接手释放 → 分层对账只核销一次。
"""
from __future__ import annotations

import pytest

from app.lake_store import lake_store
from app.services.storage_tier import StorageTierService
from app.services.time_travel import TimeTravelService


def _make_table(name: str):
    """H(热) 与 C(冷) 各 5 行，同处一个分区，同一份快照。"""
    hot = lake_store.register_file(name, "20261101", rows=5, bytes_=1280, tier="hot")
    cold = lake_store.register_file(name, "20261101", rows=5, bytes_=2560, tier="cold")
    snap = lake_store.create_snapshot(name, [hot.file_id, cold.file_id], added_rows=10)
    lake_store.mark_snapshot_available(name, snap.snapshot_id)
    return hot, cold, snap


class TestManifestExport:
    def test_manifest_only_cold_files(self):
        hot, cold, snap = _make_table("cm_export")
        svc = StorageTierService()
        result = svc.export_migration_manifest(
            "cm_export", snap.snapshot_id, manifest_id="MM-cm-export")
        assert result["ok"]
        assert result["entry_count"] == 1
        entry = result["entries"][0]
        assert entry["file_id"] == cold.file_id
        assert entry["partition_key"] == "20261101"
        assert entry["file_rows"] == 5
        assert entry["file_manifest"] == cold.file_manifest
        assert all(e["file_id"] != hot.file_id for e in result["entries"])

    def test_manifest_rejects_snapshot_of_other_table(self):
        _hot, _cold, snap = _make_table("cm_foreign_a")
        lake_store.register_file("cm_foreign_b", "20261101", rows=5, bytes_=1280, tier="cold")
        svc = StorageTierService()
        result = svc.export_migration_manifest("cm_foreign_b", snap.snapshot_id)
        assert not result["ok"]

    def test_manifest_frozen_after_compression(self):
        _hot, cold, snap = _make_table("cm_frozen")
        svc = StorageTierService()
        exported = svc.export_migration_manifest(
            "cm_frozen", snap.snapshot_id, manifest_id="MM-cm-frozen")
        original_bytes = exported["entries"][0]["file_bytes"]
        svc.migrate_cold_files("cm_frozen")
        stored = svc.get_manifest("MM-cm-frozen")
        assert stored["entries"][0]["file_bytes"] == original_bytes
        assert stored["entries"][0]["file_id"] == cold.file_id
        assert stored["entries"][0]["partition_key"] == "20261101"
        assert stored["entries"][0]["file_manifest"] == cold.file_manifest


class TestColdCopyVerification:
    def test_verify_matching_copy(self):
        _hot, cold, _snap = _make_table("cm_verify_ok")
        svc = StorageTierService()
        result = svc.verify_cold_copy("cm_verify_ok", cold.file_id)
        assert result["ok"] and result["verified"]
        assert result["copy_checksum"] == cold.file_manifest

    def test_verify_mismatched_copy(self):
        _hot, cold, _snap = _make_table("cm_verify_bad")
        svc = StorageTierService()
        result = svc.verify_cold_copy("cm_verify_bad", cold.file_id, copy_checksum="deadbeef")
        assert not result["ok"]
        assert not result["verified"]


class TestReceiptLifecycle:
    def _prepare(self, table: str, manifest_id: str):
        hot, cold, snap = _make_table(table)
        svc = StorageTierService()
        exported = svc.export_migration_manifest(table, snap.snapshot_id, manifest_id=manifest_id)
        assert exported["entry_count"] == 1
        assert svc.verify_cold_copy(table, cold.file_id)["verified"]
        return svc, hot, cold, snap

    def test_delayed_receipt_keeps_original_position(self):
        svc, hot, cold, snap = self._prepare("cm_late", "MM-cm-late")
        release = svc.open_release_request("cm_late", cold.file_id, request_id="RR-cm-late")

        # 接收确认迟到：回执已收到但未确认
        pending = svc.submit_receipt(
            "MM-cm-late", "RC-cm-late", cold.file_id, "cm_late",
            cold.file_manifest, defer=True)
        assert pending["status"] == "pending"
        assert pending["read_location"]["read_location"] == "hot"
        assert pending["read_location"]["original_retained"] is True

        # 回溯旧快照：H、C 各 5 行，共 10 行
        travel = TimeTravelService()
        read = travel.read_at_snapshot("cm_late", snap.snapshot_id, manifest_id="MM-cm-late")
        assert read["success"]
        assert read["row_count"] == 10
        assert read["partition_count"] == 1
        c_read = next(f for f in read["files"] if f["file_id"] == cold.file_id)
        assert c_read["read_location"] == "hot"
        assert c_read["source"] == "original_position"
        assert c_read["original_retained"] is True
        assert c_read["original_released"] is False
        assert read["manifest_check"]["ok"]
        check_entry = read["manifest_check"]["entries"][0]
        assert check_entry["read_location_before"] == "hot"
        assert check_entry["read_location_after"] == "hot"

        # 释放申请仍由热端持有，原位置不释放
        requests = svc.list_release_requests("cm_late", cold.file_id)
        assert requests[0]["owner"] == "hot"
        assert requests[0]["original_released"] is False
        assert svc.settle_release_request("RR-cm-late", completed=True)["ok"] is False

        # 重投同一份回执：确认成功，换到校验过的冷副本
        confirmed = svc.submit_receipt(
            "MM-cm-late", "RC-cm-late", cold.file_id, "cm_late",
            cold.file_manifest, release_request_id=release["request_id"])
        assert confirmed["ok"] and confirmed["status"] == "confirmed"
        assert confirmed["duplicate"] is True
        assert confirmed["read_location"]["read_location"] == "cold"
        # 冷端接手释放，但尚未结清前原位置仍保留
        assert confirmed["read_location"]["original_retained"] is True
        handover = svc.list_release_requests("cm_late", cold.file_id)[0]
        assert handover["owner"] == "cold"
        assert handover["status"] == "taken_over_by_cold"
        assert handover["original_released"] is False

        read2 = travel.read_at_snapshot("cm_late", snap.snapshot_id)
        c_read2 = next(f for f in read2["files"] if f["file_id"] == cold.file_id)
        assert c_read2["read_location"] == "cold"
        assert c_read2["source"] == "verified_cold_copy"
        assert read2["row_count"] == 10

        # 冷端结清后才释放原位置，旧快照仍能从冷副本读到同一文件
        settled = svc.settle_release_request("RR-cm-late", completed=True)
        assert settled["ok"] and settled["original_released"] is True
        assert lake_store.get_file("cm_late", cold.file_id) is None
        read3 = travel.read_at_snapshot("cm_late", snap.snapshot_id)
        c_read3 = next(f for f in read3["files"] if f["file_id"] == cold.file_id)
        assert c_read3["read_location"] == "cold"
        assert c_read3["original_released"] is True
        assert read3["row_count"] == 10

    def test_resubmitting_receipt_does_not_double_confirm(self):
        svc, _hot, cold, snap = self._prepare("cm_dup", "MM-cm-dup")
        first = svc.submit_receipt(
            "MM-cm-dup", "RC-cm-dup", cold.file_id, "cm_dup", cold.file_manifest)
        assert first["ok"] and first["status"] == "confirmed" and not first["duplicate"]
        second = svc.submit_receipt(
            "MM-cm-dup", "RC-cm-dup", cold.file_id, "cm_dup", cold.file_manifest)
        assert second["status"] == "confirmed"
        assert second["duplicate"] is True
        assert second["submission_count"] == 2
        receipts = svc.list_receipts("MM-cm-dup")
        assert len(receipts) == 1
        # 重复投递不改变读取位置
        assert second["read_location"]["read_location"] == "cold"
        assert TimeTravelService().read_at_snapshot("cm_dup", snap.snapshot_id)["row_count"] == 10

    def test_receipt_for_other_table_stays_unconfirmed(self):
        svc, _hot, cold, snap = self._prepare("cm_owner_a", "MM-cm-owner-a")
        lake_store.register_file("cm_owner_b", "20261101", rows=5, bytes_=1280, tier="cold")
        rejected = svc.submit_receipt(
            "MM-cm-owner-a", "RC-cm-owner-a", cold.file_id, "cm_owner_b", cold.file_manifest)
        assert not rejected["ok"]
        assert rejected["status"] == "rejected"
        # 旧快照仍可作为回读依据，文件仍在未确认列表
        read = TimeTravelService().read_at_snapshot("cm_owner_a", snap.snapshot_id)
        assert read["success"] and read["row_count"] == 10
        unconfirmed = svc.unconfirmed_files("cm_owner_a")
        assert any(item["file_id"] == cold.file_id for item in unconfirmed)
        assert svc.read_location("cm_owner_a", cold.file_id)["read_location"] == "hot"

    def test_receipt_file_registered_in_other_table_stays_unconfirmed(self):
        svc, _hot, cold, snap = self._prepare("cm_owner_c", "MM-cm-owner-c")
        # 同一 file_id 又被登记到另一张湖表：回执即使声明的湖表正确也不能确认
        lake_store._files.setdefault("cm_owner_d", {})[cold.file_id] = lake_store.get_file(
            "cm_owner_c", cold.file_id)
        rejected = svc.submit_receipt(
            "MM-cm-owner-c", "RC-cm-owner-c", cold.file_id, "cm_owner_c", cold.file_manifest)
        assert rejected["status"] == "rejected"
        assert "另一张湖表" in rejected["reason"]
        assert svc.read_location("cm_owner_c", cold.file_id)["read_location"] == "hot"
        assert TimeTravelService().read_at_snapshot("cm_owner_c", snap.snapshot_id)["row_count"] == 10
        assert svc.unconfirmed_files("cm_owner_c")[0]["file_id"] == cold.file_id

    def test_receipt_with_unverifiable_content_stays_unconfirmed(self):
        svc, _hot, cold, snap = self._prepare("cm_badsum", "MM-cm-badsum")
        rejected = svc.submit_receipt(
            "MM-cm-badsum", "RC-cm-badsum", cold.file_id, "cm_badsum", "not-the-checksum")
        assert not rejected["ok"]
        assert rejected["status"] == "rejected"
        unconfirmed = svc.unconfirmed_files("cm_badsum")
        assert len(unconfirmed) == 1
        assert unconfirmed[0]["status"] == "rejected"
        read = TimeTravelService().read_at_snapshot("cm_badsum", snap.snapshot_id)
        assert read["row_count"] == 10
        assert all(f["read_location"] == "hot" for f in read["files"])


class TestReconciliation:
    def test_reconcile_only_once(self):
        _hot, _cold, snap = _make_table("cm_recon")
        svc = StorageTierService()
        svc.export_migration_manifest("cm_recon", snap.snapshot_id, manifest_id="MM-cm-recon")
        first = svc.reconcile("MM-cm-recon")
        second = svc.reconcile("MM-cm-recon")
        assert first["reconciled_now"] == 2560
        assert second["reconciled_now"] == 0
        assert second["already_reconciled"] is True
        assert second["total_reconciled"] == 2560

    def test_reconcile_unknown_manifest(self):
        assert StorageTierService().reconcile("MM-none")["ok"] is False
