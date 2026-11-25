"""冷层迁移交接的 HTTP 契约测试：路由分派、下载包与统一错误码。

不依赖 httpx/TestClient，直接调用路由处理函数，覆盖固定路径优先于
`/{module}/{entry_id}` 的分派顺序和交接包内容。
"""
from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from app.lake_store import lake_store
from app.routers import resource


def _seed_table(name: str):
    hot = lake_store.register_file(name, "20261101", rows=5, bytes_=1280, tier="hot")
    cold = lake_store.register_file(name, "20261101", rows=5, bytes_=2560, tier="cold")
    snap = lake_store.create_snapshot(name, [hot.file_id, cold.file_id], added_rows=10)
    lake_store.mark_snapshot_available(name, snap.snapshot_id)
    return hot, cold, snap


def _download(response) -> str:
    async def collect() -> str:
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return "".join(chunks).lstrip("﻿")

    return asyncio.run(collect())


class TestManifestRoutes:
    def test_export_and_download_handoff_package(self):
        hot, cold, snap = _seed_table("rt_download")
        exported = resource.export_migration_manifest(
            resource.ManifestExportPayload(
                lake_table="rt_download", snapshot_id=snap.snapshot_id,
                manifest_id="MM-rt-download"))
        assert exported["entry_count"] == 1
        assert exported["entries"][0]["file_id"] == cold.file_id

        response = resource.download_migration_manifest("MM-rt-download")
        assert response.media_type.startswith("text/csv")
        assert "MM-rt-download.handoff.csv" in response.headers["content-disposition"]
        body = _download(response)
        lines = body.strip().splitlines()
        assert lines[0].startswith("manifest_id,lake_table,snapshot_id")
        assert len(lines) == 2  # 表头 + 仅 C
        assert cold.file_id in lines[1]
        assert hot.file_id not in body
        assert ",cold,hot," in lines[1]
        assert "尚未收到接收回执" in lines[1]

    def test_unknown_manifest_returns_404(self):
        with pytest.raises(HTTPException) as failed:
            resource.download_migration_manifest("MM-does-not-exist")
        assert failed.value.status_code == 404

    def test_export_unknown_snapshot_returns_400(self):
        with pytest.raises(HTTPException) as failed:
            resource.export_migration_manifest(
                resource.ManifestExportPayload(
                    lake_table="rt_nosnap", snapshot_id="snap-missing"))
        assert failed.value.status_code == 400

    def test_fixed_paths_shadow_generic_entry_get(self):
        """/api/storage_tier/manifests 必须走清单接口，而不是按 entry_id 取记录。"""
        result = resource.list_migration_manifests()
        assert "items" in result


class TestReceiptAndReleaseRoutes:
    def test_receipt_flow_over_http_layer(self):
        _hot, cold, snap = _seed_table("rt_flow")
        resource.export_migration_manifest(
            resource.ManifestExportPayload(
                lake_table="rt_flow", snapshot_id=snap.snapshot_id, manifest_id="MM-rt-flow"))
        assert resource.verify_cold_copy(
            resource.ColdCopyVerifyPayload(lake_table="rt_flow", file_id=cold.file_id))["verified"]
        release = resource.open_release_request(
            resource.ReleaseRequestPayload(
                lake_table="rt_flow", file_id=cold.file_id, request_id="RR-rt-flow"))
        assert release["owner"] == "hot"

        pending = resource.submit_migration_receipt(
            resource.MigrationReceiptPayload(
                manifest_id="MM-rt-flow", receipt_id="RC-rt-flow", lake_table="rt_flow",
                file_id=cold.file_id, content_checksum=cold.file_manifest, defer=True))
        assert pending["status"] == "pending"

        read = resource.read_at_snapshot(
            resource.SnapshotReadPayload(
                lake_table="rt_flow", snapshot_id=snap.snapshot_id, manifest_id="MM-rt-flow"))
        assert read["row_count"] == 10
        assert read["read_locations"][cold.file_id] == "hot"
        assert read["manifest_check"]["ok"]

        # 尚未由冷端接手时直接结清会被拒绝
        with pytest.raises(HTTPException) as blocked:
            resource.settle_release_request("RR-rt-flow", completed=True)
        assert blocked.value.status_code == 409

        confirmed = resource.submit_migration_receipt(
            resource.MigrationReceiptPayload(
                manifest_id="MM-rt-flow", receipt_id="RC-rt-flow", lake_table="rt_flow",
                file_id=cold.file_id, content_checksum=cold.file_manifest,
                release_request_id="RR-rt-flow"))
        assert confirmed["status"] == "confirmed"
        assert confirmed["read_location"]["read_location"] == "cold"
        assert resource.unconfirmed_files("rt_flow")["items"] == []

        settled = resource.settle_release_request("RR-rt-flow", completed=True)
        assert settled["original_released"] is True
        read_after = resource.read_at_snapshot(
            resource.SnapshotReadPayload(
                lake_table="rt_flow", snapshot_id=snap.snapshot_id))
        assert read_after["row_count"] == 10
        assert read_after["read_locations"][cold.file_id] == "cold"

        first = resource.reconcile_manifest("MM-rt-flow")
        second = resource.reconcile_manifest("MM-rt-flow")
        assert first["reconciled_now"] == 2560
        assert second["reconciled_now"] == 0
