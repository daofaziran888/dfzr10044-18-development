"""时间旅行领域服务：按快照/时间点回溯查询历史湖表数据。

回读时逐个文件核对实际读取位置：
- 已确认回执且冷端副本校验通过的文件，从冷端副本读取；
- 回执未确认（迟到、归属其它湖表或内容无法校验）时，只要原位置仍保留文件，
  旧快照继续从热端原位置回读；
- 原位置已释放但冷端副本已校验时，回退到冷端副本，保证旧快照仍可读。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class TimeTravelService(BaseModuleService):
    MODULE = "time_travel"
    ENTITY = "回溯查询"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["查询编号", "所属湖表", "目标快照", "回溯时间点", "查询列", "返回行数", "命中分区数", "查询耗时"]
    REQUIRED_FIELDS = ["查询编号", "所属湖表", "目标快照"]
    STATUS_ORDER = ["待执行", "执行中", "已返回", "执行失败"]
    ACTION_RULES = {"发起查询": "执行中", "取消查询": "已返回", "标记失败": "执行失败"}
    NEGATIVE_ACTIONS = []

    def _file_placement(self, lake_table: str, file_id: str) -> dict[str, Any]:
        """核对单个文件在本次回溯中由哪一端提供读取。"""
        original = lake_store.get_file(lake_table, file_id)
        copy = lake_store.get_cold_copy(lake_table, file_id)
        location = lake_store.resolve_read_location(lake_table, file_id)

        if location["read_location"] == "cold" and copy is not None:
            return {
                "file_id": file_id,
                "partition_key": copy.partition_key,
                "read_location": "cold",
                "source": "verified_cold_copy",
                "rows": copy.file_rows,
                "cold_copy_verified": True,
                "original_retained": original is not None,
                "original_released": original is None,
            }
        if original is not None:
            return {
                "file_id": file_id,
                "partition_key": original.partition_key,
                "read_location": "hot",
                "source": "original_position",
                "rows": original.file_rows,
                "cold_copy_verified": bool(copy and copy.verified),
                "original_retained": True,
                "original_released": False,
            }
        if copy is not None and copy.verified:
            # 原位置释放但回执未确认：旧快照回退到已校验冷副本，仍可读。
            return {
                "file_id": file_id,
                "partition_key": copy.partition_key,
                "read_location": "cold",
                "source": "verified_cold_copy_fallback",
                "rows": copy.file_rows,
                "cold_copy_verified": True,
                "original_retained": False,
                "original_released": True,
            }
        return {
            "file_id": file_id,
            "partition_key": "",
            "read_location": "missing",
            "source": "unavailable",
            "rows": 0,
            "cold_copy_verified": False,
            "original_retained": False,
            "original_released": False,
        }

    def read_at_snapshot(self, lake_table: str, snapshot_id: str,
                         columns: list[str] | None = None,
                         manifest_id: str | None = None) -> dict[str, Any]:
        """按指定快照 ID 读取历史数据并返回每个文件的实际读取位置。"""
        snap = lake_store.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return {"success": False, "error": "snapshot not found", "rows": 0, "partitions": 0}
        placements = [self._file_placement(lake_table, fid) for fid in snap.file_ids]
        total_rows = sum(item["rows"] for item in placements)
        partitions = sorted({item["partition_key"] for item in placements if item["partition_key"]})
        result: dict[str, Any] = {
            "success": True,
            "snapshot_id": snapshot_id,
            "rows": total_rows,
            "row_count": total_rows,
            "partition_count": len(partitions),
            "partitions_hit": partitions,
            "files": placements,
            "read_locations": {
                item["file_id"]: item["read_location"] for item in placements
            },
            "query_time": time.time(),
        }
        if manifest_id is not None:
            result["manifest_check"] = self.compare_manifest_with_snapshot(
                manifest_id, lake_table, snapshot_id, placements)
        return result

    def read_at_timepoint(self, lake_table: str, timestamp: float,
                          manifest_id: str | None = None) -> dict[str, Any]:
        """按时间点回溯查找最近可用快照并读取。"""
        snap = lake_store.get_snapshot_at_time(lake_table, timestamp)
        if snap is None:
            return {"success": False, "error": "no available snapshot at time", "rows": 0, "partitions": 0}
        return self.read_at_snapshot(lake_table, snap.snapshot_id, manifest_id=manifest_id)

    def compare_manifest_with_snapshot(
        self, manifest_id: str, lake_table: str, snapshot_id: str,
        placements: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """用已导出的迁移清单核对当时准备迁移的分区和文件身份。

        清单条目是导出瞬间冻结的，即使文件后来压缩、迁层或被释放，
        仍可按 file_id 与摘要核对“当时准备迁移的分区和文件身份”。
        """
        manifest = lake_store.get_migration_manifest(manifest_id)
        snap = lake_store.get_snapshot(lake_table, snapshot_id)
        if manifest is None:
            return {"ok": False, "error": f"迁移清单 {manifest_id} 不存在"}
        if snap is None:
            return {"ok": False, "error": f"快照 {snapshot_id} 不存在"}
        if manifest.lake_table != lake_table or snap.lake_table != lake_table:
            return {"ok": False, "error": "清单或快照不属于该湖表"}
        if placements is None:
            placements = [self._file_placement(lake_table, fid) for fid in snap.file_ids]
        location_map = {item["file_id"]: item for item in placements}
        entries = []
        all_match = True
        for entry in manifest.entries:
            location = location_map.get(entry.file_id)
            in_snapshot = entry.file_id in snap.file_ids
            matches = in_snapshot and location is not None
            all_match = all_match and matches
            entries.append({
                "file_id": entry.file_id,
                "partition_key": entry.partition_key,
                "file_rows": entry.file_rows,
                "file_bytes": entry.file_bytes,
                "file_manifest": entry.file_manifest,
                "in_snapshot": in_snapshot,
                # 清单导出时（迁移前）与本次回读时（迁移后）的同一文件读取位置
                "read_location_before": entry.read_location,
                "read_location_after": location["read_location"] if location else "missing",
                "read_location": location["read_location"] if location else "missing",
                "source": location["source"] if location else "unavailable",
            })
        return {
            "ok": all_match,
            "manifest_id": manifest_id,
            "snapshot_id": snapshot_id,
            "manifest_snapshot_matches": manifest.snapshot_id == snapshot_id,
            "entries": entries,
        }

    def compare_two_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的数据差异。"""
        s_a = lake_store.get_snapshot(lake_table, snap_a)
        s_b = lake_store.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"success": False, "error": "snapshot not found"}
        return {
            "success": True,
            "snap_a": snap_a,
            "snap_b": snap_b,
            "diff_added_rows": max(0, s_b.added_rows - s_a.added_rows),
            "diff_deleted_rows": max(0, s_b.deleted_rows - s_a.deleted_rows),
        }
