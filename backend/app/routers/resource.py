"""统一资源接口：查询、导出、登记、动作与冷层迁移交接按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services.storage_tier import StorageTierService
from app.services.time_travel import TimeTravelService

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


# ------------------------- 冷层迁移交接接口 -------------------------
# 固定路径必须注册在 /{module}/{entry_id} 之前，否则会被动态路径抢先匹配。

def _storage_tier() -> StorageTierService:
    service = service_for("storage_tier")
    assert isinstance(service, StorageTierService)
    return service


def _time_travel() -> TimeTravelService:
    service = service_for("time_travel")
    assert isinstance(service, TimeTravelService)
    return service


class ManifestExportPayload(BaseModel):
    lake_table: str
    snapshot_id: str
    compression: str = "SNAPPY"
    manifest_id: str | None = None
    hot_days: int = 7


class ColdCopyVerifyPayload(BaseModel):
    lake_table: str
    file_id: str
    copy_checksum: str | None = None
    rows: int | None = None
    file_bytes: int | None = None
    source_snapshot: str | None = None


class MigrationReceiptPayload(BaseModel):
    manifest_id: str
    receipt_id: str
    lake_table: str
    file_id: str
    content_checksum: str
    release_request_id: str | None = None
    defer: bool = False


class ReleaseRequestPayload(BaseModel):
    lake_table: str
    file_id: str
    request_id: str | None = None
    requested_by: str = "hot"
    reason: str = "热层发起旧位置释放申请"


class SnapshotReadPayload(BaseModel):
    lake_table: str
    snapshot_id: str
    manifest_id: str | None = None
    columns: list[str] | None = None


class TimepointReadPayload(BaseModel):
    lake_table: str
    timestamp: float
    manifest_id: str | None = None


@router.get("/storage_tier/manifests")
def list_migration_manifests(lake_table: str | None = Query(default=None)) -> dict[str, Any]:
    """已导出的迁移清单：条目为导出瞬间冻结的分区与文件身份。"""
    return {"items": _storage_tier().list_manifests(lake_table)}


@router.get("/storage_tier/manifests/{manifest_id}")
def get_migration_manifest(manifest_id: str) -> dict[str, Any]:
    manifest = _storage_tier().get_manifest(manifest_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"迁移清单 {manifest_id} 不存在")
    return manifest


@router.get("/storage_tier/manifests/{manifest_id}/download")
def download_migration_manifest(manifest_id: str) -> StreamingResponse:
    """下载冷文件交接包：清单条目与接收回执状态一起导出，便于交接核对。"""
    manifest = _storage_tier().get_manifest(manifest_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"迁移清单 {manifest_id} 不存在")
    receipts = _storage_tier().list_receipts(manifest_id)
    receipt_by_file = {item["file_id"]: item for item in receipts}
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "manifest_id", "lake_table", "snapshot_id", "compression",
        "file_id", "partition_key", "file_rows", "file_bytes", "file_manifest",
        "tier", "read_location", "receipt_id", "receipt_status", "receipt_reason",
    ])
    for entry in manifest["entries"]:
        receipt = receipt_by_file.get(entry["file_id"])
        writer.writerow([
            manifest["manifest_id"], manifest["lake_table"], manifest["snapshot_id"],
            manifest["compression"], entry["file_id"], entry["partition_key"],
            entry["file_rows"], entry["file_bytes"], entry["file_manifest"],
            entry["tier"], entry["read_location"],
            receipt["receipt_id"] if receipt else "",
            receipt["status"] if receipt else "pending",
            receipt["reason"] if receipt else "尚未收到接收回执",
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":
                 f'attachment; filename="{manifest_id}.handoff.csv"'},
    )


@router.post("/storage_tier/manifests/export")
def export_migration_manifest(payload: ManifestExportPayload) -> dict[str, Any]:
    """按快照导出冷文件迁移清单，只把冷层文件列为迁移对象。"""
    result = _storage_tier().export_migration_manifest(
        payload.lake_table, payload.snapshot_id,
        compression=payload.compression, manifest_id=payload.manifest_id,
        hot_days=payload.hot_days,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@router.get("/storage_tier/cold-copies")
def list_cold_copies(lake_table: str = Query(...)) -> dict[str, Any]:
    return {"items": _storage_tier().list_cold_copies(lake_table)}


@router.post("/storage_tier/cold-copies/verify")
def verify_cold_copy(payload: ColdCopyVerifyPayload) -> dict[str, Any]:
    """从冷端校验副本内容，登记校验后的副本摘要。"""
    return _storage_tier().verify_cold_copy(
        payload.lake_table, payload.file_id,
        copy_checksum=payload.copy_checksum, rows=payload.rows,
        file_bytes=payload.file_bytes, source_snapshot=payload.source_snapshot,
    )


@router.get("/storage_tier/receipts")
def list_migration_receipts(manifest_id: str | None = Query(default=None)) -> dict[str, Any]:
    return {"items": _storage_tier().list_receipts(manifest_id)}


@router.post("/storage_tier/receipts")
def submit_migration_receipt(payload: MigrationReceiptPayload) -> dict[str, Any]:
    """投递迁移接收回执；同一回执重复投递只确认一次。"""
    return _storage_tier().submit_receipt(
        payload.manifest_id, payload.receipt_id, payload.file_id,
        payload.lake_table, payload.content_checksum,
        release_request_id=payload.release_request_id, defer=payload.defer,
    )


@router.get("/storage_tier/unconfirmed")
def unconfirmed_files(lake_table: str = Query(...)) -> dict[str, Any]:
    """未确认列表：没有已确认回执的文件仍以旧快照为回读依据。"""
    return {"items": _storage_tier().unconfirmed_files(lake_table)}


@router.get("/storage_tier/read-location")
def read_location(lake_table: str = Query(...), file_id: str = Query(...)) -> dict[str, Any]:
    """核对单个文件的当前读取位置：原位置保留还是已使用校验过的冷副本。"""
    return _storage_tier().read_location(lake_table, file_id)


@router.get("/storage_tier/release-requests")
def list_release_requests(lake_table: str = Query(...),
                          file_id: str | None = Query(default=None)) -> dict[str, Any]:
    return {"items": _storage_tier().list_release_requests(lake_table, file_id)}


@router.post("/storage_tier/release-requests")
def open_release_request(payload: ReleaseRequestPayload) -> dict[str, Any]:
    """热层发起旧位置释放申请。"""
    return _storage_tier().open_release_request(
        payload.lake_table, payload.file_id, request_id=payload.request_id,
        requested_by=payload.requested_by, reason=payload.reason,
    )


@router.post("/storage_tier/release-requests/{request_id}/handover")
def hand_over_release_request(request_id: str) -> dict[str, Any]:
    """确认归属后把释放申请交给冷端接手。"""
    result = _storage_tier().hand_over_release_request(request_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result


@router.post("/storage_tier/release-requests/{request_id}/settle")
def settle_release_request(request_id: str, completed: bool = Query(default=True)) -> dict[str, Any]:
    """接手端结清释放；未由冷端接手前不释放原位置。"""
    result = _storage_tier().settle_release_request(request_id, completed=completed)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("error"))
    return result


@router.post("/storage_tier/manifests/{manifest_id}/reconcile")
def reconcile_manifest(manifest_id: str) -> dict[str, Any]:
    """分层对账：同一份清单只核销一次已迁字节。"""
    result = _storage_tier().reconcile(manifest_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result


@router.post("/time_travel/read")
def read_at_snapshot(payload: SnapshotReadPayload) -> dict[str, Any]:
    """回溯旧快照：返回 H、C 的实际读取位置，并可用已导出清单核对文件身份。"""
    return _time_travel().read_at_snapshot(
        payload.lake_table, payload.snapshot_id,
        columns=payload.columns, manifest_id=payload.manifest_id)


@router.post("/time_travel/read-timepoint")
def read_at_timepoint(payload: TimepointReadPayload) -> dict[str, Any]:
    return _time_travel().read_at_timepoint(
        payload.lake_table, payload.timestamp, manifest_id=payload.manifest_id)


@router.get("/time_travel/manifest-check")
def manifest_check(manifest_id: str = Query(...), lake_table: str = Query(...),
                   snapshot_id: str = Query(...)) -> dict[str, Any]:
    """已导出的清单与旧快照的文件身份核对。"""
    return _time_travel().compare_manifest_with_snapshot(manifest_id, lake_table, snapshot_id)


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


