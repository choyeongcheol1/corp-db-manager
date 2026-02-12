"""
동기화 HTMX 파셜 라우터
UI 단계별 HTML 조각 반환
"""
import logging
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.sync_service import get_sync_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/partials/sync", tags=["Sync Partials"])
templates = Jinja2Templates(directory="app/templates")


@router.post("/table-list", response_class=HTMLResponse)
async def get_table_list_partial(
    request: Request,
    source_server: str = Form(...),
    source_port: int = Form(1433),
    source_user: str = Form(...),
    source_password: str = Form(...),
    source_db: str = Form(...),
):
    """소스 DB 테이블 목록 파셜 반환"""
    try:
        svc = get_sync_service()
        conn_info = {
            "server": source_server,
            "port": source_port,
            "user": source_user,
            "password": source_password,
        }
        tables = svc.get_tables(conn_info, source_db)
        return templates.TemplateResponse(
            "partials/sync/table_list.html",
            {"request": request, "tables": tables, "total": len(tables)},
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">'
            f'<p class="font-medium">테이블 조회 실패</p>'
            f'<p class="text-sm mt-1">{str(e)}</p></div>',
            status_code=200,
        )


@router.get("/progress/{job_id}", response_class=HTMLResponse)
async def get_progress_partial(request: Request, job_id: str):
    """동기화 진행 상태 파셜 반환"""
    svc = get_sync_service()
    job = svc.get_job(job_id)
    if not job:
        return HTMLResponse(
            '<div class="text-gray-500 text-center py-8">작업을 찾을 수 없습니다.</div>'
        )
    return templates.TemplateResponse(
        "partials/sync/progress.html",
        {"request": request, "job": job},
    )