"""
DB 동기화 API 라우터
운영DB → 개발DB BCP 기반 테이블 동기화
"""
import uuid
import asyncio
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Request, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.services.sync_service import get_sync_service
from app.services.server_service import ServerService
from app.core.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sync", tags=["DB Sync"])


# ── Request Models ──

class TableListRequest(BaseModel):
    """테이블 목록 조회 - server_id 기반 (권장) 또는 직접 연결"""
    server_id: Optional[int] = None
    server: Optional[str] = None
    port: int = 1433
    user: Optional[str] = None
    password: Optional[str] = None
    database: str


class SyncExecuteRequest(BaseModel):
    """동기화 실행 - server_id 기반 (권장) 또는 직접 연결"""
    # server_id 기반 (권장)
    source_server_id: Optional[int] = None
    target_server_id: Optional[int] = None
    source_db: str
    target_db: str
    tables: list[dict]  # [{ schema_name, table_name }, ...]
    # 직접 연결 (하위호환)
    source_server: Optional[str] = None
    source_port: int = 1433
    source_user: Optional[str] = None
    source_password: Optional[str] = None
    target_server: Optional[str] = None
    target_port: int = 1433
    target_user: Optional[str] = None
    target_password: Optional[str] = None


def _get_conn_info_from_server(server) -> dict:
    """서버 객체에서 연결 정보 추출"""
    conn = {
        "server": server.host,
        "port": server.port or 1433,
        "user": server.username,
        "password": server.password,
    }
    logger.info(f"서버 연결 정보: {server.server_name} ({conn['server']}:{conn['port']}, user={conn['user']})")
    return conn


# ── API Endpoints ──

@router.post("/tables")
async def get_tables(req: TableListRequest, db: Session = Depends(get_db)):
    """소스 DB의 테이블 목록 조회"""
    try:
        svc = get_sync_service()

        # server_id 기반 (권장)
        if req.server_id:
            server_svc = ServerService(db)
            server = server_svc.get_server(req.server_id)
            if not server:
                raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다")
            conn_info = _get_conn_info_from_server(server)
        elif req.server and req.user and req.password:
            # 직접 연결 (하위호환)
            conn_info = {
                "server": req.server,
                "port": req.port,
                "user": req.user,
                "password": req.password,
            }
        else:
            raise HTTPException(status_code=400, detail="server_id 또는 연결 정보를 입력하세요")

        tables = await asyncio.to_thread(svc.get_tables, conn_info, req.database)
        return {"tables": tables, "total": len(tables)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"테이블 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute")
async def execute_sync(
    req: SyncExecuteRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """선택한 테이블들의 동기화 실행 (백그라운드)"""
    if not req.tables:
        raise HTTPException(status_code=400, detail="동기화할 테이블을 선택하세요.")

    server_svc = ServerService(db)
    svc = get_sync_service()

    # 소스 연결 정보
    if req.source_server_id:
        source = server_svc.get_server(req.source_server_id)
        if not source:
            raise HTTPException(status_code=404, detail="소스 서버를 찾을 수 없습니다")
        source_conn = _get_conn_info_from_server(source)
    elif req.source_server and req.source_user and req.source_password:
        source_conn = {
            "server": req.source_server,
            "port": req.source_port,
            "user": req.source_user,
            "password": req.source_password,
        }
    else:
        raise HTTPException(status_code=400, detail="source_server_id 또는 소스 연결 정보를 입력하세요")

    # 타겟 연결 정보
    if req.target_server_id:
        target = server_svc.get_server(req.target_server_id)
        if not target:
            raise HTTPException(status_code=404, detail="타겟 서버를 찾을 수 없습니다")
        target_conn = _get_conn_info_from_server(target)
    elif req.target_server and req.target_user and req.target_password:
        target_conn = {
            "server": req.target_server,
            "port": req.target_port,
            "user": req.target_user,
            "password": req.target_password,
        }
    else:
        raise HTTPException(status_code=400, detail="target_server_id 또는 타겟 연결 정보를 입력하세요")

    job_id = str(uuid.uuid4())[:8]

    # 백그라운드 작업으로 동기화 실행
    background_tasks.add_task(
        svc.sync_tables_async,
        job_id,
        source_conn, req.source_db,
        target_conn, req.target_db,
        req.tables,
    )

    return {
        "job_id": job_id,
        "message": f"{len(req.tables)}개 테이블 동기화 시작",
        "total_tables": len(req.tables),
    }


@router.get("/jobs/{job_id}")
async def get_job_progress(job_id: str):
    """동기화 작업 진행 상태 조회"""
    svc = get_sync_service()
    job = svc.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다.")

    return {
        "job_id": job.job_id,
        "status": job.status,
        "total_tables": job.total_tables,
        "completed_tables": job.completed_tables,
        "current_table": job.current_table,
        "progress_percent": job.progress_percent,
        "success_count": job.success_count,
        "fail_count": job.fail_count,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "results": [
            {
                "schema_name": r.schema_name,
                "table_name": r.table_name,
                "source_count": r.source_count,
                "target_count": r.target_count,
                "status": r.status,
                "error_msg": r.error_msg,
                "elapsed_seconds": round(r.elapsed_seconds, 1),
            }
            for r in job.results
        ],
    }


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """동기화 작업 취소"""
    svc = get_sync_service()
    if svc.cancel_job(job_id):
        return {"message": "취소 요청이 전달되었습니다."}
    raise HTTPException(status_code=404, detail="실행 중인 작업을 찾을 수 없습니다.")


@router.get("/jobs")
async def get_all_jobs():
    """전체 동기화 작업 목록"""
    svc = get_sync_service()
    jobs = svc.get_all_jobs()
    return {
        "jobs": [
            {
                "job_id": j.job_id,
                "source_db": j.source_db,
                "target_db": j.target_db,
                "status": j.status,
                "total_tables": j.total_tables,
                "completed_tables": j.completed_tables,
                "progress_percent": j.progress_percent,
                "success_count": j.success_count,
                "fail_count": j.fail_count,
                "started_at": j.started_at.isoformat() if j.started_at else None,
            }
            for j in sorted(jobs, key=lambda x: x.started_at or datetime.min, reverse=True)
        ]
    }