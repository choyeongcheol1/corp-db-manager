# app/routers/db_sync.py
"""
DB 동기화 API 라우터 (Linked Server 방식 - 기존 설정 사용)
- 타겟 서버의 Linked Server 목록 조회
- Linked Server 연결 테스트
- Linked Server를 통한 소스 DB/테이블 목록 조회
- 단일 테이블 동기화 실행
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional

from app.core.database import get_db
from app.services.db_sync_service import DbSyncService
from app.routers.auth import get_current_user, require_login
from app.models.user import User

router = APIRouter(prefix="/api/db-sync", tags=["db-sync"])


# ============================================================
# Pydantic 스키마
# ============================================================

class LinkedServerResponse(BaseModel):
    name: str
    data_source: str = ""
    provider: str = ""
    catalog: Optional[str] = None


class TableSyncInfoResponse(BaseModel):
    table_name: str
    row_count: int = 0
    has_identity: bool = False
    description: Optional[str] = None


class SyncRequest(BaseModel):
    target_server_id: int
    target_db_name: str
    linked_server_name: str
    source_db_name: str
    table_name: str
    truncate_before: bool = True
    keep_identity: bool = False


class SyncResultResponse(BaseModel):
    success: bool
    table_name: str
    source_db: str
    target_db: str
    linked_server_name: str = ""
    rows_affected: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None


# ============================================================
# Linked Server 관련
# ============================================================

@router.get("/linked-servers/{server_id}", response_model=List[LinkedServerResponse])
async def get_linked_servers(
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """타겟 서버에 등록된 Linked Server 목록"""
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = DbSyncService(db)
    servers = service.get_linked_servers(server_id)
    
    return [
        LinkedServerResponse(
            name=s.name,
            data_source=s.data_source,
            provider=s.provider,
            catalog=s.catalog
        )
        for s in servers
    ]


@router.get("/linked-server-test/{server_id}/{linked_server_name}")
async def test_linked_server(
    server_id: int,
    linked_server_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """Linked Server 연결 테스트"""
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = DbSyncService(db)
    result = service.test_linked_server(server_id, linked_server_name)
    return result


# ============================================================
# 소스 DB/테이블 조회 (Linked Server 경유)
# ============================================================

@router.get("/source-databases/{server_id}/{linked_server_name}")
async def get_source_databases(
    server_id: int,
    linked_server_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """Linked Server를 통해 소스 서버의 DB 목록 조회"""
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = DbSyncService(db)
    databases = service.get_linked_server_databases(server_id, linked_server_name)
    return databases


@router.get("/source-tables/{server_id}/{linked_server_name}/{db_name}", response_model=List[TableSyncInfoResponse])
async def get_source_tables(
    server_id: int,
    linked_server_name: str,
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """Linked Server를 통해 소스 DB의 테이블 목록 조회"""
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = DbSyncService(db)
    tables = service.get_source_tables(server_id, linked_server_name, db_name)
    
    return [
        TableSyncInfoResponse(
            table_name=t.table_name,
            row_count=t.row_count,
            has_identity=t.has_identity,
            description=t.description
        )
        for t in tables
    ]


# ============================================================
# 타겟 DB/테이블 조회 (로컬)
# ============================================================

@router.get("/target-tables/{server_id}/{db_name}", response_model=List[TableSyncInfoResponse])
async def get_target_tables(
    server_id: int,
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """타겟 서버의 로컬 DB 테이블 목록 조회"""
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = DbSyncService(db)
    tables = service.get_target_tables(server_id, db_name)
    
    return [
        TableSyncInfoResponse(
            table_name=t.table_name,
            row_count=t.row_count,
            has_identity=t.has_identity,
            description=t.description
        )
        for t in tables
    ]


# ============================================================
# 동기화 실행
# ============================================================

@router.post("/execute", response_model=SyncResultResponse)
async def execute_sync(
    request: SyncRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """단일 테이블 Linked Server 동기화 실행"""
    if user.role not in ["admin", "operator"]:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    
    service = DbSyncService(db)
    
    result = service.sync_table(
        target_server_id=request.target_server_id,
        target_db_name=request.target_db_name,
        linked_server_name=request.linked_server_name,
        source_db_name=request.source_db_name,
        table_name=request.table_name,
        truncate_before=request.truncate_before,
        keep_identity=request.keep_identity
    )
    
    # 활동 로그
    try:
        from app.services.activity_service import ActivityService
        activity_service = ActivityService(db)
        activity_service.log_activity(
            user_id=user.id,
            action="DB_SYNC",
            target_type="table",
            target_name=f"{request.target_db_name}.{request.table_name}",
            details={
                "linked_server": request.linked_server_name,
                "source_db": request.source_db_name,
                "target_db": request.target_db_name,
                "table_name": request.table_name,
                "rows_affected": result.rows_affected,
                "truncate_before": request.truncate_before,
                "keep_identity": request.keep_identity,
                "success": result.success
            }
        )
    except Exception:
        pass
    
    return SyncResultResponse(
        success=result.success,
        table_name=result.table_name,
        source_db=result.source_db,
        target_db=result.target_db,
        linked_server_name=result.linked_server_name,
        rows_affected=result.rows_affected,
        elapsed_seconds=result.elapsed_seconds,
        error_message=result.error_message
    )