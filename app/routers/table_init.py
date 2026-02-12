# app/routers/table_init.py
"""
테이블 초기화 API 라우터
- 법인 정보 조회 (DB명 기준)
- 테이블 목록/정보 조회
- 테이블 컬럼 목록 조회
- 단일 테이블 INSERT/DELETE 실행
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional

from app.core.database import get_db
from app.services.table_init_service import TableInitService
from app.routers.auth import get_current_user, require_login
from app.models.user import User

router = APIRouter(prefix="/api/table-init", tags=["table-init"])


# ============================================================
# Pydantic 스키마
# ============================================================

class CorpInfoResponse(BaseModel):
    found: bool
    corp_code: Optional[str] = None
    corp_name: Optional[str] = None
    biz_no: Optional[str] = None
    acc_db_name: Optional[str] = None
    message: Optional[str] = None


class TableInfoResponse(BaseModel):
    table_name: str
    row_count: int
    corp_code_column: Optional[str] = None
    has_identity: bool = False
    description: Optional[str] = None


class ColumnInfoResponse(BaseModel):
    column_name: str
    data_type: str
    is_nullable: bool = True


class InitRequest(BaseModel):
    source_server_id: int
    source_db_name: str
    target_server_id: int
    target_db_name: str
    table_name: str
    source_corp_code: str
    target_corp_code: str
    corp_code_column: Optional[str] = None
    action: str = "INSERT"  # INSERT 또는 DELETE
    truncate_before_copy: bool = True
    replace_corp_code: bool = True
    keep_identity: bool = False


class InitResultResponse(BaseModel):
    success: bool
    table_name: str
    source_db: str
    target_db: str
    source_corp_code: str
    target_corp_code: str
    action: str = "INSERT"
    rows_copied: int = 0
    rows_replaced: int = 0
    rows_deleted: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None
    error_detail: Optional[str] = None


# ============================================================
# API 엔드포인트
# ============================================================

@router.get("/corp-info-by-db", response_model=CorpInfoResponse)
async def get_corp_info_by_db(
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """
    DB명으로 법인 정보 조회
    """
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = TableInitService(db)
    corp_info = service.get_corp_info_by_db_name(db_name)
    
    if corp_info:
        return CorpInfoResponse(
            found=True,
            corp_code=corp_info.corp_code,
            corp_name=corp_info.corp_name,
            biz_no=corp_info.biz_no,
            acc_db_name=corp_info.acc_db_name
        )
    else:
        return CorpInfoResponse(
            found=False,
            message=f"'{db_name}'에 해당하는 법인 정보를 찾을 수 없습니다."
        )


@router.get("/tables/{server_id}/{db_name}")
async def get_tables(
    server_id: int,
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """
    DB의 테이블 목록 조회
    """
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = TableInitService(db)
    tables = service.get_tables(server_id, db_name)
    
    return [
        {
            "table_name": t.table_name,
            "row_count": t.row_count,
            "corp_code_column": t.corp_code_column,
            "description": t.description
        }
        for t in tables
    ]


@router.get("/columns/{server_id}/{db_name}/{table_name}")
async def get_table_columns(
    server_id: int,
    db_name: str,
    table_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """
    테이블의 컬럼 목록 조회
    """
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = TableInitService(db)
    columns = service.get_table_columns(server_id, db_name, table_name)
    
    return [
        {
            "column_name": c.column_name,
            "data_type": c.data_type,
            "is_nullable": c.is_nullable
        }
        for c in columns
    ]


@router.get("/table-info/{server_id}/{db_name}/{table_name}", response_model=TableInfoResponse)
async def get_table_info(
    server_id: int,
    db_name: str,
    table_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """
    단일 테이블 정보 조회
    """
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    
    service = TableInitService(db)
    table_info = service.get_table_info(server_id, db_name, table_name)
    
    if not table_info:
        raise HTTPException(status_code=404, detail="테이블 정보를 조회할 수 없습니다.")
    
    return TableInfoResponse(
        table_name=table_info.table_name,
        row_count=table_info.row_count,
        corp_code_column=table_info.corp_code_column,
        has_identity=table_info.has_identity,
        description=table_info.description
    )


@router.post("/execute", response_model=InitResultResponse)
async def execute_init(
    request: InitRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """
    테이블 초기화 실행 (INSERT 또는 DELETE)
    """
    # 권한 체크
    if user.role not in ["admin", "operator"]:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    service = TableInitService(db)

    if request.action == "DELETE":
        # DELETE 실행
        result = service.delete_table_data(
            target_server_id=request.target_server_id,
            target_db_name=request.target_db_name,
            table_name=request.table_name,
            corp_code=request.target_corp_code,
            corp_code_column=request.corp_code_column
        )
    else:
        # INSERT 실행
        result = service.init_table(
            source_server_id=request.source_server_id,
            source_db_name=request.source_db_name,
            target_server_id=request.target_server_id,
            target_db_name=request.target_db_name,
            table_name=request.table_name,
            source_corp_code=request.source_corp_code,
            target_corp_code=request.target_corp_code,
            corp_code_column=request.corp_code_column,
            truncate_before_copy=request.truncate_before_copy,
            replace_corp_code=request.replace_corp_code,
            keep_identity=request.keep_identity
        )

    # #4: 에러 메시지 한국어 매핑
    raw_error = result.error_message
    friendly_error = _map_error_message(raw_error, request.table_name) if raw_error else None

    # #5: 활동 로그 상세화
    rows = getattr(result, 'rows_copied', 0) or getattr(result, 'rows_deleted', 0)
    try:
        from app.services.activity_service import ActivityService
        activity_service = ActivityService(db)
        activity_service.log_activity(
            user_id=user.id,
            action=f"TABLE_{request.action}",
            target_type="table",
            target_name=f"{request.target_db_name}.{request.table_name}",
            details=(
                f"{request.action} {request.table_name}: "
                f"{request.source_db_name}({request.source_corp_code}) → "
                f"{request.target_db_name}({request.target_corp_code}), "
                f"corp_col={request.corp_code_column}, "
                f"rows={rows}, "
                f"{'성공' if result.success else '실패: ' + str(raw_error or '')}"
            )[:500]
        )
    except Exception:
        pass  # 로그 실패는 무시

    return InitResultResponse(
        success=result.success,
        table_name=result.table_name,
        source_db=getattr(result, 'source_db', request.source_db_name),
        target_db=result.target_db,
        source_corp_code=request.source_corp_code,
        target_corp_code=request.target_corp_code,
        action=request.action,
        rows_copied=getattr(result, 'rows_copied', 0),
        rows_replaced=getattr(result, 'rows_replaced', 0),
        rows_deleted=getattr(result, 'rows_deleted', 0),
        elapsed_seconds=result.elapsed_seconds,
        error_message=friendly_error,
        error_detail=raw_error
    )


# ============================================================
# #4: 에러 메시지 한국어 매핑
# ============================================================

def _map_error_message(error_str: str, table_name: str) -> str:
    """pyodbc 원시 에러 → 사용자 친화적 메시지"""
    if not error_str:
        return error_str

    err = error_str.lower()

    if "permission" in err or "denied" in err:
        return f"'{table_name}' 테이블에 대한 권한이 없습니다."
    elif "truncate" in err and "foreign" in err:
        return f"'{table_name}'에 외래키 제약이 있어 TRUNCATE가 불가합니다. DELETE를 사용하세요."
    elif "identity_insert" in err:
        return f"'{table_name}' Identity 컬럼 설정 오류입니다. 'Identity 값 유지' 옵션을 확인하세요."
    elif "duplicate" in err or "unique" in err or "primary" in err:
        return f"'{table_name}'에 중복 키가 존재합니다. TRUNCATE 후 재시도하세요."
    elif "timeout" in err or "timed out" in err:
        return "작업 시간이 초과되었습니다. 데이터가 많은 테이블은 DB 동기화를 사용하세요."
    elif "connection" in err:
        return "서버 연결에 실패했습니다. 서버 상태를 확인하세요."
    elif "invalid object" in err or "does not exist" in err:
        return f"'{table_name}' 테이블을 찾을 수 없습니다."
    else:
        return f"테이블 초기화 중 오류가 발생했습니다."