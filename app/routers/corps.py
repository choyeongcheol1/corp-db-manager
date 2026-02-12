"""
법인 DB 관리 라우터
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel
import uuid

from app.core.database import get_db, User
from app.models import (
    CorpInfo, CorpDetail, CreateDBRequest, CreateDBResult,
    DBStatus
)
from app.services.server_service import ServerService
from app.services.corp_service import CorpService
from app.routers.auth import require_login, require_operator

router = APIRouter(prefix="/api/corps", tags=["corps"])

# 진행 중인 작업 저장 (실제로는 Redis 등 사용)
running_tasks = {}


# ============================================================
# Pydantic Models
# ============================================================

class FetchCorpRequest(BaseModel):
    """메인 DB에서 법인 목록 조회 요청"""
    server_id: int
    db_name: str
    table_name: str
    corp_code_col: str = "CORP_CD"
    corp_name_col: str = "CORP_NM"
    biz_no_col: str = "SAUPNO"
    acc_db_col: str = "ACC_DB_NAME"


# ============================================================
# 법인 목록 조회 API
# ============================================================

@router.get("", response_model=List[CorpInfo])
async def get_corps(
    server_id: Optional[int] = None,
    keyword: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 목록 조회"""
    service = CorpService(db)
    server_service = ServerService(db)
    
    corps = service.search_corps(keyword=keyword, server_id=server_id, status=status)
    
    result = []
    for corp in corps:
        server = server_service.get_server(corp.server_id)
        result.append(CorpInfo(
            id=corp.id,
            corp_code=corp.corp_code,
            corp_name=corp.corp_name,
            biz_no=corp.biz_no,
            server_id=corp.server_id,
            server_name=server.server_name if server else "",
            db_name=corp.db_name,
            db_user=corp.db_user,
            status=DBStatus(corp.status),
            created_at=corp.created_at
        ))
    
    return result


@router.get("/{corp_id}", response_model=CorpDetail)
async def get_corp(
    corp_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 상세 조회"""
    service = CorpService(db)
    server_service = ServerService(db)
    
    corp = service.get_corp(corp_id)
    if not corp:
        raise HTTPException(status_code=404, detail="법인을 찾을 수 없습니다")
    
    server = server_service.get_server(corp.server_id)
    if not server:
        raise HTTPException(status_code=404, detail="서버 정보를 찾을 수 없습니다")
    
    # 테이블 목록 조회
    tables = service.get_db_tables(server, corp.db_name)
    
    # 용량 조회
    size_mb = service.get_db_size(server, corp.db_name)
    
    # 연결 문자열 생성
    conn_string = f"Server={server.host},{server.port};Database={corp.db_name};User Id={corp.db_user};Password=<비밀번호>;"
    
    return CorpDetail(
        id=corp.id,
        corp_code=corp.corp_code,
        corp_name=corp.corp_name,
        biz_no=corp.biz_no,
        server_id=corp.server_id,
        server_name=server.server_name,
        db_name=corp.db_name,
        db_user=corp.db_user,
        status=DBStatus(corp.status),
        size_mb=size_mb,
        table_count=len(tables),
        created_at=corp.created_at,
        host=server.host,
        port=server.port,
        connection_string=conn_string,
        tables=tables
    )


@router.post("/validate-code")
async def validate_corp_code(
    corp_code: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인코드 유효성 검사"""
    service = CorpService(db)
    is_valid, message = service.validate_corp_code(corp_code)
    return {"valid": is_valid, "message": message}


@router.post("/create", response_model=CreateDBResult)
async def create_corp_db(
    request: CreateDBRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """법인 DB 생성"""
    service = CorpService(db)
    
    # 법인코드 검증
    is_valid, message = service.validate_corp_code(request.corp_code)
    if not is_valid:
        raise HTTPException(status_code=400, detail=message)
    
    # DB 생성 실행
    result = service.create_corp_db(request, user_id=user.id)
    
    if not result.success:
        raise HTTPException(status_code=500, detail=result.message)
    
    return result


@router.post("/create-async")
async def create_corp_db_async(
    request: CreateDBRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """법인 DB 비동기 생성 (긴 작업용)"""
    service = CorpService(db)
    
    # 법인코드 검증
    is_valid, message = service.validate_corp_code(request.corp_code)
    if not is_valid:
        raise HTTPException(status_code=400, detail=message)
    
    # 작업 ID 생성
    task_id = str(uuid.uuid4())[:8]
    running_tasks[task_id] = {
        "status": "pending",
        "progress": 0,
        "steps": [],
        "result": None
    }
    
    # 백그라운드 작업 실행
    def run_creation():
        def progress_callback(step, status, message):
            running_tasks[task_id]["steps"].append({
                "step": step, "status": status, "message": message
            })
            completed = sum(1 for s in running_tasks[task_id]["steps"] if s["status"] == "완료")
            running_tasks[task_id]["progress"] = int(completed / 7 * 100)
            running_tasks[task_id]["status"] = "running"
        
        result = service.create_corp_db(request, user_id=user.id, progress_callback=progress_callback)
        running_tasks[task_id]["status"] = "completed" if result.success else "failed"
        running_tasks[task_id]["result"] = result
    
    background_tasks.add_task(run_creation)
    
    return {"task_id": task_id, "message": "작업이 시작되었습니다"}


@router.get("/task/{task_id}")
async def get_task_status(
    task_id: str,
    user: User = Depends(require_login)
):
    """작업 상태 조회"""
    if task_id not in running_tasks:
        raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다")
    
    task = running_tasks[task_id]
    return {
        "task_id": task_id,
        "status": task["status"],
        "progress": task["progress"],
        "steps": task["steps"],
        "result": task["result"]
    }


@router.get("/{corp_id}/tables")
async def get_corp_tables(
    corp_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 DB 테이블 목록"""
    service = CorpService(db)
    server_service = ServerService(db)
    
    corp = service.get_corp(corp_id)
    if not corp:
        raise HTTPException(status_code=404, detail="법인을 찾을 수 없습니다")
    
    server = server_service.get_server(corp.server_id)
    if not server:
        raise HTTPException(status_code=404, detail="서버 정보를 찾을 수 없습니다")
    
    tables = service.get_db_tables(server, corp.db_name)
    return tables


@router.post("/{corp_id}/test-connection")
async def test_corp_connection(
    corp_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 DB 연결 테스트"""
    service = CorpService(db)
    server_service = ServerService(db)
    
    corp = service.get_corp(corp_id)
    if not corp:
        raise HTTPException(status_code=404, detail="법인을 찾을 수 없습니다")
    
    server = server_service.get_server(corp.server_id)
    if not server:
        raise HTTPException(status_code=404, detail="서버 정보를 찾을 수 없습니다")
    
    try:
        conn = server_service.get_connection(server, corp.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        return {"success": True, "message": "연결 성공"}
    except Exception as e:
        return {"success": False, "message": f"연결 실패: {str(e)}"}


# ============================================================
# 메인 DB에서 법인 정보 조회 API (신규)
# ============================================================

@router.post("/fetch-from-main-db")
async def fetch_corps_from_main_db(
    request: FetchCorpRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """
    메인 DB에서 법인 목록을 조회합니다.
    조회된 법인 중 이미 DB가 생성된 법인은 has_db=True로 표시됩니다.
    """
    from app.services.drivers import get_driver
    import re
    
    server_service = ServerService(db)
    server = server_service.get_server(request.server_id)
    
    if not server:
        return {"error": "서버를 찾을 수 없습니다."}
    
    try:
        driver = get_driver(server)
        conn = driver.get_connection(request.db_name)
        cursor = conn.cursor()
        
        # SQL 식별자 검증 (알파벳, 숫자, 언더스코어만 허용)
        def validate_identifier(name):
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
                raise ValueError(f"유효하지 않은 식별자: {name}")
            return name
        
        try:
            corp_code_col = validate_identifier(request.corp_code_col)
            corp_name_col = validate_identifier(request.corp_name_col)
            biz_no_col = validate_identifier(request.biz_no_col)
            acc_db_col = validate_identifier(request.acc_db_col)
            table_name = validate_identifier(request.table_name)
        except ValueError as e:
            return {"error": str(e)}
        
        # DB 타입에 따른 쿼리 생성
        if server.db_type == 'mssql':
            query = f"""
                SELECT 
                    [{corp_code_col}] AS corp_code,
                    [{corp_name_col}] AS corp_name,
                    [{biz_no_col}] AS biz_no,
                    [{acc_db_col}] AS acc_db_name
                FROM [{table_name}]
                ORDER BY [{corp_code_col}]
            """
        elif server.db_type == 'postgresql':
            query = f"""
                SELECT 
                    "{corp_code_col}" AS corp_code,
                    "{corp_name_col}" AS corp_name,
                    "{biz_no_col}" AS biz_no,
                    "{acc_db_col}" AS acc_db_name
                FROM "{table_name}"
                ORDER BY "{corp_code_col}"
            """
        elif server.db_type == 'mysql':
            query = f"""
                SELECT 
                    `{corp_code_col}` AS corp_code,
                    `{corp_name_col}` AS corp_name,
                    `{biz_no_col}` AS biz_no,
                    `{acc_db_col}` AS acc_db_name
                FROM `{table_name}`
                ORDER BY `{corp_code_col}`
            """
        else:  # oracle
            query = f"""
                SELECT 
                    {corp_code_col} AS corp_code,
                    {corp_name_col} AS corp_name,
                    {biz_no_col} AS biz_no,
                    {acc_db_col} AS acc_db_name
                FROM {table_name}
                ORDER BY {corp_code_col}
            """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # 기존 DB 목록 조회 (이미 생성된 DB 확인용)
        existing_dbs = set()
        try:
            databases = server_service.get_server_databases(server)
            existing_dbs = {db['db_name'].upper() for db in databases}
        except:
            pass
        
        corps = []
        for row in rows:
            corp_code = str(row.corp_code).strip() if row.corp_code else ''
            corp_name = str(row.corp_name).strip() if row.corp_name else ''
            biz_no = str(row.biz_no).strip() if row.biz_no else ''
            acc_db_name = str(row.acc_db_name).strip() if row.acc_db_name else ''
            
            # DB 존재 여부 확인
            has_db = False
            if acc_db_name:
                has_db = acc_db_name.upper() in existing_dbs
            else:
                # acc_db_name이 없으면 ACC_{법인코드} 패턴으로 확인
                has_db = f"ACC_{corp_code}".upper() in existing_dbs
            
            corps.append({
                "corp_code": corp_code,
                "corp_name": corp_name,
                "biz_no": biz_no,
                "acc_db_name": acc_db_name,
                "has_db": has_db,
                "server_id": server.id if has_db else None,
                "server_name": server.server_name if has_db else None
            })
        
        conn.close()
        
        return {
            "corps": corps,
            "total": len(corps),
            "new_count": len([c for c in corps if not c["has_db"]])
        }
        
    except Exception as e:
        return {"error": f"조회 실패: {str(e)}"}