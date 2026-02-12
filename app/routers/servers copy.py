"""
서버 관리 라우터
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel

from app.core.database import get_db, User
from app.models import (
    ServerCreate, ServerUpdate, ServerInfo, ServerSummary, ServerStatus
)
from app.services.server_service import ServerService
from app.services.activity_service import log_server_activity
from app.routers.auth import require_login, require_admin, require_operator

router = APIRouter(prefix="/api/servers", tags=["servers"])


# ============================================================
# 로컬 저장 방식 지원 - 연결 테스트 요청 모델
# ============================================================

class ConnectionTestRequest(BaseModel):
    """연결 테스트 요청 (로컬 저장 방식)"""
    db_type: str = 'mssql'
    host: str
    port: int = 1433
    default_db: Optional[str] = None
    username: str
    password: str


# ============================================================
# 로컬 저장 방식 - 직접 연결 테스트 API
# ============================================================

@router.post("/test-connection")
async def test_connection_direct(request: ConnectionTestRequest):
    """
    직접 연결 정보로 테스트 (로컬 저장 방식 지원)
    
    서버 ID 없이 연결 정보를 직접 받아서 테스트
    """
    try:
        db_type = request.db_type.lower()
        
        if db_type == 'mssql':
            return await _test_mssql(request)
        elif db_type == 'postgresql':
            return await _test_postgresql(request)
        elif db_type == 'mysql':
            return await _test_mysql(request)
        elif db_type == 'oracle':
            return await _test_oracle(request)
        else:
            return {"success": False, "message": f"지원하지 않는 DB 종류: {db_type}"}
            
    except Exception as e:
        return {"success": False, "message": f"연결 테스트 실패: {str(e)}"}


async def _test_mssql(req: ConnectionTestRequest):
    """MSSQL 연결 테스트"""
    try:
        import pyodbc
        
        db = req.default_db or 'master'
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={req.host},{req.port};"
            f"DATABASE={db};"
            f"UID={req.username};"
            f"PWD={req.password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=10;"
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0].split('\n')[0]
        conn.close()
        
        return {"success": True, "message": "연결 성공", "version": version}
        
    except pyodbc.Error as e:
        return {"success": False, "message": f"연결 실패: {str(e)}"}
    except ImportError:
        return {"success": False, "message": "pyodbc 드라이버가 설치되지 않았습니다."}
    except Exception as e:
        return {"success": False, "message": f"오류: {str(e)}"}


async def _test_postgresql(req: ConnectionTestRequest):
    """PostgreSQL 연결 테스트"""
    try:
        import psycopg2
        
        db = req.default_db or 'postgres'
        conn = psycopg2.connect(
            host=req.host,
            port=req.port,
            database=db,
            user=req.username,
            password=req.password,
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0].split(',')[0]
        conn.close()
        
        return {"success": True, "message": "연결 성공", "version": version}
        
    except ImportError:
        return {"success": False, "message": "psycopg2 드라이버가 설치되지 않았습니다. pip install psycopg2-binary"}
    except Exception as e:
        return {"success": False, "message": f"연결 실패: {str(e)}"}


async def _test_mysql(req: ConnectionTestRequest):
    """MySQL 연결 테스트"""
    try:
        import pymysql
        
        conn = pymysql.connect(
            host=req.host,
            port=req.port,
            database=req.default_db or None,
            user=req.username,
            password=req.password,
            connect_timeout=10,
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        row = cursor.fetchone()
        version = f"MySQL {row[0]}" if row else "MySQL"
        conn.close()
        
        return {"success": True, "message": "연결 성공", "version": version}
        
    except ImportError:
        return {"success": False, "message": "pymysql 드라이버가 설치되지 않았습니다. pip install pymysql"}
    except Exception as e:
        return {"success": False, "message": f"연결 실패: {str(e)}"}


async def _test_oracle(req: ConnectionTestRequest):
    """Oracle 연결 테스트"""
    try:
        import oracledb
        
        service_name = req.default_db or 'ORCL'
        dsn = oracledb.makedsn(req.host, req.port, service_name=service_name)
        
        conn = oracledb.connect(
            user=req.username,
            password=req.password,
            dsn=dsn
        )
        cursor = conn.cursor()
        cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
        version = cursor.fetchone()[0]
        conn.close()
        
        return {"success": True, "message": "연결 성공", "version": version}
        
    except ImportError:
        return {"success": False, "message": "oracledb 드라이버가 설치되지 않았습니다. pip install oracledb"}
    except Exception as e:
        return {"success": False, "message": f"연결 실패: {str(e)}"}


# ============================================================
# 기존 API (서버 DB 저장 방식) - 하위 호환성 유지
# ============================================================

@router.get("", response_model=List[ServerSummary])
async def get_servers(
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """서버 목록 조회"""
    service = ServerService(db)
    return service.get_all_server_summaries()


@router.get("/{server_id}", response_model=ServerInfo)
async def get_server(
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """서버 상세 조회"""
    service = ServerService(db)
    server = service.get_server(server_id)
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다")
    
    summary = service.get_server_summary(server)
    
    return ServerInfo(
        id=server.id,
        server_name=server.server_name,
        host=server.host,
        port=server.port,
        db_type=server.db_type,
        username=server.username,
        password="********",  # 비밀번호 숨김
        default_db=server.default_db,
        data_path=server.data_path,
        log_path=server.log_path,
        description=server.description,
        is_active=server.is_active,
        status=summary.status,
        created_at=server.created_at,
        updated_at=server.updated_at,
        db_count=summary.db_count,
        total_size_mb=summary.total_size_mb
    )


@router.post("", response_model=ServerInfo)
async def create_server(
    server_data: ServerCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin)
):
    """서버 등록"""
    service = ServerService(db)
    
    # 중복 체크
    existing = service.get_server_by_name(server_data.server_name)
    if existing:
        raise HTTPException(status_code=400, detail="이미 존재하는 서버명입니다")
    
    server = service.create_server(server_data)
    
    # 활동 로그 기록
    log_server_activity(
        db, "CREATE", server.id, server.server_name, user.id,
        f"서버 등록: {server.host}:{server.port}"
    )
    
    return ServerInfo(
        id=server.id,
        server_name=server.server_name,
        host=server.host,
        port=server.port,
        db_type=server.db_type,
        username=server.username,
        password="********",
        default_db=server.default_db,
        data_path=server.data_path,
        log_path=server.log_path,
        description=server.description,
        is_active=server.is_active,
        status=ServerStatus.UNKNOWN,
        created_at=server.created_at
    )


@router.put("/{server_id}", response_model=ServerInfo)
async def update_server(
    server_id: int,
    server_data: ServerUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin)
):
    """서버 정보 수정"""
    service = ServerService(db)
    server = service.update_server(server_id, server_data)
    
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다")
    
    # 활동 로그 기록
    log_server_activity(
        db, "UPDATE", server.id, server.server_name, user.id,
        "서버 정보 수정"
    )
    
    summary = service.get_server_summary(server)
    
    return ServerInfo(
        id=server.id,
        server_name=server.server_name,
        host=server.host,
        port=server.port,
        db_type=server.db_type,
        username=server.username,
        password="********",
        default_db=server.default_db,
        data_path=server.data_path,
        log_path=server.log_path,
        description=server.description,
        is_active=server.is_active,
        status=summary.status,
        created_at=server.created_at,
        updated_at=server.updated_at,
        db_count=summary.db_count,
        total_size_mb=summary.total_size_mb
    )


@router.delete("/{server_id}")
async def delete_server(
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin)
):
    """서버 삭제"""
    service = ServerService(db)
    server = service.get_server(server_id)
    
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다")
    
    server_name = server.server_name
    
    try:
        success = service.delete_server(server_id)
        if not success:
            raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다")
        
        # 활동 로그 기록
        log_server_activity(
            db, "DELETE", server_id, server_name, user.id,
            "서버 삭제"
        )
        
        return {"message": "서버가 삭제되었습니다"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{server_id}/test")
async def test_connection(
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """연결 테스트 (서버 ID 기반)"""
    service = ServerService(db)
    server = service.get_server(server_id)
    
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다")
    
    success, message, version = service.test_connection(server)
    
    return {
        "success": success,
        "message": message,
        "version": version
    }


@router.get("/{server_id}/databases")
async def get_server_databases(
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """서버의 DB 목록 조회"""
    service = ServerService(db)
    server = service.get_server(server_id)
    
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다")
    
    databases = service.get_server_databases(server)
    return databases