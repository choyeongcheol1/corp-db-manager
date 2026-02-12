"""
설정 관리 라우터
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session
from typing import Optional, List
from pydantic import BaseModel
import json

from app.core.database import get_db, SystemConfig, DBServer, User, get_password_hash
from app.routers.auth import get_current_user, require_admin

router = APIRouter(prefix="/api/settings", tags=["settings"])


# ============================================================
# Pydantic Models
# ============================================================

class AlertSettingsUpdate(BaseModel):
    """알림 설정 업데이트"""
    capacity_warning_percent: int
    capacity_critical_percent: int


class MainDBEntry(BaseModel):
    """메인 DB 항목 (다중 등록용)"""
    id: Optional[int] = None          # 자동 부여
    server_id: int
    db_name: str
    label: Optional[str] = ""         # 표시명 (예: "운영서버-NXUNIES")


class MainDBColumnMapping(BaseModel):
    """메인 DB 컬럼 매핑 (공통)"""
    corp_table_name: str = "COMS_CMPNY"
    corp_code_column: str = "CORP_CD"
    corp_name_column: str = "CORP_NM"
    biz_no_column: str = "SAUPNO"
    repr_name_column: str = "RPRSV_NM"
    acc_db_name_column: str = "ACC_DB_NAME"


class MainDBSettingsUpdate(BaseModel):
    """메인 DB 설정 업데이트 (하위호환 유지)"""
    main_db_server_id: Optional[int] = None
    main_db_name: str = ""
    corp_table_name: str = "COMS_CMPNY"
    corp_code_column: str = "CORP_CD"
    corp_name_column: str = "CORP_NM"
    biz_no_column: str = "SAUPNO"
    repr_name_column: str = "RPRSV_NM"
    acc_db_name_column: str = "ACC_DB_NAME"


class ReplicationSettingsUpdate(BaseModel):
    """복제 설정 업데이트"""
    default_template_db: str
    db_data_path: str
    db_log_path: str
    initial_db_size_mb: int
    initial_log_size_mb: int
    file_growth_mb: int
    default_db_account_id: str
    default_db_password: str
    default_admin_id: str
    default_admin_password: str


class UserCreate(BaseModel):
    """사용자 생성"""
    username: str
    password: str
    name: str
    email: Optional[str] = None
    role: str = "viewer"


class UserUpdate(BaseModel):
    """사용자 수정"""
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


class PasswordReset(BaseModel):
    """비밀번호 초기화"""
    new_password: str


# ============================================================
# Helper Functions
# ============================================================

def get_config_value(db: Session, key: str, default: str = "") -> str:
    """설정값 조회"""
    config = db.query(SystemConfig).filter(SystemConfig.config_key == key).first()
    return config.config_value if config else default


def set_config_value(db: Session, key: str, value: str, description: str = None):
    """설정값 저장"""
    config = db.query(SystemConfig).filter(SystemConfig.config_key == key).first()
    if config:
        config.config_value = value
        if description:
            config.description = description
    else:
        config = SystemConfig(
            config_key=key,
            config_value=value,
            description=description or key
        )
        db.add(config)
    db.commit()


def get_main_db_list(db: Session) -> list:
    """메인 DB 목록 조회 (JSON 파싱)"""
    raw = get_config_value(db, "main_db_list", "[]")
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


def set_main_db_list(db: Session, entries: list):
    """메인 DB 목록 저장 (JSON 직렬화)"""
    set_config_value(db, "main_db_list", json.dumps(entries, ensure_ascii=False), "메인 DB 목록 (JSON)")


# ============================================================
# Alert Settings
# ============================================================

@router.get("/alert")
async def get_alert_settings(db: Session = Depends(get_db)):
    """알림 설정 조회"""
    return {
        "capacity_warning_percent": int(get_config_value(db, "capacity_warning_percent", "80")),
        "capacity_critical_percent": int(get_config_value(db, "capacity_critical_percent", "90"))
    }


@router.put("/alert")
async def update_alert_settings(
    settings: AlertSettingsUpdate,
    db: Session = Depends(get_db)
):
    """알림 설정 업데이트"""
    set_config_value(db, "capacity_warning_percent", str(settings.capacity_warning_percent), "용량 경고 임계치 (%)")
    set_config_value(db, "capacity_critical_percent", str(settings.capacity_critical_percent), "용량 위험 임계치 (%)")
    return {"message": "알림 설정이 저장되었습니다."}


# ============================================================
# Main DB Settings (법인 정보 조회용) - 다중 등록 지원
# ============================================================

@router.get("/main-db")
async def get_main_db_settings(db: Session = Depends(get_db)):
    """메인 DB 설정 조회 (다중 목록 + 컬럼 매핑)"""
    entries = get_main_db_list(db)
    
    # 각 entry에 서버 정보 추가
    for entry in entries:
        server = db.query(DBServer).filter(DBServer.id == entry.get("server_id")).first()
        if server:
            entry["server_name"] = server.server_name
            entry["server_host"] = f"{server.host}:{server.port}"
        else:
            entry["server_name"] = "(삭제된 서버)"
            entry["server_host"] = ""
    
    return {
        "entries": entries,
        # 컬럼 매핑 (공통)
        "corp_table_name": get_config_value(db, "corp_table_name", "COMS_CMPNY"),
        "corp_code_column": get_config_value(db, "corp_code_column", "CORP_CD"),
        "corp_name_column": get_config_value(db, "corp_name_column", "CORP_NM"),
        "biz_no_column": get_config_value(db, "biz_no_column", "SAUPNO"),
        "repr_name_column": get_config_value(db, "repr_name_column", "RPRSV_NM"),
        "acc_db_name_column": get_config_value(db, "acc_db_name_column", "ACC_DB_NAME"),
        # 하위호환: 기존 단일 설정값도 유지
        "main_db_server_id": int(get_config_value(db, "main_db_server_id", "0")) or None,
        "main_db_name": get_config_value(db, "main_db_name", ""),
    }


@router.post("/main-db/entries")
async def add_main_db_entry(
    entry: MainDBEntry,
    db: Session = Depends(get_db)
):
    """메인 DB 항목 추가"""
    entries = get_main_db_list(db)
    
    # 중복 체크
    for e in entries:
        if e["server_id"] == entry.server_id and e["db_name"] == entry.db_name:
            raise HTTPException(status_code=400, detail="이미 등록된 메인 DB입니다.")
    
    # 라벨 자동 생성
    server = db.query(DBServer).filter(DBServer.id == entry.server_id).first()
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다.")
    
    label = entry.label or f"{server.server_name} / {entry.db_name}"
    
    # ID 자동 부여
    max_id = max([e.get("id", 0) for e in entries], default=0)
    new_entry = {
        "id": max_id + 1,
        "server_id": entry.server_id,
        "db_name": entry.db_name,
        "label": label,
    }
    
    entries.append(new_entry)
    set_main_db_list(db, entries)
    
    # 첫 번째 등록이면 기존 단일 설정도 업데이트 (하위호환)
    if len(entries) == 1:
        set_config_value(db, "main_db_server_id", str(entry.server_id), "메인 DB 서버 ID")
        set_config_value(db, "main_db_name", entry.db_name, "메인 DB명")
    
    new_entry["server_name"] = server.server_name
    new_entry["server_host"] = f"{server.host}:{server.port}"
    
    return {"message": "메인 DB가 추가되었습니다.", "entry": new_entry}


@router.delete("/main-db/entries/{entry_id}")
async def delete_main_db_entry(
    entry_id: int,
    db: Session = Depends(get_db)
):
    """메인 DB 항목 삭제"""
    entries = get_main_db_list(db)
    entries = [e for e in entries if e.get("id") != entry_id]
    set_main_db_list(db, entries)
    
    # 하위호환: 첫 번째 항목을 기존 단일 설정으로 유지
    if entries:
        set_config_value(db, "main_db_server_id", str(entries[0]["server_id"]))
        set_config_value(db, "main_db_name", entries[0]["db_name"])
    else:
        set_config_value(db, "main_db_server_id", "")
        set_config_value(db, "main_db_name", "")
    
    return {"message": "메인 DB가 삭제되었습니다."}


@router.put("/main-db/columns")
async def update_main_db_columns(
    mapping: MainDBColumnMapping,
    db: Session = Depends(get_db)
):
    """컬럼 매핑 설정 업데이트 (공통)"""
    set_config_value(db, "corp_table_name", mapping.corp_table_name, "법인 테이블명")
    set_config_value(db, "corp_code_column", mapping.corp_code_column, "법인코드 컬럼")
    set_config_value(db, "corp_name_column", mapping.corp_name_column, "법인명 컬럼")
    set_config_value(db, "biz_no_column", mapping.biz_no_column, "사업자번호 컬럼")
    set_config_value(db, "repr_name_column", mapping.repr_name_column, "대표자명 컬럼")
    set_config_value(db, "acc_db_name_column", mapping.acc_db_name_column, "회계DB명 컬럼")
    return {"message": "컬럼 매핑 설정이 저장되었습니다."}


# 하위호환: 기존 PUT /main-db 유지
@router.put("/main-db")
async def update_main_db_settings(
    settings: MainDBSettingsUpdate,
    db: Session = Depends(get_db)
):
    """메인 DB 설정 업데이트 (하위호환)"""
    set_config_value(db, "main_db_server_id", str(settings.main_db_server_id) if settings.main_db_server_id else "", "메인 DB 서버 ID")
    set_config_value(db, "main_db_name", settings.main_db_name, "메인 DB명")
    set_config_value(db, "corp_table_name", settings.corp_table_name, "법인 테이블명")
    set_config_value(db, "corp_code_column", settings.corp_code_column, "법인코드 컬럼")
    set_config_value(db, "corp_name_column", settings.corp_name_column, "법인명 컬럼")
    set_config_value(db, "biz_no_column", settings.biz_no_column, "사업자번호 컬럼")
    set_config_value(db, "repr_name_column", settings.repr_name_column, "대표자명 컬럼")
    set_config_value(db, "acc_db_name_column", settings.acc_db_name_column, "회계DB명 컬럼")
    return {"message": "메인 DB 설정이 저장되었습니다."}


@router.post("/main-db/test")
async def test_main_db_connection(
    settings: MainDBSettingsUpdate,
    db: Session = Depends(get_db)
):
    """메인 DB 연결 테스트"""
    if not settings.main_db_server_id:
        raise HTTPException(status_code=400, detail="서버를 선택해주세요.")
    
    server = db.query(DBServer).filter(DBServer.id == settings.main_db_server_id).first()
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다.")
    
    try:
        from app.services.drivers import get_driver
        driver = get_driver(server)
        conn = driver.get_connection(settings.main_db_name)
        cursor = conn.cursor()
        
        query = f"""
            SELECT TOP 1 
                [{settings.corp_code_column}] AS corp_code,
                [{settings.corp_name_column}] AS corp_name,
                [{settings.biz_no_column}] AS biz_no,
                [{settings.repr_name_column}] AS repr_name,
                [{settings.acc_db_name_column}] AS acc_db_name
            FROM [{settings.corp_table_name}]
        """
        cursor.execute(query)
        row = cursor.fetchone()
        
        sample_data = None
        if row:
            sample_data = {
                settings.corp_code_column: row.corp_code,
                settings.corp_name_column: row.corp_name,
                settings.biz_no_column: row.biz_no,
                settings.repr_name_column: row.repr_name,
                settings.acc_db_name_column: row.acc_db_name
            }
        
        conn.close()
        
        return {
            "success": True,
            "message": "연결 테스트 성공",
            "sample_data": sample_data
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"연결 테스트 실패: {str(e)}")


# ============================================================
# Replication Settings
# ============================================================

@router.get("/replication")
async def get_replication_settings(db: Session = Depends(get_db)):
    """복제 설정 조회"""
    return {
        "default_template_db": get_config_value(db, "default_template_db", "ACC_STANDARD"),
        "db_data_path": get_config_value(db, "db_data_path", "/Data/mssql/data/"),
        "db_log_path": get_config_value(db, "db_log_path", "/Data/mssql/log/"),
        "initial_db_size_mb": int(get_config_value(db, "initial_db_size_mb", "100")),
        "initial_log_size_mb": int(get_config_value(db, "initial_log_size_mb", "64")),
        "file_growth_mb": int(get_config_value(db, "file_growth_mb", "64")),
        "default_db_account_id": get_config_value(db, "default_db_account_id", ""),
        "default_db_password": get_config_value(db, "default_db_password", ""),
        "default_admin_id": get_config_value(db, "default_admin_id", "admin"),
        "default_admin_password": get_config_value(db, "default_admin_password", "Admin@1234")
    }


@router.put("/replication")
async def update_replication_settings(
    settings: ReplicationSettingsUpdate,
    db: Session = Depends(get_db)
):
    """복제 설정 업데이트"""
    set_config_value(db, "default_template_db", settings.default_template_db, "기본 템플릿 DB")
    set_config_value(db, "db_data_path", settings.db_data_path, "DB 파일 경로 (Data)")
    set_config_value(db, "db_log_path", settings.db_log_path, "DB 파일 경로 (Log)")
    set_config_value(db, "initial_db_size_mb", str(settings.initial_db_size_mb), "초기 DB 크기 (MB)")
    set_config_value(db, "initial_log_size_mb", str(settings.initial_log_size_mb), "초기 로그 크기 (MB)")
    set_config_value(db, "file_growth_mb", str(settings.file_growth_mb), "파일 증가 단위 (MB)")
    set_config_value(db, "default_db_account_id", settings.default_db_account_id, "기본 DB 계정 ID")
    set_config_value(db, "default_db_password", settings.default_db_password, "기본 DB 비밀번호")
    set_config_value(db, "default_admin_id", settings.default_admin_id, "기본 관리자 ID")
    set_config_value(db, "default_admin_password", settings.default_admin_password, "기본 관리자 비밀번호")
    return {"message": "복제 설정이 저장되었습니다."}


class DBAccountTestRequest(BaseModel):
    """DB 계정 연결 테스트 요청"""
    server_id: int
    db_account_id: str
    db_password: str
    test_db_name: Optional[str] = "master"


@router.post("/replication/test-db-account")
async def test_db_account_connection(
    request: DBAccountTestRequest,
    db: Session = Depends(get_db)
):
    """DB 계정 연결 테스트"""
    if not request.server_id:
        raise HTTPException(status_code=400, detail="서버를 선택해주세요.")
    
    if not request.db_account_id or not request.db_password:
        raise HTTPException(status_code=400, detail="DB 계정 ID와 비밀번호를 입력해주세요.")
    
    server = db.query(DBServer).filter(DBServer.id == request.server_id).first()
    if not server:
        raise HTTPException(status_code=404, detail="서버를 찾을 수 없습니다.")
    
    try:
        import pyodbc
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server.host},{server.port};"
            f"DATABASE={request.test_db_name};"
            f"UID={request.db_account_id};"
            f"PWD={request.db_password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=10;"
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0].split('\n')[0]
        conn.close()
        
        return {
            "success": True,
            "message": "DB 계정 연결 테스트 성공",
            "detail": f"계정: {request.db_account_id}, 서버: {server.host}"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"연결 테스트 실패: {str(e)}")


# ============================================================
# User Management
# ============================================================

@router.get("/users")
async def get_users(db: Session = Depends(get_db)):
    """사용자 목록 조회"""
    users = db.query(User).order_by(User.id).all()
    return [
        {
            "id": u.id,
            "username": u.username,
            "name": u.name,
            "email": u.email,
            "role": u.role,
            "is_active": u.is_active,
            "created_at": u.created_at.isoformat() if u.created_at else None,
            "last_login_at": u.last_login_at.isoformat() if u.last_login_at else None
        }
        for u in users
    ]


@router.post("/users")
async def create_user(
    user: UserCreate,
    db: Session = Depends(get_db)
):
    """사용자 생성"""
    existing = db.query(User).filter(User.username == user.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="이미 존재하는 사용자 ID입니다.")
    
    new_user = User(
        username=user.username,
        password_hash=get_password_hash(user.password),
        name=user.name,
        email=user.email,
        role=user.role,
        is_active=True
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    return {"message": "사용자가 생성되었습니다.", "id": new_user.id}


@router.put("/users/{user_id}")
async def update_user(
    user_id: int,
    user: UserUpdate,
    db: Session = Depends(get_db)
):
    """사용자 수정"""
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    
    if user.name is not None:
        db_user.name = user.name
    if user.email is not None:
        db_user.email = user.email
    if user.role is not None:
        db_user.role = user.role
    if user.is_active is not None:
        db_user.is_active = user.is_active
    
    db.commit()
    return {"message": "사용자 정보가 수정되었습니다."}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    db: Session = Depends(get_db)
):
    """사용자 삭제"""
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    
    if db_user.username == "admin":
        raise HTTPException(status_code=400, detail="기본 관리자 계정은 삭제할 수 없습니다.")
    
    db.delete(db_user)
    db.commit()
    return {"message": "사용자가 삭제되었습니다."}


@router.post("/users/{user_id}/reset-password")
async def reset_user_password(
    user_id: int,
    data: PasswordReset,
    db: Session = Depends(get_db)
):
    """비밀번호 초기화"""
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    
    db_user.password_hash = get_password_hash(data.new_password)
    db.commit()
    return {"message": "비밀번호가 초기화되었습니다."}


# ============================================================
# All Settings (for initial load)
# ============================================================

@router.get("/all")
async def get_all_settings(db: Session = Depends(get_db)):
    """모든 설정 조회"""
    servers = db.query(DBServer).filter(DBServer.is_active == True).all()
    
    return {
        "alert": await get_alert_settings(db),
        "main_db": await get_main_db_settings(db),
        "replication": await get_replication_settings(db),
        "servers": [
            {"id": s.id, "name": s.server_name, "host": s.host, "port": s.port}
            for s in servers
        ]
    }