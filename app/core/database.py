"""
메타 데이터베이스 (SQLite) 설정
서버 정보, 법인 정보, 사용자, 작업 이력 등 저장

+ PostgreSQL (handsdb) 연결 추가 - 사용자 인증용
"""
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Boolean,
    DateTime, Float, ForeignKey, Enum as SQLEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from passlib.context import CryptContext
import os
from dotenv import load_dotenv

from app.config import get_settings
from app.models import DBType, ServerStatus, DBStatus, UserRole, TaskStatus

# .env 로드
load_dotenv()

settings = get_settings()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ============================================================
# SQLite (메타 데이터베이스) - 기존 유지
# ============================================================

engine = create_engine(
    settings.meta_database_url,
    connect_args={"check_same_thread": False}  # SQLite용
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ============================================================
# PostgreSQL (사용자 인증 데이터베이스) - 신규 추가
# ============================================================

PG_HOST = os.getenv("DB_HOST", "localhost")
PG_PORT = os.getenv("DB_PORT", "5432")
PG_NAME = os.getenv("DB_NAME", "handsdb")
PG_USER = os.getenv("DB_USER", "postgres")
PG_PASSWORD = os.getenv("DB_PASSWORD", "0000")

PG_DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_NAME}"

pg_engine = create_engine(
    PG_DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False
)

PgSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=pg_engine)
PgBase = declarative_base()


def get_pg_db():
    """PostgreSQL 세션 의존성"""
    db = PgSessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================
# SQLite Database Tables (기존 유지)
# ============================================================

class DBServer(Base):
    """DB 서버 정보"""
    __tablename__ = "db_servers"
    
    id = Column(Integer, primary_key=True, index=True)
    server_name = Column(String(100), nullable=False)
    host = Column(String(200), nullable=False)
    port = Column(Integer, default=1433)
    db_type = Column(String(20), default=DBType.MSSQL.value)
    username = Column(String(100), nullable=False)
    password = Column(String(200), nullable=False)  # 암호화 필요
    default_db = Column(String(100), default="master")
    data_path = Column(String(500), nullable=True)
    log_path = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, onupdate=datetime.now)
    
    # Relationships
    corps = relationship("Corp", back_populates="server")


class Corp(Base):
    """법인 정보"""
    __tablename__ = "corps"
    
    id = Column(Integer, primary_key=True, index=True)
    corp_code = Column(String(50), unique=True, nullable=False, index=True)
    corp_name = Column(String(200), nullable=False)
    biz_no = Column(String(20), nullable=True)
    server_id = Column(Integer, ForeignKey("db_servers.id"), nullable=False)
    db_name = Column(String(100), nullable=False)
    db_user = Column(String(100), nullable=False)
    status = Column(String(20), default=DBStatus.NORMAL.value)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, onupdate=datetime.now)
    
    # Relationships
    server = relationship("DBServer", back_populates="corps")


class User(Base):
    """시스템 사용자 (SQLite - 레거시, 향후 PostgreSQL로 이전)"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(200), nullable=False)
    name = Column(String(100), nullable=False)
    email = Column(String(200), nullable=True)
    role = Column(String(20), default=UserRole.VIEWER.value)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    last_login_at = Column(DateTime, nullable=True)


class ActivityLog(Base):
    """활동 로그"""
    __tablename__ = "activity_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    action = Column(String(50), nullable=False)  # CREATE, UPDATE, DELETE, BACKUP 등
    target_type = Column(String(50), nullable=False)  # SERVER, CORP, USER 등
    target_id = Column(String(100), nullable=True)
    target_name = Column(String(200), nullable=True)
    server_id = Column(Integer, nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    status = Column(String(20), default="success")
    message = Column(Text, nullable=True)
    details = Column(Text, nullable=True)  # JSON 형태로 상세 정보
    created_at = Column(DateTime, default=datetime.now)


class SystemConfig(Base):
    """시스템 설정"""
    __tablename__ = "system_configs"
    
    id = Column(Integer, primary_key=True, index=True)
    config_key = Column(String(100), unique=True, nullable=False)
    config_value = Column(Text, nullable=True)
    description = Column(String(500), nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ============================================================
# Database Functions
# ============================================================

def get_db():
    """SQLite DB 세션 의존성"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """데이터베이스 초기화"""
    Base.metadata.create_all(bind=engine)
    
    # 기본 관리자 계정 생성
    db = SessionLocal()
    try:
        admin = db.query(User).filter(User.username == settings.admin_username).first()
        if not admin:
            admin = User(
                username=settings.admin_username,
                password_hash=pwd_context.hash(settings.admin_password),
                name="시스템 관리자",
                role=UserRole.ADMIN.value,
                is_active=True
            )
            db.add(admin)
            db.commit()
            print(f"✅ 기본 관리자 계정 생성: {settings.admin_username}")
        
        # 기본 시스템 설정
        default_configs = [
            ("capacity_warning_percent", "80", "용량 경고 임계치 (%)"),
            ("capacity_critical_percent", "90", "용량 위험 임계치 (%)"),
            ("health_check_interval", "300", "헬스체크 주기 (초)"),
            ("default_clone_tables", "TB_COM_CODE,TB_MENU,TB_ROLE", "기본 복제 테이블"),
        ]
        
        for key, value, desc in default_configs:
            config = db.query(SystemConfig).filter(SystemConfig.config_key == key).first()
            if not config:
                config = SystemConfig(config_key=key, config_value=value, description=desc)
                db.add(config)
        
        db.commit()
        print("✅ 데이터베이스 초기화 완료")
        
    finally:
        db.close()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """비밀번호 검증"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """비밀번호 해시"""
    return pwd_context.hash(password)