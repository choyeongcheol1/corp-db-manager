"""
데이터 모델 정의
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


# ============================================================
# Enums
# ============================================================

class DBType(str, Enum):
    """DB 타입"""
    MSSQL = "mssql"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"


class ServerStatus(str, Enum):
    """서버 상태"""
    ONLINE = "online"
    OFFLINE = "offline"
    WARNING = "warning"
    UNKNOWN = "unknown"


class DBStatus(str, Enum):
    """DB 상태"""
    NORMAL = "normal"
    WARNING = "warning"
    ERROR = "error"


class UserRole(str, Enum):
    """사용자 역할"""
    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"


class TaskStatus(str, Enum):
    """작업 상태"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# ============================================================
# Server Models
# ============================================================

class ServerBase(BaseModel):
    """서버 기본 정보"""
    server_name: str = Field(..., description="서버 표시명")
    host: str = Field(..., description="호스트 주소")
    port: int = Field(default=1433, description="포트")
    db_type: DBType = Field(default=DBType.MSSQL, description="DB 타입")
    username: str = Field(..., description="접속 계정")
    password: str = Field(..., description="비밀번호")
    default_db: str = Field(default="master", description="기본 DB")
    data_path: Optional[str] = Field(default=None, description="데이터 파일 경로")
    log_path: Optional[str] = Field(default=None, description="로그 파일 경로")
    description: Optional[str] = Field(default=None, description="설명")


class ServerCreate(ServerBase):
    """서버 생성"""
    pass


class ServerUpdate(BaseModel):
    """서버 수정"""
    server_name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    data_path: Optional[str] = None
    log_path: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


class ServerInfo(ServerBase):
    """서버 정보 (조회용)"""
    id: int
    is_active: bool = True
    status: ServerStatus = ServerStatus.UNKNOWN
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    # 런타임 정보
    db_count: int = 0
    total_size_mb: float = 0
    response_time_ms: Optional[int] = None
    
    class Config:
        from_attributes = True


class ServerSummary(BaseModel):
    """서버 요약 (선택 화면용)"""
    id: int
    server_name: str
    host: str
    port: int
    db_type: str = "mssql"  # 추가
    default_db: Optional[str] = None  # 추가
    username: str = ""  # 추가
    description: Optional[str] = None  # 추가
    status: ServerStatus
    db_count: int
    total_size_mb: float
    warning_count: int = 0
    error_count: int = 0


# ============================================================
# Corp (법인) Models
# ============================================================

class CorpBase(BaseModel):
    """법인 기본 정보"""
    corp_code: str = Field(..., description="법인코드")
    corp_name: str = Field(..., description="법인명")
    biz_no: Optional[str] = Field(default=None, description="사업자번호")


class CorpCreate(CorpBase):
    """법인 생성 요청"""
    source_server_id: int = Field(..., description="참조 서버 ID")
    source_db_name: str = Field(..., description="참조 DB명")
    target_server_id: int = Field(..., description="타겟 서버 ID")


class CorpInfo(CorpBase):
    """법인 정보 (조회용)"""
    id: int
    server_id: int
    server_name: str
    db_name: str
    db_user: str
    status: DBStatus = DBStatus.NORMAL
    size_mb: float = 0
    table_count: int = 0
    created_at: datetime
    
    class Config:
        from_attributes = True


class CorpDetail(CorpInfo):
    """법인 상세 정보"""
    host: str
    port: int
    connection_string: str
    tables: List[dict] = []
    last_backup_at: Optional[datetime] = None


# ============================================================
# DB Creation Models
# ============================================================

class CreateDBRequest(BaseModel):
    """DB 생성 요청"""
    source_server_id: int
    source_db_name: str
    target_server_id: int
    corp_code: str
    corp_name: str
    biz_no: Optional[str] = None


class CreateDBProgress(BaseModel):
    """DB 생성 진행 상황"""
    task_id: str
    status: TaskStatus
    current_step: str
    progress: int  # 0-100
    steps: List[dict]
    message: str
    started_at: datetime
    completed_at: Optional[datetime] = None


class CreateDBResult(BaseModel):
    """DB 생성 결과"""
    success: bool
    corp_code: str
    db_name: str
    server_name: str
    host: str
    port: int
    db_user: str
    db_password: str
    admin_user: str = "admin"
    admin_password: str = "Admin@1234"
    connection_string: str
    elapsed_seconds: int
    message: str


# ============================================================
# User Models (Pydantic)
# ============================================================

class UserBase(BaseModel):
    """사용자 기본 정보"""
    username: str
    name: str
    email: Optional[str] = None
    role: UserRole = UserRole.VIEWER


class UserCreate(UserBase):
    """사용자 생성"""
    password: str


class UserInfo(UserBase):
    """사용자 정보"""
    id: int
    is_active: bool = True
    created_at: datetime
    last_login_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class Token(BaseModel):
    """인증 토큰"""
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """토큰 데이터"""
    username: Optional[str] = None
    role: Optional[UserRole] = None


# ============================================================
# Dashboard Models
# ============================================================

class DashboardSummary(BaseModel):
    """대시보드 요약"""
    total_db_count: int
    normal_count: int
    warning_count: int
    error_count: int
    total_size_gb: float
    server_count: int
    recent_activities: List[dict] = []


class ServerDashboard(DashboardSummary):
    """서버별 대시보드"""
    server_id: int
    server_name: str


# ============================================================
# Monitoring Models
# ============================================================

class HealthCheckResult(BaseModel):
    """헬스체크 결과"""
    server_id: int
    server_name: str
    db_name: str
    corp_code: str
    status: DBStatus
    response_time_ms: int
    message: str
    checked_at: datetime


class CapacityInfo(BaseModel):
    """용량 정보"""
    corp_code: str
    db_name: str
    size_mb: float
    used_percent: float
    status: DBStatus


# ============================================================
# Task/Activity Models  
# ============================================================

class ActivityLog(BaseModel):
    """활동 로그"""
    id: int
    action: str
    target: str
    user: str
    server_name: Optional[str] = None
    status: str
    message: Optional[str] = None
    created_at: datetime


class TaskInfo(BaseModel):
    """작업 정보"""
    task_id: str
    task_type: str
    status: TaskStatus
    target_count: int
    completed_count: int
    failed_count: int
    started_at: datetime
    completed_at: Optional[datetime] = None