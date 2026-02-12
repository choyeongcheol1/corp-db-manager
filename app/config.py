"""
애플리케이션 설정
"""
import os
from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache


class Settings(BaseSettings):
    """애플리케이션 설정"""
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    
    # Database (SQLite - 메타 데이터)
    meta_database_url: str = "sqlite:///./data/meta.db"
    
    # PostgreSQL (사용자 인증)
    db_host: str = "localhost"
    db_port: str = "5432"
    db_name: str = "handsdb"
    db_user: str = "postgres"
    db_password: str = "0000"
    
    @property
    def effective_db_host(self) -> str:
        """Docker 환경 자동 감지하여 DB 호스트 반환"""
        if os.path.exists("/.dockerenv"):
            return "host.docker.internal"
        return self.db_host
    
    @property
    def pg_database_url(self) -> str:
        """PostgreSQL 연결 URL (환경 자동 감지)"""
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.effective_db_host}:{self.db_port}/{self.db_name}"
        )
    
    # Security
    secret_key: str = "your-secret-key-change-in-production"
    access_token_expire_minutes: int = 480
    algorithm: str = "HS256"
    
    # Default Admin
    admin_username: str = "admin"
    admin_password: str = "Admin@1234"
    
    # DB 생성 기본값
    db_prefix: str = ""
    db_initial_size_mb: int = 100
    db_log_size_mb: int = 50
    db_collation: str = "Korean_Wansung_CI_AS"
    
    # Gmail SMTP
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from_name: str = "DB 관리 시스템"
    
    # App URL
    app_url: str = "http://localhost:8000"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """설정 싱글톤"""
    return Settings()


# 복제 대상 테이블 설정
CLONE_TABLES = {
    # 데이터 포함 복제 (코드성 데이터)
    "with_data": [
        "TB_COM_CODE",
        "TB_COM_CODE_GRP",
        "TB_MENU",
        "TB_ROLE",
        "TB_ROLE_MENU",
        "TB_CONFIG",
        "TB_ACCT_CD",
        "TB_DEPT",
    ],
    # 구조만 복제 (거래 데이터)
    "schema_only": [
        "TB_USER",
        "TB_TAX_INVOICE",
        "TB_TAX_INVOICE_ITEM",
        "TB_JOURNAL",
        "TB_VOUCHER",
        "TB_BIZPARTNER",
        "TB_SYS_LOG",
    ]
}