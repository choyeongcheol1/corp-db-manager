"""
사용자 모델 (SQLAlchemy)
테이블: cmm_users
"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.sql import func
from app.core.database import PgBase


class User(PgBase):
    """
    사용자 테이블 모델
    """
    __tablename__ = "cmm_users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    name = Column(String(100), nullable=False)
    email = Column(String(100))
    phone = Column(String(20))  
    role = Column(String(20), default="viewer", nullable=False)  # admin, operator, viewer
    status = Column(String(20), default="pending", nullable=False)  # pending, approved, rejected
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    last_login_at = Column(DateTime)
    rejected_reason = Column(String(255))
    email_verified = Column(Boolean, default=False)
    email_token = Column(String(100))
    email_token_expires = Column(DateTime)
    # 비밀번호 재설정 (신규 추가)
    password_reset_token = Column(String(100), index=True)
    password_reset_expires = Column(DateTime)
    
    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', role='{self.role}')>"
    
    @property
    def is_admin(self) -> bool:
        """관리자 여부"""
        return self.role == "admin"
    
    @property
    def is_approved(self) -> bool:
        """승인된 사용자 여부"""
        return self.status == "approved"
    
    @property
    def can_login(self) -> bool:
        """로그인 가능 여부"""
        return self.is_active and self.is_approved and self.email_verified