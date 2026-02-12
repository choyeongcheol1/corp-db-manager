"""
알림(Notification) 모델 - PostgreSQL
"""
from datetime import datetime
from enum import Enum
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, Index

from app.core.database import Base


class NotificationType(str, Enum):
    """알림 유형"""
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"


class NotificationCategory(str, Enum):
    """알림 카테고리"""
    SYSTEM = "system"
    SERVER = "server"
    DATABASE = "database"
    BACKUP = "backup"
    COPY = "copy"
    CAPACITY = "capacity"
    SECURITY = "security"


class Notification(Base):
    """알림 테이블 (PostgreSQL)"""
    __tablename__ = "notifications"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 알림 대상 사용자 ID (users.id 참조, NULL이면 전체)
    user_id = Column(Integer, nullable=True, index=True)
    
    # 알림 내용
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=True)
    
    # 알림 분류
    type = Column(String(20), default="info")
    category = Column(String(20), default="system")
    
    # 관련 리소스
    resource_type = Column(String(50), nullable=True)
    resource_id = Column(Integer, nullable=True)
    resource_name = Column(String(100), nullable=True)
    
    # 링크
    link = Column(String(500), nullable=True)
    
    # 상태
    is_read = Column(Boolean, default=False, index=True)
    read_at = Column(DateTime, nullable=True)
    
    # 타임스탬프
    created_at = Column(DateTime, default=datetime.now)
    
    # 복합 인덱스
    __table_args__ = (
        Index('ix_notifications_user_read', 'user_id', 'is_read'),
    )
    
    def __repr__(self):
        return f"<Notification(id={self.id}, title='{self.title}')>"
    
    @property
    def time_ago(self) -> str:
        """생성 시간을 '~전' 형식으로 반환"""
        if not self.created_at:
            return ""
        
        now = datetime.now()
        diff = now - self.created_at
        seconds = diff.total_seconds()
        
        if seconds < 60:
            return "방금 전"
        elif seconds < 3600:
            return f"{int(seconds / 60)}분 전"
        elif seconds < 86400:
            return f"{int(seconds / 3600)}시간 전"
        elif seconds < 604800:
            return f"{int(seconds / 86400)}일 전"
        else:
            return self.created_at.strftime("%m월 %d일")