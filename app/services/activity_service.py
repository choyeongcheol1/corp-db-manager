"""
활동 로그 서비스
"""
from sqlalchemy.orm import Session
from datetime import datetime
from app.core.database import ActivityLog


class ActivityService:
    """활동 로그 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def log(
        self,
        action: str,
        target_type: str,
        target_id: str = None,
        target_name: str = None,
        server_id: int = None,
        user_id: int = None,
        status: str = "success",
        message: str = None,
        details: str = None
    ) -> ActivityLog:
        """활동 로그 기록"""
        log = ActivityLog(
            action=action,
            target_type=target_type,
            target_id=str(target_id) if target_id else None,
            target_name=target_name,
            server_id=server_id,
            user_id=user_id,
            status=status,
            message=message,
            details=details,
            created_at=datetime.now()
        )
        self.db.add(log)
        self.db.commit()
        return log
    
    def get_recent(self, limit: int = 10, server_id: int = None):
        """최근 활동 조회"""
        query = self.db.query(ActivityLog).order_by(ActivityLog.created_at.desc())
        
        if server_id:
            query = query.filter(ActivityLog.server_id == server_id)
        
        return query.limit(limit).all()


# ============================================================
# 편의 함수
# ============================================================

def log_server_activity(db: Session, action: str, server_id: int, server_name: str, user_id: int, message: str = None):
    """서버 관련 활동 기록"""
    service = ActivityService(db)
    return service.log(
        action=action,
        target_type="SERVER",
        target_id=server_id,
        target_name=server_name,
        server_id=server_id,
        user_id=user_id,
        message=message
    )


def log_login_activity(db: Session, user_id: int, username: str, success: bool, message: str = None):
    """로그인 활동 기록"""
    service = ActivityService(db)
    return service.log(
        action="LOGIN" if success else "LOGIN_FAILED",
        target_type="USER",
        target_id=user_id,
        target_name=username,
        user_id=user_id if success else None,
        status="success" if success else "failed",
        message=message
    )


def log_health_check(db: Session, server_id: int, server_name: str, user_id: int, check_type: str, result: str):
    """점검 활동 기록"""
    service = ActivityService(db)
    return service.log(
        action=f"HEALTH_CHECK_{check_type.upper()}",
        target_type="SERVER",
        target_id=server_id,
        target_name=server_name,
        server_id=server_id,
        user_id=user_id,
        message=result
    )


def log_corp_activity(db: Session, action: str, server_id: int, corp_code: str, user_id: int, message: str = None):
    """법인 관련 활동 기록"""
    service = ActivityService(db)
    return service.log(
        action=action,
        target_type="CORP",
        target_id=corp_code,
        target_name=corp_code,
        server_id=server_id,
        user_id=user_id,
        message=message
    )

# ============================================================
# 문서 다운로드 로그
# ============================================================

def log_download_schema(db: Session, user_id: int, username: str, server_id: int, db_name: str, table_count: int = None, filename: str = None):
    """테이블 정의서 다운로드 기록"""
    import json
    service = ActivityService(db)
    details = json.dumps({"filename": filename, "table_count": table_count}, ensure_ascii=False) if filename or table_count else None
    return service.log(
        action="DOWNLOAD_SCHEMA",
        target_type="DOCUMENT",
        target_id=db_name,
        target_name=db_name,
        server_id=server_id,
        user_id=user_id,
        message=f"테이블 정의서 다운로드: {db_name}" + (f" ({table_count}개 테이블)" if table_count else ""),
        details=details
    )


def log_download_schema_all(db: Session, user_id: int, username: str, server_id: int, server_name: str, db_count: int = None, filename: str = None):
    """전체 DB 정의서 다운로드 기록"""
    import json
    service = ActivityService(db)
    details = json.dumps({"filename": filename, "db_count": db_count}, ensure_ascii=False) if filename or db_count else None
    return service.log(
        action="DOWNLOAD_SCHEMA_ALL",
        target_type="DOCUMENT",
        target_id=str(server_id),
        target_name=server_name,
        server_id=server_id,
        user_id=user_id,
        message=f"전체 DB 정의서 다운로드: {server_name}" + (f" ({db_count}개 DB)" if db_count else ""),
        details=details
    )