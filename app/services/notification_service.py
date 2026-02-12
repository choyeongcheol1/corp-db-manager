"""
알림 서비스
"""
from datetime import datetime, timedelta
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, or_

from app.models.notification import Notification, NotificationType, NotificationCategory


class NotificationService:
    """알림 CRUD 서비스"""
    
    @staticmethod
    def create(
        db: Session,
        title: str,
        message: Optional[str] = None,
        type: str = "info",
        category: str = "system",
        user_id: Optional[int] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[int] = None,
        resource_name: Optional[str] = None,
        link: Optional[str] = None,
    ) -> Notification:
        """알림 생성"""
        notification = Notification(
            title=title,
            message=message,
            type=type,
            category=category,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            resource_name=resource_name,
            link=link,
        )
        db.add(notification)
        db.commit()
        db.refresh(notification)
        return notification
    
    @staticmethod
    def get_list(
        db: Session,
        user_id: int,
        unread_only: bool = False,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Notification]:
        """사용자 알림 목록 조회"""
        query = db.query(Notification).filter(
            or_(Notification.user_id == user_id, Notification.user_id.is_(None))
        )
        
        if unread_only:
            query = query.filter(Notification.is_read == False)
        
        return query.order_by(desc(Notification.created_at)).offset(offset).limit(limit).all()
    
    @staticmethod
    def get_unread_count(db: Session, user_id: int) -> int:
        """읽지 않은 알림 개수"""
        return db.query(Notification).filter(
            and_(
                or_(Notification.user_id == user_id, Notification.user_id.is_(None)),
                Notification.is_read == False
            )
        ).count()
    
    @staticmethod
    def mark_as_read(db: Session, notification_id: int, user_id: int) -> bool:
        """알림 읽음 처리"""
        notification = db.query(Notification).filter(
            Notification.id == notification_id,
            or_(Notification.user_id == user_id, Notification.user_id.is_(None))
        ).first()
        
        if notification and not notification.is_read:
            notification.is_read = True
            notification.read_at = datetime.now()
            db.commit()
            return True
        return False
    
    @staticmethod
    def mark_all_as_read(db: Session, user_id: int) -> int:
        """모든 알림 읽음 처리"""
        result = db.query(Notification).filter(
            and_(
                or_(Notification.user_id == user_id, Notification.user_id.is_(None)),
                Notification.is_read == False
            )
        ).update({
            "is_read": True,
            "read_at": datetime.now()
        }, synchronize_session=False)
        db.commit()
        return result
    
    @staticmethod
    def delete(db: Session, notification_id: int, user_id: int) -> bool:
        """알림 삭제"""
        result = db.query(Notification).filter(
            Notification.id == notification_id,
            or_(Notification.user_id == user_id, Notification.user_id.is_(None))
        ).delete(synchronize_session=False)
        db.commit()
        return result > 0
    
    @staticmethod
    def cleanup_old(db: Session, days: int = 30) -> int:
        """오래된 알림 정리 (기본 30일)"""
        cutoff = datetime.now() - timedelta(days=days)
        result = db.query(Notification).filter(
            Notification.created_at < cutoff
        ).delete(synchronize_session=False)
        db.commit()
        return result


# ============================================================
# 알림 생성 헬퍼 함수들 (다른 서비스에서 호출)
# ============================================================

def notify_capacity_warning(db: Session, server_name: str, db_name: str, 
                            usage_percent: float, user_id: Optional[int] = None):
    """용량 경고 알림"""
    if usage_percent >= 90:
        noti_type = "error"
        title = f"[위험] {db_name} 용량 {usage_percent:.1f}%"
    else:
        noti_type = "warning"
        title = f"[경고] {db_name} 용량 {usage_percent:.1f}%"
    
    return NotificationService.create(
        db=db,
        title=title,
        message=f"{server_name} 서버의 {db_name} DB 용량이 임계치를 초과했습니다.",
        type=noti_type,
        category="capacity",
        user_id=user_id,
        resource_type="database",
        resource_name=db_name,
        link=f"/databases?db={db_name}",
    )


def notify_backup_warning(db: Session, server_name: str, db_name: str,
                          hours_since_backup: int, user_id: Optional[int] = None):
    """백업 경과 경고"""
    return NotificationService.create(
        db=db,
        title=f"[경고] {db_name} 백업 필요",
        message=f"{server_name}의 {db_name}이 {hours_since_backup}시간 동안 백업되지 않았습니다.",
        type="warning",
        category="backup",
        user_id=user_id,
        resource_type="database",
        resource_name=db_name,
    )


def notify_server_error(db: Session, server_name: str, server_id: int,
                        error_message: str, user_id: Optional[int] = None):
    """서버 연결 오류"""
    return NotificationService.create(
        db=db,
        title=f"[오류] {server_name} 연결 실패",
        message=error_message,
        type="error",
        category="server",
        user_id=user_id,
        resource_type="server",
        resource_id=server_id,
        resource_name=server_name,
        link=f"/servers",
    )


def notify_copy_complete(db: Session, source_db: str, target_db: str,
                         table_count: int, row_count: int, user_id: Optional[int] = None):
    """데이터 복사 완료"""
    return NotificationService.create(
        db=db,
        title="[완료] 데이터 복사 성공",
        message=f"{source_db} → {target_db}: {table_count}개 테이블, {row_count:,}건",
        type="success",
        category="copy",
        user_id=user_id,
        link="/activity-logs?type=COPY",
    )


def notify_copy_failed(db: Session, source_db: str, target_db: str,
                       error_message: str, user_id: Optional[int] = None):
    """데이터 복사 실패"""
    return NotificationService.create(
        db=db,
        title="[실패] 데이터 복사 오류",
        message=f"{source_db} → {target_db}: {error_message}",
        type="error",
        category="copy",
        user_id=user_id,
        link="/activity-logs?type=COPY",
    )


def notify_db_created(db: Session, server_name: str, db_name: str,
                      user_id: Optional[int] = None):
    """DB 생성 완료"""
    return NotificationService.create(
        db=db,
        title=f"[완료] {db_name} 생성됨",
        message=f"{server_name} 서버에 DB가 생성되었습니다.",
        type="success",
        category="database",
        user_id=user_id,
        resource_type="database",
        resource_name=db_name,
        link="/databases",
    )


def notify_user_approved(db: Session, username: str, user_id: int):
    """사용자 승인 완료 (해당 사용자에게)"""
    return NotificationService.create(
        db=db,
        title="계정이 승인되었습니다",
        message="이제 모든 기능을 사용할 수 있습니다.",
        type="success",
        category="system",
        user_id=user_id,
    )


def notify_new_user_pending(db: Session, username: str, admin_user_id: int):
    """신규 사용자 승인 대기 (관리자에게)"""
    return NotificationService.create(
        db=db,
        title=f"[승인 대기] 신규 사용자: {username}",
        message="새로운 사용자가 승인을 기다리고 있습니다.",
        type="info",
        category="system",
        user_id=admin_user_id,
        link="/users?status=pending",
    )