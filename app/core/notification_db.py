"""
알림 데이터베이스 설정
- PostgreSQL (메인 DB와 동일한 연결 사용)
- 기존 SQLite(notifications.db)에서 이전됨
"""
from app.core.database import get_db, Base


# 하위 호환용 alias (기존 코드에서 import하는 경우 대비)
NotificationBase = Base
get_notification_db = get_db


def init_notification_db():
    """알림 테이블 초기화 - main.py의 lifespan에서 호출
    
    PostgreSQL에서는 init_db()가 Base.metadata.create_all()을 
    이미 호출하므로, 여기서는 알림 모델만 import하여 등록합니다.
    """
    from app.models.notification import Notification  # noqa: F401
    print("✅ 알림 DB 초기화 완료 (PostgreSQL)")