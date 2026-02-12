"""
알림 API 라우터
"""
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.routers.auth import get_current_user
from app.services.notification_service import NotificationService

router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.get("", response_class=HTMLResponse)
async def get_notifications_dropdown(
    request: Request,
    db: Session = Depends(get_db),
    user = Depends(get_current_user),
):
    """알림 드롭다운 내용 (HTMX)"""
    from app.routers.pages import templates
    
    notifications = NotificationService.get_list(
        db=db,
        user_id=user.id,
        limit=10,
    )
    unread_count = NotificationService.get_unread_count(db, user.id)
    
    return templates.TemplateResponse(
        "components/notification_dropdown.html",
        {
            "request": request,
            "notifications": notifications,
            "unread_count": unread_count,
        }
    )


@router.get("/badge", response_class=HTMLResponse)
async def get_notification_badge(
    request: Request,
    db: Session = Depends(get_db),
):
    """알림 배지 (HTMX 폴링용)"""
    user = get_current_user(request, db)
    
    # 로그인하지 않은 경우 빈 응답
    if not user:
        return HTMLResponse("")
    
    count = NotificationService.get_unread_count(db, user.id)
    
    if count > 0:
        badge_text = str(count) if count < 100 else "99+"
        return HTMLResponse(
            f'<span class="absolute -top-1 -right-1 min-w-[1.25rem] h-5 px-1 bg-red-500 text-white text-xs rounded-full flex items-center justify-center">{badge_text}</span>'
        )
    return HTMLResponse("")


@router.post("/{notification_id}/read", response_class=HTMLResponse)
async def mark_as_read(
    notification_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user = Depends(get_current_user),
):
    """알림 읽음 처리"""
    from app.routers.pages import templates
    
    NotificationService.mark_as_read(db, notification_id, user.id)
    
    notifications = NotificationService.get_list(db, user.id, limit=10)
    unread_count = NotificationService.get_unread_count(db, user.id)
    
    return templates.TemplateResponse(
        "components/notification_dropdown.html",
        {
            "request": request,
            "notifications": notifications,
            "unread_count": unread_count,
        }
    )


@router.post("/read-all", response_class=HTMLResponse)
async def mark_all_as_read(
    request: Request,
    db: Session = Depends(get_db),
    user = Depends(get_current_user),
):
    """모든 알림 읽음 처리"""
    from app.routers.pages import templates
    
    NotificationService.mark_all_as_read(db, user.id)
    
    notifications = NotificationService.get_list(db, user.id, limit=10)
    
    return templates.TemplateResponse(
        "components/notification_dropdown.html",
        {
            "request": request,
            "notifications": notifications,
            "unread_count": 0,
        }
    )


@router.delete("/{notification_id}", response_class=HTMLResponse)
async def delete_notification(
    notification_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user = Depends(get_current_user),
):
    """알림 삭제"""
    from app.routers.pages import templates
    
    NotificationService.delete(db, notification_id, user.id)
    
    notifications = NotificationService.get_list(db, user.id, limit=10)
    unread_count = NotificationService.get_unread_count(db, user.id)
    
    return templates.TemplateResponse(
        "components/notification_dropdown.html",
        {
            "request": request,
            "notifications": notifications,
            "unread_count": unread_count,
        }
    )


@router.get("/all", response_class=HTMLResponse)
async def notifications_page(
    request: Request,
    db: Session = Depends(get_db),
    user = Depends(get_current_user),
):
    """전체 알림 페이지"""
    from app.routers.pages import templates
    
    notifications = NotificationService.get_list(db, user.id, limit=100)
    unread_count = NotificationService.get_unread_count(db, user.id)
    
    return templates.TemplateResponse(
        "pages/notifications.html",
        {
            "request": request,
            "user": user,
            "notifications": notifications,
            "unread_count": unread_count,
            "page_title": "전체 알림",
        }
    )