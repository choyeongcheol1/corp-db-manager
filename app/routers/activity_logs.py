"""
활동 로그 API 라우터
"""
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_
from typing import Optional
from datetime import datetime, timedelta
from io import BytesIO
import csv
import json

from app.core.database import get_db, User, ActivityLog
from app.routers.auth import require_login, require_admin

router = APIRouter(prefix="/api/logs", tags=["activity-logs"])


@router.get("")
async def get_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=10, le=100),
    category: Optional[str] = None,
    action: Optional[str] = None,
    user_id: Optional[int] = None,
    server_id: Optional[int] = None,
    status: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """활동 로그 목록 조회"""
    query = db.query(ActivityLog)
    
    # 필터 적용
    if category:
        query = query.filter(ActivityLog.target_type == category)
    if action:
        query = query.filter(ActivityLog.action == action)
    if user_id:
        query = query.filter(ActivityLog.user_id == user_id)
    if server_id:
        query = query.filter(ActivityLog.server_id == server_id)
    if status:
        query = query.filter(ActivityLog.status == status)
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            query = query.filter(ActivityLog.created_at >= start_dt)
        except:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            query = query.filter(ActivityLog.created_at <= end_dt)
        except:
            pass
    if search:
        query = query.filter(
            or_(
                ActivityLog.message.ilike(f"%{search}%"),
                ActivityLog.target_name.ilike(f"%{search}%"),
                ActivityLog.target_id.ilike(f"%{search}%")
            )
        )
    
    # 전체 개수
    total = query.count()
    
    # 페이징 및 정렬
    logs = query.order_by(desc(ActivityLog.created_at)) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    
    # 사용자 정보 조회
    user_ids = [log.user_id for log in logs if log.user_id]
    users = {}
    if user_ids:
        user_list = db.query(User).filter(User.id.in_(user_ids)).all()
        users = {u.id: u.username for u in user_list}
    
    # 결과 변환
    items = []
    for log in logs:
        details = None
        if log.details:
            try:
                details = json.loads(log.details)
            except:
                details = {"raw": log.details}
        
        items.append({
            "id": log.id,
            "action": log.action,
            "category": log.target_type,
            "target_id": log.target_id,
            "target_name": log.target_name,
            "server_id": log.server_id,
            "user_id": log.user_id,
            "username": users.get(log.user_id, "-"),
            "status": log.status,
            "message": log.message,
            "details": details,
            "created_at": log.created_at
        })
    
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size
    }


@router.get("/recent")
async def get_recent_logs(
    limit: int = Query(10, ge=5, le=50),
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """최근 활동 로그"""
    logs = db.query(ActivityLog).order_by(desc(ActivityLog.created_at)).limit(limit).all()
    
    user_ids = [log.user_id for log in logs if log.user_id]
    users = {}
    if user_ids:
        user_list = db.query(User).filter(User.id.in_(user_ids)).all()
        users = {u.id: u.username for u in user_list}
    
    items = []
    for log in logs:
        items.append({
            "id": log.id,
            "action": log.action,
            "category": log.target_type,
            "target_name": log.target_name,
            "username": users.get(log.user_id, "-"),
            "status": log.status,
            "message": log.message,
            "created_at": log.created_at
        })
    
    return {"items": items}


@router.get("/stats")
async def get_log_stats(
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """로그 통계"""
    start_date = datetime.now() - timedelta(days=days)
    
    query = db.query(ActivityLog).filter(ActivityLog.created_at >= start_date)
    
    total = query.count()
    success = query.filter(ActivityLog.status == "success").count()
    failure = query.filter(ActivityLog.status.in_(["failed", "failure"])).count()
    
    return {
        "total": total,
        "success": success,
        "failure": failure
    }


@router.get("/export")
async def export_logs(
    category: Optional[str] = None,
    status: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin)
):
    """로그 CSV 내보내기 (관리자 전용)"""
    query = db.query(ActivityLog)
    
    if category:
        query = query.filter(ActivityLog.target_type == category)
    if status:
        query = query.filter(ActivityLog.status == status)
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            query = query.filter(ActivityLog.created_at >= start_dt)
        except:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            query = query.filter(ActivityLog.created_at <= end_dt)
        except:
            pass
    if search:
        query = query.filter(
            or_(
                ActivityLog.message.ilike(f"%{search}%"),
                ActivityLog.target_name.ilike(f"%{search}%")
            )
        )
    
    logs = query.order_by(desc(ActivityLog.created_at)).limit(10000).all()
    
    # 사용자 정보
    user_ids = [log.user_id for log in logs if log.user_id]
    users = {}
    if user_ids:
        user_list = db.query(User).filter(User.id.in_(user_ids)).all()
        users = {u.id: u.username for u in user_list}
    
    # CSV 생성
    output = BytesIO()
    output.write(b'\xef\xbb\xbf')  # BOM
    
    import codecs
    writer = csv.writer(codecs.getwriter('utf-8')(output))
    writer.writerow(['ID', '일시', '카테고리', '액션', '사용자', '대상', '상태', '메시지'])
    
    for log in logs:
        writer.writerow([
            log.id,
            log.created_at.strftime("%Y-%m-%d %H:%M:%S") if log.created_at else "",
            log.target_type,
            log.action,
            users.get(log.user_id, "-"),
            log.target_name or log.target_id or "",
            log.status,
            log.message or ""
        ])
    
    output.seek(0)
    filename = f"activity_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    return StreamingResponse(
        output,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )