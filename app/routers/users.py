"""
사용자 관리 API (PostgreSQL 기반)
- 사용자 목록 조회
- 승인/거부 처리
- 역할 변경
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel

from app.core.database import get_pg_db
from app.core.security import get_password_hash
from app.core.email import send_approval_notification
from app.models.user import User as PgUser
from app.routers.auth import require_admin

router = APIRouter(prefix="/api/users", tags=["users"])


# ============================================================
# Pydantic 스키마
# ============================================================

class UserListResponse(BaseModel):
    id: int
    username: str
    name: str
    email: Optional[str]
    phone: Optional[str]
    role: str
    status: str
    is_active: bool
    email_verified: bool
    created_at: datetime
    last_login_at: Optional[datetime]
    
    class Config:
        from_attributes = True


class UserUpdateRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


class UserCreateRequest(BaseModel):
    username: str
    password: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    role: str = "viewer"


class ApproveRequest(BaseModel):
    pass  # 추가 데이터 없음


class RejectRequest(BaseModel):
    reason: Optional[str] = None


class ResetPasswordRequest(BaseModel):
    new_password: str


# ============================================================
# API 엔드포인트
# ============================================================

@router.get("", response_model=List[UserListResponse])
async def get_users(
    status: Optional[str] = None,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """
    사용자 목록 조회
    - status: all, pending, approved, rejected, email_pending
    """
    query = pg_db.query(PgUser)
    
    if status and status != "all":
        query = query.filter(PgUser.status == status)
    
    users = query.order_by(PgUser.created_at.desc()).all()
    
    return [
        UserListResponse(
            id=u.id,
            username=u.username,
            name=u.name,
            email=u.email,
            phone=u.phone,
            role=u.role,
            status=u.status,
            is_active=u.is_active,
            email_verified=u.email_verified,
            created_at=u.created_at,
            last_login_at=u.last_login_at
        )
        for u in users
    ]


@router.get("/pending", response_model=List[UserListResponse])
async def get_pending_users(
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """승인 대기 사용자 목록"""
    users = pg_db.query(PgUser).filter(PgUser.status == "pending").order_by(PgUser.created_at.desc()).all()
    
    return [
        UserListResponse(
            id=u.id,
            username=u.username,
            name=u.name,
            email=u.email,
            phone=u.phone,
            role=u.role,
            status=u.status,
            is_active=u.is_active,
            email_verified=u.email_verified,
            created_at=u.created_at,
            last_login_at=u.last_login_at
        )
        for u in users
    ]


@router.get("/stats")
async def get_user_stats(
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """사용자 통계"""
    total = pg_db.query(PgUser).count()
    pending = pg_db.query(PgUser).filter(PgUser.status == "pending").count()
    approved = pg_db.query(PgUser).filter(PgUser.status == "approved").count()
    rejected = pg_db.query(PgUser).filter(PgUser.status == "rejected").count()
    email_pending = pg_db.query(PgUser).filter(PgUser.status == "email_pending").count()
    
    return {
        "total": total,
        "pending": pending,
        "approved": approved,
        "rejected": rejected,
        "email_pending": email_pending
    }


@router.get("/{user_id}", response_model=UserListResponse)
async def get_user(
    user_id: int,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """사용자 상세 조회"""
    user = pg_db.query(PgUser).filter(PgUser.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")
    
    return UserListResponse(
        id=user.id,
        username=user.username,
        name=user.name,
        email=user.email,
        phone=user.phone,
        role=user.role,
        status=user.status,
        is_active=user.is_active,
        email_verified=user.email_verified,
        created_at=user.created_at,
        last_login_at=user.last_login_at
    )


@router.post("")
async def create_user(
    request: UserCreateRequest,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """
    사용자 생성 (관리자용)
    - 이메일 인증 없이 바로 승인 상태로 생성
    """
    # 중복 확인
    existing = pg_db.query(PgUser).filter(PgUser.username == request.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="이미 존재하는 아이디입니다")
    
    if request.email:
        existing_email = pg_db.query(PgUser).filter(PgUser.email == request.email).first()
        if existing_email:
            raise HTTPException(status_code=400, detail="이미 사용 중인 이메일입니다")
    
    # 사용자 생성
    new_user = PgUser(
        username=request.username,
        password_hash=get_password_hash(request.password),
        name=request.name,
        email=request.email,
        phone=request.phone,
        role=request.role,
        status="approved",  # 관리자가 생성 → 바로 승인
        is_active=True,
        email_verified=True  # 이메일 인증 생략
    )
    
    pg_db.add(new_user)
    pg_db.commit()
    pg_db.refresh(new_user)
    
    return {"message": "사용자가 생성되었습니다", "id": new_user.id}


@router.put("/{user_id}")
async def update_user(
    user_id: int,
    request: UserUpdateRequest,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """사용자 정보 수정"""
    user = pg_db.query(PgUser).filter(PgUser.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")
    
    if request.name is not None:
        user.name = request.name
    if request.email is not None:
        user.email = request.email
    if request.phone is not None:
        user.phone = request.phone
    if request.role is not None:
        user.role = request.role
    if request.is_active is not None:
        user.is_active = request.is_active
    
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    return {"message": "사용자 정보가 수정되었습니다"}


@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """사용자 삭제"""
    user = pg_db.query(PgUser).filter(PgUser.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")
    
    if user.username == "admin":
        raise HTTPException(status_code=400, detail="기본 관리자 계정은 삭제할 수 없습니다")
    
    pg_db.delete(user)
    pg_db.commit()
    
    return {"message": "사용자가 삭제되었습니다"}


@router.post("/{user_id}/approve")
async def approve_user(
    user_id: int,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """
    사용자 승인
    - status: pending → approved
    - 승인 알림 이메일 발송
    """
    user = pg_db.query(PgUser).filter(PgUser.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")
    
    if user.status != "pending":
        raise HTTPException(status_code=400, detail="승인 대기 상태가 아닙니다")
    
    # 승인 처리
    user.status = "approved"
    user.approved_at = datetime.utcnow()
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    # 승인 알림 이메일 발송
    if user.email:
        try:
            send_approval_notification(user.email, user.name, approved=True)
        except Exception as e:
            print(f"[WARNING] 승인 알림 메일 발송 실패: {e}")
    
    return {"message": f"{user.name}님이 승인되었습니다"}


@router.post("/{user_id}/reject")
async def reject_user(
    user_id: int,
    request: RejectRequest,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """
    사용자 거부
    - status: pending → rejected
    - 거부 알림 이메일 발송
    """
    user = pg_db.query(PgUser).filter(PgUser.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")
    
    if user.status != "pending":
        raise HTTPException(status_code=400, detail="승인 대기 상태가 아닙니다")
    
    # 거부 처리
    user.status = "rejected"
    user.rejected_reason = request.reason
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    # 거부 알림 이메일 발송
    if user.email:
        try:
            send_approval_notification(user.email, user.name, approved=False, reason=request.reason)
        except Exception as e:
            print(f"[WARNING] 거부 알림 메일 발송 실패: {e}")
    
    return {"message": f"{user.name}님이 거부되었습니다"}


@router.post("/{user_id}/reset-password")
async def reset_password(
    user_id: int,
    request: ResetPasswordRequest,
    pg_db: Session = Depends(get_pg_db),
    admin = Depends(require_admin)
):
    """비밀번호 초기화"""
    user = pg_db.query(PgUser).filter(PgUser.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")
    
    if len(request.new_password) < 4:
        raise HTTPException(status_code=400, detail="비밀번호는 4자 이상이어야 합니다")
    
    user.password_hash = get_password_hash(request.new_password)
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    return {"message": "비밀번호가 초기화되었습니다"}