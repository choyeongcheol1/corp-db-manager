"""
사용자 관련 Pydantic 스키마
"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime
from enum import Enum


class UserRole(str, Enum):
    """사용자 역할"""
    admin = "admin"
    operator = "operator"
    viewer = "viewer"


class UserStatus(str, Enum):
    """사용자 상태"""
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


# ============================================================
# 요청 스키마
# ============================================================

class UserLogin(BaseModel):
    """로그인 요청"""
    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=4)


class UserRegister(BaseModel):
    """회원가입 요청"""
    username: str = Field(..., min_length=2, max_length=50, pattern="^[a-zA-Z0-9_]+$")
    password: str = Field(..., min_length=4, max_length=100)
    name: str = Field(..., min_length=2, max_length=100)
    email: Optional[str] = Field(None, max_length=100)


class UserCreate(BaseModel):
    """관리자 사용자 생성"""
    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=4)
    name: str = Field(..., min_length=2, max_length=100)
    email: Optional[str] = None
    role: UserRole = UserRole.viewer


class UserUpdate(BaseModel):
    """사용자 정보 수정"""
    name: Optional[str] = Field(None, max_length=100)
    email: Optional[str] = Field(None, max_length=100)
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None


class UserApprove(BaseModel):
    """사용자 승인"""
    status: UserStatus
    rejected_reason: Optional[str] = Field(None, max_length=255)


class PasswordReset(BaseModel):
    """비밀번호 초기화"""
    new_password: str = Field(..., min_length=4, max_length=100)


class PasswordChange(BaseModel):
    """비밀번호 변경"""
    current_password: str
    new_password: str = Field(..., min_length=4, max_length=100)


# ============================================================
# 응답 스키마
# ============================================================

class UserResponse(BaseModel):
    """사용자 정보 응답"""
    id: int
    username: str
    name: str
    email: Optional[str]
    role: str
    status: str
    is_active: bool
    email_verified: bool = False
    created_at: Optional[datetime]
    last_login_at: Optional[datetime]
    rejected_reason: Optional[str] = None
    
    class Config:
        from_attributes = True


class UserListResponse(BaseModel):
    """사용자 목록 응답"""
    items: list[UserResponse]
    total: int


class TokenResponse(BaseModel):
    """토큰 응답"""
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class MessageResponse(BaseModel):
    """메시지 응답"""
    message: str
    success: bool = True