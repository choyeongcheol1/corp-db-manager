"""
인증 라우터
- 로그인/로그아웃
- 회원가입 (이메일 인증 포함)
- 이메일 인증
"""
from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import Optional
import secrets

from app.core.database import get_db, get_pg_db, User as SqliteUser, verify_password, get_password_hash
from app.core.security import verify_password as pg_verify_password, get_password_hash as pg_get_password_hash
from app.core.email import send_verification_email
from app.models import Token, UserInfo, UserRole
from app.models.user import User as PgUser
from app.schemas.user import UserResponse, TokenResponse, MessageResponse
from app.config import get_settings
from app.services.activity_service import log_login_activity

settings = get_settings()
router = APIRouter(prefix="/auth", tags=["auth"])
templates = Jinja2Templates(directory="app/templates")

# 이메일 토큰 유효 시간
EMAIL_TOKEN_EXPIRE_HOURS = 24


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """액세스 토큰 생성"""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.access_token_expire_minutes))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def decode_access_token(token: str) -> Optional[dict]:
    """JWT 토큰 디코딩"""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        return payload
    except JWTError:
        return None


def generate_email_token() -> str:
    """이메일 인증 토큰 생성"""
    return secrets.token_urlsafe(32)


def get_current_user(request: Request, db: Session = Depends(get_db)) -> Optional[SqliteUser]:
    """현재 로그인 사용자 조회 (SQLite)"""
    token = request.cookies.get("access_token")
    if not token:
        return None
    
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        username: str = payload.get("sub")
        if username is None:
            return None
    except JWTError:
        return None
    
    user = db.query(SqliteUser).filter(SqliteUser.username == username).first()
    return user


def get_current_user_pg(request: Request, db: Session = Depends(get_pg_db)) -> Optional[PgUser]:
    """현재 로그인 사용자 조회 (PostgreSQL)"""
    token = request.cookies.get("access_token")
    if not token:
        return None
    
    payload = decode_access_token(token)
    if not payload:
        return None
    
    username = payload.get("sub")
    if not username:
        return None
    
    user = db.query(PgUser).filter(PgUser.username == username).first()
    return user


def require_login(request: Request, db: Session = Depends(get_db)) -> SqliteUser:
    """로그인 필수"""
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="로그인이 필요합니다"
        )
    return user


def require_admin(request: Request, db: Session = Depends(get_db)) -> SqliteUser:
    """관리자 권한 필수"""
    user = require_login(request, db)
    if user.role != UserRole.ADMIN.value:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="관리자 권한이 필요합니다"
        )
    return user


def require_operator(request: Request, db: Session = Depends(get_db)) -> SqliteUser:
    """운영자 이상 권한 필수"""
    user = require_login(request, db)
    if user.role not in [UserRole.ADMIN.value, UserRole.OPERATOR.value]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="운영자 이상 권한이 필요합니다"
        )
    return user


# ============================================================
# 페이지 라우트
# ============================================================

@router.get("/login")
async def login_page(request: Request, db: Session = Depends(get_db)):
    """로그인 페이지"""
    user = get_current_user(request, db)
    if user:
        return RedirectResponse(url="/servers", status_code=302)
    
    return templates.TemplateResponse("pages/login.html", {
        "request": request,
        "error": None,
        "success": None
    })

@router.get("/register")
async def register_page(request: Request, pg_db: Session = Depends(get_pg_db)):
    """회원가입 페이지"""
    user = get_current_user_pg(request, pg_db)
    if user and user.can_login:
        return RedirectResponse(url="/dashboard", status_code=302)
    
    return templates.TemplateResponse("pages/register.html", {
        "request": request,
        "error": None
    })


@router.get("/verify-email")
async def verify_email_page(request: Request, token: str, pg_db: Session = Depends(get_pg_db)):
    """이메일 인증 처리"""
    # 토큰으로 사용자 찾기
    user = pg_db.query(PgUser).filter(PgUser.email_token == token).first()
    
    if not user:
        return templates.TemplateResponse("pages/email_verify.html", {
            "request": request,
            "success": False,
            "message": "유효하지 않은 인증 링크입니다.",
            "detail": "링크가 잘못되었거나 이미 사용된 링크입니다."
        })
    
    # 만료 확인
    if user.email_token_expires and user.email_token_expires < datetime.utcnow():
        return templates.TemplateResponse("pages/email_verify.html", {
            "request": request,
            "success": False,
            "message": "인증 링크가 만료되었습니다.",
            "detail": "회원가입을 다시 진행해주세요."
        })
    
    # 이미 인증된 경우
    if user.email_verified:
        return templates.TemplateResponse("pages/email_verify.html", {
            "request": request,
            "success": True,
            "message": "이미 인증이 완료된 계정입니다.",
            "detail": "로그인 페이지로 이동하여 로그인해주세요." if user.status == "approved" else "관리자 승인을 기다려주세요."
        })
    
    # 인증 처리
    user.email_verified = True
    user.email_token = None
    user.email_token_expires = None
    user.status = "pending"  # 이메일 인증 완료 → 관리자 승인 대기
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    return templates.TemplateResponse("pages/email_verify.html", {
        "request": request,
        "success": True,
        "message": "이메일 인증이 완료되었습니다!",
        "detail": "관리자 승인 후 로그인할 수 있습니다. 승인 완료 시 이메일로 알려드립니다."
    })


# ============================================================
# API 라우트
# ============================================================

@router.post("/login")
async def login(
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """로그인 (PostgreSQL 전용)"""
    form = await request.form()
    username = form.get("username")
    password = form.get("password")
    
    # PostgreSQL에서 조회
    pg_user = pg_db.query(PgUser).filter(PgUser.username == username).first()
    
    if not pg_user:
        log_login_activity(db, None, username, False, "사용자 없음")
        return templates.TemplateResponse("pages/login.html", {
            "request": request,
            "error": "아이디 또는 비밀번호가 올바르지 않습니다"
        })
    
    # 비밀번호 확인
    if not pg_verify_password(password, pg_user.password_hash):
        log_login_activity(db, None, username, False, "비밀번호 오류")
        return templates.TemplateResponse("pages/login.html", {
            "request": request,
            "error": "아이디 또는 비밀번호가 올바르지 않습니다"
        })
    
    # 이메일 미인증
    if not pg_user.email_verified:
        return templates.TemplateResponse("pages/login.html", {
            "request": request,
            "error": "이메일 인증이 완료되지 않았습니다. 이메일을 확인해주세요."
        })
    
    # 승인 대기 상태
    if pg_user.status == "pending":
        return templates.TemplateResponse("pages/login.html", {
            "request": request,
            "error": "가입 승인 대기 중입니다. 관리자 승인 후 로그인할 수 있습니다."
        })
    
    # 거부된 상태
    if pg_user.status == "rejected":
        reason = pg_user.rejected_reason or "관리자에 의해 거부되었습니다"
        return templates.TemplateResponse("pages/login.html", {
            "request": request,
            "error": f"가입이 거부되었습니다: {reason}"
        })
    
    # 비활성화된 계정
    if not pg_user.is_active:
        log_login_activity(db, None, username, False, "비활성화된 계정")
        return templates.TemplateResponse("pages/login.html", {
            "request": request,
            "error": "비활성화된 계정입니다. 관리자에게 문의하세요."
        })
    
    # 마지막 로그인 시간 업데이트
    pg_user.last_login_at = datetime.utcnow()
    pg_db.commit()
    
    # 로그인 성공 기록
    log_login_activity(db, pg_user.id, username, True, "로그인 성공")
    
    # 토큰 생성
    access_token = create_access_token(
        data={"sub": pg_user.username, "role": pg_user.role}
    )
    
    # 쿠키에 토큰 저장
    response = RedirectResponse(url="/servers", status_code=status.HTTP_302_FOUND)
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        max_age=settings.access_token_expire_minutes * 60
    )
    
    return response


@router.post("/register")
async def register(
    request: Request,
    pg_db: Session = Depends(get_pg_db)
):
    """회원가입 처리 (이메일 인증 포함)"""
    form = await request.form()
    username = form.get("username", "").strip()
    password = form.get("password", "")
    password_confirm = form.get("password_confirm", "")
    name = form.get("name", "").strip()
    email = form.get("email", "").strip()
    phone = form.get("phone", "").strip() or None
    
    errors = []
    
    # 유효성 검사
    if not username or len(username) < 2:
        errors.append("아이디는 2자 이상이어야 합니다")
    elif not username.replace("_", "").isalnum():
        errors.append("아이디는 영문, 숫자, 언더스코어(_)만 사용 가능합니다")
    
    if not password or len(password) < 4:
        errors.append("비밀번호는 4자 이상이어야 합니다")
    
    if password != password_confirm:
        errors.append("비밀번호가 일치하지 않습니다")
    
    if not name or len(name) < 2:
        errors.append("이름은 2자 이상이어야 합니다")
    
    # 이메일 필수
    if not email:
        errors.append("이메일은 필수입니다")
    elif "@" not in email:
        errors.append("올바른 이메일 형식이 아닙니다")
    
    # 중복 확인
    if username:
        existing_user = pg_db.query(PgUser).filter(PgUser.username == username).first()
        if existing_user:
            errors.append("이미 사용 중인 아이디입니다")
    
    if email:
        existing_email = pg_db.query(PgUser).filter(PgUser.email == email).first()
        if existing_email:
            errors.append("이미 사용 중인 이메일입니다")
    
    if errors:
        return templates.TemplateResponse("pages/register.html", {
            "request": request,
            "error": errors[0],
            "username": username,
            "name": name,
            "email": email
        })
    
    # 이메일 토큰 생성
    email_token = generate_email_token()
    email_token_expires = datetime.utcnow() + timedelta(hours=EMAIL_TOKEN_EXPIRE_HOURS)
    
    # 사용자 생성
    new_user = PgUser(
        username=username,
        password_hash=pg_get_password_hash(password),
        name=name,
        email=email,
        phone=phone,
        role="viewer",
        status="email_pending",
        is_active=True,
        email_verified=False,
        email_token=email_token,
        email_token_expires=email_token_expires
    )
    
    pg_db.add(new_user)
    pg_db.commit()
    pg_db.refresh(new_user)
    
    # 인증 메일 발송
    email_sent = send_verification_email(email, name, email_token)
    
    if not email_sent:
        print(f"[WARNING] 인증 메일 발송 실패: {email}")
    
    return templates.TemplateResponse("pages/register_complete.html", {
        "request": request,
        "email": email,
        "name": name
    })


@router.post("/resend-verification")
async def resend_verification(
    request: Request,
    pg_db: Session = Depends(get_pg_db)
):
    """인증 메일 재발송"""
    data = await request.json()
    email = data.get("email", "").strip()
    
    if not email:
        raise HTTPException(status_code=400, detail="이메일을 입력해주세요")
    
    user = pg_db.query(PgUser).filter(PgUser.email == email).first()
    
    if not user:
        raise HTTPException(status_code=404, detail="등록되지 않은 이메일입니다")
    
    if user.email_verified:
        raise HTTPException(status_code=400, detail="이미 인증이 완료된 계정입니다")
    
    # 새 토큰 생성
    email_token = generate_email_token()
    email_token_expires = datetime.utcnow() + timedelta(hours=EMAIL_TOKEN_EXPIRE_HOURS)
    
    user.email_token = email_token
    user.email_token_expires = email_token_expires
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    # 메일 발송
    email_sent = send_verification_email(user.email, user.name, email_token)
    
    if not email_sent:
        raise HTTPException(status_code=500, detail="메일 발송에 실패했습니다. 잠시 후 다시 시도해주세요.")
    
    return MessageResponse(message="인증 메일이 재발송되었습니다.")


@router.get("/logout")
async def logout(request: Request, db: Session = Depends(get_db)):
    """로그아웃"""
    user = get_current_user(request, db)
    if user:
        log_login_activity(db, user.id, user.username, True, "로그아웃")
    
    response = RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)
    response.delete_cookie("access_token")
    return response


@router.get("/me")
async def get_me(user: SqliteUser = Depends(require_login)) -> UserInfo:
    """현재 사용자 정보"""
    return UserInfo(
        id=user.id,
        username=user.username,
        name=user.name,
        email=user.email,
        role=UserRole(user.role),
        is_active=user.is_active,
        created_at=user.created_at,
        last_login_at=user.last_login_at
    )


@router.post("/change-password")
async def change_password(
    request: Request,
    pg_db: Session = Depends(get_pg_db)
):
    """비밀번호 변경 (PostgreSQL 사용자)"""
    user = get_current_user_pg(request, pg_db)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다")
    
    data = await request.json()
    current_password = data.get("current_password", "")
    new_password = data.get("new_password", "")
    
    if not pg_verify_password(current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="현재 비밀번호가 올바르지 않습니다")
    
    if len(new_password) < 4:
        raise HTTPException(status_code=400, detail="새 비밀번호는 4자 이상이어야 합니다")
    
    user.password_hash = pg_get_password_hash(new_password)
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    return MessageResponse(message="비밀번호가 변경되었습니다")

# ============================================================
# 비밀번호 찾기/재설정
# ============================================================

@router.get("/forgot-password")
async def forgot_password_page(request: Request):
    """비밀번호 찾기 페이지"""
    return templates.TemplateResponse("pages/forgot_password.html", {
        "request": request
    })


@router.post("/forgot-password")
async def forgot_password(
    request: Request,
    pg_db: Session = Depends(get_pg_db)
):
    """비밀번호 찾기 - 재설정 링크 이메일 발송"""
    from app.core.email import send_password_reset_email
    
    form = await request.form()
    email = form.get("email", "").strip()
    
    # 이메일로 사용자 조회
    user = pg_db.query(PgUser).filter(PgUser.email == email).first()
    
    # 사용자가 없어도 보안상 동일한 응답
    if user:
        # 토큰 생성 (1시간 유효)
        reset_token = secrets.token_urlsafe(32)
        user.password_reset_token = reset_token
        user.password_reset_expires = datetime.utcnow() + timedelta(hours=1)
        pg_db.commit()
        
        # 이메일 발송
        try:
            await send_password_reset_email(
                to_email=user.email,
                username=user.username,
                name=user.name,
                reset_token=reset_token
            )
        except Exception as e:
            print(f"비밀번호 재설정 이메일 발송 실패: {e}")
    
    return templates.TemplateResponse("pages/forgot_password.html", {
        "request": request,
        "success": "입력하신 이메일로 비밀번호 재설정 링크를 발송했습니다."
    })


@router.get("/reset-password")
async def reset_password_page(
    request: Request,
    token: str = None,
    pg_db: Session = Depends(get_pg_db)
):
    """비밀번호 재설정 페이지"""
    if not token:
        return RedirectResponse(url="/auth/forgot-password", status_code=302)
    
    # 토큰 검증
    user = pg_db.query(PgUser).filter(
        PgUser.password_reset_token == token,
        PgUser.password_reset_expires > datetime.utcnow()
    ).first()
    
    if not user:
        return templates.TemplateResponse("pages/reset_password.html", {
            "request": request,
            "token": token,
            "valid": False
        })
    
    return templates.TemplateResponse("pages/reset_password.html", {
        "request": request,
        "token": token,
        "valid": True,
        "username": user.username
    })


@router.post("/reset-password")
async def reset_password(
    request: Request,
    pg_db: Session = Depends(get_pg_db)
):
    """비밀번호 재설정 실행"""
    form = await request.form()
    token = form.get("token", "")
    new_password = form.get("new_password", "")
    confirm_password = form.get("confirm_password", "")
    
    # 토큰으로 사용자 조회
    user = pg_db.query(PgUser).filter(
        PgUser.password_reset_token == token,
        PgUser.password_reset_expires > datetime.utcnow()
    ).first()
    
    if not user:
        return templates.TemplateResponse("pages/reset_password.html", {
            "request": request,
            "token": token,
            "valid": False
        })
    
    # 비밀번호 확인
    if new_password != confirm_password:
        return templates.TemplateResponse("pages/reset_password.html", {
            "request": request,
            "token": token,
            "valid": True,
            "username": user.username,
            "error": "비밀번호가 일치하지 않습니다."
        })
    
    if len(new_password) < 8:
        return templates.TemplateResponse("pages/reset_password.html", {
            "request": request,
            "token": token,
            "valid": True,
            "username": user.username,
            "error": "비밀번호는 8자 이상이어야 합니다."
        })
    
    # 비밀번호 변경
    user.password_hash = pg_get_password_hash(new_password)
    user.password_reset_token = None
    user.password_reset_expires = None
    user.updated_at = datetime.utcnow()
    pg_db.commit()
    
    return templates.TemplateResponse("pages/reset_password.html", {
        "request": request,
        "success": True
    })