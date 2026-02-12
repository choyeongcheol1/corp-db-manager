"""
페이지 라우터 (HTML 렌더링)
"""
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db, get_pg_db, User
from app.services.server_service import ServerService
from app.services.corp_service import CorpService
from app.routers.auth import get_current_user, get_current_user_pg
from app.models.user import User as PgUser

router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory="app/templates")


def get_current_user_any(request: Request, db: Session, pg_db: Session):
    """현재 로그인 사용자 조회 (PostgreSQL 우선, SQLite 폴백)"""
    # PostgreSQL 먼저 확인
    pg_user = get_current_user_pg(request, pg_db)
    if pg_user:
        return pg_user
    
    # SQLite 확인 (기존 사용자)
    sqlite_user = get_current_user(request, db)
    return sqlite_user


def get_context(request: Request, user, **kwargs):
    """공통 컨텍스트"""
    return {
        "request": request,
        "user": user,
        **kwargs
    }


def has_servers(db: Session) -> bool:
    """등록된 서버가 있는지 확인"""
    from app.core.database import DBServer
    return db.query(DBServer).filter(DBServer.is_active == True).count() > 0


@router.get("/", response_class=HTMLResponse)
async def root(request: Request, db: Session = Depends(get_db), pg_db: Session = Depends(get_pg_db)):
    """루트 - 로그인 체크 후 리다이렉트"""
    user = get_current_user_any(request, db, pg_db)
    if user:
        return RedirectResponse(url="/servers", status_code=302)
    return RedirectResponse(url="/login", status_code=302)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, db: Session = Depends(get_db), pg_db: Session = Depends(get_pg_db)):
    """로그인 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if user:
        return RedirectResponse(url="/servers", status_code=302)
    
    return templates.TemplateResponse("pages/login.html", {
        "request": request
    })


@router.get("/servers", response_class=HTMLResponse)
async def servers_page(request: Request, db: Session = Depends(get_db), pg_db: Session = Depends(get_pg_db)):
    """서버 선택 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    # ※ 성능 개선: 서버 목록은 API(/api/servers)에서 비동기 로드
    #   기존에는 get_all_server_summaries()가 모든 서버에 연결 테스트를 수행하여
    #   오프라인 서버가 있으면 타임아웃(10초)만큼 페이지 로딩이 블로킹됨
    return templates.TemplateResponse("pages/servers.html", get_context(
        request, user
    ))


@router.get("/dashboard", response_class=HTMLResponse)
@router.get("/dashboard/{server_id}", response_class=HTMLResponse)
async def dashboard_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """대시보드 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    # 등록된 서버가 없으면 서버 관리로 이동
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
        if not current_server:
            return RedirectResponse(url="/servers", status_code=302)
    
    # ※ 성능 개선: servers, stats는 프론트에서 HTMX/API로 비동기 로드
    #   서버 미선택 시 get_all_server_summaries() 호출 제거
    return templates.TemplateResponse("pages/dashboard.html", get_context(
        request, user,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/corps", response_class=HTMLResponse)
@router.get("/corps/server/{server_id}", response_class=HTMLResponse)
async def corps_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """법인 DB 목록 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    corp_service = CorpService(db)
    
    servers = server_service.get_all_server_summaries()
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/corps.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/corps/create", response_class=HTMLResponse)
@router.get("/corps/create/{server_id}", response_class=HTMLResponse)
async def create_corp_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """신규 법인 생성 페이지 (기존 URL 유지)"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    # 운영자 이상 권한 체크
    if user.role not in ["admin", "operator"]:
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_server_summaries()
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/db_create.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/server-management", response_class=HTMLResponse)
async def server_management_page(
    request: Request,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """서버 관리 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_servers(active_only=False)
    
    return templates.TemplateResponse("pages/server_management.html", get_context(
        request, user,
        servers=servers
    ))


@router.get("/monitoring", response_class=HTMLResponse)
@router.get("/monitoring/{server_id}", response_class=HTMLResponse)
async def monitoring_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """모니터링 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_server_summaries()
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/monitoring.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(
    request: Request,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """설정 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    return templates.TemplateResponse("pages/settings.html", get_context(
        request, user
    ))


@router.get("/tables", response_class=HTMLResponse)
@router.get("/tables/{server_id}", response_class=HTMLResponse)
async def tables_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """테이블 현황 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_servers(active_only=True)
    current_server = None
    databases = []
    
    if server_id:
        current_server = server_service.get_server(server_id)
        if current_server:
            try:
                databases = server_service.get_server_databases(current_server)
            except Exception as e:
                print(f"DB 목록 조회 실패: {e}")
                databases = []
    
    return templates.TemplateResponse("pages/tables.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id,
        databases=databases
    ))


@router.get("/copy-data", response_class=HTMLResponse)
@router.get("/copy-data/{server_id}", response_class=HTMLResponse)
async def copy_data_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """데이터 복사 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    # 운영자 이상 권한 체크
    if user.role not in ["admin", "operator"]:
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_servers(active_only=True)
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/copy_data.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/schema-export", response_class=HTMLResponse)
async def schema_export_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """테이블 정의서 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_servers(active_only=True)
    
    return templates.TemplateResponse("pages/schema_export.html", get_context(
        request, user,
        servers=servers,
        selected_server_id=server_id
    ))

@router.get("/activity-logs", response_class=HTMLResponse)
async def activity_logs_page(
    request: Request,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """활동 로그 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    return templates.TemplateResponse("pages/activity_logs.html", get_context(
        request, user
    ))

# ============================================================
# DB 관리 라우트
# ============================================================

@router.get("/db/list", response_class=HTMLResponse)
@router.get("/db/list/{server_id}", response_class=HTMLResponse)
async def db_list_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """DB 목록 페이지 (법인 DB 목록과 동일)"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_server_summaries()
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/corps.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/db/create", response_class=HTMLResponse)
@router.get("/db/create/{server_id}", response_class=HTMLResponse)
async def db_create_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """DB 생성 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    # 운영자 이상 권한 체크
    if user.role not in ["admin", "operator"]:
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_server_summaries()
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/db_create.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/db/table-init", response_class=HTMLResponse)
@router.get("/db/table-init/{server_id}", response_class=HTMLResponse)
async def table_init_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """테이블 초기화 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    # 운영자 이상 권한 체크
    if user.role not in ["admin", "operator"]:
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_server_summaries()
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/table_init.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/data-copy", response_class=HTMLResponse)
@router.get("/data-copy/{server_id}", response_class=HTMLResponse)
async def data_copy_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """데이터 복사 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    # 운영자 이상 권한 체크
    if user.role not in ["admin", "operator"]:
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_servers(active_only=True)
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/copy_data.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))

@router.get("/db-sync", response_class=HTMLResponse)
@router.get("/db-sync/{server_id}", response_class=HTMLResponse)
async def db_sync_page(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """DB 동기화 페이지"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    if not has_servers(db):
        return RedirectResponse(url="/servers", status_code=302)
    
    # 운영자 이상 권한 체크
    if user.role not in ["admin", "operator"]:
        return RedirectResponse(url="/servers", status_code=302)
    
    server_service = ServerService(db)
    servers = server_service.get_all_servers(active_only=True)
    current_server = None
    
    if server_id:
        current_server = server_service.get_server(server_id)
    
    return templates.TemplateResponse("pages/db_sync.html", get_context(
        request, user,
        servers=servers,
        current_server=current_server,
        server_id=server_id
    ))


@router.get("/user-management", response_class=HTMLResponse)
async def user_management_page(
    request: Request,
    db: Session = Depends(get_db),
    pg_db: Session = Depends(get_pg_db)
):
    """사용자 관리 페이지 (관리자 전용)"""
    user = get_current_user_any(request, db, pg_db)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    
    # 관리자 권한 체크
    if user.role != "admin":
        return RedirectResponse(url="/servers", status_code=302)
    
    return templates.TemplateResponse("pages/user_management.html", get_context(
        request, user
    ))