"""
법인 DB 관리 시스템 - FastAPI 메인 애플리케이션
"""
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import uvicorn

from app.config import get_settings
from app.core.database import init_db
from app.core.notification_db import init_notification_db
from app.routers import auth_router, servers_router, corps_router, pages_router, partials_router
from app.routers.schema_export import router as schema_export_router
from app.routers.activity_logs import router as activity_logs_router
from app.routers.settings import router as settings_router
from app.routers.users import router as users_router
from app.routers.notifications import router as notifications_router
from app.routers.table_init import router as table_init_router
from app.routers.db_sync import router as db_sync_router         

from app.routers import sync, sync_partials

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기"""
    # 시작 시
    print("[INFO] 법인 DB 관리 시스템 시작")
    init_db()
    init_notification_db()
    yield
    # 종료 시
    print("[INFO] 시스템 종료")


# FastAPI 앱 생성
app = FastAPI(
    title="법인 DB 관리 시스템",
    description="다중 서버 환경의 법인 데이터베이스 관리 시스템",
    version="1.2.0",
    lifespan=lifespan
)

# 정적 파일 마운트
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# 라우터 등록
app.include_router(auth_router)
app.include_router(servers_router)
app.include_router(corps_router)
app.include_router(partials_router)
app.include_router(schema_export_router)
app.include_router(activity_logs_router)
app.include_router(settings_router)
app.include_router(users_router)
app.include_router(notifications_router)
app.include_router(table_init_router)
app.include_router(db_sync_router)          
app.include_router(pages_router)
app.include_router(sync.router)
app.include_router(sync_partials.router)



# 전역 예외 처리
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """전역 예외 핸들러"""
    return JSONResponse(
        status_code=500,
        content={"detail": f"서버 오류: {str(exc)}"}
    )


# 헬스체크
@app.get("/health")
async def health_check():
    """헬스체크"""
    return {"status": "healthy", "version": "1.2.0"}


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )