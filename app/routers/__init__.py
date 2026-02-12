"""
라우터 모듈
"""
from app.routers.auth import router as auth_router
from app.routers.servers import router as servers_router
from app.routers.corps import router as corps_router
from app.routers.pages import router as pages_router
from app.routers.partials import router as partials_router

__all__ = ["auth_router", "servers_router", "corps_router", "pages_router", "partials_router"]
