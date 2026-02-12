"""
HTMX용 부분 템플릿 라우터
- 전체 페이지가 아닌 HTML 조각 반환
- hx-get, hx-post 등에서 호출
"""
from fastapi import APIRouter, Depends, Request, Form, Query
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional

from app.core.database import get_db, User
from app.services.server_service import ServerService
from app.services.corp_service import CorpService
from app.services.sql_templates import SqlTemplateService, CreateDBParams
from app.models import CreateDBRequest
from app.routers.auth import get_current_user, require_login, require_operator

router = APIRouter(prefix="/partials", tags=["partials"])
templates = Jinja2Templates(directory="app/templates")


# ============================================================
# 서버 관련 Partials
# ============================================================

@router.get("/servers/list", response_class=HTMLResponse)
async def servers_list(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """서버 카드 목록"""
    service = ServerService(db)
    servers = service.get_all_server_summaries()
    
    return templates.TemplateResponse("partials/servers/list.html", {
        "request": request,
        "servers": servers,
        "user": user
    })


@router.get("/servers/{server_id}/databases", response_class=HTMLResponse)
async def server_databases(
    request: Request,
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """서버의 DB 목록 (select options)"""
    service = ServerService(db)
    server = service.get_server(server_id)
    
    if not server:
        return HTMLResponse("<option value=''>서버를 찾을 수 없습니다</option>")
    
    databases = service.get_server_databases(server)
    
    return templates.TemplateResponse("partials/servers/db_options.html", {
        "request": request,
        "databases": databases
    })


@router.post("/servers/{server_id}/test", response_class=HTMLResponse)
async def test_server_connection(
    request: Request,
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """서버 연결 테스트 결과"""
    service = ServerService(db)
    server = service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    success, message, version = service.test_connection(server)
    
    return templates.TemplateResponse("partials/common/alert.html", {
        "request": request,
        "type": "success" if success else "error",
        "message": message,
        "detail": version
    })


# ============================================================
# partials.py 에서 기존 corps_list 함수를 아래로 교체
# 
# 변경점:
#   1. main_db_id 파라미터 추가 (콤보박스 선택값)
#   2. get_main_db_list()로 다중 메인 DB 목록 조회
#   3. 선택된 메인 DB 또는 전체에서 법인 정보 매핑
# ============================================================

@router.get("/corps/list", response_class=HTMLResponse)
async def corps_list(
    request: Request,
    server_id: Optional[int] = None,
    keyword: Optional[str] = None,
    status: Optional[str] = None,
    main_db_id: Optional[int] = None,       # ← 추가: 메인 DB 선택
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 목록 테이블 - MSSQL DB 목록 + 메인 DB 법인 정보 매칭"""
    server_service = ServerService(db)

    # ── 1) 설정에서 컬럼 매핑 + 메인 DB 목록 조회 ──
    from app.routers.settings import get_config_value, get_main_db_list

    corp_table_name = get_config_value(db, "corp_table_name", "COMS_CMPNY")
    corp_code_column = get_config_value(db, "corp_code_column", "CORP_CD")
    corp_name_column = get_config_value(db, "corp_name_column", "CORP_NM")
    biz_no_column = get_config_value(db, "biz_no_column", "SAUPNO")
    acc_db_name_column = get_config_value(db, "acc_db_name_column", "ACC_DB_NAME")

    # 메인 DB 목록
    main_db_entries = get_main_db_list(db)

    # 사용할 메인 DB 결정
    if main_db_id:
        # 특정 메인 DB 선택
        target_entries = [e for e in main_db_entries if e.get("id") == main_db_id]
    elif main_db_entries:
        # 전체 메인 DB에서 매핑
        target_entries = main_db_entries
    else:
        # 메인 DB 목록이 비어있으면 기존 단일 설정 사용 (하위호환)
        old_server_id = get_config_value(db, "main_db_server_id", "")
        old_db_name = get_config_value(db, "main_db_name", "")
        if old_server_id and old_db_name:
            target_entries = [{"server_id": int(old_server_id), "db_name": old_db_name}]
        else:
            target_entries = []

    # ── 2) 메인 DB에서 법인 정보 조회 → ACC_DB_NAME 기준 매핑 ──
    corp_info_map = {}

    for entry in target_entries:
        entry_server_id = entry.get("server_id")
        entry_db_name = entry.get("db_name")
        if not entry_server_id or not entry_db_name:
            continue

        try:
            main_server = server_service.get_server(int(entry_server_id))
            if not main_server:
                continue

            conn = server_service.get_connection(main_server, entry_db_name)
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT 
                    [{corp_code_column}] AS corp_code,
                    [{corp_name_column}] AS corp_name,
                    [{biz_no_column}] AS biz_no,
                    [{acc_db_name_column}] AS acc_db_name
                FROM [{corp_table_name}]
                WHERE [{acc_db_name_column}] IS NOT NULL 
                  AND [{acc_db_name_column}] <> ''
            """)
            for row in cursor.fetchall():
                acc_name = (row.acc_db_name or "").strip()
                if acc_name:
                    corp_info_map[acc_name] = {
                        "corp_code": (row.corp_code or "").strip(),
                        "corp_name": (row.corp_name or "").strip(),
                        "biz_no": (row.biz_no or "").strip(),
                    }
            conn.close()
        except Exception as e:
            print(f"[corps_list] 메인 DB 법인 정보 조회 실패 ({entry_db_name}): {e}")

    # ── 3) DB 목록 조회 + 법인 정보 매칭 ──
    corps_with_server = []

    def build_corp_item(db_info, server):
        db_name = db_info['db_name']
        info = corp_info_map.get(db_name)

        if info:
            corp_code = info["corp_code"]
            corp_name = info["corp_name"]
        else:
            corp_code = db_name
            corp_name = db_name

        # 키워드 필터
        if keyword:
            kw = keyword.lower()
            if (kw not in corp_code.lower() and
                kw not in corp_name.lower() and
                kw not in db_name.lower()):
                return None

        corp_status = "normal"
        if status and corp_status != status:
            return None

        return {
            "corp": {
                "id": 0,
                "corp_code": corp_code,
                "corp_name": corp_name,
                "db_name": db_name,
                "status": corp_status,
                "created_at": db_info['create_date'],
                "server_id": server.id
            },
            "server_name": server.server_name
        }

    if server_id:
        server = server_service.get_server(server_id)
        if server:
            dbs = server_service.get_server_databases(server)
            for db_info in dbs:
                item = build_corp_item(db_info, server)
                if item:
                    corps_with_server.append(item)
    else:
        servers = server_service.get_all_servers(active_only=True)
        for svr in servers:
            dbs = server_service.get_server_databases(svr)
            for db_info in dbs:
                item = build_corp_item(db_info, svr)
                if item:
                    corps_with_server.append(item)

    # DB명 기준 오름차순 정렬
    corps_with_server.sort(key=lambda x: (x["corp"]["db_name"] or "").lower())

    return templates.TemplateResponse("partials/corps/list.html", {
        "request": request,
        "corps": corps_with_server,
        "show_server_column": server_id is None
    })


@router.get("/corps/{corp_id}/detail", response_class=HTMLResponse)
async def corp_detail(
    request: Request,
    corp_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 상세 정보 패널"""
    corp_service = CorpService(db)
    server_service = ServerService(db)
    
    corp = corp_service.get_corp(corp_id)
    if not corp:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "법인을 찾을 수 없습니다"
        })
    
    server = server_service.get_server(corp.server_id)
    tables = corp_service.get_db_tables(server, corp.db_name) if server else []
    size_mb = corp_service.get_db_size(server, corp.db_name) if server else 0
    
    return templates.TemplateResponse("partials/corps/detail.html", {
        "request": request,
        "corp": corp,
        "server": server,
        "tables": tables,
        "size_mb": size_mb,
        "user": user
    })

@router.get("/corps/detail/{server_id}/{db_name}", response_class=HTMLResponse)
async def corp_detail_by_db(
    request: Request,
    server_id: int,
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 상세 정보 패널 - DB 이름 기반"""
    server_service = ServerService(db)
    
    server = server_service.get_server(server_id)
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    # DB 정보 조회
    dbs = server_service.get_server_databases(server)
    db_info = next((d for d in dbs if d['db_name'] == db_name), None)
    
    if not db_info:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "DB를 찾을 수 없습니다"
        })
    
    # 테이블 목록 조회
    tables = []
    try:
        conn = server_service.get_connection(server, db_name)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                t.name AS table_name,
                p.rows AS row_count,
                SUM(a.total_pages) * 8.0 / 1024 AS size_mb
            FROM sys.tables t
            INNER JOIN sys.indexes i ON t.object_id = i.object_id
            INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE i.index_id <= 1
            GROUP BY t.name, p.rows
            ORDER BY t.name
        """)
        for row in cursor.fetchall():
            tables.append({
                "table_name": row.table_name,
                "row_count": row.row_count or 0,
                "size_mb": round(row.size_mb or 0, 2)
            })
        conn.close()
    except Exception as e:
        print(f"테이블 목록 조회 실패: {e}")
    
    # corp 객체 생성 (표시용)
    corp = {
        "id": 0,
        "corp_code": db_name,
        "corp_name": db_name,
        "db_name": db_name,
        "db_user": "sa",
        "biz_no": "",
        "status": "normal",
        "created_at": db_info['create_date']
    }
    
    return templates.TemplateResponse("partials/corps/detail.html", {
        "request": request,
        "corp": corp,
        "server": server,
        "tables": tables,
        "size_mb": db_info['size_mb'],
        "user": user
    })


@router.post("/corps/{corp_id}/test", response_class=HTMLResponse)
async def test_corp_connection(
    request: Request,
    corp_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인 DB 연결 테스트"""
    corp_service = CorpService(db)
    server_service = ServerService(db)
    
    corp = corp_service.get_corp(corp_id)
    if not corp:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "법인을 찾을 수 없습니다"
        })
    
    server = server_service.get_server(corp.server_id)
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버 정보를 찾을 수 없습니다"
        })
    
    try:
        conn = server_service.get_connection(server, corp.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "success",
            "message": "연결 성공"
        })
    except Exception as e:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": f"연결 실패: {str(e)}"
        })


@router.get("/corps/validate-code", response_class=HTMLResponse)
async def validate_corp_code(
    request: Request,
    corp_code: str = Query(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """법인코드 유효성 검사"""
    service = CorpService(db)
    is_valid, message = service.validate_corp_code(corp_code)
    
    return templates.TemplateResponse("partials/corps/code_validation.html", {
        "request": request,
        "valid": is_valid,
        "message": message,
        "corp_code": corp_code
    })


@router.post("/corps/create", response_class=HTMLResponse)
async def create_corp(
    request: Request,
    source_server_id: int = Form(...),
    source_db_name: str = Form(...),
    target_server_id: int = Form(...),
    corp_code: str = Form(...),
    corp_name: str = Form(...),
    biz_no: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """법인 DB 생성"""
    service = CorpService(db)
    
    # 법인코드 검증
    is_valid, message = service.validate_corp_code(corp_code.upper())
    if not is_valid:
        return templates.TemplateResponse("partials/corps/create_result.html", {
            "request": request,
            "success": False,
            "message": message
        })
    
    # 생성 요청
    create_request = CreateDBRequest(
        source_server_id=source_server_id,
        source_db_name=source_db_name,
        target_server_id=target_server_id,
        corp_code=corp_code.upper(),
        corp_name=corp_name,
        biz_no=biz_no
    )
    
    result = service.create_corp_db(create_request, user_id=user.id)
    
    return templates.TemplateResponse("partials/corps/create_result.html", {
        "request": request,
        "success": result.success,
        "result": result,
        "message": result.message
    })


# ============================================================
# 대시보드 Partials
# ============================================================

@router.get("/dashboard/stats", response_class=HTMLResponse)
async def dashboard_stats(
    request: Request,
    server_id: Optional[int] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """대시보드 통계 카드"""
    server_service = ServerService(db)
    
    total = 0
    normal = 0
    warning = 0
    error = 0
    
    if server_id:
        # 특정 서버
        server = server_service.get_server(server_id)
        if server:
            dbs = server_service.get_server_databases(server)
            total = len(dbs)
            normal = total  # 일단 모두 정상으로 표시
    else:
        # 전체 서버
        servers = server_service.get_all_servers(active_only=True)
        for server in servers:
            dbs = server_service.get_server_databases(server)
            total += len(dbs)
            normal += len(dbs)  # 일단 모두 정상으로 표시
    
    stats = {
        "total": total,
        "normal": normal,
        "warning": warning,
        "error": error
    }
    
    return templates.TemplateResponse("partials/dashboard/stats.html", {
        "request": request,
        "stats": stats,
        "server_id": server_id
    })


@router.get("/dashboard/activities", response_class=HTMLResponse)
async def dashboard_activities(
    request: Request,
    server_id: Optional[int] = None,
    limit: int = 5,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """최근 활동 이력"""
    from app.core.database import ActivityLog
    
    query = db.query(ActivityLog).order_by(ActivityLog.created_at.desc())
    
    if server_id:
        query = query.filter(ActivityLog.server_id == server_id)
    
    activities = query.limit(limit).all()
    
    return templates.TemplateResponse("partials/dashboard/activities.html", {
        "request": request,
        "activities": activities
    })

# ============================================================
# 헬스체크 Partials
# ============================================================

@router.get("/health/server/{server_id}", response_class=HTMLResponse)
async def server_health_check(
    request: Request,
    server_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """서버 상태 점검"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    health = server_service.check_server_health(server)
    
    return templates.TemplateResponse("partials/health/server_check.html", {
        "request": request,
        "health": health,
        "server": server
    })


@router.get("/health/databases/{server_id}", response_class=HTMLResponse)
async def databases_health_check(
    request: Request,
    server_id: int,
    status_filter: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """전체 DB 상태 점검"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    health = server_service.check_all_databases_health(server)
    
    # 상태 필터 적용
    if status_filter and status_filter != "all":
        health["databases"] = [
            d for d in health["databases"] 
            if d["status"] == status_filter
        ]
    
    return templates.TemplateResponse("partials/health/db_check.html", {
        "request": request,
        "health": health,
        "server": server,
        "status_filter": status_filter
    })


@router.get("/health/database/{server_id}/{db_name}", response_class=HTMLResponse)
async def database_health_detail(
    request: Request,
    server_id: int,
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """개별 DB 상태 점검 상세"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    health = server_service.check_database_health(server, db_name)
    
    return templates.TemplateResponse("partials/health/db_detail.html", {
        "request": request,
        "health": health,
        "server": server,
        "db_name": db_name
    })

@router.get("/dashboard/capacity-chart/{server_id}", response_class=HTMLResponse)
async def capacity_chart(
    request: Request,
    server_id: int,
    limit: int = 200,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """DB별 용량 + 디스크 사용률 차트 데이터"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    # DB 목록 조회 (디스크 사용률 포함)
    dbs = server_service.get_databases_with_disk_usage(server)
    dbs_sorted = sorted(dbs, key=lambda x: x['size_mb'], reverse=True)[:limit]
    
    return templates.TemplateResponse("partials/dashboard/capacity_chart.html", {
        "request": request,
        "databases": dbs_sorted,
        "server": server
    })

# ============================================================
# 테이블 현황 Partials
# ============================================================

@router.get("/tables/list/{server_id}/{db_name}", response_class=HTMLResponse)
async def tables_list(
    request: Request,
    server_id: int,
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """테이블 목록 조회"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    # 드라이버를 통해 테이블 목록 조회
    from app.services.drivers import get_driver
    driver = get_driver(server)
    
    try:
        tables = driver.get_tables(db_name)
        
        # 통계 계산
        total_rows = sum(t.get('row_count', 0) for t in tables)
        total_size = sum(t.get('size_mb', 0) for t in tables)
        
        return templates.TemplateResponse("partials/tables/list.html", {
            "request": request,
            "server": server,
            "db_name": db_name,
            "tables": tables,
            "total_rows": total_rows,
            "total_size": total_size
        })
    except Exception as e:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": f"테이블 조회 실패: {str(e)}"
        })


@router.get("/tables/columns/{server_id}/{db_name}/{table_name}", response_class=HTMLResponse)
async def table_columns(
    request: Request,
    server_id: int,
    db_name: str,
    table_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """테이블 컬럼 정보 (정의서)"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    from app.services.drivers import get_driver
    driver = get_driver(server)
    
    try:
        columns = driver.get_table_columns(db_name, table_name)
        
        return templates.TemplateResponse("partials/tables/columns.html", {
            "request": request,
            "server": server,
            "db_name": db_name,
            "table_name": table_name,
            "columns": columns
        })
    except Exception as e:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": f"컬럼 조회 실패: {str(e)}"
        }) 

# ============================================================
# 데이터 복사 Partials
# ============================================================

@router.get("/copy-data/tables", response_class=JSONResponse)
async def copy_data_tables_json(
    request: Request,
    source_server_id: int = None,
    source_db: str = None,
    target_server_id: int = None,
    target_db: str = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """테이블 목록 JSON API (Alpine.js용)"""
    from fastapi.responses import JSONResponse
    
    server_service = ServerService(db)
    source_server = server_service.get_server(source_server_id)
    
    if not source_server:
        return JSONResponse({"error": "소스 서버를 찾을 수 없습니다", "tables": []})
    
    from app.services.drivers import get_driver
    source_driver = get_driver(source_server)
    
    try:
        tables = source_driver.get_tables(source_db)
        
        # 대상 DB 테이블 목록 조회
        target_table_names = set()
        if target_server_id and target_db:
            try:
                target_server = server_service.get_server(target_server_id)
                if target_server:
                    target_driver = get_driver(target_server)
                    target_table_names = {t['table_name'] for t in target_driver.get_tables(target_db)}
            except Exception as te:
                print(f"[copy_data_tables_json] 대상 DB 테이블 조회 실패: {te}")
        
        # in_target 플래그 추가
        result = []
        for t in tables:
            t['in_target'] = t['table_name'] in target_table_names if target_table_names else True
            result.append(t)
        
        return JSONResponse({"tables": result})
    except Exception as e:
        return JSONResponse({"error": f"테이블 조회 실패: {str(e)}", "tables": []})


@router.get("/copy-data/tables/{server_id}/{db_name}", response_class=HTMLResponse)
async def copy_data_tables(
    request: Request,
    server_id: int,
    db_name: str,
    target_server_id: Optional[int] = None,
    target_db_name: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """복사할 테이블 목록 조회 (#8: 대상 DB 테이블 존재 여부 비교 포함)"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    from app.services.drivers import get_driver
    driver = get_driver(server)
    
    try:
        tables = driver.get_tables(db_name)
        
        # #8: 대상 DB 테이블 목록 조회 (존재 여부 비교용)
        target_tables = set()
        if target_server_id and target_db_name:
            try:
                target_server = server_service.get_server(target_server_id)
                if target_server:
                    target_driver = get_driver(target_server)
                    target_tables = {t['table_name'] for t in target_driver.get_tables(target_db_name)}
            except Exception as te:
                print(f"[copy_data_tables] 대상 DB 테이블 조회 실패: {te}")
        
        return templates.TemplateResponse("partials/copy_data/tables.html", {
            "request": request,
            "tables": tables,
            "server": server,
            "db_name": db_name,
            "target_tables": target_tables
        })
    except Exception as e:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": f"테이블 조회 실패: {str(e)}"
        })


@router.post("/copy-data/execute", response_class=HTMLResponse)
async def copy_data_execute(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """데이터 복사 실행
    
    개선사항:
    - #1 대상 테이블 기존 데이터 처리 옵션 (truncate/delete/append)
    - #2 배치 처리 + executemany (메모리/속도)
    - #3 커넥션 누수 방지 (try/finally)
    - #4 테이블명 화이트리스트 검증
    - #5 운영서버 복사 시 경고 로그 (서버 is_production 플래그 활용)
    - #6 삽입 실패 행 로깅 (대표 에러 수집)
    """
    import json
    from datetime import datetime
    
    BATCH_SIZE = 5000  # 배치 단위 (#2)
    MAX_ERROR_SAMPLES = 5  # 대표 에러 수집 수 (#6)
    
    body = await request.body()
    data = json.loads(body)
    
    source_server_id = data.get('source_server_id')
    source_db = data.get('source_db')
    target_server_id = data.get('target_server_id')
    target_db = data.get('target_db')
    tables = data.get('tables', [])
    copy_mode = data.get('copy_mode', 'truncate')  # #1: truncate / delete / append
    
    server_service = ServerService(db)
    source_server = server_service.get_server(source_server_id)
    target_server = server_service.get_server(target_server_id)
    
    if not source_server or not target_server:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": "서버를 찾을 수 없습니다"
        })
    
    from app.services.drivers import get_driver
    source_driver = get_driver(source_server)
    target_driver = get_driver(target_server)
    
    # ── #4: 테이블명 화이트리스트 검증 ──
    try:
        valid_source_tables = {t['table_name'] for t in source_driver.get_tables(source_db)}
        valid_target_tables = {t['table_name'] for t in target_driver.get_tables(target_db)}
    except Exception as e:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": f"테이블 목록 조회 실패: {str(e)}"
        })
    
    # 유효하지 않은 테이블 사전 검증
    invalid_tables = [t for t in tables if t not in valid_source_tables]
    if invalid_tables:
        return templates.TemplateResponse("partials/common/alert.html", {
            "request": request,
            "type": "error",
            "message": f"소스 DB에 존재하지 않는 테이블: {', '.join(invalid_tables)}"
        })
    
    results = []
    success_count = 0
    fail_count = 0
    total_rows = 0
    start_time = datetime.now()
    
    for table_name in tables:
        source_conn = None
        target_conn = None
        
        try:
            # ── #8: 대상 테이블 존재 여부 검증 ──
            if table_name not in valid_target_tables:
                results.append({
                    "table_name": table_name,
                    "status": "error",
                    "message": "대상 DB에 테이블이 존재하지 않습니다",
                    "row_count": 0,
                    "error_samples": []
                })
                fail_count += 1
                continue
            
            # ── 소스에서 데이터 조회 (커서 유지, fetchmany 사용) ──
            source_conn = source_driver.get_connection(source_db)
            source_cursor = source_conn.cursor()
            source_cursor.execute(f"SELECT * FROM [{table_name}]")
            columns = [desc[0] for desc in source_cursor.description]
            
            # 첫 번째 배치를 읽어서 데이터 존재 여부 확인
            first_batch = source_cursor.fetchmany(BATCH_SIZE)
            if not first_batch:
                results.append({
                    "table_name": table_name,
                    "status": "skipped",
                    "message": "데이터 없음",
                    "row_count": 0,
                    "error_samples": []
                })
                continue
            
            # ── 대상 연결 + 사전 처리 ──
            target_conn = target_driver.get_connection(target_db)
            target_cursor = target_conn.cursor()
            
            # #1: 복사 모드에 따른 기존 데이터 처리
            if copy_mode == 'truncate':
                target_cursor.execute(f"TRUNCATE TABLE [{table_name}]")
            elif copy_mode == 'delete':
                target_cursor.execute(f"DELETE FROM [{table_name}]")
                target_conn.commit()
            # copy_mode == 'append': 기존 데이터 유지
            
            # IDENTITY_INSERT 설정 (MSSQL)
            has_identity = False
            try:
                target_cursor.execute(f"""
                    SELECT 1 FROM sys.columns 
                    WHERE object_id = OBJECT_ID('[{table_name}]') AND is_identity = 1
                """)
                has_identity = target_cursor.fetchone() is not None
            except:
                pass
            
            if has_identity:
                target_cursor.execute(f"SET IDENTITY_INSERT [{table_name}] ON")
            
            # ── #2: 배치 단위 INSERT ──
            placeholders = ', '.join(['?' for _ in columns])
            column_list = ', '.join([f'[{col}]' for col in columns])
            insert_sql = f"INSERT INTO [{table_name}] ({column_list}) VALUES ({placeholders})"
            
            inserted = 0
            skipped = 0
            error_samples = []  # #6: 대표 에러 수집
            
            # 첫 번째 배치 처리
            batch = first_batch
            while batch:
                for row in batch:
                    try:
                        target_cursor.execute(insert_sql, row)
                        inserted += 1
                    except Exception as row_err:
                        skipped += 1
                        # #6: 대표 에러 최대 N개 수집
                        if len(error_samples) < MAX_ERROR_SAMPLES:
                            error_samples.append(str(row_err)[:200])
                
                # 배치 단위 커밋 (대량 데이터 시 트랜잭션 로그 관리)
                target_conn.commit()
                
                # 다음 배치 읽기 (#2: fetchmany로 메모리 절약)
                batch = source_cursor.fetchmany(BATCH_SIZE)
            
            if has_identity:
                target_cursor.execute(f"SET IDENTITY_INSERT [{table_name}] OFF")
                target_conn.commit()
            
            # 결과 기록
            status = "success" if skipped == 0 else "warning"
            message = f"{inserted:,}행 복사 완료"
            if skipped > 0:
                message += f" ({skipped:,}행 건너뜀)"
            
            results.append({
                "table_name": table_name,
                "status": status,
                "message": message,
                "row_count": inserted,
                "skipped_count": skipped,
                "error_samples": error_samples
            })
            success_count += 1
            total_rows += inserted
            
        except Exception as e:
            # 에러 발생 시 롤백
            if target_conn:
                try:
                    target_conn.rollback()
                except:
                    pass
            
            results.append({
                "table_name": table_name,
                "status": "error",
                "message": str(e)[:300],
                "row_count": 0,
                "error_samples": []
            })
            fail_count += 1
        
        finally:
            # ── #3: 커넥션 누수 방지 ──
            if source_conn:
                try:
                    source_conn.close()
                except:
                    pass
            if target_conn:
                try:
                    target_conn.close()
                except:
                    pass
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # 활동 로그 기록 (#5: 운영서버 복사 시 경고 포함)
    from app.services.activity_service import ActivityService
    activity_service = ActivityService(db)
    
    log_message = f"{len(tables)}개 테이블, {total_rows:,}행 복사 (성공: {success_count}, 실패: {fail_count}, 모드: {copy_mode})"
    
    # #5: 대상이 운영서버인 경우 경고 로그
    is_target_production = getattr(target_server, 'is_production', False) or \
                           getattr(target_server, 'server_type', '') in ('production', 'prod', '운영')
    if is_target_production:
        log_message = f"[⚠ 운영서버 대상] {log_message}"
    
    activity_service.log(
        action="COPY_DATA",
        target_type="DATABASE",
        target_name=f"{source_db} → {target_db}",
        server_id=target_server_id,
        user_id=user.id,
        message=log_message
    )
    
    return templates.TemplateResponse("partials/copy_data/result.html", {
        "request": request,
        "results": results,
        "source_server": source_server,
        "source_db": source_db,
        "target_server": target_server,
        "target_db": target_db,
        "success_count": success_count,
        "fail_count": fail_count,
        "total_rows": total_rows,
        "elapsed": round(elapsed, 1),
        "copy_mode": copy_mode
    })  

# ============================================================
# DB 존재 여부 확인 (중복 검사)
# ============================================================
@router.get("/corps/check-db-exists", response_class=JSONResponse)
async def check_db_exists(
    request: Request,
    server_id: int,
    db_name: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """대상 서버에 DB가 이미 존재하는지 확인"""
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return {"exists": False, "error": "서버를 찾을 수 없습니다"}
    
    try:
        from app.services.drivers import get_driver
        driver = get_driver(server)
        databases = driver.get_databases()
        exists = any(d['db_name'].upper() == db_name.strip().upper() for d in databases)
        return {"exists": exists, "db_name": db_name}
    except Exception as e:
        return {"exists": False, "error": str(e)}


# ============================================================
# 법인 DB 생성 SQL Partials
# ============================================================
@router.post("/corps/generate-sql", response_class=JSONResponse)
async def generate_corp_sql(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """법인 DB 생성 SQL 생성 (SqlTemplateService 위임)"""
    data = await request.json()
    
    source_server_id = data.get('source_server_id')
    source_db_name = data.get('source_db_name')
    target_server_id = data.get('target_server_id')
    corp_code = data.get('corp_code', '').upper()
    corp_name = data.get('corp_name', '')
    biz_no = data.get('biz_no', '')
    acc_db_name = data.get('acc_db_name', '')  # 메인 DB에서 가져온 회계DB명
    
    # 복제 설정에서 전달받은 경로 값들
    db_data_path = data.get('db_data_path')
    db_log_path = data.get('db_log_path')
    initial_db_size_mb = data.get('initial_db_size_mb', 100)
    initial_log_size_mb = data.get('initial_log_size_mb', 64)
    file_growth_mb = data.get('file_growth_mb', 100)
    log_growth_mb = data.get('log_growth_mb', 1024)  # Log 증가 단위 별도
    
    server_service = ServerService(db)
    target_server = server_service.get_server(target_server_id)
    
    if not target_server:
        return {"error": "대상 서버를 찾을 수 없습니다"}
    
    # DB명 결정
    db_name = acc_db_name.strip() if acc_db_name else f"ACC_{corp_code}"
    
    # 경로 결정: 요청값 > 서버 설정 > 기본값
    data_path = db_data_path or target_server.data_path or "C:\\Data"
    log_path = db_log_path or target_server.log_path or "C:\\Log"
    
    # SqlTemplateService로 SQL 생성
    params = CreateDBParams(
        db_name=db_name,
        corp_code=corp_code,
        corp_name=corp_name,
        biz_no=biz_no,
        source_db_name=source_db_name or "",
        data_path=data_path,
        log_path=log_path,
        initial_db_size_mb=initial_db_size_mb,
        initial_log_size_mb=initial_log_size_mb,
        file_growth_mb=file_growth_mb,
        log_growth_mb=log_growth_mb,
    )
    
    sql = SqlTemplateService.generate_create_db_sql(params)
    
    return {
        "sql": sql,
        "source_db": source_db_name,
        "target_db": db_name,
        "corp_code": corp_code
    }


# partials.py의 execute_corp_sql 함수만 수정
# 기존 파일에서 이 부분만 교체하세요

@router.post("/corps/execute-sql", response_class=JSONResponse)
async def execute_corp_sql(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_operator)
):
    """법인 DB 생성 SQL 실행"""
    import re
    from datetime import datetime
    
    data = await request.json()
    
    source_server_id = data.get('source_server_id')
    source_db_name = data.get('source_db_name')
    target_server_id = data.get('target_server_id')
    corp_code = data.get('corp_code', '').upper()
    corp_name = data.get('corp_name', '')
    biz_no = data.get('biz_no', '')
    custom_sql = data.get('sql', '')
    
    # SQL 키워드 검증 (위험 명령 차단)
    FORBIDDEN_KEYWORDS = [
        'DROP DATABASE', 'DROP TABLE', 'DROP SCHEMA',
        'TRUNCATE', 'xp_cmdshell', 'sp_configure',
        'SHUTDOWN', 'RECONFIGURE', 'OPENROWSET', 'OPENDATASOURCE'
    ]
    sql_upper = custom_sql.upper()
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql_upper:
            return {"success": False, "error": f"보안: 허용되지 않는 SQL 명령어가 포함되어 있습니다 ({keyword})"}
    
    server_service = ServerService(db)
    corp_service = CorpService(db)
    
    source_server = server_service.get_server(source_server_id)
    target_server = server_service.get_server(target_server_id)
    
    if not source_server or not target_server:
        return {"success": False, "error": "서버를 찾을 수 없습니다"}
    
    start_time = datetime.now()
    
    try:
        # SQL에서 DB명 추출
        db_name_match = re.search(r"CREATE DATABASE \[([^\]]+)\]", custom_sql)
        db_name = db_name_match.group(1) if db_name_match else f"ACC_{corp_code}"
        
        # corp_service의 create_corp_db_with_sql 호출 (target_db_name 전달)
        result = corp_service.create_corp_db_with_sql(
            source_server=source_server,
            source_db_name=source_db_name,
            target_server=target_server,
            corp_code=corp_code,
            corp_name=corp_name,
            biz_no=biz_no,
            custom_sql=custom_sql,
            db_password=None,
            target_db_name=db_name  # ← 추가: SQL에서 추출한 DB명 전달
        )
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # 활동 로그 기록 (실행 SQL 포함)
        from app.services.activity_service import ActivityService
        activity_service = ActivityService(db)
        sql_summary = custom_sql[:500] + ('...' if len(custom_sql) > 500 else '')
        activity_service.log(
            action="CREATE_CORP_DB",
            target_type="DATABASE",
            target_name=db_name,
            server_id=target_server_id,
            user_id=user.id,
            message=f"법인 DB 생성: {corp_name} ({corp_code})",
            details=sql_summary
        )
        
        return {
            "success": True,
            "message": result.get("message", "법인 DB 생성 완료"),
            "db_name": db_name,
            "target_db": db_name,
            "server_name": target_server.server_name,
            "elapsed_seconds": int(elapsed),
            "table_count": result.get("table_count", 0),
            "warnings": result.get("warnings", []),
            "source_server_id": source_server_id,
            "source_db": source_db_name,
            "target_server_id": target_server_id
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        
        error_msg = str(e)
        # 사용자 친화적 에러 매핑
        if "already exists" in error_msg.lower() or "이미 존재" in error_msg:
            user_error = f"DB '{db_name}'이(가) 이미 존재합니다."
        elif "permission" in error_msg.lower() or "denied" in error_msg.lower():
            user_error = "DB 생성 권한이 없습니다. 관리자에게 문의하세요."
        elif "disk" in error_msg.lower() or "space" in error_msg.lower():
            user_error = "디스크 공간이 부족합니다."
        elif "path" in error_msg.lower() or "경로" in error_msg or "directory" in error_msg.lower():
            user_error = "파일 경로가 올바르지 않습니다. 서버 설정을 확인하세요."
        elif "timeout" in error_msg.lower() or "시간 초과" in error_msg:
            user_error = "작업 시간이 초과되었습니다. 다시 시도하세요."
        elif "connection" in error_msg.lower() or "연결" in error_msg:
            user_error = "서버 연결에 실패했습니다. 서버 상태를 확인하세요."
        else:
            user_error = "DB 생성 중 오류가 발생했습니다."
        
        return {
            "success": False,
            "error": user_error,
            "detail": error_msg
        }

# ============================================================
# 생성된 DB 연결 테스트 (step2 성공 후 사용)
# ============================================================

@router.post("/corps/test-created-db", response_class=JSONResponse)
async def test_created_db(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_login)
):
    """생성된 DB 연결 테스트 및 테이블 수 확인"""
    data = await request.json()
    
    server_id = data.get('server_id')
    db_name = data.get('db_name')
    
    if not server_id or not db_name:
        return {"success": False, "error": "서버 또는 DB 정보가 없습니다"}
    
    server_service = ServerService(db)
    server = server_service.get_server(server_id)
    
    if not server:
        return {"success": False, "error": "서버를 찾을 수 없습니다"}
    
    try:
        from app.services.drivers import get_driver
        driver = get_driver(server)
        tables = driver.get_tables(db_name)
        table_count = len(tables)
        
        return {
            "success": True,
            "table_count": table_count,
            "message": f"연결 성공 ({table_count}개 테이블)"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }