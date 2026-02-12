# DB 동기화 모듈 통합 가이드

Corp DB Manager에 운영DB → 개발DB BCP 기반 동기화 기능을 추가하는 방법입니다.

## 📁 추가 파일 목록

```
corp-db-manager/
├── app/
│   ├── routers/
│   │   ├── sync.py              ← API 라우터 (NEW)
│   │   └── sync_partials.py     ← HTMX 파셜 라우터 (NEW)
│   ├── services/
│   │   └── sync_service.py      ← BCP 동기화 서비스 (NEW)
│   └── templates/
│       ├── pages/
│       │   └── db_sync.html     ← 동기화 메인 페이지 (NEW)
│       └── partials/
│           └── sync/
│               ├── table_list.html  ← 테이블 목록 파셜 (NEW)
│               └── progress.html    ← 진행 상태 파셜 (NEW)
```

---

## 🔧 통합 단계

### 1. 파일 복사

위 파일들을 기존 프로젝트 해당 경로에 복사합니다.

### 2. 라우터 등록 (app/main.py)

```python
# 기존 라우터 import 부분에 추가
from app.routers import sync, sync_partials

# 기존 router include 부분에 추가
app.include_router(sync.router)
app.include_router(sync_partials.router)
```

### 3. 페이지 라우트 추가 (app/routers/pages.py)

기존 `data_copy_page` 근처에 추가:

```python
@router.get("/db-sync/{server_id}")
async def db_sync_page(request: Request, server_id: int, user=Depends(get_current_user_any)):
    """DB 동기화 페이지"""
    return templates.TemplateResponse(
        "pages/db_sync.html",
        {"request": request, "user": user, "server_id": server_id}
    )
```

> 기존 프로젝트의 `get_current_user_any` 의존성과 동일한 패턴입니다.

### 4. 사이드바 메뉴 추가 (app/templates/components/sidebar_menu.html)

`데이터 복사` 메뉴 아래 (도구 그룹 내)에 추가:

```jinja2
{{ nav_link('/data-copy' + sid, 'document-duplicate', '데이터 복사', '/data-copy') }}
{{ nav_link('/db-sync' + sid, 'arrow-path-rounded-square', 'DB 동기화', '/db-sync') }}  {# ← 추가 #}
{{ nav_link('/tables' + sid_query, 'table-cells', '테이블 현황', '/tables') }}
```

> `arrow-path-rounded-square` 아이콘이 icons.html에 없다면 기존 `arrow-path` 아이콘을 사용하세요.

### 5. 의존성 확인 (requirements.txt)

```
pymssql>=2.2.8    # 이미 있을 가능성 높음
```

### 6. BCP 유틸리티 설치 (서버)

BCP는 SQL Server 도구의 일부입니다. 서버에 설치되어 있어야 합니다.

**Linux (Ubuntu/Debian):**
```bash
# Microsoft 저장소 추가
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev

# PATH 추가
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc

# 확인
bcp -v
```

**Windows:**
- SQL Server Management Studio 설치 시 자동 포함
- 또는 [Microsoft Command Line Utilities](https://docs.microsoft.com/sql/tools/bcp-utility) 별도 설치

---

## 🛠 기존 프로젝트와의 호환성

### 템플릿 구조
- `db_sync.html`은 `base.html`을 extends하며 `{% block content %}`와 `{% block scripts %}`를 사용합니다.
- Alpine.js 함수는 `{% block scripts %}` 안에 위치하여, HTMX SPA 페이지 전환 시 `base.html`의 `htmx:afterSettle` 핸들러가 자동으로 script를 실행하고 Alpine을 재초기화합니다.
- 색상 시스템은 `primary-*` (Tailwind config에서 정의된 blue 계열)을 사용하여 기존 UI와 일관됩니다.

### API 호출 패턴
- 서버 목록: 기존 `/api/servers` 엔드포인트 활용
- DB 목록: 기존 `/api/servers/{id}/databases` 엔드포인트 활용
- 서버 객체에서 `username` 또는 `user` 필드를 자동 감지 (`svr.username || svr.user`)

---

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/api/sync/tables` | 소스 DB 테이블 목록 조회 |
| POST | `/api/sync/execute` | 동기화 실행 (백그라운드) |
| GET | `/api/sync/jobs/{job_id}` | 작업 진행 상태 조회 |
| POST | `/api/sync/jobs/{job_id}/cancel` | 작업 취소 |
| GET | `/api/sync/jobs` | 전체 작업 목록 |
| POST | `/partials/sync/table-list` | 테이블 목록 HTML 파셜 |
| GET | `/partials/sync/progress/{job_id}` | 진행 상태 HTML 파셜 |

---

## 🖥 UI 흐름

```
Step 1: 서버/DB 선택
  ├─ 소스 서버 선택 → DB 목록 로드
  ├─ 타겟 서버 선택 → DB 목록 로드
  └─ [테이블 조회] 버튼

Step 2: 테이블 선택
  ├─ 전체 선택 / 대용량만 / 소용량만
  ├─ 테이블 검색 (필터)
  ├─ 개별 체크박스 선택
  └─ [동기화 실행] 버튼

Step 3: 실행 확인 → 모니터링
  ├─ 경고 메시지 확인
  ├─ [실행] 버튼 → 백그라운드 작업 시작
  ├─ 1.5초 간격 폴링으로 진행 상태 갱신
  ├─ 프로그레스 바 + 통계
  ├─ 테이블별 결과 (원본/대상 건수, 소요시간, 상태)
  └─ 취소 / 새로 시작
```

---

## ⚡ BCP 동기화 처리 순서

```
1. FK 제약조건 비활성화 (타겟 DB 전체)
2. 트리거 비활성화
3. 테이블별 순차 처리:
   a. BCP OUT: 운영DB → .dat 파일 (Native 포맷)
   b. TRUNCATE: 개발DB 대상 테이블 비우기
   c. BCP IN: .dat 파일 → 개발DB (TABLOCK 힌트)
   d. 건수 검증: 원본 vs 대상 COUNT 비교
   e. 임시 파일 삭제
4. FK 제약조건 재활성화
5. 트리거 재활성화
```

---

## 🔒 보안 고려사항

- 서버 접속 정보는 기존 서버 관리 모듈의 암호화 저장 방식을 활용
- BCP 임시 파일은 작업 완료 후 즉시 삭제
- 동기화 실행은 operator 이상 권한 필요 (기존 권한 체계 활용)

---

## 🔄 향후 확장

- [ ] 동기화 스케줄 (cron 기반 정기 실행)
- [ ] 동기화 이력 DB 저장 (현재는 메모리)
- [ ] 병렬 BCP 실행 (대용량 최적화)
- [ ] 특정 조건 데이터만 동기화 (WHERE 절)
- [ ] 활동 로그 연동