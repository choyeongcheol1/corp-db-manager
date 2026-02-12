# 법인 DB 관리 시스템 (Corp DB Manager)

멀티 데이터베이스 환경에서 법인별 DB를 효율적으로 관리하기 위한 웹 기반 관리 시스템입니다.

## 📋 주요 기능

### 서버 관리
- 다중 DB 서버 등록 및 관리 (MSSQL, PostgreSQL, MySQL, Oracle)
- 서버별 연결 상태 모니터링
- 서버 접속 정보 암호화 저장

### DB 관리
- **신규 법인 DB 생성**: 메인 DB에서 법인 정보 조회 → 템플릿 DB 복제
- **설정 자동 로드**: 페이지 진입 시 메인 DB 설정 자동 채우기 + 법인 목록 자동 조회
- **DB 중복 검사**: 생성 실행 전 대상 서버에 동일 DB 존재 여부 자동 확인
- **SQL 키워드 검증**: DROP/TRUNCATE 등 위험 명령어 10종 실행 차단
- **DB 옵션 자동 설정**: 운영 표준 기반 30여 개 옵션 일괄 적용 (SQL 템플릿 서비스)
- **생성 후 연결 테스트**: 생성된 DB 접속 검증 및 테이블 수 확인
- **실패 시 자동 롤백**: DB + 로그인 계정 삭제 + 롤백 로깅
- **사용자 친화적 에러**: pyodbc 원시 에러 → 한국어 안내 메시지 매핑
- **테이블 초기화**: 기준 데이터 복사 (법인코드 자동 치환)
- **DB 동기화**: Linked Server 기반 서버 간 테이블 데이터 동기화
- DB 목록 조회 및 검색

### 데이터 도구
- **테이블 현황 조회**: 서버/DB별 테이블 목록 및 상세 정보
- **테이블 정의서 추출**: 엑셀 형식으로 다운로드
- **데이터 복사**: 서버/DB 간 테이블 데이터 복사
- **DB 동기화**: Linked Server를 통한 대량 데이터 동기화 (INSERT INTO SELECT)
- 모니터링 대시보드

### 시스템 관리
- **설정 관리**: 알림 임계치, 메인 DB 연결 (다중 등록), 복제 설정
- **사용자 관리**: 회원가입 → 이메일 인증 → 관리자 승인 방식
- **활동 로그**: 시스템 작업 이력 조회 및 CSV 내보내기 (실행 SQL 기록 포함)

---

## 🛠 기술 스택

| 구분 | 기술 |
|------|------|
| **Backend** | Python 3.12+, FastAPI, SQLAlchemy |
| **Frontend** | Jinja2, Tailwind CSS, Alpine.js, HTMX |
| **Database** | SQLite + PostgreSQL (시스템), MSSQL/PostgreSQL/MySQL/Oracle (대상) |
| **인증** | JWT Token, 이메일 인증 (Gmail SMTP) |
| **SPA** | HTMX 기반 페이지 전환 (hx-select + outerHTML) |
| **컨테이너** | Docker, Docker Compose |

---

## 📁 프로젝트 구조

```
corp-db-manager/
├── app/
│   ├── main.py                 # FastAPI 앱 진입점
│   ├── core/
│   │   ├── database.py         # DB 연결 및 모델 정의 (SQLite + PostgreSQL)
│   │   ├── security.py         # JWT, 비밀번호 해싱
│   │   └── email.py            # 이메일 발송 서비스
│   ├── models/
│   │   ├── schemas.py          # Pydantic 스키마
│   │   └── user.py             # PostgreSQL 사용자 모델
│   ├── routers/
│   │   ├── auth.py             # 인증 API
│   │   ├── pages.py            # 페이지 라우터
│   │   ├── partials.py         # HTMX 파셜 API (DB 생성/중복검사/테스트)
│   │   ├── servers.py          # 서버 관리 API
│   │   ├── corps.py            # 법인 DB 관리 API
│   │   ├── table_init.py       # 테이블 초기화 API
│   │   ├── db_sync.py          # DB 동기화 API (Linked Server)
│   │   ├── sync.py             # 데이터 동기화 API (pymssql)
│   │   ├── settings.py         # 설정 API
│   │   └── activity_logs.py    # 활동 로그 API
│   ├── services/
│   │   ├── server_service.py   # 서버 관리 서비스
│   │   ├── corp_service.py     # 법인 DB 서비스 (생성/복제/롤백)
│   │   ├── table_init_service.py  # 테이블 초기화 서비스
│   │   ├── db_sync_service.py  # DB 동기화 서비스 (Linked Server)
│   │   ├── sync_service.py     # 데이터 동기화 서비스 (pymssql)
│   │   ├── sql_templates.py    # SQL 템플릿 서비스 (DB 옵션 설정)
│   │   ├── activity_service.py # 활동 로그 서비스
│   │   └── drivers/            # DB 드라이버
│   │       ├── base.py
│   │       ├── mssql.py
│   │       ├── postgresql.py
│   │       ├── mysql.py
│   │       └── oracle.py
│   ├── templates/
│   │   ├── base.html           # 기본 레이아웃 (HTMX SPA 핸들러 포함)
│   │   ├── components/         # 공통 컴포넌트
│   │   │   ├── icons.html      # SVG 아이콘 매크로
│   │   │   ├── sidebar_menu.html  # HTMX 네비게이션 사이드바
│   │   │   └── modals.html     # 공통 모달 (삭제 확인, 토스트)
│   │   ├── pages/              # 페이지 템플릿
│   │   │   ├── login.html
│   │   │   ├── register.html
│   │   │   ├── dashboard.html
│   │   │   ├── servers.html        # 서버 관리
│   │   │   ├── corps.html          # DB 목록
│   │   │   ├── db_create.html      # DB 생성 (설정 자동 로드)
│   │   │   ├── table_init.html     # 테이블 초기화
│   │   │   ├── db_sync.html        # DB 동기화
│   │   │   ├── tables.html         # 테이블 현황
│   │   │   ├── schema_export.html  # 테이블 정의서
│   │   │   ├── copy_data.html      # 데이터 복사
│   │   │   ├── monitoring.html     # 모니터링
│   │   │   ├── settings.html       # 설정
│   │   │   ├── user_management.html  # 사용자 관리
│   │   │   └── activity_logs.html  # 활동 로그
│   │   ├── partials/           # HTMX 파셜
│   │   │   ├── db_create/
│   │   │   │   ├── step1_corp_select.html  # 1단계: 법인 선택
│   │   │   │   └── step2_sql_preview.html  # 2단계: SQL 확인 및 실행
│   │   │   └── corps/
│   │   └── emails/             # 이메일 템플릿
│   └── static/                 # 정적 파일
│       └── docs/               # 제품 설명서
├── data/
│   └── corp_db.sqlite          # SQLite 데이터베이스
├── Dockerfile                  # Docker 이미지 빌드
├── docker-compose.yml          # Docker Compose 설정
├── .dockerignore               # Docker 빌드 제외 파일
├── .env.example                # 환경 변수 템플릿
├── .env                        # 환경 변수 (Git 제외)
├── requirements.txt
├── start.sh                    # Linux/Mac 실행 스크립트
└── README.md
```

---

## ⚙️ 설치 및 실행

### 방법 1: 로컬 실행 (개발 모드)

#### 1. 환경 설정

```bash
# 저장소 클론
git clone <repository-url>
cd corp-db-manager

# 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt
```

#### 2. 환경 변수 설정

```bash
cp .env.example .env
# .env 파일을 열어 DB 비밀번호, SECRET_KEY, SMTP 설정 변경
```

#### 3. 서버 실행

```bash
# 개발 모드 (자동 리로드)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 또는 Linux/Mac에서 start.sh 사용
chmod +x start.sh
./start.sh
```

#### 4. 접속

- URL: http://localhost:8000
- 기본 계정: `admin` / `Admin@1234`
- API 문서: http://localhost:8000/docs

---

### 방법 2: Docker 실행

#### 사전 요구사항

- Docker Desktop 설치
- 호스트 PC에 PostgreSQL 실행 중 (사용자 인증용)

#### 1. 환경 변수 설정

```bash
cp .env.example .env
# .env 파일을 열어 설정 변경
# DB_HOST는 localhost로 유지 (docker-compose.yml에서 자동 오버라이드)
```

#### 2. 빌드 및 실행

```bash
docker compose up -d --build
```

#### 3. 상태 확인

```bash
# 컨테이너 상태
docker ps --filter name=corp-db-manager

# 헬스체크 확인
docker inspect --format="{{.State.Health.Status}}" corp-db-manager

# 실시간 로그
docker compose logs -f app
```

#### 4. 접속

- URL: http://localhost:8000
- 기본 계정: `admin` / `Admin@1234`

#### Docker 주요 명령어

```bash
docker compose up -d --build     # 빌드 및 시작
docker compose logs -f app       # 실시간 로그
docker compose restart app       # 앱 재시작
docker compose down              # 중지 및 제거
docker compose up -d             # 설정 변경 후 재시작
```

#### Docker 네트워크 참고사항

- `.env`의 `DB_HOST=localhost`는 로컬 실행용입니다
- `docker-compose.yml`의 `environment`에서 `DB_HOST=host.docker.internal`로 오버라이드하여 컨테이너에서 호스트 PC의 PostgreSQL에 접근합니다
- 대상 MSSQL 서버는 `localhost`가 아닌 실제 IP 주소로 등록해야 합니다

#### Docker 구성 파일

| 파일 | 설명 |
|------|------|
| `Dockerfile` | Python 3.12 + ODBC Driver 18 + 한글폰트 + 헬스체크 |
| `docker-compose.yml` | 앱 서비스 정의 (포트/볼륨/헬스체크/로그 로테이션) |
| `.dockerignore` | 빌드 컨텍스트 최적화 (venv, data, .env 등 제외) |
| `.env.example` | 환경 변수 템플릿 (자격 증명 미포함) |

---

## 📖 사용 가이드

### 서버 등록

1. **서버 관리** 메뉴 접속
2. **서버 추가** 버튼 클릭
3. 서버 정보 입력 (이름, 호스트, 포트, DB 유형, 계정 정보)
4. **연결 테스트** 후 저장

### 신규 법인 DB 생성

1. **DB 생성** 메뉴 접속
2. 설정에 메인 DB 정보가 저장되어 있으면 자동 로드 + 법인 목록 자동 조회
   - 미등록 시: 직접 입력 후 "법인 목록 조회" 클릭
3. 생성할 법인 선택 (DB 미생성 법인만 선택 가능)
4. 소스 서버/템플릿 DB, 대상 서버 선택
5. **SQL 미리보기** → 필요시 수정 → **실행**
   - 실행 전 DB 중복 검사 자동 실행
   - 위험 SQL 키워드 (DROP/TRUNCATE 등) 자동 차단
6. 생성 완료 후 결과 확인 (DB명, 서버, 테이블 수, 소요시간)
7. **연결 테스트**로 접속 확인
8. **테이블 초기화**로 기준 데이터 복사

### 테이블 초기화

1. **테이블 초기화** 메뉴 접속
2. 소스 서버/DB 선택 (원본), 타겟 서버/DB 선택 (대상)
3. 법인코드 컬럼이 있는 테이블은 자동으로 법인코드 치환
4. 개별 테이블 **INSERT** 또는 **DELETE** 실행

### DB 동기화 (Linked Server)

1. **DB 동기화** 메뉴 접속
2. **타겟 서버** 선택 (INSERT가 실행될 서버)
3. **Linked Server** 선택 (SSMS에서 미리 설정 필요)
   - 연결 테스트 버튼으로 확인 가능
4. **소스 DB** 선택 (Linked Server를 통해 원격 DB 목록 자동 조회)
5. **타겟 DB** 선택 (로컬 DB 목록)
6. 동기화 옵션 설정 (TRUNCATE 후 INSERT, Identity 값 유지)
7. 테이블 목록에서 개별 **동기화** 버튼 클릭

> **사전 준비**: SSMS에서 타겟 서버에 Linked Server를 미리 설정해야 합니다.
> Server Objects → Linked Servers → 우클릭 → New Linked Server

### 테이블 정의서 추출

1. **테이블 정의서** 메뉴 접속
2. 서버 및 DB 선택
3. 테이블 선택 (전체 선택 가능)
4. **엑셀 다운로드** 버튼 클릭

### 설정

| 탭 | 설명 |
|----|------|
| **알림 설정** | 용량 경고/위험 임계치 설정 |
| **메인 DB 설정** | 법인 정보 조회용 ERP DB 연결 (서버별 다중 등록, 컬럼 매핑, 연결 테스트) |
| **복제 설정** | DB 생성 시 적용되는 기본 경로, 크기, 계정 설정 |

---

## 🏗 아키텍처

### HTMX SPA 네비게이션

사이드바 메뉴 클릭 시 전체 페이지를 새로고침하지 않고, HTMX를 통해 `#page-content` 영역만 교체하는 SPA 방식으로 동작합니다.

```
[사이드바 메뉴 클릭]
  → hx-get: 페이지 AJAX 요청
  → hx-select="#page-content": 응답에서 콘텐츠 영역만 추출
  → hx-swap="outerHTML": 현재 콘텐츠 교체
  → hx-push-url="true": 브라우저 URL 변경 (뒤로가기 지원)
```

페이지 전환 시 JavaScript/Alpine.js 재초기화 처리 (`base.html`):
1. `htmx:beforeSwap` — 응답 HTML에서 `<script>` 태그를 미리 추출
2. `htmx:afterSettle` — DOM 교체 완료 후 script 실행 + Alpine.js `initTree()` 호출
3. 사이드바 active 메뉴 자동 업데이트

### DB 동기화 아키텍처 (Linked Server)

파일 I/O 없이 서버 간 직접 데이터 전송하는 최고 성능 방식입니다.

```
[타겟 서버에서 실행]

① Linked Server 확인
   → SSMS에서 미리 설정된 Linked Server를 드롭다운으로 선택
   → sp_testlinkedserver로 연결 테스트

② 소스 DB/테이블 조회
   → Linked Server 경유로 원격 DB 목록 조회
   → sys.tables + sys.partitions 조인으로 건수/Identity 정보 조회

③ TRUNCATE (옵션)
   → 타겟 테이블 데이터 삭제

④ INSERT INTO SELECT (핵심)
   → INSERT INTO [타겟DB].dbo.[테이블] (컬럼...)
      SELECT 컬럼... FROM [LinkedServer].[소스DB].dbo.[테이블]
   → 파일 I/O 없이 서버 간 직접 복사
   → 수백만 건도 단일 쿼리로 처리

⑤ IDENTITY_INSERT ON/OFF (옵션)
   → Identity 컬럼 값 유지 시 사용
```

**테이블 초기화 vs DB 동기화 비교:**

| 항목 | 테이블 초기화 | DB 동기화 |
|------|-------------|----------|
| **방식** | 앱에서 SELECT → INSERT (pyodbc) | Linked Server INSERT INTO SELECT |
| **법인코드 치환** | ✅ 지원 | ❌ 미지원 (원본 그대로) |
| **대상** | 소규모 기준 데이터 | 대량 데이터 (수백만 건) |
| **성능** | 건별 INSERT | 단일 쿼리 (최고 성능) |
| **사전 설정** | 없음 | SSMS에서 Linked Server 설정 필요 |

### SQL 템플릿 서비스 (`sql_templates.py`)

DB 생성 시 적용되는 운영 표준 옵션을 중앙 관리합니다. Single Source of Truth 원칙으로 SQL 로직은 이 파일에서만 관리합니다.

| 섹션 | 설정 내용 |
|------|-----------|
| **fulltext** | Full-Text 비활성화 |
| **ansi** | ANSI_NULL_DEFAULT, ANSI_NULLS 등 9개 옵션 OFF |
| **performance** | AUTO_CLOSE OFF, AUTO_SHRINK OFF 등 8개 |
| **security** | TRUSTWORTHY OFF, SNAPSHOT_ISOLATION OFF 등 4개 |
| **recovery** | RECOVERY SIMPLE, MULTI_USER, DISABLE_BROKER |
| **storage** | PAGE_VERIFY CHECKSUM, FILESTREAM OFF, TARGET_RECOVERY_TIME 60 |
| **query_store** | QUERY_STORE ON (READ_WRITE), 7개 세부 옵션 |
| **finalize** | READ_WRITE 최종 설정 |

개별 섹션 실패는 경고로 처리하고 전체 프로세스를 중단하지 않습니다. 설정 후 `sys.databases` + `sys.master_files` 검증 쿼리를 실행하여 결과를 반환합니다.

### DB 생성 흐름

```
[법인 선택] → [SQL 생성/미리보기] → [DB 중복 검사] → [SQL 키워드 검증] → [실행]
                                                                            ├── CREATE DATABASE
                                                                            ├── 옵션 설정 (sql_templates)
                                                                            ├── 계정 생성 (로그인 + 사용자)
                                                                            ├── 스키마 복제
                                                                            ├── 확장 속성 복제 (테이블/컬럼 설명)
                                                                            ├── 인덱스/PK 복제
                                                                            ├── 기초 데이터 복제
                                                                            └── 관리자 계정 생성
                                                                        → [결과 (테이블 수/소요시간)]
                                                                        → [연결 테스트] → [테이블 초기화]

실패 시 자동 롤백: DB 삭제 + 로그인 삭제 + 롤백 로깅
```

### 인증 아키텍처

PostgreSQL 우선, SQLite 폴백 방식의 듀얼 DB 인증을 지원합니다.

```
[요청] → get_current_user_any()
           ├── PostgreSQL 사용자 확인 (우선)
           └── SQLite 사용자 확인 (폴백)
```

---

## 🔐 권한 체계

| 역할 | 권한 |
|------|------|
| **admin** | 전체 기능 + 사용자 관리 + 설정 |
| **operator** | DB 생성/수정, 테이블 초기화, DB 동기화, 데이터 복사 |
| **viewer** | 조회만 가능 |

---

## 📝 API 문서

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### 주요 페이지 라우트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/servers` | 서버 관리 |
| GET | `/dashboard/{server_id}` | 대시보드 |
| GET | `/db/create/{server_id}` | DB 생성 |
| GET | `/db/table-init/{server_id}` | 테이블 초기화 |
| GET | `/db-sync/{server_id}` | DB 동기화 |
| GET | `/db/list/{server_id}` | DB 목록 |
| GET | `/monitoring/{server_id}` | 모니터링 |
| GET | `/data-copy/{server_id}` | 데이터 복사 |
| GET | `/tables` | 테이블 현황 |
| GET | `/schema-export` | 테이블 정의서 |
| GET | `/settings` | 설정 |
| GET | `/activity-logs` | 활동 로그 |
| GET | `/user-management` | 사용자 관리 (admin) |

### 주요 API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/api/servers` | 서버 목록 조회 |
| POST | `/api/servers` | 서버 등록 |
| POST | `/api/servers/{id}/test` | 서버 연결 테스트 |
| GET | `/api/corps` | 법인 목록 조회 |
| POST | `/api/corps/fetch-from-main-db` | 메인 DB에서 법인 정보 조회 |
| GET | `/api/settings/main-db` | 메인 DB 설정 조회 (자동 로드용) |
| GET | `/partials/corps/check-db-exists` | DB 존재 여부 확인 (중복 검사) |
| POST | `/partials/corps/generate-sql` | DB 생성 SQL 생성 |
| POST | `/partials/corps/execute-sql` | DB 생성 SQL 실행 (키워드 검증 포함) |
| POST | `/partials/corps/test-created-db` | 생성된 DB 연결 테스트 (드라이버 패턴) |
| GET | `/api/db-sync/linked-servers/{server_id}` | Linked Server 목록 조회 |
| GET | `/api/db-sync/linked-server-test/{server_id}/{name}` | Linked Server 연결 테스트 |
| GET | `/api/db-sync/source-databases/{server_id}/{name}` | 소스 DB 목록 조회 (Linked Server 경유) |
| GET | `/api/db-sync/source-tables/{server_id}/{name}/{db}` | 소스 테이블 목록 조회 |
| GET | `/api/db-sync/target-tables/{server_id}/{db}` | 타겟 테이블 목록 조회 |
| POST | `/api/db-sync/execute` | 테이블 동기화 실행 |
| GET | `/api/settings/all` | 전체 설정 조회 |
| PUT | `/api/settings/alert` | 알림 설정 저장 |
| PUT | `/api/settings/replication` | 복제 설정 저장 |
| POST | `/api/settings/main-db/entries` | 메인 DB 등록 추가 |
| DELETE | `/api/settings/main-db/entries/{id}` | 메인 DB 등록 삭제 |
| POST | `/api/settings/main-db/test` | 메인 DB 연결 테스트 |
| PUT | `/api/settings/main-db/columns` | 컬럼 매핑 저장 |
| POST | `/api/settings/replication/test-db-account` | DB 계정 연결 테스트 |
| GET | `/api/logs` | 활동 로그 조회 |

---

## 🔧 개발 가이드

### 아이콘 사용

```jinja2
{% from "components/icons.html" import svg_icon %}

{{ svg_icon('server', 'w-5 h-5') }}
{{ svg_icon('plus-circle', 'w-6 h-6 text-primary-600') }}
```

### 모달 사용

```jinja2
{% from "components/modals.html" import confirm_delete_modal, toast_message %}

{{ confirm_delete_modal() }}
{{ toast_message() }}
```

### HTMX 네비게이션 매크로

사이드바 메뉴 링크는 `sidebar_menu.html`의 `nav_link` 매크로를 사용합니다:

```jinja2
{{ nav_link('/settings', 'cog', '설정', '/settings') }}
{{ nav_link('/servers', 'server', '서버 관리', '/servers,/server-management') }}
{{ nav_link('/db-sync' + sid, 'arrows-right-left', 'DB 동기화', '/db-sync') }}
```

### 페이지 템플릿 작성 규칙

Alpine.js `x-data` + `x-init`을 사용하는 페이지는 `<script>` 태그를 `{% block content %}` 내에 배치합니다. base.html의 HTMX 핸들러가 페이지 전환 시 자동으로 script를 추출하여 실행하고 Alpine을 재초기화합니다.

```jinja2
{% extends "base.html" %}

{% block content %}
<div x-data="myPage()" x-init="init()">
    <!-- 페이지 HTML -->
</div>

<script>
function myPage() {
    return {
        loading: true,
        async init() {
            // API 호출 등 초기화
            this.loading = false;
        }
    };
}
</script>
{% endblock %}
```

### Jinja/JS 혼재 방지 패턴

JS 템플릿 리터럴 내에서 Jinja 매크로를 직접 사용하지 않고 `<template>` 프리렌더 방식을 사용합니다:

```html
<!-- 아이콘 프리렌더 -->
<template id="icon-rocket">{{ svg_icon('rocket-launch', 'w-5 h-5') }}</template>

<script>
// JS에서 참조
function getIcon(name) {
    const tpl = document.getElementById('icon-' + name);
    return tpl ? tpl.innerHTML : '';
}
btn.innerHTML = getIcon('rocket') + ' 실행';
</script>
```

### DB 드라이버 추가

`app/services/drivers/` 에 새 드라이버 파일 생성 후 `BaseDriver` 상속:

```python
from .base import BaseDriver

class NewDBDriver(BaseDriver):
    def get_connection(self, db_name: str):
        # 구현
        pass
    
    def get_databases(self):
        # 구현
        pass
```

### SQL 템플릿 수정

`app/services/sql_templates.py`의 `_CONFIGURE_SECTIONS` dict에서 섹션별 SQL을 수정합니다:

```python
# 예: Query Store 최대 저장 크기 변경
"query_store": [
    """
    ALTER DATABASE [{db_name}] SET QUERY_STORE = ON (
        OPERATION_MODE = READ_WRITE,
        MAX_STORAGE_SIZE_MB = 2000,  -- 기존 1000에서 변경
        ...
    )
    """,
],
```

---

## 🐳 Docker 운영

### 업데이트 배포

```bash
# 소스 업데이트
git pull origin main

# 이미지 재빌드 및 컨테이너 교체
docker compose up -d --build

# 사용하지 않는 이전 이미지 정리
docker image prune -f
```

### 백업

```bash
# SQLite 메타 DB 백업
cp data/corp_db.sqlite data/corp_db_$(date +%Y%m%d_%H%M%S).sqlite
```

### 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| PostgreSQL 연결 실패 | 컨테이너에서 `localhost` 접근 불가 | `docker-compose.yml`에 `DB_HOST=host.docker.internal` 설정 |
| MSSQL 서버 연결 실패 | Docker 네트워크에서 호스트명 해석 불가 | 서버 등록 시 `localhost` 대신 실제 IP 사용 |
| `ModuleNotFoundError` | `requirements.txt` 패키지 누락 | `pip freeze > requirements.txt` 후 재빌드 |
| 헬스체크 `unhealthy` | 앱 기동 실패 | `docker compose logs app`으로 에러 확인 |

---

## 📌 버전 히스토리

### v1.3.1 (2026-02-08)
- **DB 생성 보안/UX 개선** (14개 항목)
  - 설정 자동 로드: 페이지 진입 시 메인 DB 설정 자동 채우기 + 법인 목록 자동 조회
  - 타겟 서버 기본 선택: 현재 서버가 기본 선택됨
  - 더블 클릭 방지: 모달 실행 버튼 포함 전체 비활성화 + 에러 시 원복
  - SQL 키워드 검증: DROP/TRUNCATE/xp_cmdshell 등 10개 위험 명령어 실행 차단
  - `request.json()`: 수동 파싱 3곳 → FastAPI 기본 기능으로 교체
  - 에러 메시지 친화화: pyodbc 원시 에러 → 한국어 안내 메시지 매핑 + detail 분리
  - DB 중복 검사: 실행 전 대상 서버에 동일 DB 존재 여부 자동 확인 API 추가
  - 성공 화면 테이블 수 표시: 3컬럼 → 4컬럼 (DB명/서버/테이블/소요시간)
  - `test_created_db` 드라이버 패턴: MSSQL `sys.tables` 직접 쿼리 → `driver.get_tables()` 추상화
  - 롤백 보강: 실패 시 로그인도 함께 삭제 (`DROP LOGIN`) + 롤백 콘솔 로깅
  - 활동 로그 SQL 기록: 실행 SQL 500자까지 details 필드에 기록
  - Jinja/JS 혼재 정리: `<template>` 프리렌더 + `getIcon()` 헬퍼로 분리
- **Docker 배포 구성**
  - Dockerfile: Python 3.12 + ODBC Driver 18 + 한글폰트 + 헬스체크
  - docker-compose.yml: DB_HOST 오버라이드 + 로그 로테이션
  - .dockerignore, .env.example, start.sh 추가

### v1.3.0 (2026-02-07)
- **DB 동기화 기능 추가** (Linked Server 방식)
  - SSMS에서 설정한 Linked Server를 선택하여 서버 간 테이블 데이터 동기화
  - 타겟 서버에서 `INSERT INTO SELECT` 실행 (파일 I/O 없이 최고 성능)
  - Linked Server 목록 조회, 연결 테스트, 소스 DB/테이블 원격 조회
  - TRUNCATE 후 INSERT, Identity 값 유지 옵션 지원
  - `sys.tables` + `sys.partitions` 직접 조인으로 원격 테이블 건수/Identity 정확 조회
  - 동기화 결과 모달 (행수, 소요 시간, 에러 메시지)
  - 활동 로그 자동 기록
- 사이드바 메뉴에 DB 동기화 추가 (아이콘: `arrows-right-left`)
- main.py에 db_sync 라우터 등록

### v1.2.0 (2026-02-06)
- HTMX SPA 네비게이션 안정화
  - `htmx:afterSwap` → `htmx:afterSettle`로 변경 (outerHTML 교체 완료 보장)
  - 교체된 `#page-content`를 `document.getElementById()`로 직접 조회
  - script 태그 `replaceChild` 방식으로 브라우저 실행 보장
  - Alpine.js initTree를 `Promise.resolve().then()`으로 지연 실행
  - Alpine `x-data` + `x-init` 기반 페이지(설정 등)의 HTMX 전환 시 초기화 실패 수정

### v1.1.0 (2026-02-04)
- SQL 템플릿 서비스 분리 (`sql_templates.py`)
  - DB 옵션 설정 로직을 Single Source of Truth로 중앙 관리
  - 섹션별 분리 구조 (fulltext, ansi, performance, security, recovery, storage, query_store, finalize)
  - 설정 후 검증 쿼리 실행 및 결과 반환
  - `configure_db.sql` 운영 표준과 동기화 (Full-Text, FILESTREAM, Query Store 추가 옵션)
- `corp_service.py` 리팩토링
  - `_configure_db_options`: 30줄 → SqlTemplateService 위임 (3줄)
  - SQL 파싱/DB명 추출을 SqlTemplateService 유틸리티로 통합
- `partials.py` 리팩토링
  - `generate_corp_sql`: 인라인 SQL 생성 → SqlTemplateService.generate_create_db_sql() 호출
  - `test-created-db` 엔드포인트 추가 (생성 DB 연결 테스트)
- DB 생성 완료 UI 개선
  - 연결 테스트 기능 추가 (접속 확인 + 테이블 수 표시)
  - 버튼 우선순위 정리 (연결 테스트 → 테이블 초기화)
  - 보조 링크 축소 (DB 목록, 다른 법인 생성)

### v1.0.0 (2026-02-02)
- 초기 릴리즈
- 멀티 DB 지원 (MSSQL, PostgreSQL, MySQL, Oracle)
- 신규 법인 DB 생성 (메인 DB 연동)
- 테이블 정의서 엑셀 추출
- 이메일 인증 기반 회원가입
- 활동 로그 기능

---

## 📄 라이선스

MIT License

---

## 👥 기여

이슈 및 PR 환영합니다.