# =============================================================================
# Corp DB Manager Dockerfile
# Base: Python 3.12 (Debian Bookworm)
# ODBC Driver 18 for SQL Server
# Version: v1.3.1 (2026-02-08)
# =============================================================================

FROM python:3.12-slim-bookworm

# 환경 변수 설정
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Seoul \
    ACCEPT_EULA=Y

# 작업 디렉토리
WORKDIR /app

# =============================================================================
# 시스템 패키지 + ODBC Driver 18 + 한글 폰트 설치
# =============================================================================
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        apt-transport-https \
        unixodbc \
        unixodbc-dev \
        ca-certificates \
        # 한글 폰트 (엑셀 정의서 등 한글 표시용)
        fonts-nanum \
    # Microsoft 저장소 키 추가
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    # ODBC Driver 18 설치
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    # 정리
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# Python 패키지 설치
# =============================================================================
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# =============================================================================
# 애플리케이션 복사
# =============================================================================
COPY app/ ./app/

# 데이터 디렉토리 생성 (SQLite 메타 DB용)
RUN mkdir -p /app/data

# =============================================================================
# 헬스체크
# =============================================================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/login || exit 1

# 포트 노출
EXPOSE 8000

# 실행 (프로덕션: workers=4)
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "4", \
     "--access-log", \
     "--log-level", "info"]