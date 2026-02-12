#!/bin/bash
# =============================================================================
# Corp DB Manager 실행 스크립트
# Version: v1.3.1 (2026-02-08)
# =============================================================================

set -e

echo "🏢 법인 DB 관리 시스템"
echo "======================"
echo ""

# ── 가상환경 확인/생성 ──
if [ ! -d "venv" ]; then
    echo "📦 가상환경 생성 중..."
    python3 -m venv venv
fi

# 가상환경 활성화
source venv/bin/activate

# ── 패키지 설치 ──
echo "📦 패키지 설치 확인 중..."
pip install -r requirements.txt -q

# ── .env 파일 확인 ──
if [ ! -f ".env" ]; then
    echo "⚙️  환경 설정 파일 생성 중..."
    cp .env.example .env
    echo "   .env.example → .env 복사 완료"
    echo "   ⚠️  .env 파일을 열어 DB 비밀번호, SECRET_KEY, SMTP 설정을 변경하세요!"
    echo ""
fi

# ── data 디렉토리 확인 ──
mkdir -p data

# ── PostgreSQL 연결 확인 ──
if command -v pg_isready &> /dev/null; then
    DB_HOST=$(grep -E "^DB_HOST=" .env | cut -d'=' -f2)
    DB_PORT=$(grep -E "^DB_PORT=" .env | cut -d'=' -f2)
    DB_HOST=${DB_HOST:-localhost}
    DB_PORT=${DB_PORT:-5432}

    if pg_isready -h "$DB_HOST" -p "$DB_PORT" -q 2>/dev/null; then
        echo "✅ PostgreSQL 연결 확인 ($DB_HOST:$DB_PORT)"
    else
        echo "⚠️  PostgreSQL 연결 실패 ($DB_HOST:$DB_PORT)"
        echo "   사용자 인증 기능이 SQLite 폴백으로 동작합니다."
    fi
else
    echo "ℹ️  pg_isready 미설치 - PostgreSQL 연결 확인 건너뜀"
fi

# ── 서버 실행 ──
echo ""
echo "🚀 서버 시작..."
echo "   URL: http://localhost:8000"
echo "   초기 계정: admin / Admin@1234"
echo "   Swagger: http://localhost:8000/docs"
echo ""

python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload