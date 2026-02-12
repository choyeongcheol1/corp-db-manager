"""
SQL 템플릿 관리 서비스
─────────────────────────────────────────────────────────────────
- DB 생성 SQL 템플릿 생성
- DB 옵션 설정 SQL 실행 (configure_db)
- SQL 치환 변수 관리

운영 기준:
  create_db  → 템플릿 문자열 생성 (사용자 미리보기/수정 가능)
  configure_db → 시스템 내부 자동 실행 (사용자 수정 불가)
"""
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field


# ============================================================
# 데이터 클래스
# ============================================================

@dataclass
class CreateDBParams:
    """DB 생성 SQL 파라미터"""
    db_name: str
    corp_code: str
    corp_name: str = ""
    biz_no: str = ""
    source_db_name: str = ""

    # 파일 경로
    data_path: str = "C:\\Data"
    log_path: str = "C:\\Log"

    # 크기 설정
    initial_db_size_mb: int = 100
    initial_log_size_mb: int = 64
    file_growth_mb: int = 100
    log_growth_mb: int = 1024

    # Collation
    collation: str = "Korean_Wansung_CI_AS"


@dataclass
class ConfigureDBResult:
    """DB 옵션 설정 실행 결과"""
    success: bool = False
    db_name: str = ""
    warnings: List[str] = field(default_factory=list)
    verification: Optional[Dict] = None
    error: Optional[str] = None


# ============================================================
# SQL 템플릿 상수
# ============================================================

# DB 옵션 설정 — 운영 표준 (configure_db.sql 기반)
# 섹션별 분리: 개별 실패 시 경고 처리, 전체 중단 방지
_CONFIGURE_SECTIONS: Dict[str, List[str]] = {
    # ── [1] Full-Text 비활성화 ──
    "fulltext": [
        """
        IF CONVERT(int, FULLTEXTSERVICEPROPERTY('IsFullTextInstalled')) = 1
        BEGIN
            DECLARE @ftsql nvarchar(max) = N'USE [{db_name}]; EXEC dbo.sp_fulltext_database @action = ''disable'';';
            EXEC sys.sp_executesql @ftsql;
        END
        """,
    ],

    # ── [2] ANSI / 레거시 호환 ──
    "ansi": [
        "ALTER DATABASE [{db_name}] SET ANSI_NULL_DEFAULT OFF",
        "ALTER DATABASE [{db_name}] SET ANSI_NULLS OFF",
        "ALTER DATABASE [{db_name}] SET ANSI_PADDING OFF",
        "ALTER DATABASE [{db_name}] SET ANSI_WARNINGS OFF",
        "ALTER DATABASE [{db_name}] SET ARITHABORT OFF",
        "ALTER DATABASE [{db_name}] SET CONCAT_NULL_YIELDS_NULL OFF",
        "ALTER DATABASE [{db_name}] SET NUMERIC_ROUNDABORT OFF",
        "ALTER DATABASE [{db_name}] SET QUOTED_IDENTIFIER OFF",
        "ALTER DATABASE [{db_name}] SET RECURSIVE_TRIGGERS OFF",
    ],

    # ── [3] 성능 / 동작 ──
    "performance": [
        "ALTER DATABASE [{db_name}] SET AUTO_CLOSE OFF",
        "ALTER DATABASE [{db_name}] SET AUTO_SHRINK OFF",
        "ALTER DATABASE [{db_name}] SET AUTO_UPDATE_STATISTICS ON",
        "ALTER DATABASE [{db_name}] SET AUTO_UPDATE_STATISTICS_ASYNC OFF",
        "ALTER DATABASE [{db_name}] SET CURSOR_CLOSE_ON_COMMIT OFF",
        "ALTER DATABASE [{db_name}] SET CURSOR_DEFAULT GLOBAL",
        "ALTER DATABASE [{db_name}] SET PARAMETERIZATION SIMPLE",
        "ALTER DATABASE [{db_name}] SET DATE_CORRELATION_OPTIMIZATION OFF",
    ],

    # ── [4] 보안 / 격리 ──
    "security": [
        "ALTER DATABASE [{db_name}] SET TRUSTWORTHY OFF",
        "ALTER DATABASE [{db_name}] SET DB_CHAINING OFF",
        "ALTER DATABASE [{db_name}] SET ALLOW_SNAPSHOT_ISOLATION OFF",
        "ALTER DATABASE [{db_name}] SET READ_COMMITTED_SNAPSHOT OFF",
    ],

    # ── [5] Broker / 복구 모델 ──
    "recovery": [
        "ALTER DATABASE [{db_name}] SET DISABLE_BROKER",
        "ALTER DATABASE [{db_name}] SET HONOR_BROKER_PRIORITY OFF",
        "ALTER DATABASE [{db_name}] SET RECOVERY SIMPLE",
        "ALTER DATABASE [{db_name}] SET MULTI_USER",
    ],

    # ── [6] 스토리지 / 복구 옵션 ──
    "storage": [
        "ALTER DATABASE [{db_name}] SET PAGE_VERIFY CHECKSUM",
        "ALTER DATABASE [{db_name}] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF )",
        "ALTER DATABASE [{db_name}] SET TARGET_RECOVERY_TIME = 60 SECONDS",
        "ALTER DATABASE [{db_name}] SET DELAYED_DURABILITY = DISABLED",
        "ALTER DATABASE [{db_name}] SET ACCELERATED_DATABASE_RECOVERY = OFF",
    ],

    # ── [7] Query Store ──
    "query_store": [
        """ALTER DATABASE [{db_name}] SET QUERY_STORE = ON (
            OPERATION_MODE              = READ_WRITE,
            CLEANUP_POLICY              = (STALE_QUERY_THRESHOLD_DAYS = 30),
            DATA_FLUSH_INTERVAL_SECONDS = 900,
            INTERVAL_LENGTH_MINUTES     = 60,
            MAX_STORAGE_SIZE_MB         = 1000,
            QUERY_CAPTURE_MODE          = AUTO,
            SIZE_BASED_CLEANUP_MODE     = AUTO,
            MAX_PLANS_PER_QUERY         = 200,
            WAIT_STATS_CAPTURE_MODE     = ON
        )""",
    ],

    # ── [8] 최종 상태 ──
    "finalize": [
        "ALTER DATABASE [{db_name}] SET READ_WRITE",
    ],
}

# 설정 결과 검증 쿼리
_VERIFY_SQL = """
    SELECT
        d.name                           AS db_name,
        d.create_date                    AS created_at,
        d.recovery_model_desc            AS recovery_model,
        d.page_verify_option_desc        AS page_verify,
        d.is_query_store_on              AS query_store,
        d.is_broker_enabled              AS broker_enabled,
        d.is_read_committed_snapshot_on  AS rcsi,
        d.snapshot_isolation_state_desc  AS snapshot_isolation,
        d.is_fulltext_enabled            AS fulltext_enabled,
        mf_data.size * 8                 AS data_size_kb,
        mf_log.size * 8                  AS log_size_kb
    FROM sys.databases d
    LEFT JOIN sys.master_files mf_data
        ON mf_data.database_id = d.database_id AND mf_data.type = 0
    LEFT JOIN sys.master_files mf_log
        ON mf_log.database_id = d.database_id AND mf_log.type = 1
    WHERE d.name = ?
"""


# ============================================================
# SQL 템플릿 서비스
# ============================================================

class SqlTemplateService:
    """SQL 템플릿 생성 및 실행"""

    # --------------------------------------------------------
    # CREATE DATABASE SQL 생성 (사용자 미리보기/수정용)
    # --------------------------------------------------------

    @staticmethod
    def generate_create_db_sql(params: CreateDBParams) -> str:
        """
        DB 생성 SQL 생성
        - 사용자에게 보여주는 SQL (수정 가능)
        - DB 생성 DDL만 포함, 옵션 설정은 시스템 자동 적용
        """
        # 경로 정리
        data_path = params.data_path.rstrip('/\\')
        log_path = params.log_path.rstrip('/\\')
        sep = '/' if '/' in data_path else '\\'

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sql = f"""-- ============================================================
-- 법인 DB 생성 스크립트
-- ============================================================
-- 생성일시 : {now}
-- 법인코드 : {params.corp_code}
-- 법인명   : {params.corp_name}
-- 사업자번호: {params.biz_no or '-'}
-- 소스 DB  : {params.source_db_name or '-'}
-- ============================================================
-- ※ DB 옵션 설정(RECOVERY, Query Store 등)은 생성 후 자동 적용됩니다.
-- ============================================================

CREATE DATABASE [{params.db_name}]
ON PRIMARY (
    NAME       = N'{params.db_name}_data',
    FILENAME   = N'{data_path}{sep}{params.db_name}.mdf',
    SIZE       = {params.initial_db_size_mb}MB,
    FILEGROWTH = {params.file_growth_mb}MB
)
LOG ON (
    NAME       = N'{params.db_name}_log',
    FILENAME   = N'{log_path}{sep}{params.db_name}.ldf',
    SIZE       = {params.initial_log_size_mb}MB,
    FILEGROWTH = {params.log_growth_mb}MB
)
COLLATE {params.collation};
GO

-- ============================================================
-- 생성 요약
-- ============================================================
-- DB명       : {params.db_name}
-- Data 파일  : {data_path}{sep}{params.db_name}.mdf
-- Log 파일   : {log_path}{sep}{params.db_name}.ldf
-- 초기 크기  : Data {params.initial_db_size_mb}MB / Log {params.initial_log_size_mb}MB
-- 증가 단위  : Data {params.file_growth_mb}MB / Log {params.log_growth_mb}MB
-- Collation  : {params.collation}
-- ============================================================
"""
        return sql

    @staticmethod
    def extract_db_name_from_sql(sql: str) -> Optional[str]:
        """SQL 문에서 DB명 추출"""
        match = re.search(r"CREATE\s+DATABASE\s+\[([^\]]+)\]", sql, re.IGNORECASE)
        return match.group(1) if match else None

    # --------------------------------------------------------
    # CONFIGURE DATABASE 실행 (시스템 내부 전용)
    # --------------------------------------------------------

    @staticmethod
    def configure_database(conn_func, db_name: str) -> ConfigureDBResult:
        """
        DB 옵션 설정 실행 (운영 표준)

        Parameters:
            conn_func: callable — master DB 커넥션을 반환하는 함수
                       예: lambda: server_service.get_connection(server, "master")
            db_name:   대상 DB명

        Returns:
            ConfigureDBResult
        """
        result = ConfigureDBResult(db_name=db_name)
        warnings = []

        try:
            conn = conn_func()
            conn.autocommit = True
            cursor = conn.cursor()

            # DB 존재 확인
            cursor.execute(
                "SELECT DB_ID(?)", (db_name,)
            )
            row = cursor.fetchone()
            if row[0] is None:
                result.error = f"대상 DB가 존재하지 않습니다: {db_name}"
                conn.close()
                return result

            # 섹션별 실행 — 개별 실패는 경고, 전체 중단 안 함
            for section_name, statements in _CONFIGURE_SECTIONS.items():
                for stmt_template in statements:
                    stmt = stmt_template.format(db_name=db_name)
                    try:
                        cursor.execute(stmt)
                    except Exception as e:
                        warnings.append(f"[{section_name}] {e}")

            # 설정 결과 검증
            try:
                cursor.execute(_VERIFY_SQL, (db_name,))
                row = cursor.fetchone()
                if row:
                    result.verification = {
                        "db_name": row.db_name,
                        "created_at": str(row.created_at) if row.created_at else None,
                        "recovery_model": row.recovery_model,
                        "page_verify": row.page_verify,
                        "query_store": bool(row.query_store),
                        "broker_enabled": bool(row.broker_enabled),
                        "rcsi": bool(row.rcsi),
                        "snapshot_isolation": row.snapshot_isolation,
                        "fulltext_enabled": bool(row.fulltext_enabled),
                        "data_size_kb": row.data_size_kb,
                        "log_size_kb": row.log_size_kb,
                    }
            except Exception as e:
                warnings.append(f"[verify] 검증 쿼리 실패: {e}")

            conn.close()

            result.success = True
            result.warnings = warnings
            return result

        except Exception as e:
            result.error = str(e)
            result.warnings = warnings
            return result

    # --------------------------------------------------------
    # 유틸: SQL 문 파싱 (GO 분리, 주석/USE 제거)
    # --------------------------------------------------------

    @staticmethod
    def parse_sql_statements(raw_sql: str) -> List[str]:
        """
        SQL 텍스트를 실행 가능한 개별 문장으로 분리
        - GO 구분자로 분리
        - 빈 문장, 주석만 있는 문장 제거
        - USE 문 제거 (master 컨텍스트에서 실행하므로)
        """
        blocks = re.split(r'^\s*GO\s*$', raw_sql, flags=re.MULTILINE | re.IGNORECASE)
        statements = []

        for block in blocks:
            # 주석 제거 후 유효한 라인만
            lines = []
            for line in block.split('\n'):
                stripped = line.strip()
                if stripped and not stripped.startswith('--'):
                    lines.append(line)

            if not lines:
                continue

            clean = '\n'.join(lines).strip()

            # USE 문 제거
            if clean.upper().startswith('USE '):
                continue

            if clean:
                statements.append(clean)

        return statements