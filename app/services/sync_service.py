"""
DB 동기화 서비스 (BCP 기반)
운영DB → 개발DB 단방향 테이블 동기화
"""
import subprocess
import shutil
import os
import tempfile
import logging
import asyncio
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
import pymssql

logger = logging.getLogger(__name__)


@dataclass
class SyncTableResult:
    """테이블 동기화 결과"""
    schema_name: str
    table_name: str
    source_count: int = 0
    target_count: int = 0
    status: str = "PENDING"  # PENDING, RUNNING, SUCCESS, FAIL, MISMATCH, SKIPPED
    error_msg: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    elapsed_seconds: float = 0


@dataclass
class SyncJobProgress:
    """동기화 작업 진행 상태"""
    job_id: str
    source_server: str
    source_db: str
    target_server: str
    target_db: str
    total_tables: int = 0
    completed_tables: int = 0
    current_table: str = ""
    status: str = "PENDING"  # PENDING, RUNNING, COMPLETED, FAILED, CANCELLED
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    results: list = field(default_factory=list)
    error_msg: Optional[str] = None

    @property
    def progress_percent(self) -> int:
        if self.total_tables == 0:
            return 0
        return int((self.completed_tables / self.total_tables) * 100)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.status == "SUCCESS")

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.results if r.status in ("FAIL", "MISMATCH"))


class SyncService:
    """BCP 기반 DB 동기화 서비스"""

    def __init__(self):
        self._jobs: dict[str, SyncJobProgress] = {}
        self._cancel_flags: dict[str, bool] = {}
        self._data_dir = tempfile.mkdtemp(prefix="db_sync_")
        self._bcp_path = self._find_bcp()

    def _find_bcp(self) -> str:
        """BCP 실행 파일 경로 탐색"""
        # 1) PATH에서 찾기
        bcp_path = shutil.which("bcp")
        if bcp_path:
            logger.info(f"BCP 발견: {bcp_path}")
            return bcp_path

        # 2) 일반적인 설치 경로 확인
        common_paths = [
            "/opt/mssql-tools/bin/bcp",
            "/opt/mssql-tools18/bin/bcp",
            r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
            r"C:\Program Files\Microsoft SQL Server\150\Tools\Binn\bcp.exe",
            r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\130\Tools\Binn\bcp.exe",
        ]
        for p in common_paths:
            if os.path.isfile(p):
                logger.info(f"BCP 발견: {p}")
                return p

        logger.warning("BCP 유틸리티를 찾을 수 없습니다. pymssql 대체 방식을 사용합니다.")
        return ""

    def get_job(self, job_id: str) -> Optional[SyncJobProgress]:
        return self._jobs.get(job_id)

    def get_all_jobs(self) -> list[SyncJobProgress]:
        return list(self._jobs.values())

    def cancel_job(self, job_id: str) -> bool:
        if job_id in self._cancel_flags:
            self._cancel_flags[job_id] = True
            return True
        return False

    def _connect(self, conn_info: dict, db_name: str, timeout: int = 30) -> 'pymssql.Connection':
        """pymssql 연결 헬퍼 - 로그 포함"""
        server = conn_info["server"]
        port = int(conn_info.get("port", 1433))
        user = conn_info["user"]
        logger.info(f"DB 연결 시도: {server}:{port}, db={db_name}, user={user}")
        try:
            conn = pymssql.connect(
                server=server,
                user=user,
                password=conn_info["password"],
                database=db_name,
                port=port,
                charset="utf8",
                login_timeout=10,
                timeout=timeout,
            )
            logger.info(f"DB 연결 성공: {server}:{port}/{db_name}")
            return conn
        except Exception as e:
            logger.error(f"DB 연결 실패: {server}:{port}/{db_name} - {e}")
            raise

    def get_tables(self, conn_info: dict, db_name: str) -> list[dict]:
        """대상 DB의 유저 테이블 목록 조회 (건수 포함)"""
        conn = self._connect(conn_info, db_name)
        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                ISNULL(SUM(p.rows), 0) AS row_count,
                CAST(ROUND(SUM(a.total_pages) * 8.0 / 1024, 2) AS DECIMAL(18,2)) AS size_mb,
                CASE WHEN EXISTS(
                    SELECT 1 FROM sys.identity_columns ic WHERE ic.object_id = t.object_id
                ) THEN 1 ELSE 0 END AS has_identity,
                CASE WHEN EXISTS(
                    SELECT 1 FROM sys.foreign_keys fk WHERE fk.referenced_object_id = t.object_id
                ) THEN 1 ELSE 0 END AS has_fk_ref
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
            LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE t.type = 'U' AND t.is_ms_shipped = 0
            GROUP BY s.name, t.name, t.object_id
            ORDER BY SUM(p.rows) DESC
        """)
        tables = cursor.fetchall()
        conn.close()
        return tables

    def get_table_count(self, conn_info: dict, db_name: str,
                        schema_name: str, table_name: str) -> int:
        """테이블 건수 조회"""
        conn = self._connect(conn_info, db_name, timeout=60)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def _build_bcp_cmd(self, conn_info: dict, db_name: str, schema: str,
                       table: str, direction: str, file_path: str,
                       batch_size: int = 50000) -> list[str]:
        """BCP 명령어 생성"""
        bcp = self._bcp_path or "bcp"
        cmd = [
            bcp,
            f"[{db_name}].[{schema}].[{table}]",
            direction,
            file_path,
            "-S", f"{conn_info['server']},{conn_info.get('port', 1433)}",
            "-U", conn_info["user"],
            "-P", conn_info["password"],
            "-n",                    # Native 포맷 (가장 빠름)
            "-b", str(batch_size),   # 배치 사이즈
        ]
        if direction == "in":
            cmd.extend(["-h", "TABLOCK"])  # 테이블 락 → 속도 향상
        return cmd

    def _run_bcp(self, cmd: list[str], direction_label: str) -> str:
        """BCP 명령 실행 및 에러 처리"""
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=3600
            )
            # BCP는 stderr에도 정보를 출력하므로 stdout+stderr 모두 확인
            output = (proc.stdout or "") + "\n" + (proc.stderr or "")

            if proc.returncode != 0:
                # 에러 메시지에서 유용한 부분 추출
                error_detail = output.strip()
                if not error_detail:
                    error_detail = f"종료 코드: {proc.returncode}"
                raise Exception(f"{direction_label}: {error_detail}")

            return output

        except FileNotFoundError:
            raise Exception(
                f"BCP 유틸리티를 찾을 수 없습니다. "
                f"mssql-tools 설치가 필요합니다. "
                f"(Ubuntu: sudo apt install mssql-tools / "
                f"Windows: SQL Server Feature Pack에서 설치)"
            )
        except subprocess.TimeoutExpired:
            raise Exception(f"{direction_label}: 제한시간(1시간) 초과")

    def _disable_constraints(self, conn_info: dict, db_name: str,
                             tables: list[dict]) -> None:
        """FK 제약조건 및 트리거 비활성화"""
        conn = self._connect(conn_info, db_name)
        cursor = conn.cursor()
        for tbl in tables:
            schema = tbl["schema_name"]
            table = tbl["table_name"]
            try:
                cursor.execute(f"ALTER TABLE [{schema}].[{table}] NOCHECK CONSTRAINT ALL")
                cursor.execute(f"DISABLE TRIGGER ALL ON [{schema}].[{table}]")
            except Exception as e:
                logger.warning(f"제약조건 비활성화 실패: {schema}.{table} - {e}")
        conn.commit()
        conn.close()

    def _enable_constraints(self, conn_info: dict, db_name: str,
                            tables: list[dict]) -> None:
        """FK 제약조건 및 트리거 재활성화"""
        conn = self._connect(conn_info, db_name)
        cursor = conn.cursor()
        for tbl in tables:
            schema = tbl["schema_name"]
            table = tbl["table_name"]
            try:
                cursor.execute(f"ALTER TABLE [{schema}].[{table}] WITH CHECK CHECK CONSTRAINT ALL")
                cursor.execute(f"ENABLE TRIGGER ALL ON [{schema}].[{table}]")
            except Exception as e:
                logger.warning(f"제약조건 재활성화 실패: {schema}.{table} - {e}")
        conn.commit()
        conn.close()

    def _truncate_table(self, conn_info: dict, db_name: str,
                        schema: str, table: str) -> None:
        """대상 테이블 TRUNCATE (FK 있으면 DELETE)"""
        conn = self._connect(conn_info, db_name)
        cursor = conn.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE [{schema}].[{table}]")
        except Exception:
            cursor.execute(f"DELETE FROM [{schema}].[{table}]")
        conn.commit()
        conn.close()

    def _sync_table_pymssql(self, source_conn: dict, source_db: str,
                            target_conn: dict, target_db: str,
                            schema: str, table: str,
                            batch_size: int = 5000) -> SyncTableResult:
        """
        pymssql 기반 테이블 동기화 (BCP 없을 때 대체)
        SELECT → INSERT 방식으로 배치 복사
        """
        result = SyncTableResult(schema_name=schema, table_name=table)
        result.started_at = datetime.now()
        result.status = "RUNNING"

        src_conn = None
        tgt_conn = None

        try:
            # 소스 연결
            src_conn = self._connect(source_conn, source_db, timeout=300)

            # 타겟 연결
            tgt_conn = self._connect(target_conn, target_db, timeout=300)

            # 1) 컬럼 정보 조회
            src_cursor = src_conn.cursor()
            src_cursor.execute(f"""
                SELECT c.name, t.name AS type_name, c.is_identity
                FROM sys.columns c
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID('[{schema}].[{table}]')
                ORDER BY c.column_id
            """)
            columns_info = src_cursor.fetchall()

            # identity 컬럼 제외한 컬럼 목록
            has_identity = any(c[2] for c in columns_info)
            if has_identity:
                col_names = [c[0] for c in columns_info if not c[2]]
            else:
                col_names = [c[0] for c in columns_info]

            col_list = ", ".join(f"[{c}]" for c in col_names)
            placeholders = ", ".join(["%s"] * len(col_names))

            # 2) 대상 테이블 TRUNCATE
            self._truncate_table(target_conn, target_db, schema, table)

            # 3) 소스에서 읽어서 타겟에 삽입
            src_cursor.execute(f"SELECT {col_list} FROM [{schema}].[{table}]")

            tgt_cursor = tgt_conn.cursor()

            if has_identity:
                tgt_cursor.execute(f"SET IDENTITY_INSERT [{schema}].[{table}] ON")

            insert_sql = f"INSERT INTO [{schema}].[{table}] ({col_list}) VALUES ({placeholders})"
            total_inserted = 0
            batch = []

            for row in src_cursor:
                batch.append(row)
                if len(batch) >= batch_size:
                    tgt_cursor.executemany(insert_sql, batch)
                    tgt_conn.commit()
                    total_inserted += len(batch)
                    batch = []

            # 나머지 배치 처리
            if batch:
                tgt_cursor.executemany(insert_sql, batch)
                tgt_conn.commit()
                total_inserted += len(batch)

            if has_identity:
                tgt_cursor.execute(f"SET IDENTITY_INSERT [{schema}].[{table}] OFF")
                tgt_conn.commit()

            # 4) 건수 검증
            result.source_count = self.get_table_count(source_conn, source_db, schema, table)
            result.target_count = self.get_table_count(target_conn, target_db, schema, table)

            if result.source_count == result.target_count:
                result.status = "SUCCESS"
            else:
                result.status = "MISMATCH"
                result.error_msg = (
                    f"건수 불일치: 원본 {result.source_count:,} / "
                    f"대상 {result.target_count:,}"
                )

        except Exception as e:
            result.status = "FAIL"
            result.error_msg = str(e)
            logger.error(f"pymssql 동기화 실패 [{schema}].[{table}]: {e}")

        finally:
            if src_conn:
                try: src_conn.close()
                except: pass
            if tgt_conn:
                try: tgt_conn.close()
                except: pass

            result.completed_at = datetime.now()
            if result.started_at:
                result.elapsed_seconds = (
                    result.completed_at - result.started_at
                ).total_seconds()

        return result

    def sync_table_bcp(self, source_conn: dict, source_db: str,
                       target_conn: dict, target_db: str,
                       schema: str, table: str) -> SyncTableResult:
        """BCP로 단일 테이블 동기화 (BCP 없으면 pymssql 대체)"""
        result = SyncTableResult(schema_name=schema, table_name=table)
        result.started_at = datetime.now()
        result.status = "RUNNING"

        # BCP 유틸리티가 없으면 pymssql 대체 방식 사용
        if not self._bcp_path:
            logger.info(f"BCP 없음 → pymssql 방식으로 동기화: [{schema}].[{table}]")
            return self._sync_table_pymssql(
                source_conn, source_db, target_conn, target_db, schema, table
            )

        dat_file = os.path.join(self._data_dir, f"{source_db}_{schema}_{table}.dat")

        try:
            # 1) BCP OUT - 운영DB에서 추출
            export_cmd = self._build_bcp_cmd(
                source_conn, source_db, schema, table, "out", dat_file
            )
            logger.info(f"BCP OUT: [{schema}].[{table}]")
            self._run_bcp(export_cmd, "BCP OUT 실패")

            # dat 파일 확인
            if not os.path.exists(dat_file):
                raise Exception("BCP OUT 후 데이터 파일이 생성되지 않았습니다.")

            file_size = os.path.getsize(dat_file)
            logger.info(f"BCP OUT 완료: {file_size:,} bytes")

            # 2) 대상 테이블 TRUNCATE
            self._truncate_table(target_conn, target_db, schema, table)

            # 3) BCP IN - 개발DB로 적재
            if file_size > 0:
                import_cmd = self._build_bcp_cmd(
                    target_conn, target_db, schema, table, "in", dat_file
                )
                logger.info(f"BCP IN: [{schema}].[{table}]")
                self._run_bcp(import_cmd, "BCP IN 실패")
            else:
                logger.info(f"원본 데이터 없음 (0건), BCP IN 생략: [{schema}].[{table}]")

            # 4) 건수 검증
            result.source_count = self.get_table_count(
                source_conn, source_db, schema, table
            )
            result.target_count = self.get_table_count(
                target_conn, target_db, schema, table
            )

            if result.source_count == result.target_count:
                result.status = "SUCCESS"
            else:
                result.status = "MISMATCH"
                result.error_msg = (
                    f"건수 불일치: 원본 {result.source_count:,} / "
                    f"대상 {result.target_count:,}"
                )

        except Exception as e:
            result.status = "FAIL"
            result.error_msg = str(e)
            logger.error(f"테이블 동기화 실패 [{schema}].[{table}]: {e}")

        finally:
            # 임시 파일 정리
            if os.path.exists(dat_file):
                try:
                    os.remove(dat_file)
                except OSError:
                    pass

            result.completed_at = datetime.now()
            if result.started_at:
                result.elapsed_seconds = (
                    result.completed_at - result.started_at
                ).total_seconds()

        return result

    async def sync_tables_async(
        self,
        job_id: str,
        source_conn: dict,
        source_db: str,
        target_conn: dict,
        target_db: str,
        tables: list[dict],
    ) -> SyncJobProgress:
        """선택된 테이블들을 비동기로 순차 동기화"""
        job = SyncJobProgress(
            job_id=job_id,
            source_server=source_conn["server"],
            source_db=source_db,
            target_server=target_conn["server"],
            target_db=target_db,
            total_tables=len(tables),
            status="RUNNING",
            started_at=datetime.now(),
        )
        self._jobs[job_id] = job
        self._cancel_flags[job_id] = False

        # BCP 사용 가능 여부 로그
        if self._bcp_path:
            logger.info(f"동기화 시작 [BCP 모드]: {job_id}, {len(tables)}개 테이블")
        else:
            logger.info(f"동기화 시작 [pymssql 모드]: {job_id}, {len(tables)}개 테이블")

        try:
            # FK 제약조건 비활성화
            await asyncio.to_thread(
                self._disable_constraints, target_conn, target_db, tables
            )

            for idx, tbl in enumerate(tables):
                # 취소 확인
                if self._cancel_flags.get(job_id):
                    job.status = "CANCELLED"
                    break

                schema = tbl["schema_name"]
                table = tbl["table_name"]
                job.current_table = f"{schema}.{table}"
                job.completed_tables = idx

                # BCP 동기화 (블로킹 작업을 스레드에서 실행)
                result = await asyncio.to_thread(
                    self.sync_table_bcp,
                    source_conn, source_db,
                    target_conn, target_db,
                    schema, table,
                )
                job.results.append(result)
                job.completed_tables = idx + 1

            # FK 제약조건 재활성화
            await asyncio.to_thread(
                self._enable_constraints, target_conn, target_db, tables
            )

            if job.status != "CANCELLED":
                job.status = "COMPLETED" if job.fail_count == 0 else "FAILED"

        except Exception as e:
            job.status = "FAILED"
            job.error_msg = str(e)
            logger.error(f"동기화 작업 실패 [{job_id}]: {e}")

        finally:
            job.completed_at = datetime.now()
            self._cancel_flags.pop(job_id, None)

        return job


# 싱글톤 인스턴스
_sync_service: Optional[SyncService] = None


def get_sync_service() -> SyncService:
    global _sync_service
    if _sync_service is None:
        _sync_service = SyncService()
    return _sync_service