"""
MSSQL 드라이버
"""
import pyodbc
import time
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
from app.services.drivers.base import BaseDriver
from app.core.database import DBServer
from app.config import get_settings

settings = get_settings()


class MSSQLDriver(BaseDriver):
    """MSSQL 드라이버"""
    
    def __init__(self, server: DBServer):
        super().__init__(server)
    
    # ============================================================
    # Connection Methods
    # ============================================================
    
    def _get_connection_string(self, database: str = None) -> str:
        """연결 문자열 생성"""
        db = database or self.server.default_db or "master"
        
        # 서버별 ODBC 드라이버 (기본: Driver 18)
        driver = getattr(self.server, 'odbc_driver', None) or "ODBC Driver 18 for SQL Server"
        timeout = getattr(self.server, 'connection_timeout', None) or 30
        
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={self.server.host},{self.server.port};"
            f"DATABASE={db};"
            f"UID={self.server.username};"
            f"PWD={self.server.password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={timeout};"
        )
    
    def get_connection(self, database: str = None, max_retries: int = 3) -> pyodbc.Connection:
        """DB 연결 획득 (재시도 포함)"""
        conn_str = self._get_connection_string(database)
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return pyodbc.connect(conn_str)
            except pyodbc.Error as e:
                last_error = e
                if attempt < max_retries - 1:
                    print(f"[MSSQL] 연결 재시도 ({attempt + 1}/{max_retries}): {self.server.host} / {database}")
                    time.sleep(1)
        
        raise last_error
    
    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
        """연결 테스트"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0].split('\n')[0]
            conn.close()
            return True, "연결 성공", version
        except pyodbc.Error as e:
            return False, f"연결 실패: {str(e)}", None
        except Exception as e:
            return False, f"오류: {str(e)}", None
    
    # ============================================================
    # Query Execution Methods (헬퍼 메서드)
    # ============================================================
    
    def execute_query(self, query: str, params: Tuple = None, database: str = None) -> List[Dict[str, Any]]:
        """
        SELECT 쿼리 실행 후 결과를 딕셔너리 리스트로 반환
        """
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # 컬럼명 추출
            columns = [column[0] for column in cursor.description] if cursor.description else []
            
            # 결과를 딕셔너리 리스트로 변환
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MSSQL 쿼리 실행 실패: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Tuple = None, database: str = None) -> int:
        """
        INSERT, UPDATE, DELETE 등 비조회 쿼리 실행
        영향받은 행 수 반환
        """
        try:
            conn = self.get_connection(database)
            conn.autocommit = True
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            rowcount = cursor.rowcount
            conn.close()
            return rowcount
            
        except Exception as e:
            print(f"MSSQL 비조회 쿼리 실행 실패: {e}")
            raise
    
    # ============================================================
    # Database Methods
    # ============================================================
    
    def get_databases(self, prefix: str = None) -> List[Dict]:
        """DB 목록 조회"""
        prefix = prefix or settings.db_prefix
        
        try:
            conn = self.get_connection("master")
            cursor = conn.cursor()
            
            if prefix:
                where_clause = f"d.name LIKE '{prefix}%'"
            else:
                where_clause = "d.name NOT IN ('master', 'tempdb', 'model', 'msdb')"
            
            cursor.execute(f"""
                SELECT 
                    d.name AS db_name,
                    d.create_date,
                    d.state_desc AS state,
                    (SELECT SUM(size) * 8.0 / 1024 
                     FROM sys.master_files 
                     WHERE database_id = d.database_id) AS size_mb
                FROM sys.databases d
                WHERE {where_clause}
                  AND d.state = 0
                ORDER BY d.create_date DESC
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "db_name": row.db_name,
                    "create_date": row.create_date,
                    "state": row.state,
                    "size_mb": round(row.size_mb or 0, 2)
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MSSQL DB 목록 조회 실패: {e}")
            return []
    
    def get_tables(self, database: str) -> List[Dict]:
        """테이블 목록 조회"""
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    t.name AS table_name,
                    p.rows AS row_count,
                    SUM(a.total_pages) * 8.0 / 1024 AS size_mb,
                    CAST(ep.value AS NVARCHAR(500)) AS description
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                LEFT JOIN sys.extended_properties ep 
                    ON ep.major_id = t.object_id 
                    AND ep.minor_id = 0 
                    AND ep.name = 'MS_Description'
                WHERE i.index_id <= 1
                GROUP BY t.name, p.rows, ep.value
                ORDER BY t.name
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "table_name": row.table_name,
                    "row_count": row.row_count or 0,
                    "size_mb": round(row.size_mb or 0, 2),
                    "description": row.description or ''
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MSSQL 테이블 목록 조회 실패: {e}")
            return []

    def get_table_columns(self, database: str, table_name: str) -> List[Dict]:
        """테이블 컬럼 정보 조회"""
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT 
                    c.name AS column_name,
                    t.name AS data_type,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable,
                    c.is_identity,
                    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
                    ISNULL(dc.definition, '') AS default_value,
                    ISNULL(CAST(ep.value AS NVARCHAR(500)), '') AS description
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                LEFT JOIN sys.extended_properties ep 
                    ON ep.major_id = c.object_id 
                    AND ep.minor_id = c.column_id 
                    AND ep.name = 'MS_Description'
                LEFT JOIN (
                    SELECT ic.object_id, ic.column_id
                    FROM sys.index_columns ic
                    INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    WHERE i.is_primary_key = 1
                ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
                WHERE c.object_id = OBJECT_ID('{table_name}')
                ORDER BY c.column_id
            """)
            
            results = []
            for row in cursor.fetchall():
                # 데이터 타입 포맷팅
                data_type = row.data_type
                if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                    length = row.max_length
                    if data_type.startswith('n'):
                        length = length // 2
                    if length == -1:
                        data_type = f"{data_type}(MAX)"
                    else:
                        data_type = f"{data_type}({length})"
                elif data_type in ('decimal', 'numeric'):
                    data_type = f"{data_type}({row.precision},{row.scale})"
                
                results.append({
                    "column_name": row.column_name,
                    "data_type": data_type.upper(),
                    "max_length": row.max_length,
                    "is_nullable": row.is_nullable,
                    "is_identity": row.is_identity,
                    "is_primary_key": bool(row.is_primary_key),
                    "default_value": row.default_value.replace('(', '').replace(')', '') if row.default_value else '',
                    "description": row.description or ''
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MSSQL 테이블 컬럼 조회 실패: {e}")
            return []       
    
    def get_db_size(self, database: str) -> float:
        """DB 용량 조회 (MB)"""
        try:
            conn = self.get_connection("master")
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT SUM(size) * 8.0 / 1024 AS size_mb
                FROM sys.master_files
                WHERE database_id = DB_ID('{database}')
            """)
            
            row = cursor.fetchone()
            conn.close()
            return round(row.size_mb or 0, 2) if row else 0
            
        except Exception as e:
            print(f"MSSQL DB 용량 조회 실패: {e}")
            return 0
    
    def create_database(self, db_name: str, data_path: str = None, log_path: str = None) -> bool:
        """DB 생성"""
        try:
            paths = self.get_file_paths()
            data_path = data_path or paths["data_path"]
            log_path = log_path or paths["log_path"]
            
            conn = self.get_connection("master")
            conn.autocommit = True
            cursor = conn.cursor()
            
            sql = f"""
                CREATE DATABASE [{db_name}]
                ON PRIMARY (
                    NAME = N'{db_name}',
                    FILENAME = N'{data_path}{db_name}.mdf',
                    SIZE = {settings.db_initial_size_mb}MB,
                    FILEGROWTH = 64MB
                )
                LOG ON (
                    NAME = N'{db_name}_log',
                    FILENAME = N'{log_path}{db_name}_log.ldf',
                    SIZE = {settings.db_log_size_mb}MB,
                    FILEGROWTH = 64MB
                )
            """
            
            cursor.execute(sql)
            conn.close()
            return True
            
        except Exception as e:
            print(f"MSSQL DB 생성 실패: {e}")
            return False
    
    # ============================================================
    # File Path Methods
    # ============================================================
    
    def _get_default_paths(self) -> Dict[str, str]:
        """기본 파일 경로"""
        return {
            "data_path": r"D:\MSSQL\Data\\",
            "log_path": r"D:\MSSQL\Log\\"
        }
    
    def get_file_paths(self, reference_db: str = None) -> Dict[str, str]:
        """파일 경로 조회"""
        if self.server.data_path and self.server.log_path:
            return {
                "data_path": self.server.data_path,
                "log_path": self.server.log_path
            }
        
        if reference_db:
            try:
                conn = self.get_connection("master")
                cursor = conn.cursor()
                
                cursor.execute(f"""
                    SELECT 
                        (SELECT LEFT(physical_name, LEN(physical_name) - CHARINDEX('\\', REVERSE(physical_name)) + 1)
                         FROM sys.master_files WHERE database_id = DB_ID('{reference_db}') AND type = 0) AS data_path,
                        (SELECT LEFT(physical_name, LEN(physical_name) - CHARINDEX('\\', REVERSE(physical_name)) + 1)
                         FROM sys.master_files WHERE database_id = DB_ID('{reference_db}') AND type = 1) AS log_path
                """)
                
                row = cursor.fetchone()
                conn.close()
                
                if row and row.data_path:
                    return {
                        "data_path": row.data_path,
                        "log_path": row.log_path
                    }
            except:
                pass
        
        return self._get_default_paths()
    
    # ============================================================
    # Health Check Methods
    # ============================================================
    
    def check_server_health(self) -> Dict:
        """서버 상태 점검"""
        result = {
            "server_id": self.server.id,
            "server_name": self.server.server_name,
            "host": self.server.host,
            "port": self.server.port,
            "status": "normal",
            "checked_at": datetime.now(),
            "checks": [],
            "issues": []
        }
        
        # 1. 연결 테스트 및 응답 시간
        start_time = time.time()
        success, message, version = self.test_connection()
        response_time = int((time.time() - start_time) * 1000)
        
        result["checks"].append({
            "name": "연결 상태",
            "status": "normal" if success else "error",
            "value": message,
            "detail": version or "-"
        })
        
        if response_time < 1000:
            resp_status = "normal"
        elif response_time < 3000:
            resp_status = "warning"
        else:
            resp_status = "error"
        
        result["checks"].append({
            "name": "응답 시간",
            "status": resp_status,
            "value": f"{response_time}ms",
            "detail": "1초 이하 권장"
        })
        
        if resp_status == "warning":
            result["issues"].append(f"응답 시간이 느립니다: {response_time}ms")
        elif resp_status == "error":
            result["issues"].append(f"응답 시간이 매우 느립니다: {response_time}ms")
        
        if not success:
            result["status"] = "error"
            result["issues"].append(f"서버 연결 실패: {message}")
            return result
        
        try:
            conn = self.get_connection("master")
            cursor = conn.cursor()
            
            # 2. CPU 사용량
            try:
                cursor.execute("""
                    SELECT TOP 1 
                        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS cpu_idle,
                        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS cpu_sql
                    FROM (
                        SELECT CONVERT(xml, record) AS record 
                        FROM sys.dm_os_ring_buffers 
                        WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                        AND record LIKE '%<SystemHealth>%'
                    ) AS x
                """)
                row = cursor.fetchone()
                if row and row.cpu_idle is not None:
                    cpu_usage = 100 - row.cpu_idle
                    cpu_sql = row.cpu_sql or 0
                    
                    # OS CPU 상태 판정 (VM 보정 포함)
                    if cpu_usage >= 90 and cpu_sql < 30:
                        # OS 100%인데 SQL은 낮음 → VM 환경 추정, warning으로 완화
                        cpu_status = "warning"
                        os_detail = "VM 환경에서 부정확할 수 있음"
                    elif cpu_usage < 70:
                        cpu_status = "normal"
                        os_detail = ""
                    elif cpu_usage < 90:
                        cpu_status = "warning"
                        os_detail = ""
                    else:
                        cpu_status = "error"
                        os_detail = ""
                    
                    result["checks"].append({
                        "name": "CPU 사용률(OS)",
                        "status": cpu_status,
                        "value": f"{cpu_usage}%",
                        "detail": os_detail or f"OS 전체 사용률"
                    })
                    
                    # SQL Server CPU 별도 표시
                    if cpu_sql < 30:
                        sql_cpu_status = "normal"
                    elif cpu_sql < 70:
                        sql_cpu_status = "warning"
                    else:
                        sql_cpu_status = "error"
                    
                    result["checks"].append({
                        "name": "CPU 사용률(SQL)",
                        "status": sql_cpu_status,
                        "value": f"{cpu_sql}%",
                        "detail": "SQL Server 프로세스 사용률"
                    })
                    
                    if cpu_status != "normal":
                        result["issues"].append(f"OS CPU 사용률 높음: {cpu_usage}% (SQL Server: {cpu_sql}%)")
            except:
                result["checks"].append({
                    "name": "CPU 사용률",
                    "status": "normal",
                    "value": "측정 불가",
                    "detail": "권한 필요"
                })
            
            # 3. 메모리 사용량
            try:
                cursor.execute("""
                    SELECT 
                        (total_physical_memory_kb / 1024) AS total_mb,
                        (available_physical_memory_kb / 1024) AS available_mb
                    FROM sys.dm_os_sys_memory
                """)
                row = cursor.fetchone()
                if row and row.total_mb:
                    used_mb = row.total_mb - row.available_mb
                    used_percent = int((used_mb / row.total_mb) * 100)
                    
                    if used_percent < 80:
                        mem_status = "normal"
                    elif used_percent < 95:
                        mem_status = "warning"
                    else:
                        mem_status = "error"
                    
                    result["checks"].append({
                        "name": "메모리 사용률",
                        "status": mem_status,
                        "value": f"{used_percent}%",
                        "detail": f"{used_mb:,}MB / {row.total_mb:,}MB"
                    })
                    
                    if mem_status != "normal":
                        result["issues"].append(f"메모리 사용률 높음: {used_percent}%")
            except:
                result["checks"].append({
                    "name": "메모리 사용률",
                    "status": "normal",
                    "value": "측정 불가",
                    "detail": "권한 필요"
                })
            
            # 4. 디스크 공간
            try:
                cursor.execute("""
                    SELECT DISTINCT
                        vs.volume_mount_point AS drive,
                        vs.logical_volume_name AS label,
                        vs.total_bytes / 1024 / 1024 / 1024 AS total_gb,
                        vs.available_bytes / 1024 / 1024 / 1024 AS free_gb
                    FROM sys.master_files mf
                    CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
                """)
                for row in cursor.fetchall():
                    if row.total_gb and row.total_gb > 0:
                        used_percent = int(((row.total_gb - row.free_gb) / row.total_gb) * 100)
                        free_percent = 100 - used_percent
                        
                        if free_percent > 20:
                            disk_status = "normal"
                        elif free_percent > 5:
                            disk_status = "warning"
                        else:
                            disk_status = "error"
                        
                        # 드라이브 표시: "D:\" 또는 레이블 또는 용량 기반
                        mount = str(row.drive or '').strip()
                        label = str(row.label or '').strip()
                        if mount:
                            drive_display = mount
                        elif label:
                            drive_display = label
                        else:
                            drive_display = f"{int(row.total_gb)}GB 볼륨"
                        
                        result["checks"].append({
                            "name": f"디스크 {drive_display}",
                            "status": disk_status,
                            "value": f"{used_percent}% 사용",
                            "detail": f"여유: {int(row.free_gb)}GB / 전체: {int(row.total_gb)}GB"
                        })
                        
                        if disk_status != "normal":
                            result["issues"].append(f"디스크 {drive_display} 여유 공간 부족: {int(row.free_gb)}GB")
            except:
                result["checks"].append({
                    "name": "디스크 공간",
                    "status": "normal",
                    "value": "측정 불가",
                    "detail": "권한 필요"
                })
            
            # 5. 차단된 세션
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS blocked_count
                    FROM sys.dm_exec_requests
                    WHERE blocking_session_id > 0
                """)
                row = cursor.fetchone()
                blocked = row.blocked_count if row else 0
                
                if blocked == 0:
                    block_status = "normal"
                elif blocked <= 5:
                    block_status = "warning"
                else:
                    block_status = "error"
                
                result["checks"].append({
                    "name": "차단된 세션",
                    "status": block_status,
                    "value": f"{blocked}개",
                    "detail": "0개 권장"
                })
                
                if block_status != "normal":
                    result["issues"].append(f"차단된 세션 발견: {blocked}개")
            except:
                pass
            
            # 6. 장시간 실행 쿼리 (1분 이상)
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS long_query_count
                    FROM sys.dm_exec_requests r
                    WHERE r.status = 'running'
                    AND r.start_time < DATEADD(MINUTE, -1, GETDATE())
                    AND r.session_id > 50
                """)
                row = cursor.fetchone()
                long_queries = row.long_query_count if row else 0
                
                if long_queries == 0:
                    query_status = "normal"
                elif long_queries <= 3:
                    query_status = "warning"
                else:
                    query_status = "error"
                
                result["checks"].append({
                    "name": "장시간 쿼리",
                    "status": query_status,
                    "value": f"{long_queries}개",
                    "detail": "1분 이상 실행 중"
                })
                
                if query_status != "normal":
                    result["issues"].append(f"장시간 실행 쿼리: {long_queries}개")
            except:
                pass
            
            conn.close()
            
        except Exception as e:
            result["issues"].append(f"점검 중 오류: {str(e)}")
        
        # 최종 상태 결정
        statuses = [c["status"] for c in result["checks"]]
        if "error" in statuses:
            result["status"] = "error"
        elif "warning" in statuses:
            result["status"] = "warning"
        
        return result
    
    def check_database_health(self, database: str) -> Dict:
        """개별 DB 상태 점검"""
        result = {
            "db_name": database,
            "status": "normal",
            "checks": [],
            "issues": []
        }
        
        try:
            conn = self.get_connection("master")
            cursor = conn.cursor()
            
            # 1. DB 상태
            cursor.execute(f"""
                SELECT state_desc 
                FROM sys.databases 
                WHERE name = '{database}'
            """)
            row = cursor.fetchone()
            db_state = row.state_desc if row else "UNKNOWN"
            
            state_status = "normal" if db_state == "ONLINE" else "error"
            result["checks"].append({
                "name": "DB 상태",
                "status": state_status,
                "value": db_state,
                "detail": "-"
            })
            
            if state_status == "error":
                result["issues"].append(f"DB 상태 비정상: {db_state}")
            
            # 2. 데이터 파일 용량
            cursor.execute(f"""
                SELECT 
                    SUM(CASE WHEN type = 0 THEN size END) * 8.0 / 1024 AS data_size_mb,
                    SUM(CASE WHEN type = 0 THEN max_size END) * 8.0 / 1024 AS data_max_mb,
                    SUM(CASE WHEN type = 1 THEN size END) * 8.0 / 1024 AS log_size_mb,
                    SUM(CASE WHEN type = 1 THEN max_size END) * 8.0 / 1024 AS log_max_mb
                FROM sys.master_files
                WHERE database_id = DB_ID('{database}')
            """)
            row = cursor.fetchone()
            
            if row and row.data_size_mb:
                # 데이터 파일
                data_size = row.data_size_mb or 0
                data_max = row.data_max_mb if row.data_max_mb and row.data_max_mb > 0 else data_size * 10
                data_percent = int((data_size / data_max) * 100) if data_max > 0 else 0
                
                if data_percent < 80:
                    data_status = "normal"
                elif data_percent < 95:
                    data_status = "warning"
                else:
                    data_status = "error"
                
                result["checks"].append({
                    "name": "데이터 용량",
                    "status": data_status,
                    "value": f"{data_percent}%",
                    "detail": f"{int(data_size):,}MB"
                })
                
                if data_status != "normal":
                    result["issues"].append(f"데이터 파일 용량 {data_percent}%")
                
                # 로그 파일
                log_size = row.log_size_mb or 0
                log_max = row.log_max_mb if row.log_max_mb and row.log_max_mb > 0 else log_size * 10
                log_percent = int((log_size / log_max) * 100) if log_max > 0 else 0
                
                if log_percent < 70:
                    log_status = "normal"
                elif log_percent < 90:
                    log_status = "warning"
                else:
                    log_status = "error"
                
                result["checks"].append({
                    "name": "로그 용량",
                    "status": log_status,
                    "value": f"{log_percent}%",
                    "detail": f"{int(log_size):,}MB"
                })
                
                if log_status != "normal":
                    result["issues"].append(f"로그 파일 용량 {log_percent}%")
            
            # 3. 마지막 백업
            cursor.execute(f"""
                SELECT 
                    MAX(CASE WHEN type = 'D' THEN backup_finish_date END) AS last_full,
                    MAX(CASE WHEN type = 'L' THEN backup_finish_date END) AS last_log
                FROM msdb.dbo.backupset
                WHERE database_name = '{database}'
            """)
            row = cursor.fetchone()
            
            last_full = row.last_full if row else None
            
            if last_full:
                days_ago = (datetime.now() - last_full).days
                
                if days_ago <= 1:
                    backup_status = "normal"
                    backup_value = "1일 이내"
                elif days_ago <= 7:
                    backup_status = "warning"
                    backup_value = f"{days_ago}일 전"
                else:
                    backup_status = "error"
                    backup_value = f"{days_ago}일 전"
            else:
                backup_status = "error"
                backup_value = "없음"
            
            result["checks"].append({
                "name": "마지막 백업",
                "status": backup_status,
                "value": backup_value,
                "detail": str(last_full)[:19] if last_full else "기록 없음"
            })
            
            if backup_status != "normal":
                result["issues"].append(f"백업 필요: {backup_value}")
            
            conn.close()
            
        except Exception as e:
            result["issues"].append(f"점검 오류: {str(e)}")
            result["status"] = "error"
            return result
        
        # 최종 상태 결정
        statuses = [c["status"] for c in result["checks"]]
        if "error" in statuses:
            result["status"] = "error"
        elif "warning" in statuses:
            result["status"] = "warning"
        
        return result