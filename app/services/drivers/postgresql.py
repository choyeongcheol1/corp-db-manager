"""
PostgreSQL 드라이버
"""
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
import time
from app.services.drivers.base import BaseDriver
from app.core.database import DBServer
from app.config import get_settings

settings = get_settings()

# psycopg2 임포트 (옵션)
try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


class PostgreSQLDriver(BaseDriver):
    """PostgreSQL 드라이버"""
    
    def __init__(self, server: DBServer):
        super().__init__(server)
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 패키지가 설치되지 않았습니다. pip install psycopg2-binary")
    
    # ============================================================
    # Connection Methods
    # ============================================================
    
    def get_connection(self, database: str = None) -> Any:
        """DB 연결 획득"""
        db = database or self.server.default_db or "postgres"
        return psycopg2.connect(
            host=self.server.host,
            port=self.server.port,
            database=db,
            user=self.server.username,
            password=self.server.password,
            connect_timeout=10
        )
    
    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
        """연결 테스트"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0].split(',')[0]
            conn.close()
            return True, "연결 성공", version
        except Exception as e:
            return False, f"연결 실패: {str(e)}", None
    
    # ============================================================
    # Database Methods
    # ============================================================
    
    def get_databases(self, prefix: str = None) -> List[Dict]:
        """DB 목록 조회"""
        prefix = prefix or settings.db_prefix
        
        try:
            conn = self.get_connection("postgres")
            cursor = conn.cursor()
            
            if prefix:
                where_clause = f"d.datname LIKE '{prefix}%'"
            else:
                where_clause = "d.datname NOT IN ('postgres', 'template0', 'template1')"
            
            cursor.execute(f"""
                SELECT 
                    d.datname AS db_name,
                    pg_catalog.pg_database_size(d.datname) / 1024.0 / 1024.0 AS size_mb
                FROM pg_catalog.pg_database d
                WHERE {where_clause}
                  AND d.datistemplate = false
                ORDER BY d.datname
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "db_name": row[0],
                    "create_date": datetime.now(),
                    "state": "ONLINE",
                    "size_mb": round(row[1] or 0, 2)
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"PostgreSQL DB 목록 조회 실패: {e}")
            return []
    
    def get_databases_with_disk_usage(self, prefix: str = None) -> List[Dict]:
        """DB별 용량 + 디스크 사용률 조회"""
        prefix = prefix or settings.db_prefix
        
        try:
            conn = self.get_connection("postgres")
            cursor = conn.cursor()
            
            if prefix:
                where_clause = f"d.datname LIKE '{prefix}%'"
            else:
                where_clause = "d.datname NOT IN ('postgres', 'template0', 'template1')"
            
            # DB 용량 조회
            cursor.execute(f"""
                SELECT 
                    d.datname AS db_name,
                    pg_catalog.pg_database_size(d.datname) / 1024.0 / 1024.0 AS size_mb
                FROM pg_catalog.pg_database d
                WHERE {where_clause}
                  AND d.datistemplate = false
                ORDER BY size_mb DESC
            """)
            
            db_list = cursor.fetchall()
            
            # 데이터 디렉토리 디스크 사용률 (전체 서버 공유)
            disk_total_gb = 0
            disk_free_gb = 0
            try:
                cursor.execute("""
                    SELECT 
                        pg_catalog.pg_tablespace_size('pg_default') / 1024.0 / 1024.0 / 1024.0 AS used_gb
                """)
                ts_row = cursor.fetchone()
                # pg에서는 OS 디스크 전체 크기를 직접 조회 불가 → 0으로 표시
            except:
                pass
            
            results = []
            for row in db_list:
                results.append({
                    "db_name": row[0],
                    "create_date": datetime.now(),
                    "state": "ONLINE",
                    "size_mb": round(row[1] or 0, 2),
                    "disk_total_gb": disk_total_gb,
                    "disk_free_gb": disk_free_gb,
                    "disk_used_pct": 0,
                    "db_disk_pct": 0,
                    "drive": "data"
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"PostgreSQL DB 디스크 사용률 조회 실패: {e}")
            return [dict(item, disk_total_gb=0, disk_free_gb=0, disk_used_pct=0, db_disk_pct=0, drive='')
                    for item in self.get_databases(prefix)]

    def get_tables(self, database: str) -> List[Dict]:
        """테이블 목록 조회"""
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    t.tablename AS table_name,
                    pg_relation_size(quote_ident(t.tablename)::text) / 1024.0 / 1024.0 AS size_mb,
                    COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = t.tablename), 0) AS row_count
                FROM pg_tables t
                WHERE t.schemaname = 'public'
                ORDER BY t.tablename
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "table_name": row[0],
                    "row_count": row[2] or 0,
                    "size_mb": round(row[1] or 0, 2)
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"PostgreSQL 테이블 목록 조회 실패: {e}")
            return []
    
    def get_table_columns(self, database: str, table_name: str) -> List[Dict]:
        """테이블 컬럼 정보 조회"""
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            # 단순화된 쿼리
            cursor.execute("""
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    CASE WHEN c.column_default LIKE 'nextval%%' THEN 'YES' ELSE 'NO' END AS is_identity,
                    CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
                    COALESCE(c.column_default, '') AS default_value,
                    COALESCE(pd.description, '') AS description
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_name = %s
                        AND tc.table_schema = 'public'
                        AND tc.constraint_type = 'PRIMARY KEY'
                ) pk ON c.column_name = pk.column_name
                LEFT JOIN pg_catalog.pg_class pc 
                    ON pc.relname = c.table_name
                LEFT JOIN pg_catalog.pg_namespace pn 
                    ON pn.oid = pc.relnamespace AND pn.nspname = c.table_schema
                LEFT JOIN pg_catalog.pg_description pd 
                    ON pd.objoid = pc.oid AND pd.objsubid = c.ordinal_position
                WHERE c.table_name = %s
                    AND c.table_schema = 'public'
                ORDER BY c.ordinal_position
            """, (table_name, table_name))
            
            results = []
            for row in cursor.fetchall():
                # 데이터 타입 포맷팅
                data_type = row[1]
                if row[2]:  # character_maximum_length
                    data_type = f"{data_type}({row[2]})"
                elif row[3] and row[4]:  # numeric precision/scale
                    data_type = f"{data_type}({row[3]},{row[4]})"
                
                results.append({
                    "column_name": row[0],
                    "data_type": data_type.upper(),
                    "max_length": row[2] or 0,
                    "is_nullable": row[5] == 'YES',
                    "is_identity": row[6] == 'YES',
                    "is_primary_key": bool(row[7]),
                    "default_value": row[8] if row[8] and not row[8].startswith('nextval') else '',
                    "description": row[9] or ''
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"PostgreSQL 테이블 컬럼 조회 실패: {e}")
            return []
        
    def get_db_size(self, database: str) -> float:
        """DB 용량 조회 (MB)"""
        try:
            conn = self.get_connection("postgres")
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT pg_database_size('{database}') / 1024.0 / 1024.0 AS size_mb
            """)
            
            row = cursor.fetchone()
            conn.close()
            return round(row[0] or 0, 2) if row else 0
            
        except Exception as e:
            print(f"PostgreSQL DB 용량 조회 실패: {e}")
            return 0
    
    def create_database(self, db_name: str, data_path: str = None, log_path: str = None) -> bool:
        """DB 생성"""
        try:
            conn = self.get_connection("postgres")
            conn.autocommit = True
            cursor = conn.cursor()
            
            cursor.execute(f'CREATE DATABASE "{db_name}"')
            conn.close()
            return True
            
        except Exception as e:
            print(f"PostgreSQL DB 생성 실패: {e}")
            return False
    
    # ============================================================
    # File Path Methods
    # ============================================================
    
    def _get_default_paths(self) -> Dict[str, str]:
        """기본 파일 경로"""
        return {
            "data_path": "/var/lib/postgresql/data/",
            "log_path": "/var/lib/postgresql/data/"
        }
    
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
            conn = self.get_connection("postgres")
            cursor = conn.cursor()
            
            # 2. 활성 연결 수
            try:
                cursor.execute("""
                    SELECT 
                        (SELECT count(*) FROM pg_stat_activity) AS active_connections,
                        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections
                """)
                row = cursor.fetchone()
                if row:
                    active = row[0] or 0
                    max_conn = row[1] or 100
                    conn_percent = int((active / max_conn) * 100)
                    
                    if conn_percent < 70:
                        conn_status = "normal"
                    elif conn_percent < 90:
                        conn_status = "warning"
                    else:
                        conn_status = "error"
                    
                    result["checks"].append({
                        "name": "연결 수",
                        "status": conn_status,
                        "value": f"{active} / {max_conn}",
                        "detail": f"{conn_percent}% 사용"
                    })
                    
                    if conn_status != "normal":
                        result["issues"].append(f"연결 수 높음: {conn_percent}%")
            except:
                pass
            
            # 3. 데이터베이스 크기
            try:
                cursor.execute("""
                    SELECT 
                        pg_size_pretty(sum(pg_database_size(datname))) AS total_size,
                        sum(pg_database_size(datname)) / 1024 / 1024 / 1024 AS total_gb
                    FROM pg_database
                    WHERE datistemplate = false
                """)
                row = cursor.fetchone()
                if row:
                    result["checks"].append({
                        "name": "전체 DB 크기",
                        "status": "normal",
                        "value": row[0],
                        "detail": f"{round(row[1] or 0, 1)} GB"
                    })
            except:
                pass
            
            # 4. 장시간 실행 쿼리
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM pg_stat_activity 
                    WHERE state = 'active' 
                    AND query_start < NOW() - INTERVAL '1 minute'
                    AND pid != pg_backend_pid()
                """)
                row = cursor.fetchone()
                long_queries = row[0] if row else 0
                
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
            
            # 5. 잠금 대기
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM pg_locks 
                    WHERE NOT granted
                """)
                row = cursor.fetchone()
                waiting_locks = row[0] if row else 0
                
                if waiting_locks == 0:
                    lock_status = "normal"
                elif waiting_locks <= 5:
                    lock_status = "warning"
                else:
                    lock_status = "error"
                
                result["checks"].append({
                    "name": "잠금 대기",
                    "status": lock_status,
                    "value": f"{waiting_locks}개",
                    "detail": "0개 권장"
                })
                
                if lock_status != "normal":
                    result["issues"].append(f"잠금 대기: {waiting_locks}개")
            except:
                pass
            
            # 6. 복제 지연 (스탠바이 서버인 경우)
            try:
                cursor.execute("""
                    SELECT 
                        CASE WHEN pg_is_in_recovery() THEN
                            EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))
                        ELSE NULL END AS replication_lag_seconds
                """)
                row = cursor.fetchone()
                if row and row[0] is not None:
                    lag_seconds = int(row[0])
                    
                    if lag_seconds < 60:
                        lag_status = "normal"
                    elif lag_seconds < 300:
                        lag_status = "warning"
                    else:
                        lag_status = "error"
                    
                    result["checks"].append({
                        "name": "복제 지연",
                        "status": lag_status,
                        "value": f"{lag_seconds}초",
                        "detail": "스탠바이 서버"
                    })
                    
                    if lag_status != "normal":
                        result["issues"].append(f"복제 지연: {lag_seconds}초")
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
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            # 1. DB 연결 가능 여부
            result["checks"].append({
                "name": "DB 상태",
                "status": "normal",
                "value": "ONLINE",
                "detail": "-"
            })
            
            # 2. DB 크기
            cursor.execute(f"""
                SELECT pg_database_size('{database}') / 1024.0 / 1024.0 AS size_mb
            """)
            row = cursor.fetchone()
            size_mb = row[0] if row else 0
            
            result["checks"].append({
                "name": "DB 크기",
                "status": "normal",
                "value": f"{int(size_mb):,} MB",
                "detail": "-"
            })
            
            # 3. 테이블 수
            cursor.execute("""
                SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'
            """)
            row = cursor.fetchone()
            table_count = row[0] if row else 0
            
            result["checks"].append({
                "name": "테이블 수",
                "status": "normal",
                "value": f"{table_count}개",
                "detail": "-"
            })
            
            # 4. Dead Tuple (VACUUM 필요 여부)
            try:
                cursor.execute("""
                    SELECT 
                        SUM(n_dead_tup) AS dead_tuples,
                        SUM(n_live_tup) AS live_tuples
                    FROM pg_stat_user_tables
                """)
                row = cursor.fetchone()
                if row and row[1] and row[1] > 0:
                    dead = row[0] or 0
                    live = row[1] or 1
                    dead_percent = int((dead / live) * 100)
                    
                    if dead_percent < 10:
                        dead_status = "normal"
                    elif dead_percent < 30:
                        dead_status = "warning"
                    else:
                        dead_status = "error"
                    
                    result["checks"].append({
                        "name": "Dead Tuple",
                        "status": dead_status,
                        "value": f"{dead_percent}%",
                        "detail": f"{dead:,} / {live:,}"
                    })
                    
                    if dead_status != "normal":
                        result["issues"].append(f"VACUUM 필요: Dead Tuple {dead_percent}%")
            except:
                pass
            
            # 5. 마지막 VACUUM
            try:
                cursor.execute("""
                    SELECT MIN(last_vacuum), MIN(last_autovacuum)
                    FROM pg_stat_user_tables
                    WHERE n_live_tup > 0
                """)
                row = cursor.fetchone()
                last_vacuum = row[0] or row[1] if row else None
                
                if last_vacuum:
                    days_ago = (datetime.now() - last_vacuum).days
                    
                    if days_ago <= 1:
                        vacuum_status = "normal"
                        vacuum_value = "1일 이내"
                    elif days_ago <= 7:
                        vacuum_status = "warning"
                        vacuum_value = f"{days_ago}일 전"
                    else:
                        vacuum_status = "error"
                        vacuum_value = f"{days_ago}일 전"
                else:
                    vacuum_status = "warning"
                    vacuum_value = "기록 없음"
                
                result["checks"].append({
                    "name": "마지막 VACUUM",
                    "status": vacuum_status,
                    "value": vacuum_value,
                    "detail": str(last_vacuum)[:19] if last_vacuum else "-"
                })
                
                if vacuum_status != "normal":
                    result["issues"].append(f"VACUUM 필요: {vacuum_value}")
            except:
                pass
            
            conn.close()
            
        except Exception as e:
            result["checks"].append({
                "name": "DB 상태",
                "status": "error",
                "value": "연결 실패",
                "detail": str(e)
            })
            result["issues"].append(f"DB 연결 실패: {str(e)}")
            result["status"] = "error"
            return result
        
        # 최종 상태 결정
        statuses = [c["status"] for c in result["checks"]]
        if "error" in statuses:
            result["status"] = "error"
        elif "warning" in statuses:
            result["status"] = "warning"
        
        return result