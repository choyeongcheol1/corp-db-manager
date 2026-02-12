"""
MySQL 드라이버
"""
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
import time
from app.services.drivers.base import BaseDriver
from app.core.database import DBServer
from app.config import get_settings

settings = get_settings()

# pymysql 임포트 (옵션)
try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False


class MySQLDriver(BaseDriver):
    """MySQL 드라이버"""
    
    def __init__(self, server: DBServer):
        super().__init__(server)
        if not HAS_PYMYSQL:
            raise ImportError("pymysql 패키지가 설치되지 않았습니다. pip install pymysql")
    
    # ============================================================
    # Connection Methods
    # ============================================================
    
    def get_connection(self, database: str = None) -> Any:
        """DB 연결 획득"""
        db = database or self.server.default_db or None
        return pymysql.connect(
            host=self.server.host,
            port=self.server.port,
            database=db,
            user=self.server.username,
            password=self.server.password,
            connect_timeout=10,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    
    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
        """연결 테스트"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            row = cursor.fetchone()
            version = f"MySQL {row['VERSION()']}" if row else "MySQL"
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
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if prefix:
                where_clause = f"schema_name LIKE '{prefix}%'"
            else:
                where_clause = "schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"
            
            cursor.execute(f"""
                SELECT 
                    s.schema_name AS db_name,
                    ROUND(SUM(t.data_length + t.index_length) / 1024 / 1024, 2) AS size_mb
                FROM information_schema.schemata s
                LEFT JOIN information_schema.tables t ON s.schema_name = t.table_schema
                WHERE {where_clause}
                GROUP BY s.schema_name
                ORDER BY s.schema_name
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "db_name": row['db_name'],
                    "create_date": datetime.now(),
                    "state": "ONLINE",
                    "size_mb": round(row['size_mb'] or 0, 2)
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MySQL DB 목록 조회 실패: {e}")
            return []
    
    def get_databases_with_disk_usage(self, prefix: str = None) -> List[Dict]:
        """DB별 용량 + 디스크 사용률 조회"""
        prefix = prefix or settings.db_prefix
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if prefix:
                where_clause = f"schema_name LIKE '{prefix}%'"
            else:
                where_clause = "schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"
            
            cursor.execute(f"""
                SELECT 
                    s.schema_name AS db_name,
                    ROUND(SUM(t.data_length + t.index_length) / 1024 / 1024, 2) AS size_mb
                FROM information_schema.schemata s
                LEFT JOIN information_schema.tables t ON s.schema_name = t.table_schema
                WHERE {where_clause}
                GROUP BY s.schema_name
                ORDER BY size_mb DESC
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "db_name": row['db_name'],
                    "create_date": datetime.now(),
                    "state": "ONLINE",
                    "size_mb": round(row['size_mb'] or 0, 2),
                    "disk_total_gb": 0,
                    "disk_free_gb": 0,
                    "disk_used_pct": 0,
                    "db_disk_pct": 0,
                    "drive": "data"
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MySQL DB 디스크 사용률 조회 실패: {e}")
            return [dict(item, disk_total_gb=0, disk_free_gb=0, disk_used_pct=0, db_disk_pct=0, drive='')
                    for item in self.get_databases(prefix)]

    def get_tables(self, database: str) -> List[Dict]:
        """테이블 목록 조회"""
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    table_name,
                    table_rows AS row_count,
                    ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb,
                    table_comment AS description
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "table_name": row['table_name'],
                    "row_count": row['row_count'] or 0,
                    "size_mb": round(row['size_mb'] or 0, 2),
                    "description": row['description'] or ''
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MySQL 테이블 목록 조회 실패: {e}")
            return []
    
    def get_table_columns(self, database: str, table_name: str) -> List[Dict]:
        """테이블 컬럼 정보 조회"""
        try:
            conn = self.get_connection(database)
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT 
                    c.column_name,
                    c.column_type AS data_type,
                    c.character_maximum_length AS max_length,
                    c.is_nullable,
                    c.extra,
                    c.column_key,
                    c.column_default AS default_value,
                    c.column_comment AS description
                FROM information_schema.columns c
                WHERE c.table_schema = DATABASE()
                  AND c.table_name = '{table_name}'
                ORDER BY c.ordinal_position
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "column_name": row['column_name'],
                    "data_type": row['data_type'].upper(),
                    "max_length": row['max_length'] or 0,
                    "is_nullable": row['is_nullable'] == 'YES',
                    "is_identity": 'auto_increment' in (row['extra'] or '').lower(),
                    "is_primary_key": row['column_key'] == 'PRI',
                    "default_value": row['default_value'] or '',
                    "description": row['description'] or ''
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"MySQL 테이블 컬럼 조회 실패: {e}")
            return []
    
    def get_db_size(self, database: str) -> float:
        """DB 용량 조회 (MB)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT 
                    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
                FROM information_schema.tables
                WHERE table_schema = '{database}'
            """)
            
            row = cursor.fetchone()
            conn.close()
            return round(row['size_mb'] or 0, 2) if row else 0
            
        except Exception as e:
            print(f"MySQL DB 용량 조회 실패: {e}")
            return 0
    
    def create_database(self, db_name: str, data_path: str = None, log_path: str = None) -> bool:
        """DB 생성"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"MySQL DB 생성 실패: {e}")
            return False
    
    # ============================================================
    # File Path Methods
    # ============================================================
    
    def _get_default_paths(self) -> Dict[str, str]:
        """기본 파일 경로"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT @@datadir AS data_dir")
            row = cursor.fetchone()
            data_dir = row['data_dir'] if row else '/var/lib/mysql/'
            
            conn.close()
            
            return {
                "data_path": data_dir,
                "log_path": data_dir
            }
        except:
            return {
                "data_path": "/var/lib/mysql/",
                "log_path": "/var/lib/mysql/"
            }
    
    # ============================================================
    # Health Check Methods
    # ============================================================
    
    def check_server_health(self) -> Dict:
        """서버 상태 점검"""
        result = {
            "server_id": self.server.id,
            "server_name": self.server.server_name,
            "status": "normal",
            "checked_at": datetime.now(),
            "checks": [],
            "issues": []
        }
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 1. 연결 테스트
            result["checks"].append({
                "name": "연결 상태",
                "status": "normal",
                "value": "정상",
                "detail": f"{self.server.host}:{self.server.port}"
            })
            
            # 2. 버전 정보
            cursor.execute("SELECT VERSION()")
            row = cursor.fetchone()
            version = row['VERSION()'] if row else "Unknown"
            result["checks"].append({
                "name": "MySQL 버전",
                "status": "normal",
                "value": version,
                "detail": "-"
            })
            
            # 3. 업타임
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Uptime'")
            row = cursor.fetchone()
            if row:
                uptime_seconds = int(row['Value'])
                uptime_days = uptime_seconds // 86400
                uptime_hours = (uptime_seconds % 86400) // 3600
                
                result["checks"].append({
                    "name": "업타임",
                    "status": "normal",
                    "value": f"{uptime_days}일 {uptime_hours}시간",
                    "detail": f"{uptime_seconds:,}초"
                })
            
            # 4. 현재 연결 수
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            row = cursor.fetchone()
            current_conn = int(row['Value']) if row else 0
            
            cursor.execute("SELECT @@max_connections AS max_conn")
            row = cursor.fetchone()
            max_conn = int(row['max_conn']) if row else 151
            
            conn_percent = int((current_conn / max_conn) * 100)
            
            if conn_percent < 70:
                conn_status = "normal"
            elif conn_percent < 90:
                conn_status = "warning"
            else:
                conn_status = "error"
            
            result["checks"].append({
                "name": "연결 수",
                "status": conn_status,
                "value": f"{current_conn} / {max_conn}",
                "detail": f"{conn_percent}%"
            })
            
            if conn_status != "normal":
                result["issues"].append(f"연결 수 {conn_percent}% 사용 중")
            
            # 5. 슬로우 쿼리
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Slow_queries'")
            row = cursor.fetchone()
            slow_queries = int(row['Value']) if row else 0
            
            result["checks"].append({
                "name": "슬로우 쿼리",
                "status": "normal" if slow_queries < 100 else "warning",
                "value": f"{slow_queries:,}개",
                "detail": "누적"
            })
            
            # 6. 테이블 잠금 대기
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Table_locks_waited'")
            row = cursor.fetchone()
            locks_waited = int(row['Value']) if row else 0
            
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Table_locks_immediate'")
            row = cursor.fetchone()
            locks_immediate = int(row['Value']) if row else 1
            
            if locks_immediate > 0:
                lock_ratio = (locks_waited / (locks_waited + locks_immediate)) * 100
            else:
                lock_ratio = 0
            
            if lock_ratio < 1:
                lock_status = "normal"
            elif lock_ratio < 5:
                lock_status = "warning"
            else:
                lock_status = "error"
            
            result["checks"].append({
                "name": "잠금 대기율",
                "status": lock_status,
                "value": f"{lock_ratio:.2f}%",
                "detail": f"대기: {locks_waited:,}"
            })
            
            if lock_status != "normal":
                result["issues"].append(f"잠금 대기율: {lock_ratio:.2f}%")
            
            conn.close()
            
        except Exception as e:
            result["checks"].append({
                "name": "연결 상태",
                "status": "error",
                "value": "연결 실패",
                "detail": str(e)
            })
            result["issues"].append(f"서버 연결 실패: {str(e)}")
            result["status"] = "error"
            return result
        
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
                SELECT 
                    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
                FROM information_schema.tables
                WHERE table_schema = '{database}'
            """)
            row = cursor.fetchone()
            size_mb = row['size_mb'] if row else 0
            
            result["checks"].append({
                "name": "DB 크기",
                "status": "normal",
                "value": f"{int(size_mb or 0):,} MB",
                "detail": "-"
            })
            
            # 3. 테이블 수
            cursor.execute(f"""
                SELECT COUNT(*) AS cnt
                FROM information_schema.tables
                WHERE table_schema = '{database}'
                  AND table_type = 'BASE TABLE'
            """)
            row = cursor.fetchone()
            table_count = row['cnt'] if row else 0
            
            result["checks"].append({
                "name": "테이블 수",
                "status": "normal",
                "value": f"{table_count}개",
                "detail": "-"
            })
            
            # 4. 단편화된 테이블
            try:
                cursor.execute(f"""
                    SELECT COUNT(*) AS cnt
                    FROM information_schema.tables
                    WHERE table_schema = '{database}'
                      AND data_free > data_length * 0.1
                      AND data_length > 10485760
                """)
                row = cursor.fetchone()
                fragmented = row['cnt'] if row else 0
                
                if fragmented == 0:
                    frag_status = "normal"
                elif fragmented <= 3:
                    frag_status = "warning"
                else:
                    frag_status = "error"
                
                result["checks"].append({
                    "name": "단편화 테이블",
                    "status": frag_status,
                    "value": f"{fragmented}개",
                    "detail": "OPTIMIZE TABLE 권장"
                })
                
                if frag_status != "normal":
                    result["issues"].append(f"단편화된 테이블: {fragmented}개")
            except:
                pass
            
            # 5. 엔진 타입
            try:
                cursor.execute(f"""
                    SELECT engine, COUNT(*) AS cnt
                    FROM information_schema.tables
                    WHERE table_schema = '{database}'
                      AND table_type = 'BASE TABLE'
                    GROUP BY engine
                """)
                engines = []
                for row in cursor.fetchall():
                    engines.append(f"{row['engine']}({row['cnt']})")
                
                result["checks"].append({
                    "name": "스토리지 엔진",
                    "status": "normal",
                    "value": ", ".join(engines) if engines else "N/A",
                    "detail": "-"
                })
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