"""
Oracle 드라이버
"""
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
import time
from app.services.drivers.base import BaseDriver
from app.core.database import DBServer
from app.config import get_settings

settings = get_settings()

# oracledb 임포트 (옵션)
try:
    import oracledb
    HAS_ORACLEDB = True
except ImportError:
    HAS_ORACLEDB = False


class OracleDriver(BaseDriver):
    """Oracle 드라이버"""
    
    def __init__(self, server: DBServer):
        super().__init__(server)
        if not HAS_ORACLEDB:
            raise ImportError("oracledb 패키지가 설치되지 않았습니다. pip install oracledb")
    
    # ============================================================
    # Connection Methods
    # ============================================================
    
    def get_connection(self, database: str = None) -> Any:
        """DB 연결 획득"""
        service_name = database or self.server.default_db or "ORCL"
        
        dsn = oracledb.makedsn(
            self.server.host,
            self.server.port,
            service_name=service_name
        )
        
        return oracledb.connect(
            user=self.server.username,
            password=self.server.password,
            dsn=dsn
        )
    
    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
        """연결 테스트"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
            version = cursor.fetchone()[0]
            conn.close()
            return True, "연결 성공", version
        except Exception as e:
            return False, f"연결 실패: {str(e)}", None
    
    # ============================================================
    # Database Methods
    # ============================================================
    
    def get_databases(self, prefix: str = None) -> List[Dict]:
        """스키마(사용자) 목록 조회"""
        prefix = prefix or settings.db_prefix
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if prefix:
                where_clause = f"username LIKE '{prefix.upper()}%'"
            else:
                where_clause = """
                    username NOT IN (
                        'SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                        'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                        'XDB', 'ANONYMOUS', 'ORDSYS', 'ORDDATA', 'ORDPLUGINS',
                        'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA',
                        'SPATIAL_WFS_ADMIN_USR', 'SPATIAL_CSW_ADMIN_USR',
                        'SYSMAN', 'MGMT_VIEW', 'APEX_PUBLIC_USER', 'FLOWS_FILES',
                        'APEX_030200', 'OWBSYS', 'OWBSYS_AUDIT'
                    )
                """
            
            cursor.execute(f"""
                SELECT 
                    u.username AS db_name,
                    u.created AS create_date,
                    u.account_status AS state,
                    NVL((SELECT SUM(bytes) / 1024 / 1024 
                         FROM dba_segments 
                         WHERE owner = u.username), 0) AS size_mb
                FROM dba_users u
                WHERE {where_clause}
                ORDER BY u.created DESC
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "db_name": row[0],
                    "create_date": row[1],
                    "state": row[2],
                    "size_mb": round(row[3] or 0, 2)
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Oracle 스키마 목록 조회 실패: {e}")
            return []
    
    def get_databases_with_disk_usage(self, prefix: str = None) -> List[Dict]:
        """스키마별 용량 + 테이블스페이스 사용률 조회"""
        prefix = prefix or settings.db_prefix
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if prefix:
                where_clause = f"username LIKE '{prefix.upper()}%'"
            else:
                where_clause = """
                    username NOT IN (
                        'SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                        'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                        'XDB', 'ANONYMOUS', 'ORDSYS', 'ORDDATA', 'ORDPLUGINS',
                        'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA',
                        'SPATIAL_WFS_ADMIN_USR', 'SPATIAL_CSW_ADMIN_USR',
                        'SYSMAN', 'MGMT_VIEW', 'APEX_PUBLIC_USER', 'FLOWS_FILES',
                        'APEX_030200', 'OWBSYS', 'OWBSYS_AUDIT'
                    )
                """
            
            # 스키마별 용량 + 기본 테이블스페이스 사용률
            cursor.execute(f"""
                SELECT 
                    u.username AS db_name,
                    u.created AS create_date,
                    u.account_status AS state,
                    NVL((SELECT SUM(bytes) / 1024 / 1024 
                         FROM dba_segments 
                         WHERE owner = u.username), 0) AS size_mb,
                    ts.total_gb,
                    ts.free_gb,
                    u.default_tablespace AS drive
                FROM dba_users u
                LEFT JOIN (
                    SELECT 
                        tablespace_name,
                        ROUND(SUM(bytes) / 1024 / 1024 / 1024, 1) AS total_gb,
                        ROUND(SUM(bytes - NVL(used_bytes, 0)) / 1024 / 1024 / 1024, 1) AS free_gb
                    FROM (
                        SELECT 
                            df.tablespace_name,
                            df.bytes,
                            (SELECT SUM(bytes) FROM dba_free_space fs 
                             WHERE fs.tablespace_name = df.tablespace_name 
                               AND fs.file_id = df.file_id) AS used_bytes
                        FROM dba_data_files df
                    )
                    GROUP BY tablespace_name
                ) ts ON u.default_tablespace = ts.tablespace_name
                WHERE {where_clause}
                ORDER BY size_mb DESC
            """)
            
            results = []
            for row in cursor.fetchall():
                size_mb = round(row[3] or 0, 2)
                total_gb = row[4] or 0
                free_gb = row[5] or 0
                disk_used_pct = 0
                db_disk_pct = 0
                
                if total_gb > 0:
                    disk_used_pct = round(((total_gb - free_gb) / total_gb) * 100, 1)
                    db_disk_pct = round((size_mb / 1024 / total_gb) * 100, 1)
                
                results.append({
                    "db_name": row[0],
                    "create_date": row[1],
                    "state": row[2],
                    "size_mb": size_mb,
                    "disk_total_gb": round(total_gb, 1),
                    "disk_free_gb": round(free_gb, 1),
                    "disk_used_pct": disk_used_pct,
                    "db_disk_pct": db_disk_pct,
                    "drive": str(row[6] or '')
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Oracle 디스크 사용률 조회 실패: {e}")
            return [dict(item, disk_total_gb=0, disk_free_gb=0, disk_used_pct=0, db_disk_pct=0, drive='')
                    for item in self.get_databases(prefix)]

    def get_tables(self, database: str) -> List[Dict]:
        """테이블 목록 조회"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 단순화된 쿼리 - dba_segments 조인 제거
            cursor.execute(f"""
                SELECT 
                    t.table_name,
                    NVL(t.num_rows, 0) AS row_count,
                    0 AS size_mb
                FROM all_tables t
                WHERE UPPER(t.owner) = UPPER('{database}')
                ORDER BY t.table_name
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "table_name": row[0],
                    "row_count": row[1] or 0,
                    "size_mb": round(row[2] or 0, 2)
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Oracle 테이블 목록 조회 실패: {e}")
            return []
    

    def get_table_columns(self, database: str, table_name: str) -> List[Dict]:
        """테이블 컬럼 정보 조회"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.data_length,
                    c.data_precision,
                    c.data_scale,
                    c.nullable,
                    CASE WHEN c.identity_column = 'YES' THEN 'YES' ELSE 'NO' END AS is_identity,
                    CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
                    NVL(c.data_default, '') AS default_value,
                    NVL(cc.comments, '') AS description
                FROM all_tab_columns c
                LEFT JOIN (
                    SELECT acc.column_name
                    FROM all_constraints ac
                    JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name
                    WHERE ac.table_name = UPPER('{table_name}')
                        AND ac.owner = UPPER('{database}')
                        AND ac.constraint_type = 'P'
                ) pk ON c.column_name = pk.column_name
                LEFT JOIN all_col_comments cc 
                    ON c.owner = cc.owner 
                    AND c.table_name = cc.table_name 
                    AND c.column_name = cc.column_name
                WHERE c.table_name = UPPER('{table_name}')
                    AND c.owner = UPPER('{database}')
                ORDER BY c.column_id
            """)
            
            results = []
            for row in cursor.fetchall():
                # 데이터 타입 포맷팅
                data_type = row[1]
                if data_type in ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR'):
                    data_type = f"{data_type}({row[2]})"
                elif data_type == 'NUMBER' and row[3]:
                    if row[4]:
                        data_type = f"NUMBER({row[3]},{row[4]})"
                    else:
                        data_type = f"NUMBER({row[3]})"
                
                results.append({
                    "column_name": row[0],
                    "data_type": data_type.upper(),
                    "max_length": row[2] or 0,
                    "is_nullable": row[5] == 'Y',
                    "is_identity": row[6] == 'YES',
                    "is_primary_key": bool(row[7]),
                    "default_value": str(row[8]).strip() if row[8] else '',
                    "description": row[9] or ''
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Oracle 테이블 컬럼 조회 실패: {e}")
            return []
        
    def get_db_size(self, database: str) -> float:
        """스키마 용량 조회 (MB)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT NVL(SUM(bytes) / 1024 / 1024, 0) AS size_mb
                FROM dba_segments
                WHERE owner = '{database.upper()}'
            """)
            
            row = cursor.fetchone()
            conn.close()
            return round(row[0] or 0, 2) if row else 0
            
        except Exception as e:
            print(f"Oracle 스키마 용량 조회 실패: {e}")
            return 0
    
    def create_database(self, db_name: str, data_path: str = None, log_path: str = None) -> bool:
        """스키마(사용자) 생성"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            default_password = f"{db_name}_pwd123"
            
            cursor.execute(f"""
                CREATE USER {db_name} IDENTIFIED BY "{default_password}"
                DEFAULT TABLESPACE USERS
                TEMPORARY TABLESPACE TEMP
                QUOTA UNLIMITED ON USERS
            """)
            
            cursor.execute(f"GRANT CONNECT, RESOURCE TO {db_name}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Oracle 스키마 생성 실패: {e}")
            return False
    
    # ============================================================
    # File Path Methods
    # ============================================================
    
    def _get_default_paths(self) -> Dict[str, str]:
        """기본 파일 경로"""
        return {
            "data_path": "/u01/app/oracle/oradata/",
            "log_path": "/u01/app/oracle/oradata/"
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
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 2. 인스턴스 상태
            try:
                cursor.execute("""
                    SELECT status, instance_name, host_name
                    FROM v$instance
                """)
                row = cursor.fetchone()
                if row:
                    inst_status = "normal" if row[0] == "OPEN" else "error"
                    result["checks"].append({
                        "name": "인스턴스 상태",
                        "status": inst_status,
                        "value": row[0],
                        "detail": f"{row[1]}@{row[2]}"
                    })
                    if inst_status != "normal":
                        result["issues"].append(f"인스턴스 상태 비정상: {row[0]}")
            except:
                pass
            
            # 3. 세션 수
            try:
                cursor.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM v$session WHERE type = 'USER') AS active_sessions,
                        (SELECT value FROM v$parameter WHERE name = 'sessions') AS max_sessions
                """)
                row = cursor.fetchone()
                if row:
                    active = row[0] or 0
                    max_sess = int(row[1]) if row[1] else 100
                    sess_percent = int((active / max_sess) * 100)
                    
                    if sess_percent < 70:
                        sess_status = "normal"
                    elif sess_percent < 90:
                        sess_status = "warning"
                    else:
                        sess_status = "error"
                    
                    result["checks"].append({
                        "name": "세션 수",
                        "status": sess_status,
                        "value": f"{active} / {max_sess}",
                        "detail": f"{sess_percent}% 사용"
                    })
                    
                    if sess_status != "normal":
                        result["issues"].append(f"세션 수 높음: {sess_percent}%")
            except:
                pass
            
            # 4. 테이블스페이스 용량
            try:
                cursor.execute("""
                    SELECT 
                        tablespace_name,
                        ROUND((used_space / total_space) * 100) AS used_percent,
                        ROUND(total_space / 1024, 1) AS total_gb,
                        ROUND((total_space - used_space) / 1024, 1) AS free_gb
                    FROM (
                        SELECT 
                            tablespace_name,
                            SUM(bytes) / 1024 / 1024 AS total_space,
                            SUM(bytes - NVL(free_bytes, 0)) / 1024 / 1024 AS used_space
                        FROM (
                            SELECT 
                                a.tablespace_name,
                                a.bytes,
                                b.free_bytes
                            FROM dba_data_files a
                            LEFT JOIN (
                                SELECT tablespace_name, file_id, SUM(bytes) AS free_bytes
                                FROM dba_free_space
                                GROUP BY tablespace_name, file_id
                            ) b ON a.tablespace_name = b.tablespace_name AND a.file_id = b.file_id
                        )
                        GROUP BY tablespace_name
                    )
                    WHERE total_space > 100
                    ORDER BY used_percent DESC
                """)
                
                for row in cursor.fetchall():
                    ts_name = row[0]
                    used_percent = row[1] or 0
                    
                    if used_percent < 80:
                        ts_status = "normal"
                    elif used_percent < 95:
                        ts_status = "warning"
                    else:
                        ts_status = "error"
                    
                    result["checks"].append({
                        "name": f"테이블스페이스 {ts_name}",
                        "status": ts_status,
                        "value": f"{used_percent}% 사용",
                        "detail": f"여유: {row[3]}GB / 전체: {row[2]}GB"
                    })
                    
                    if ts_status != "normal":
                        result["issues"].append(f"테이블스페이스 {ts_name} 용량 부족: {used_percent}%")
            except:
                pass
            
            # 5. 잠금 대기
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM v$lock 
                    WHERE request > 0
                """)
                row = cursor.fetchone()
                waiting = row[0] if row else 0
                
                if waiting == 0:
                    lock_status = "normal"
                elif waiting <= 5:
                    lock_status = "warning"
                else:
                    lock_status = "error"
                
                result["checks"].append({
                    "name": "잠금 대기",
                    "status": lock_status,
                    "value": f"{waiting}개",
                    "detail": "0개 권장"
                })
                
                if lock_status != "normal":
                    result["issues"].append(f"잠금 대기: {waiting}개")
            except:
                pass
            
            # 6. 장시간 실행 쿼리
            try:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM v$session s
                    JOIN v$sqlarea a ON s.sql_id = a.sql_id
                    WHERE s.status = 'ACTIVE'
                    AND s.type = 'USER'
                    AND s.last_call_et > 60
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
        """개별 스키마 상태 점검"""
        result = {
            "db_name": database,
            "status": "normal",
            "checks": [],
            "issues": []
        }
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 1. 사용자 상태
            cursor.execute(f"""
                SELECT account_status
                FROM dba_users
                WHERE username = '{database.upper()}'
            """)
            row = cursor.fetchone()
            user_status = row[0] if row else "UNKNOWN"
            
            status_check = "normal" if user_status == "OPEN" else "warning"
            result["checks"].append({
                "name": "계정 상태",
                "status": status_check,
                "value": user_status,
                "detail": "-"
            })
            
            if status_check != "normal":
                result["issues"].append(f"계정 상태: {user_status}")
            
            # 2. 스키마 크기
            cursor.execute(f"""
                SELECT NVL(SUM(bytes) / 1024 / 1024, 0) AS size_mb
                FROM dba_segments
                WHERE owner = '{database.upper()}'
            """)
            row = cursor.fetchone()
            size_mb = row[0] if row else 0
            
            result["checks"].append({
                "name": "스키마 크기",
                "status": "normal",
                "value": f"{int(size_mb):,} MB",
                "detail": "-"
            })
            
            # 3. 테이블 수
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM all_tables
                WHERE owner = '{database.upper()}'
            """)
            row = cursor.fetchone()
            table_count = row[0] if row else 0
            
            result["checks"].append({
                "name": "테이블 수",
                "status": "normal",
                "value": f"{table_count}개",
                "detail": "-"
            })
            
            # 4. 인덱스 상태
            try:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM all_indexes
                    WHERE owner = '{database.upper()}'
                    AND status != 'VALID'
                """)
                row = cursor.fetchone()
                invalid_idx = row[0] if row else 0
                
                if invalid_idx == 0:
                    idx_status = "normal"
                else:
                    idx_status = "warning"
                
                result["checks"].append({
                    "name": "인덱스 상태",
                    "status": idx_status,
                    "value": f"비정상 {invalid_idx}개",
                    "detail": "0개 권장"
                })
                
                if idx_status != "normal":
                    result["issues"].append(f"비정상 인덱스: {invalid_idx}개")
            except:
                pass
            
            # 5. 마지막 통계 수집
            try:
                cursor.execute(f"""
                    SELECT MIN(last_analyzed)
                    FROM all_tables
                    WHERE owner = '{database.upper()}'
                    AND num_rows > 0
                """)
                row = cursor.fetchone()
                last_analyzed = row[0] if row else None
                
                if last_analyzed:
                    days_ago = (datetime.now() - last_analyzed).days
                    
                    if days_ago <= 7:
                        stat_status = "normal"
                        stat_value = f"{days_ago}일 전"
                    elif days_ago <= 30:
                        stat_status = "warning"
                        stat_value = f"{days_ago}일 전"
                    else:
                        stat_status = "error"
                        stat_value = f"{days_ago}일 전"
                else:
                    stat_status = "warning"
                    stat_value = "기록 없음"
                
                result["checks"].append({
                    "name": "통계 수집",
                    "status": stat_status,
                    "value": stat_value,
                    "detail": str(last_analyzed)[:19] if last_analyzed else "-"
                })
                
                if stat_status != "normal":
                    result["issues"].append(f"통계 수집 필요: {stat_value}")
            except:
                pass
            
            conn.close()
            
        except Exception as e:
            result["checks"].append({
                "name": "스키마 상태",
                "status": "error",
                "value": "조회 실패",
                "detail": str(e)
            })
            result["issues"].append(f"스키마 점검 실패: {str(e)}")
            result["status"] = "error"
            return result
        
        # 최종 상태 결정
        statuses = [c["status"] for c in result["checks"]]
        if "error" in statuses:
            result["status"] = "error"
        elif "warning" in statuses:
            result["status"] = "warning"
        
        return result