# app/services/db_sync_service.py
"""
DB 동기화 서비스 (Linked Server 방식 - 기존 설정 사용)
- SSMS에서 미리 설정된 Linked Server를 선택하여 사용
- 타겟 서버에서 INSERT INTO SELECT 실행 (성능 최적)
- 수백만 건 대량 데이터 대응
- TRUNCATE 후 INSERT 옵션
- Identity 값 유지 옵션
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import time
import logging

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ============================================================
# 데이터 클래스
# ============================================================

@dataclass
class LinkedServerInfo:
    """Linked Server 정보"""
    name: str
    data_source: str = ""
    provider: str = ""
    catalog: str = ""


@dataclass
class TableSyncInfo:
    """테이블 동기화 정보"""
    table_name: str
    row_count: int = 0
    has_identity: bool = False
    description: Optional[str] = None


@dataclass
class ColumnInfo:
    """컬럼 정보"""
    column_name: str
    data_type: str
    is_identity: bool = False


@dataclass
class SyncResult:
    """동기화 결과"""
    success: bool
    table_name: str
    source_db: str
    target_db: str
    linked_server_name: str = ""
    rows_affected: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None


# ============================================================
# DB 동기화 서비스
# ============================================================

class DbSyncService:
    """Linked Server 기반 DB 동기화 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _get_driver(self, server_id: int):
        """서버별 DB 드라이버 반환"""
        from app.core.database import DBServer
        from app.services.drivers import get_driver
        server = self.db.query(DBServer).filter(DBServer.id == server_id).first()
        if not server:
            raise ValueError(f"서버를 찾을 수 없습니다: {server_id}")
        return get_driver(server)
    
    # --------------------------------------------------------
    # Linked Server 조회
    # --------------------------------------------------------
    
    def get_linked_servers(self, server_id: int) -> List[LinkedServerInfo]:
        """서버에 등록된 Linked Server 목록 조회"""
        try:
            driver = self._get_driver(server_id)
            
            query = """
                SELECT 
                    s.name AS server_name,
                    s.data_source,
                    s.provider,
                    s.catalog
                FROM sys.servers s
                WHERE s.is_linked = 1
                ORDER BY s.name
            """
            result = driver.execute_query(query)
            
            return [
                LinkedServerInfo(
                    name=row['server_name'],
                    data_source=row.get('data_source', ''),
                    provider=row.get('provider', ''),
                    catalog=row.get('catalog', '')
                )
                for row in result
            ]
        except Exception as e:
            logger.error(f"Linked Server 목록 조회 실패: {e}")
            return []
    
    def test_linked_server(self, server_id: int, linked_server_name: str) -> Dict[str, Any]:
        """Linked Server 연결 테스트"""
        try:
            driver = self._get_driver(server_id)
            driver.execute_non_query(
                f"EXEC sp_testlinkedserver @servername = N'{linked_server_name}'"
            )
            return {"success": True, "message": "연결 성공"}
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    # --------------------------------------------------------
    # Linked Server를 통한 DB 목록 조회
    # --------------------------------------------------------
    
    def get_linked_server_databases(self, server_id: int, linked_server_name: str) -> List[str]:
        """Linked Server를 통해 원격 서버의 DB 목록 조회"""
        try:
            driver = self._get_driver(server_id)
            
            query = f"""
                SELECT name 
                FROM [{linked_server_name}].master.sys.databases 
                WHERE state = 0 
                  AND name NOT IN ('master', 'tempdb', 'model', 'msdb')
                ORDER BY name
            """
            result = driver.execute_query(query)
            return [row['name'] for row in result]
            
        except Exception as e:
            logger.error(f"Linked Server DB 목록 조회 실패: {e}")
            return []
    
    # --------------------------------------------------------
    # 테이블 목록 조회
    # --------------------------------------------------------
    
    def get_source_tables(
        self, server_id: int, linked_server_name: str, db_name: str
    ) -> List[TableSyncInfo]:
        """Linked Server를 통해 소스 DB의 테이블 목록 조회"""
        try:
            driver = self._get_driver(server_id)
            
            # Linked Server 경유 시 OBJECT_ID()가 동작하지 않으므로
            # sys.tables + sys.partitions 직접 조인
            query = f"""
                SELECT 
                    st.name AS TABLE_NAME,
                    SUM(p.rows) AS row_count,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM [{linked_server_name}].[{db_name}].sys.identity_columns ic
                        WHERE ic.object_id = st.object_id
                    ) THEN 1 ELSE 0 END AS has_identity,
                    (SELECT CAST(ep.value AS NVARCHAR(500))
                     FROM [{linked_server_name}].[{db_name}].sys.extended_properties ep
                     WHERE ep.major_id = st.object_id
                     AND ep.minor_id = 0
                     AND ep.name = 'MS_Description'
                    ) AS table_description
                FROM [{linked_server_name}].[{db_name}].sys.tables st
                LEFT JOIN [{linked_server_name}].[{db_name}].sys.partitions p
                    ON st.object_id = p.object_id AND p.index_id IN (0, 1)
                WHERE st.type = 'U'
                GROUP BY st.name, st.object_id
                ORDER BY st.name
            """
            result = driver.execute_query(query)
            
            tables = []
            for row in result:
                tables.append(TableSyncInfo(
                    table_name=row['TABLE_NAME'],
                    row_count=row.get('row_count') or 0,
                    has_identity=bool(row.get('has_identity', 0)),
                    description=row.get('table_description')
                ))
            return tables
            
        except Exception as e:
            logger.error(f"소스 테이블 목록 조회 실패: {e}")
            # Linked Server를 통한 sys 테이블 접근 실패 시 간단 조회
            try:
                driver = self._get_driver(server_id)
                fallback_query = f"""
                    SELECT TABLE_NAME
                    FROM [{linked_server_name}].[{db_name}].INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """
                result = driver.execute_query(fallback_query)
                return [
                    TableSyncInfo(table_name=row['TABLE_NAME'])
                    for row in result
                ]
            except Exception as e2:
                logger.error(f"소스 테이블 폴백 조회도 실패: {e2}")
                return []
    
    def get_target_tables(self, server_id: int, db_name: str) -> List[TableSyncInfo]:
        """타겟 서버의 로컬 DB 테이블 목록 조회"""
        try:
            driver = self._get_driver(server_id)
            
            query = f"""
                SELECT 
                    t.TABLE_NAME,
                    (SELECT SUM(p.rows) 
                     FROM [{db_name}].sys.partitions p 
                     WHERE p.object_id = OBJECT_ID('[{db_name}].dbo.' + QUOTENAME(t.TABLE_NAME)) 
                     AND p.index_id IN (0,1)) AS row_count,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM [{db_name}].sys.identity_columns ic
                        JOIN [{db_name}].sys.tables st ON ic.object_id = st.object_id
                        WHERE st.name = t.TABLE_NAME
                    ) THEN 1 ELSE 0 END AS has_identity,
                    (SELECT CAST(ep.value AS NVARCHAR(500))
                     FROM [{db_name}].sys.extended_properties ep
                     JOIN [{db_name}].sys.tables st ON ep.major_id = st.object_id
                     WHERE st.name = t.TABLE_NAME
                     AND ep.minor_id = 0
                     AND ep.name = 'MS_Description'
                    ) AS table_description
                FROM [{db_name}].INFORMATION_SCHEMA.TABLES t
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_NAME
            """
            result = driver.execute_query(query)
            
            return [
                TableSyncInfo(
                    table_name=row['TABLE_NAME'],
                    row_count=row.get('row_count') or 0,
                    has_identity=bool(row.get('has_identity', 0)),
                    description=row.get('table_description')
                )
                for row in result
            ]
        except Exception as e:
            logger.error(f"타겟 테이블 목록 조회 실패: {e}")
            return []
    
    # --------------------------------------------------------
    # 컬럼 목록 조회
    # --------------------------------------------------------
    
    def _get_target_columns(self, driver, db_name: str, table_name: str) -> List[ColumnInfo]:
        """타겟 테이블의 컬럼 목록 (Identity 여부 포함)"""
        query = f"""
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                CASE WHEN ic.object_id IS NOT NULL THEN 1 ELSE 0 END AS is_identity
            FROM [{db_name}].INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN [{db_name}].sys.identity_columns ic 
                ON ic.object_id = OBJECT_ID('[{db_name}].dbo.[{table_name}]')
                AND ic.name = c.COLUMN_NAME
            WHERE c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """
        result = driver.execute_query(query, (table_name,))
        
        return [
            ColumnInfo(
                column_name=row['COLUMN_NAME'],
                data_type=row['DATA_TYPE'],
                is_identity=bool(row.get('is_identity', 0))
            )
            for row in result
        ]
    
    # --------------------------------------------------------
    # 단일 테이블 동기화
    # --------------------------------------------------------
    
    def sync_table(
        self,
        target_server_id: int,
        target_db_name: str,
        linked_server_name: str,
        source_db_name: str,
        table_name: str,
        truncate_before: bool = True,
        keep_identity: bool = False
    ) -> SyncResult:
        """
        단일 테이블 Linked Server 동기화
        
        실행 위치: 타겟 서버
        INSERT INTO [타겟DB].dbo.[테이블]
        SELECT ... FROM [LinkedServer].[소스DB].dbo.[테이블]
        """
        start_time = time.time()
        
        try:
            target_driver = self._get_driver(target_server_id)
            
            # 1. 컬럼 목록 조회 (타겟 기준)
            columns = self._get_target_columns(target_driver, target_db_name, table_name)
            if not columns:
                raise ValueError(f"테이블 컬럼을 조회할 수 없습니다: {table_name}")
            
            has_identity = any(c.is_identity for c in columns)
            
            # INSERT 대상 컬럼 (Identity 유지 시 포함, 아니면 제외)
            if keep_identity or not has_identity:
                insert_columns = columns
            else:
                insert_columns = [c for c in columns if not c.is_identity]
            
            column_list = ", ".join([f"[{c.column_name}]" for c in insert_columns])
            
            # 2. TRUNCATE (옵션)
            if truncate_before:
                target_driver.execute_non_query(
                    f"TRUNCATE TABLE [{target_db_name}].dbo.[{table_name}]"
                )
                logger.info(f"TRUNCATE 완료: [{target_db_name}].dbo.[{table_name}]")
            
            # 3. IDENTITY_INSERT ON (옵션)
            if keep_identity and has_identity:
                target_driver.execute_non_query(
                    f"SET IDENTITY_INSERT [{target_db_name}].dbo.[{table_name}] ON"
                )
            
            # 4. INSERT INTO SELECT (핵심)
            insert_sql = f"""
                INSERT INTO [{target_db_name}].dbo.[{table_name}] ({column_list})
                SELECT {column_list}
                FROM [{linked_server_name}].[{source_db_name}].dbo.[{table_name}]
            """
            
            logger.info(f"동기화 시작: [{linked_server_name}].[{source_db_name}].dbo.[{table_name}] → [{target_db_name}].dbo.[{table_name}]")
            
            rows_affected = target_driver.execute_non_query(insert_sql)
            
            # execute_non_query가 행 수를 반환하지 않는 경우
            if rows_affected is None or rows_affected == 0:
                count_result = target_driver.execute_query(
                    f"SELECT COUNT(*) AS cnt FROM [{target_db_name}].dbo.[{table_name}]"
                )
                rows_affected = count_result[0]['cnt'] if count_result else 0
            
            logger.info(f"동기화 완료: {rows_affected}행")
            
            # 5. IDENTITY_INSERT OFF (옵션)
            if keep_identity and has_identity:
                try:
                    target_driver.execute_non_query(
                        f"SET IDENTITY_INSERT [{target_db_name}].dbo.[{table_name}] OFF"
                    )
                except Exception:
                    pass
            
            return SyncResult(
                success=True,
                table_name=table_name,
                source_db=source_db_name,
                target_db=target_db_name,
                linked_server_name=linked_server_name,
                rows_affected=rows_affected or 0,
                elapsed_seconds=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"테이블 동기화 실패 ({table_name}): {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            # Identity INSERT OFF 보장
            try:
                target_driver = self._get_driver(target_server_id)
                target_driver.execute_non_query(
                    f"SET IDENTITY_INSERT [{target_db_name}].dbo.[{table_name}] OFF"
                )
            except Exception:
                pass
            
            return SyncResult(
                success=False,
                table_name=table_name,
                source_db=source_db_name,
                target_db=target_db_name,
                linked_server_name=linked_server_name,
                elapsed_seconds=time.time() - start_time,
                error_message=str(e)
            )