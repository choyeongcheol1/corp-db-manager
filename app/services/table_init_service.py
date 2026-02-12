# app/services/table_init_service.py
"""
테이블 초기화 서비스 (법인코드 치환 포함)
- 단일 테이블 초기화 (소스 → 타겟)
- 테이블 데이터 삭제
- 메인 DB에서 법인코드 조회
- 법인코드 자동 치환
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
class CorpInfo:
    """법인 정보"""
    corp_code: str
    corp_name: str
    biz_no: Optional[str] = None
    acc_db_name: Optional[str] = None


@dataclass
class TableInfo:
    """테이블 정보"""
    table_name: str
    row_count: int = 0
    corp_code_column: Optional[str] = None
    has_identity: bool = False
    description: Optional[str] = None


@dataclass
class ColumnInfo:
    """컬럼 정보"""
    column_name: str
    data_type: str
    is_nullable: bool = True


@dataclass
class InitResult:
    """초기화 결과"""
    success: bool
    table_name: str
    source_db: str
    target_db: str
    source_corp_code: str
    target_corp_code: str
    rows_copied: int = 0
    rows_replaced: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None


@dataclass
class DeleteResult:
    """삭제 결과"""
    success: bool
    table_name: str
    target_db: str
    rows_deleted: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None


# ============================================================
# 테이블 초기화 서비스
# ============================================================

class TableInitService:
    """테이블 초기화 서비스"""
    
    # 법인코드 컬럼 후보 목록
    CORP_CODE_COLUMNS = ['CORP_CD', 'COMPANY_CD', 'CO_CD', 'CMPNY_CD', 'CORP_CODE']
    
    def __init__(self, db: Session):
        self.db = db
        self._settings = None
    
    def _get_config_value(self, key: str, default: str = "") -> str:
        """설정값 조회"""
        from app.core.database import SystemConfig
        config = self.db.query(SystemConfig).filter(SystemConfig.config_key == key).first()
        return config.config_value if config else default
    
    def _get_settings(self) -> Dict[str, Any]:
        """설정 조회 (캐싱)"""
        if self._settings is None:
            server_id = self._get_config_value("main_db_server_id", "")
            self._settings = {
                "main_db_server_id": int(server_id) if server_id else None,
                "main_db_name": self._get_config_value("main_db_name", ""),
                "corp_table_name": self._get_config_value("corp_table_name", "COMS_CMPNY"),
                "corp_code_column": self._get_config_value("corp_code_column", "CORP_CD"),
                "corp_name_column": self._get_config_value("corp_name_column", "CORP_NM"),
                "biz_no_column": self._get_config_value("biz_no_column", "SAUPNO"),
                "acc_db_name_column": self._get_config_value("acc_db_name_column", "ACC_DB_NAME"),
            }
        return self._settings
    
    def _get_driver(self, server_id: int):
        """서버별 DB 드라이버 반환"""
        from app.core.database import DBServer
        from app.services.drivers import get_driver
        
        server = self.db.query(DBServer).filter(DBServer.id == server_id).first()
        
        if not server:
            raise ValueError(f"서버를 찾을 수 없습니다: {server_id}")
        
        return get_driver(server)
    
    # --------------------------------------------------------
    # 법인 정보 조회
    # --------------------------------------------------------
    
    def get_corp_info_by_db_name(self, db_name: str) -> Optional[CorpInfo]:
        """
        회계DB명(ACC_DB_NAME) 기준으로 메인 DB에서 법인 정보 조회
        """
        try:
            settings = self._get_settings()
            
            main_server_id = settings.get('main_db_server_id')
            main_db_name = settings.get('main_db_name')
            corp_table = settings.get('corp_table_name', 'COMS_CMPNY')
            
            if not main_server_id or not main_db_name:
                logger.warning("메인 DB 설정이 없습니다.")
                return None
            
            corp_code_col = settings.get('corp_code_column', 'CORP_CD')
            corp_name_col = settings.get('corp_name_column', 'CORP_NM')
            biz_no_col = settings.get('biz_no_column', 'SAUPNO')
            acc_db_col = settings.get('acc_db_name_column', 'ACC_DB_NAME')
            
            driver = self._get_driver(int(main_server_id))
            
            query = f"""
                SELECT [{corp_code_col}] AS corp_code,
                       [{corp_name_col}] AS corp_name,
                       [{biz_no_col}] AS biz_no,
                       [{acc_db_col}] AS acc_db_name
                FROM [{main_db_name}].dbo.[{corp_table}]
                WHERE [{acc_db_col}] = ?
            """
            
            result = driver.execute_query(query, (db_name,), main_db_name)
            
            if result and len(result) > 0:
                row = result[0]
                return CorpInfo(
                    corp_code=str(row.get('corp_code', '')),
                    corp_name=str(row.get('corp_name', '')),
                    biz_no=str(row.get('biz_no')) if row.get('biz_no') else None,
                    acc_db_name=str(row.get('acc_db_name')) if row.get('acc_db_name') else None
                )
            
            return None
            
        except Exception as e:
            logger.error(f"법인 정보 조회 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    # --------------------------------------------------------
    # 테이블 목록 조회
    # --------------------------------------------------------
    
    def get_tables(self, server_id: int, db_name: str) -> List[TableInfo]:
        """
        DB의 테이블 목록 + 법인코드 컬럼 정보 + 테이블 설명
        """
        try:
            driver = self._get_driver(server_id)
            
            query = f"""
                SELECT 
                    t.TABLE_NAME,
                    (SELECT SUM(p.rows) 
                     FROM [{db_name}].sys.partitions p 
                     WHERE p.object_id = OBJECT_ID('[{db_name}].dbo.' + QUOTENAME(t.TABLE_NAME)) 
                     AND p.index_id IN (0,1)) AS row_count,
                    (SELECT TOP 1 c.COLUMN_NAME 
                     FROM [{db_name}].INFORMATION_SCHEMA.COLUMNS c
                     WHERE c.TABLE_NAME = t.TABLE_NAME
                     AND c.COLUMN_NAME IN ({','.join(["'" + col + "'" for col in self.CORP_CODE_COLUMNS])})
                    ) AS corp_code_column,
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
            
            tables = []
            for row in result:
                tables.append(TableInfo(
                    table_name=row['TABLE_NAME'],
                    row_count=row.get('row_count') or 0,
                    corp_code_column=row.get('corp_code_column'),
                    description=row.get('table_description')
                ))
            
            return tables
            
        except Exception as e:
            logger.error(f"테이블 목록 조회 실패: {e}")
            return []
    
    # --------------------------------------------------------
    # 테이블 컬럼 목록 조회
    # --------------------------------------------------------
    
    def get_table_columns(self, server_id: int, db_name: str, table_name: str) -> List[ColumnInfo]:
        """
        테이블의 컬럼 목록 조회
        """
        try:
            driver = self._get_driver(server_id)
            
            query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE
                FROM [{db_name}].INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            
            result = driver.execute_query(query, (table_name,))
            
            columns = []
            for row in result:
                columns.append(ColumnInfo(
                    column_name=row['COLUMN_NAME'],
                    data_type=row['DATA_TYPE'],
                    is_nullable=row['IS_NULLABLE'] == 'YES'
                ))
            
            return columns
            
        except Exception as e:
            logger.error(f"컬럼 목록 조회 실패: {e}")
            return []
    
    def get_table_info(self, server_id: int, db_name: str, table_name: str) -> Optional[TableInfo]:
        """
        단일 테이블 정보 조회
        """
        try:
            driver = self._get_driver(server_id)
            
            # 행 수 조회
            count_query = f"SELECT COUNT(*) AS cnt FROM [{db_name}].dbo.[{table_name}]"
            count_result = driver.execute_query(count_query)
            row_count = count_result[0]['cnt'] if count_result else 0
            
            # 법인코드 컬럼 확인
            column_query = f"""
                SELECT COLUMN_NAME
                FROM [{db_name}].INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                AND COLUMN_NAME IN ({','.join(['?' for _ in self.CORP_CODE_COLUMNS])})
            """
            params = [table_name] + self.CORP_CODE_COLUMNS
            column_result = driver.execute_query(column_query, tuple(params))
            
            corp_code_column = None
            if column_result and len(column_result) > 0:
                corp_code_column = column_result[0]['COLUMN_NAME']
            
            # Identity 확인
            identity_query = f"""
                SELECT COUNT(*) AS cnt
                FROM [{db_name}].sys.identity_columns ic
                JOIN [{db_name}].sys.tables t ON ic.object_id = t.object_id
                WHERE t.name = ?
            """
            identity_result = driver.execute_query(identity_query, (table_name,))
            has_identity = identity_result[0]['cnt'] > 0 if identity_result else False
            
            # 테이블 설명 조회
            desc_query = f"""
                SELECT CAST(ep.value AS NVARCHAR(500)) AS table_description
                FROM [{db_name}].sys.extended_properties ep
                JOIN [{db_name}].sys.tables st ON ep.major_id = st.object_id
                WHERE st.name = ?
                AND ep.minor_id = 0
                AND ep.name = 'MS_Description'
            """
            desc_result = driver.execute_query(desc_query, (table_name,))
            description = desc_result[0]['table_description'] if desc_result else None
            
            return TableInfo(
                table_name=table_name,
                row_count=row_count,
                corp_code_column=corp_code_column,
                has_identity=has_identity,
                description=description
            )
            
        except Exception as e:
            logger.error(f"테이블 정보 조회 실패: {e}")
            return None
    
    # --------------------------------------------------------
    # 단일 테이블 초기화 (INSERT)
    # --------------------------------------------------------
    
    def init_table(
        self,
        source_server_id: int,
        source_db_name: str,
        target_server_id: int,
        target_db_name: str,
        table_name: str,
        source_corp_code: str,
        target_corp_code: str,
        corp_code_column: Optional[str] = None,
        truncate_before_copy: bool = True,
        replace_corp_code: bool = True,
        keep_identity: bool = False
    ) -> InitResult:
        """
        단일 테이블 초기화 (소스 → 타겟, 법인코드 치환)
        """
        start_time = time.time()
        
        try:
            source_driver = self._get_driver(source_server_id)
            target_driver = self._get_driver(target_server_id)
            
            # 1. TRUNCATE (옵션)
            if truncate_before_copy:
                truncate_sql = f"TRUNCATE TABLE [{target_db_name}].dbo.[{table_name}]"
                target_driver.execute_non_query(truncate_sql)
            
            # 2. 컬럼 목록 조회
            column_query = f"""
                SELECT COLUMN_NAME
                FROM [{source_db_name}].INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            columns_result = source_driver.execute_query(column_query, (table_name,))
            columns = [row['COLUMN_NAME'] for row in columns_result]
            
            if not columns:
                raise ValueError(f"테이블 컬럼을 조회할 수 없습니다: {table_name}")
            
            # 3. SELECT 쿼리 생성 (법인코드 치환)
            select_columns = []
            for col in columns:
                if (replace_corp_code and 
                    corp_code_column and 
                    col.upper() == corp_code_column.upper() and
                    source_corp_code and target_corp_code):
                    select_columns.append(
                        f"REPLACE([{col}], '{source_corp_code}', '{target_corp_code}') AS [{col}]"
                    )
                else:
                    select_columns.append(f"[{col}]")
            
            # 4. 데이터 조회
            select_sql = f"""
                SELECT {', '.join(select_columns)}
                FROM [{source_db_name}].dbo.[{table_name}]
            """
            rows = source_driver.execute_query(select_sql)
            
            if not rows:
                return InitResult(
                    success=True,
                    table_name=table_name,
                    source_db=source_db_name,
                    target_db=target_db_name,
                    source_corp_code=source_corp_code,
                    target_corp_code=target_corp_code,
                    rows_copied=0,
                    rows_replaced=0,
                    elapsed_seconds=time.time() - start_time
                )
            
            # 5. Identity INSERT 설정
            if keep_identity:
                target_driver.execute_non_query(
                    f"SET IDENTITY_INSERT [{target_db_name}].dbo.[{table_name}] ON"
                )
            
            # 6. INSERT 실행 (배치)
            column_list = ", ".join([f"[{c}]" for c in columns])
            placeholders = ", ".join(["?" for _ in columns])
            insert_sql = f"""
                INSERT INTO [{target_db_name}].dbo.[{table_name}] ({column_list})
                VALUES ({placeholders})
            """
            
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                for row in batch:
                    values = tuple(row.values())
                    target_driver.execute_non_query(insert_sql, values)
            
            # 7. Identity INSERT 해제
            if keep_identity:
                target_driver.execute_non_query(
                    f"SET IDENTITY_INSERT [{target_db_name}].dbo.[{table_name}] OFF"
                )
            
            # 8. 치환 건수
            rows_replaced = len(rows) if (replace_corp_code and corp_code_column and source_corp_code and target_corp_code) else 0
            
            return InitResult(
                success=True,
                table_name=table_name,
                source_db=source_db_name,
                target_db=target_db_name,
                source_corp_code=source_corp_code,
                target_corp_code=target_corp_code,
                rows_copied=len(rows),
                rows_replaced=rows_replaced,
                elapsed_seconds=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"테이블 초기화 실패 ({table_name}): {e}")
            return InitResult(
                success=False,
                table_name=table_name,
                source_db=source_db_name,
                target_db=target_db_name,
                source_corp_code=source_corp_code,
                target_corp_code=target_corp_code,
                elapsed_seconds=time.time() - start_time,
                error_message=str(e)
            )
    
    # --------------------------------------------------------
    # 테이블 데이터 삭제 (DELETE)
    # --------------------------------------------------------
    
    def delete_table_data(
        self,
        target_server_id: int,
        target_db_name: str,
        table_name: str,
        corp_code: Optional[str] = None,
        corp_code_column: Optional[str] = None
    ) -> DeleteResult:
        """
        테이블 데이터 삭제 (법인코드 기준 또는 전체)
        """
        start_time = time.time()
        
        try:
            target_driver = self._get_driver(target_server_id)
            
            # 삭제 전 건수 확인
            if corp_code and corp_code_column:
                count_sql = f"""
                    SELECT COUNT(*) AS cnt 
                    FROM [{target_db_name}].dbo.[{table_name}]
                    WHERE [{corp_code_column}] = ?
                """
                count_result = target_driver.execute_query(count_sql, (corp_code,))
            else:
                count_sql = f"SELECT COUNT(*) AS cnt FROM [{target_db_name}].dbo.[{table_name}]"
                count_result = target_driver.execute_query(count_sql)
            
            rows_to_delete = count_result[0]['cnt'] if count_result else 0
            
            # DELETE 실행
            if corp_code and corp_code_column:
                delete_sql = f"""
                    DELETE FROM [{target_db_name}].dbo.[{table_name}]
                    WHERE [{corp_code_column}] = ?
                """
                target_driver.execute_non_query(delete_sql, (corp_code,))
            else:
                delete_sql = f"TRUNCATE TABLE [{target_db_name}].dbo.[{table_name}]"
                target_driver.execute_non_query(delete_sql)
            
            return DeleteResult(
                success=True,
                table_name=table_name,
                target_db=target_db_name,
                rows_deleted=rows_to_delete,
                elapsed_seconds=time.time() - start_time
            )
            
        except Exception as e:
            logger.error(f"테이블 삭제 실패 ({table_name}): {e}")
            return DeleteResult(
                success=False,
                table_name=table_name,
                target_db=target_db_name,
                elapsed_seconds=time.time() - start_time,
                error_message=str(e)
            )