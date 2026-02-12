"""
법인 DB 생성 서비스
"""
import pyodbc
import secrets
import string
import time
import re
from typing import Optional, List, Dict, Callable, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.database import DBServer, Corp, ActivityLog
from app.models import (
    CorpCreate, CorpInfo, CorpDetail, CreateDBRequest, CreateDBResult,
    DBStatus, TaskStatus
)
from app.services.server_service import ServerService
from app.services.sql_templates import SqlTemplateService, ConfigureDBResult
from app.config import get_settings, CLONE_TABLES

settings = get_settings()


class CorpService:
    """법인 DB 관리 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.server_service = ServerService(db)
    
    # ============================================================
    # CRUD Operations
    # ============================================================
    
    def get_all_corps(self, server_id: Optional[int] = None) -> List[Corp]:
        """법인 목록 조회"""
        query = self.db.query(Corp)
        if server_id:
            query = query.filter(Corp.server_id == server_id)
        return query.order_by(Corp.corp_code).all()
    
    def get_corp(self, corp_id: int) -> Optional[Corp]:
        """법인 정보 조회"""
        return self.db.query(Corp).filter(Corp.id == corp_id).first()
    
    def get_corp_by_code(self, corp_code: str) -> Optional[Corp]:
        """법인코드로 조회"""
        return self.db.query(Corp).filter(Corp.corp_code == corp_code).first()
    
    def search_corps(
        self, 
        keyword: str = None,
        server_id: int = None,
        status: str = None
    ) -> List[Corp]:
        """법인 검색"""
        query = self.db.query(Corp)
        
        if server_id:
            query = query.filter(Corp.server_id == server_id)
        
        if status:
            query = query.filter(Corp.status == status)
        
        if keyword:
            query = query.filter(
                (Corp.corp_code.contains(keyword)) |
                (Corp.corp_name.contains(keyword))
            )
        
        return query.order_by(Corp.corp_code).all()
    
    def validate_corp_code(self, corp_code: str) -> Tuple[bool, str]:
        """법인코드 유효성 검사"""
        if not corp_code:
            return False, "법인코드를 입력하세요"
        
        if len(corp_code) < 3:
            return False, "법인코드는 3자 이상이어야 합니다"
        
        if not corp_code.replace('_', '').isalnum():
            return False, "법인코드는 영문/숫자만 가능합니다"
        
        # 전체 서버에서 중복 체크 (메타 DB 기준)
        existing = self.get_corp_by_code(corp_code)
        if existing:
            return False, f"이미 존재하는 법인코드입니다: {corp_code}"
        
        return True, "사용 가능한 법인코드입니다"
    
    # ============================================================
    # Helper Methods
    # ============================================================
    
    def _generate_password(self, length: int = 16) -> str:
        """보안 비밀번호 생성"""
        alphabet = string.ascii_letters + string.digits + "!@#$%"
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    def _wait_for_db_ready(self, server: DBServer, db_name: str, timeout: int = 30) -> bool:
        """DB 생성 완료 대기 (ONLINE 상태 확인)"""
        start = time.time()
        while time.time() - start < timeout:
            try:
                conn = self.server_service.get_connection(server, "master")
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT state_desc FROM sys.databases WHERE name = '{db_name}'
                """)
                row = cursor.fetchone()
                conn.close()
                if row and row.state_desc == 'ONLINE':
                    return True
            except:
                pass
            time.sleep(1)
        raise Exception(f"DB '{db_name}' 생성 대기 시간 초과 ({timeout}초)")
    
    def _rollback_db(self, server: DBServer, db_name: str):
        """실패 시 생성된 DB 삭제"""
        try:
            conn = self.server_service.get_connection(server, "master")
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"""
                IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{db_name}')
                BEGIN
                    ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                    DROP DATABASE [{db_name}]
                END
            """)
            conn.close()
        except:
            pass
    
    def _log_activity(self, action: str, corp_code: str, corp_name: str,
                      server_id: int, user_id: int, status: str, message: str):
        """활동 로그 기록"""
        try:
            log = ActivityLog(
                action=action,
                target_type="CORP",
                target_id=corp_code,
                target_name=corp_name,
                server_id=server_id,
                user_id=user_id,
                status=status,
                message=message
            )
            self.db.add(log)
            self.db.commit()
        except:
            pass
    
    def _configure_db_options(self, server: DBServer, db_name: str) -> ConfigureDBResult:
        """
        DB 옵션 설정 (운영 표준)
        — SqlTemplateService.configure_database() 위임
        """
        return SqlTemplateService.configure_database(
            conn_func=lambda: self.server_service.get_connection(server, "master"),
            db_name=db_name
        )
    
    # ============================================================
    # DB Creation — 경로 A (API 직접 호출용)
    # ============================================================
    
    def create_corp_db(
        self,
        request: CreateDBRequest,
        user_id: int = None,
        progress_callback: Callable = None
    ) -> CreateDBResult:
        """신규 법인 DB 생성"""
        
        steps = []
        start_time = datetime.now()
        
        # 서버 정보 조회
        source_server = self.server_service.get_server(request.source_server_id)
        target_server = self.server_service.get_server(request.target_server_id)
        
        if not source_server or not target_server:
            return CreateDBResult(
                success=False,
                corp_code=request.corp_code,
                db_name="",
                server_name="",
                host="",
                port=0,
                db_user="",
                db_password="",
                connection_string="",
                elapsed_seconds=0,
                message="서버 정보를 찾을 수 없습니다"
            )
        
        # 변수 설정
        db_name = f"{settings.db_prefix}{request.corp_code}"
        login_name = f"{request.corp_code}_user"
        password = self._generate_password()
        
        same_server = (source_server.id == target_server.id)
        
        def update_progress(step: str, status: str, message: str = ""):
            step_info = {"step": step, "status": status, "message": message}
            steps.append(step_info)
            if progress_callback:
                progress_callback(step, status, message)
        
        try:
            # ============================================
            # STEP 1: 데이터베이스 생성
            # ============================================
            update_progress("DB 생성", "진행중", f"{db_name} 생성 중...")
            
            paths = self.server_service.get_file_paths(target_server, request.source_db_name if same_server else None)
            
            conn = self.server_service.get_connection(target_server, "master")
            conn.autocommit = True
            cursor = conn.cursor()
            
            create_sql = f"""
                CREATE DATABASE [{db_name}]
                ON PRIMARY (
                    NAME = N'{db_name}',
                    FILENAME = N'{paths["data_path"]}{db_name}.mdf',
                    SIZE = {settings.db_initial_size_mb}MB,
                    MAXSIZE = UNLIMITED,
                    FILEGROWTH = 100MB
                )
                LOG ON (
                    NAME = N'{db_name}_log',
                    FILENAME = N'{paths["log_path"]}{db_name}_log.ldf',
                    SIZE = {settings.db_log_size_mb}MB,
                    MAXSIZE = 10GB,
                    FILEGROWTH = 50MB
                )
                COLLATE {settings.db_collation}
            """
            cursor.execute(create_sql)
            conn.close()
            
            # DB 상태 확인
            self._wait_for_db_ready(target_server, db_name)
            
            update_progress("DB 생성", "완료", f"{db_name} 생성 완료")
            
            # ============================================
            # STEP 1.5: DB 옵션 설정
            # ============================================
            update_progress("옵션 설정", "진행중", "운영 기준 옵션 적용 중...")
            
            config_result = self._configure_db_options(target_server, db_name)

            if config_result.warnings:
                update_progress("옵션 설정", "완료",
                                f"옵션 설정 완료 (경고 {len(config_result.warnings)}건)")
            else:
                update_progress("옵션 설정", "완료", "옵션 설정 완료")
            
            # ============================================
            # STEP 2: 로그인/사용자 생성
            # ============================================
            update_progress("계정 생성", "진행중", f"{login_name} 생성 중...")
            
            conn = self.server_service.get_connection(target_server, "master")
            conn.autocommit = True
            cursor = conn.cursor()
            
            cursor.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '{login_name}')
                    CREATE LOGIN [{login_name}] WITH PASSWORD = N'{password}',
                    DEFAULT_DATABASE = [{db_name}], CHECK_POLICY = ON
            """)
            conn.close()
            
            conn = self.server_service.get_connection(target_server, db_name)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"CREATE USER [{login_name}] FOR LOGIN [{login_name}]")
            cursor.execute(f"ALTER ROLE db_datareader ADD MEMBER [{login_name}]")
            cursor.execute(f"ALTER ROLE db_datawriter ADD MEMBER [{login_name}]")
            cursor.execute(f"GRANT EXECUTE TO [{login_name}]")
            conn.close()
            
            update_progress("계정 생성", "완료", f"{login_name} 생성 완료")
            
            # ============================================
            # STEP 3: 스키마 복제
            # ============================================
            update_progress("스키마 복제", "진행중", "테이블 구조 복제 중...")
            
            if same_server:
                table_count = self._copy_schema_same_server(
                    target_server, request.source_db_name, db_name
                )
            else:
                table_count = self._copy_schema_cross_server(
                    source_server, request.source_db_name,
                    target_server, db_name
                )
            
            update_progress("스키마 복제", "완료", f"{table_count}개 테이블 복제 완료")
            
            # ============================================
            # STEP 3.5: 확장 속성(컬럼/테이블 설명) 복제
            # ============================================
            update_progress("설명 복제", "진행중", "테이블/컬럼 설명 복제 중...")
            
            try:
                if same_server:
                    ep_count = self._copy_extended_properties_same_server(
                        target_server, request.source_db_name, db_name
                    )
                else:
                    ep_count = self._copy_extended_properties_cross_server(
                        source_server, request.source_db_name,
                        target_server, db_name
                    )
                update_progress("설명 복제", "완료", f"{ep_count}개 설명 복제 완료")
            except Exception as e:
                update_progress("설명 복제", "완료", f"설명 복제 경고: {e}")
            
            # ============================================
            # STEP 4: 인덱스/PK 복제
            # ============================================
            update_progress("인덱스 복제", "진행중", "PK 및 인덱스 생성 중...")
            
            if same_server:
                self._copy_indexes_same_server(target_server, request.source_db_name, db_name)
            else:
                self._copy_indexes_cross_server(
                    source_server, request.source_db_name,
                    target_server, db_name
                )
            
            update_progress("인덱스 복제", "완료", "인덱스 생성 완료")
            
            # ============================================
            # STEP 5: 기초 데이터 복제
            # ============================================
            update_progress("데이터 복제", "진행중", "기초 데이터 복제 중...")
            
            if same_server:
                data_count = self._copy_data_same_server(
                    target_server, request.source_db_name, db_name
                )
            else:
                data_count = self._copy_data_cross_server(
                    source_server, request.source_db_name,
                    target_server, db_name
                )
            
            update_progress("데이터 복제", "완료", f"{data_count}개 테이블 데이터 복제 완료")
            
            # ============================================
            # STEP 6: 관리자 계정 생성
            # ============================================
            update_progress("관리자 생성", "진행중", "관리자 계정 생성 중...")
            
            conn = self.server_service.get_connection(target_server, db_name)
            conn.autocommit = True
            cursor = conn.cursor()
            
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TB_USER')
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM TB_USER WHERE USER_ID = 'admin')
                    BEGIN
                        INSERT INTO TB_USER (USER_ID, USER_NM, USER_PWD, ROLE_CD, USE_YN, REG_DT)
                        VALUES (
                            'admin', 
                            N'시스템관리자',
                            CONVERT(VARCHAR(256), HASHBYTES('SHA2_256', 'Admin@1234'), 2),
                            'ADMIN', 'Y', GETDATE()
                        )
                    END
                END
            """)
            conn.close()
            
            update_progress("관리자 생성", "완료", "admin / Admin@1234")
            
            # ============================================
            # STEP 7: 메타 DB 등록
            # ============================================
            update_progress("시스템 등록", "진행중", "법인 정보 등록 중...")
            
            corp = Corp(
                corp_code=request.corp_code,
                corp_name=request.corp_name,
                biz_no=request.biz_no,
                server_id=target_server.id,
                db_name=db_name,
                db_user=login_name,
                status=DBStatus.NORMAL.value
            )
            self.db.add(corp)
            
            # 활동 로그
            self._log_activity("CREATE", request.corp_code, request.corp_name,
                               target_server.id, user_id, "success",
                               f"법인 DB 생성 완료: {db_name}")
            
            update_progress("시스템 등록", "완료", "등록 완료")
            
            # 결과 반환
            elapsed = (datetime.now() - start_time).seconds
            conn_string = f"Server={target_server.host},{target_server.port};Database={db_name};User Id={login_name};Password={password};"
            
            return CreateDBResult(
                success=True,
                corp_code=request.corp_code,
                db_name=db_name,
                server_name=target_server.server_name,
                host=target_server.host,
                port=target_server.port,
                db_user=login_name,
                db_password=password,
                connection_string=conn_string,
                elapsed_seconds=elapsed,
                message="신규 법인 DB 생성 완료"
            )
            
        except Exception as e:
            update_progress("오류", "실패", str(e))
            
            # 롤백
            self._rollback_db(target_server, db_name)
            
            # 실패 로그
            self._log_activity("CREATE", request.corp_code, request.corp_name,
                               target_server.id, user_id, "failed", str(e))
            
            elapsed = (datetime.now() - start_time).seconds
            
            return CreateDBResult(
                success=False,
                corp_code=request.corp_code,
                db_name=db_name,
                server_name=target_server.server_name,
                host=target_server.host,
                port=target_server.port,
                db_user="",
                db_password="",
                connection_string="",
                elapsed_seconds=elapsed,
                message=f"생성 실패: {str(e)}"
            )
    
    # ============================================================
    # DB Creation — 경로 B (사용자 정의 SQL, UI에서 사용)
    # ============================================================
    
    def create_corp_db_with_sql(
        self,
        source_server: DBServer,
        source_db_name: str,
        target_server: DBServer,
        corp_code: str,
        corp_name: str,
        biz_no: str,
        custom_sql: str,
        db_password: str,
        user_id: int = None,
        target_db_name: str = None
    ) -> Dict:
        """사용자 정의 SQL로 법인 DB 생성"""
        
        # DB명 결정: 파라미터 > SQL에서 추출 > 기본값
        if target_db_name:
            db_name = target_db_name
        else:
            db_name = SqlTemplateService.extract_db_name_from_sql(custom_sql) or f"ACC_{corp_code}"
        
        login_name = f"{db_name}_user"
        password = db_password or self._generate_password()
        same_server = (source_server.id == target_server.id)
        start_time = datetime.now()
        errors = []
        warnings = []
        
        try:
            # ============================================
            # STEP 1: 데이터베이스 생성 (사용자 SQL)
            # ============================================
            conn = self.server_service.get_connection(target_server, "master")
            conn.autocommit = True
            cursor = conn.cursor()
            
            statements = SqlTemplateService.parse_sql_statements(custom_sql)
            for stmt in statements:
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    errors.append(f"SQL 실행 실패: {e}")
            conn.close()
            
            # DB 생성 실패 시 즉시 반환
            if errors:
                self._log_activity("CREATE", corp_code, corp_name,
                                   target_server.id, user_id, "failed",
                                   "; ".join(errors))
                return {
                    "success": False,
                    "error": errors[0],
                    "errors": errors
                }
            
            # DB 상태 확인 (time.sleep 대체)
            self._wait_for_db_ready(target_server, db_name)
            
            # ============================================
            # STEP 2: DB 옵션 설정 (운영 기준)
            # ============================================
            config_result = self._configure_db_options(target_server, db_name)
            if config_result.warnings:
                warnings.extend(config_result.warnings)
            if config_result.error:
                warnings.append(f"옵션 설정 오류: {config_result.error}")
            
            # ============================================
            # STEP 3: 로그인 생성 (master에서)
            # ============================================
            try:
                conn = self.server_service.get_connection(target_server, "master")
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(f"""
                    IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '{login_name}')
                    BEGIN
                        CREATE LOGIN [{login_name}] WITH PASSWORD = N'{password}',
                        DEFAULT_DATABASE = [{db_name}], CHECK_POLICY = ON
                    END
                """)
                conn.close()
            except Exception as e:
                errors.append(f"로그인 생성 실패: {e}")
            
            # ============================================
            # STEP 4: DB 사용자 생성
            # ============================================
            try:
                conn = self.server_service.get_connection(target_server, db_name)
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(f"""
                    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '{login_name}')
                    BEGIN
                        CREATE USER [{login_name}] FOR LOGIN [{login_name}]
                    END
                """)
                cursor.execute(f"""
                    IF IS_ROLEMEMBER('db_owner', '{login_name}') = 0
                    BEGIN
                        ALTER ROLE db_owner ADD MEMBER [{login_name}]
                    END
                """)
                conn.close()
            except Exception as e:
                errors.append(f"사용자 생성 실패: {e}")
            
            # 계정 생성 실패 시 롤백
            if errors:
                self._rollback_db(target_server, db_name)
                self._log_activity("CREATE", corp_code, corp_name,
                                   target_server.id, user_id, "failed",
                                   "; ".join(errors))
                return {
                    "success": False,
                    "error": errors[0],
                    "errors": errors
                }
            
            # ============================================
            # STEP 5: 스키마 복제
            # ============================================
            table_count = 0
            try:
                if same_server:
                    table_count = self._copy_schema_same_server(
                        target_server, source_db_name, db_name)
                else:
                    table_count = self._copy_schema_cross_server(
                        source_server, source_db_name, target_server, db_name)
            except Exception as e:
                errors.append(f"스키마 복제 실패: {e}")
            
            # 스키마 복제 실패 시 롤백 (핵심 단계)
            if errors:
                self._rollback_db(target_server, db_name)
                self._log_activity("CREATE", corp_code, corp_name,
                                   target_server.id, user_id, "failed",
                                   "; ".join(errors))
                return {
                    "success": False,
                    "error": errors[0],
                    "errors": errors
                }
            
            # ============================================
            # STEP 5.5: 확장 속성(컬럼/테이블 설명) 복제 (비치명적)
            # ============================================
            ep_count = 0
            try:
                if same_server:
                    ep_count = self._copy_extended_properties_same_server(
                        target_server, source_db_name, db_name)
                else:
                    ep_count = self._copy_extended_properties_cross_server(
                        source_server, source_db_name, target_server, db_name)
                if ep_count > 0:
                    print(f"  ✅ 확장 속성 {ep_count}개 복제 완료")
            except Exception as e:
                warnings.append(f"확장 속성 복제 경고: {e}")
            
            # ============================================
            # STEP 6: 인덱스/PK 복제 (비치명적)
            # ============================================
            try:
                if same_server:
                    self._copy_indexes_same_server(
                        target_server, source_db_name, db_name)
                else:
                    self._copy_indexes_cross_server(
                        source_server, source_db_name, target_server, db_name)
            except Exception as e:
                warnings.append(f"인덱스 복제 경고: {e}")
            
            # ============================================
            # STEP 7: 기초 데이터 복제 (비치명적)
            # ============================================
            data_count = 0
            try:
                if same_server:
                    data_count = self._copy_data_same_server(
                        target_server, source_db_name, db_name)
                else:
                    data_count = self._copy_data_cross_server(
                        source_server, source_db_name, target_server, db_name)
            except Exception as e:
                warnings.append(f"데이터 복제 경고: {e}")
            
            # ============================================
            # STEP 8: 관리자 계정 생성 (비치명적)
            # ============================================
            try:
                conn = self.server_service.get_connection(target_server, db_name)
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("""
                    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TB_USER')
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM TB_USER WHERE USER_ID = 'admin')
                        BEGIN
                            INSERT INTO TB_USER (USER_ID, USER_NM, USER_PWD, ROLE_CD, USE_YN, REG_DT)
                            VALUES (
                                'admin',
                                N'시스템관리자',
                                CONVERT(VARCHAR(256), HASHBYTES('SHA2_256', 'Admin@1234'), 2),
                                'ADMIN', 'Y', GETDATE()
                            )
                        END
                    END
                """)
                conn.close()
            except Exception as e:
                warnings.append(f"관리자 생성 경고: {e}")
            
            # ============================================
            # STEP 9: 메타 DB 등록
            # ============================================
            try:
                existing = self.get_corp_by_code(corp_code)
                if not existing:
                    corp = Corp(
                        corp_code=corp_code,
                        corp_name=corp_name,
                        biz_no=biz_no,
                        server_id=target_server.id,
                        db_name=db_name,
                        db_user=login_name,
                        status=DBStatus.NORMAL.value
                    )
                    self.db.add(corp)
                    self.db.commit()
            except Exception as e:
                warnings.append(f"메타 DB 등록 경고: {e}")
            
            # 성공 결과 반환
            elapsed = (datetime.now() - start_time).seconds
            
            self._log_activity("CREATE", corp_code, corp_name,
                               target_server.id, user_id, "success",
                               f"법인 DB 생성 완료: {db_name} ({table_count}개 테이블, {ep_count}개 설명)")
            
            return {
                "success": True,
                "db_name": db_name,
                "db_user": login_name,
                "db_password": password,
                "server_name": target_server.server_name,
                "table_count": table_count,
                "data_count": data_count,
                "ep_count": ep_count,
                "elapsed_seconds": elapsed,
                "warnings": warnings,
                "config_verification": config_result.verification,
                "message": f"DB 생성 완료 ({table_count}개 테이블, {ep_count}개 설명 복제)"
            }
        
        except Exception as e:
            # 최상위 예외 — 롤백
            self._rollback_db(target_server, db_name)
            elapsed = (datetime.now() - start_time).seconds
            self._log_activity("CREATE", corp_code, corp_name,
                               target_server.id, user_id, "failed", str(e))
            return {
                "success": False,
                "error": f"생성 실패: {str(e)}",
                "elapsed_seconds": elapsed
            }
    
    # ============================================================
    # Schema Copy Methods
    # ============================================================
    
    def _copy_schema_same_server(self, server: DBServer, source_db: str, target_db: str) -> int:
        """동일 서버 스키마 복제"""
        conn = self.server_service.get_connection(server, source_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'
        """)
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        conn.close()
        
        conn = self.server_service.get_connection(server, "master")
        conn.autocommit = True
        cursor = conn.cursor()
        
        count = 0
        for table in tables:
            try:
                cursor.execute(f"""
                    SELECT * INTO [{target_db}].dbo.[{table}]
                    FROM [{source_db}].dbo.[{table}] WHERE 1=0
                """)
                count += 1
            except:
                pass
        
        conn.close()
        return count
    
    def _copy_schema_cross_server(
        self, 
        source_server: DBServer, source_db: str,
        target_server: DBServer, target_db: str
    ) -> int:
        """크로스 서버 스키마 복제"""
        # 소스에서 테이블 구조 조회
        source_conn = self.server_service.get_connection(source_server, source_db)
        cursor = source_conn.cursor()
        
        cursor.execute("""
            SELECT 
                t.name AS table_name,
                c.name AS column_name,
                tp.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                IDENT_SEED(t.name) AS seed_value,
                IDENT_INCR(t.name) AS increment_value,
                c.column_id
            FROM sys.tables t
            JOIN sys.columns c ON t.object_id = c.object_id
            JOIN sys.types tp ON c.user_type_id = tp.user_type_id
            WHERE t.is_ms_shipped = 0
            ORDER BY t.name, c.column_id
        """)
        
        columns_data = cursor.fetchall()
        source_conn.close()
        
        # 테이블별 그룹화
        tables = {}
        for row in columns_data:
            if row.table_name not in tables:
                tables[row.table_name] = []
            tables[row.table_name].append(row)
        
        # 타겟에 테이블 생성
        target_conn = self.server_service.get_connection(target_server, target_db)
        target_conn.autocommit = True
        cursor = target_conn.cursor()
        
        count = 0
        for table_name, columns in tables.items():
            try:
                col_defs = []
                for col in columns:
                    col_def = f"[{col.column_name}] {self._get_column_type(col)}"
                    
                    if col.is_identity:
                        seed = int(col.seed_value or 1)
                        incr = int(col.increment_value or 1)
                        col_def += f" IDENTITY({seed},{incr})"
                    
                    col_def += " NULL" if col.is_nullable else " NOT NULL"
                    col_defs.append(col_def)
                
                create_sql = f"CREATE TABLE [{table_name}] (\n  " + ",\n  ".join(col_defs) + "\n)"
                cursor.execute(create_sql)
                count += 1
            except Exception as e:
                print(f"테이블 생성 실패 [{table_name}]: {e}")
        
        target_conn.close()
        return count
    
    def _get_column_type(self, col) -> str:
        """컬럼 타입 문자열"""
        dtype = col.data_type
        
        if dtype in ('varchar', 'nvarchar', 'char', 'nchar'):
            length = col.max_length
            if dtype.startswith('n'):
                length = length // 2 if length > 0 else length
            if length == -1:
                return f"{dtype}(MAX)"
            return f"{dtype}({length})"
        elif dtype in ('decimal', 'numeric'):
            return f"{dtype}({col.precision},{col.scale})"
        elif dtype == 'varbinary' and col.max_length == -1:
            return "varbinary(MAX)"
        
        return dtype
    
    # ============================================================
    # Extended Properties Copy Methods (컬럼/테이블 설명 복제)
    # ============================================================
    
    def _copy_extended_properties_same_server(
        self, server: DBServer, source_db: str, target_db: str
    ) -> int:
        """동일 서버 확장 속성(테이블·컬럼 설명) 복제"""
        conn = self.server_service.get_connection(server, source_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT
                OBJECT_NAME(ep.major_id) AS table_name,
                ep.minor_id,
                COL_NAME(ep.major_id, ep.minor_id) AS column_name,
                ep.name AS property_name,
                CAST(ep.value AS NVARCHAR(4000)) AS property_value
            FROM sys.extended_properties ep
            INNER JOIN sys.tables t ON ep.major_id = t.object_id
            WHERE ep.class = 1
              AND ep.name = 'MS_Description'
            ORDER BY table_name, ep.minor_id
        """)
        
        props = cursor.fetchall()
        conn.close()
        
        if not props:
            return 0
        
        conn = self.server_service.get_connection(server, target_db)
        conn.autocommit = True
        cursor = conn.cursor()
        
        count = 0
        for prop in props:
            try:
                if prop.minor_id == 0:
                    # 테이블 설명
                    cursor.execute("""
                        IF OBJECT_ID(?, 'U') IS NOT NULL
                            EXEC sp_addextendedproperty
                                @name = N'MS_Description',
                                @value = ?,
                                @level0type = N'SCHEMA', @level0name = N'dbo',
                                @level1type = N'TABLE',  @level1name = ?
                    """, (prop.table_name, prop.property_value, prop.table_name))
                else:
                    # 컬럼 설명
                    cursor.execute("""
                        IF OBJECT_ID(?, 'U') IS NOT NULL
                        AND COL_LENGTH(?, ?) IS NOT NULL
                            EXEC sp_addextendedproperty
                                @name = N'MS_Description',
                                @value = ?,
                                @level0type = N'SCHEMA',  @level0name = N'dbo',
                                @level1type = N'TABLE',   @level1name = ?,
                                @level2type = N'COLUMN',  @level2name = ?
                    """, (
                        prop.table_name,
                        prop.table_name, prop.column_name,
                        prop.property_value,
                        prop.table_name, prop.column_name
                    ))
                count += 1
            except:
                # 이미 존재하거나 대상 테이블/컬럼 없으면 건너뜀
                pass
        
        conn.close()
        return count
    
    def _copy_extended_properties_cross_server(
        self,
        source_server: DBServer, source_db: str,
        target_server: DBServer, target_db: str
    ) -> int:
        """크로스 서버 확장 속성(테이블·컬럼 설명) 복제"""
        source_conn = self.server_service.get_connection(source_server, source_db)
        cursor = source_conn.cursor()
        
        cursor.execute("""
            SELECT
                OBJECT_NAME(ep.major_id) AS table_name,
                ep.minor_id,
                COL_NAME(ep.major_id, ep.minor_id) AS column_name,
                ep.name AS property_name,
                CAST(ep.value AS NVARCHAR(4000)) AS property_value
            FROM sys.extended_properties ep
            INNER JOIN sys.tables t ON ep.major_id = t.object_id
            WHERE ep.class = 1
              AND ep.name = 'MS_Description'
            ORDER BY table_name, ep.minor_id
        """)
        
        props = [
            (row.table_name, row.minor_id, row.column_name,
             row.property_name, row.property_value)
            for row in cursor.fetchall()
        ]
        source_conn.close()
        
        if not props:
            return 0
        
        target_conn = self.server_service.get_connection(target_server, target_db)
        target_conn.autocommit = True
        cursor = target_conn.cursor()
        
        count = 0
        for table_name, minor_id, column_name, prop_name, prop_value in props:
            try:
                if minor_id == 0:
                    cursor.execute("""
                        IF OBJECT_ID(?, 'U') IS NOT NULL
                            EXEC sp_addextendedproperty
                                @name = N'MS_Description',
                                @value = ?,
                                @level0type = N'SCHEMA', @level0name = N'dbo',
                                @level1type = N'TABLE',  @level1name = ?
                    """, (table_name, prop_value, table_name))
                else:
                    cursor.execute("""
                        IF OBJECT_ID(?, 'U') IS NOT NULL
                        AND COL_LENGTH(?, ?) IS NOT NULL
                            EXEC sp_addextendedproperty
                                @name = N'MS_Description',
                                @value = ?,
                                @level0type = N'SCHEMA',  @level0name = N'dbo',
                                @level1type = N'TABLE',   @level1name = ?,
                                @level2type = N'COLUMN',  @level2name = ?
                    """, (
                        table_name,
                        table_name, column_name,
                        prop_value,
                        table_name, column_name
                    ))
                count += 1
            except:
                pass
        
        target_conn.close()
        return count
    
    # ============================================================
    # Index Copy Methods
    # ============================================================
    
    def _copy_indexes_same_server(self, server: DBServer, source_db: str, target_db: str):
        """동일 서버 인덱스 복제"""
        conn = self.server_service.get_connection(server, source_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                t.name AS table_name,
                kc.name AS pk_name,
                STUFF((
                    SELECT ', [' + c.name + ']'
                    FROM sys.index_columns ic
                    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                    ORDER BY ic.key_ordinal
                    FOR XML PATH('')
                ), 1, 2, '') AS columns
            FROM sys.key_constraints kc
            JOIN sys.indexes i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
            JOIN sys.tables t ON kc.parent_object_id = t.object_id
            WHERE kc.type = 'PK'
        """)
        
        pk_list = cursor.fetchall()
        conn.close()
        
        conn = self.server_service.get_connection(server, "master")
        conn.autocommit = True
        cursor = conn.cursor()
        
        for pk in pk_list:
            try:
                cursor.execute(f"""
                    ALTER TABLE [{target_db}].dbo.[{pk.table_name}]
                    ADD CONSTRAINT [{pk.pk_name}] PRIMARY KEY ({pk.columns})
                """)
            except:
                pass
        
        conn.close()
    
    def _copy_indexes_cross_server(
        self,
        source_server: DBServer, source_db: str,
        target_server: DBServer, target_db: str
    ):
        """크로스 서버 인덱스 복제"""
        source_conn = self.server_service.get_connection(source_server, source_db)
        cursor = source_conn.cursor()
        
        cursor.execute("""
            SELECT 
                t.name AS table_name,
                kc.name AS pk_name,
                STUFF((
                    SELECT ', [' + c.name + ']'
                    FROM sys.index_columns ic
                    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                    ORDER BY ic.key_ordinal
                    FOR XML PATH('')
                ), 1, 2, '') AS columns
            FROM sys.key_constraints kc
            JOIN sys.indexes i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
            JOIN sys.tables t ON kc.parent_object_id = t.object_id
            WHERE kc.type = 'PK'
        """)
        
        pk_list = [(row.table_name, row.pk_name, row.columns) for row in cursor.fetchall()]
        source_conn.close()
        
        target_conn = self.server_service.get_connection(target_server, target_db)
        target_conn.autocommit = True
        cursor = target_conn.cursor()
        
        for table_name, pk_name, columns in pk_list:
            try:
                cursor.execute(f"""
                    ALTER TABLE [{table_name}]
                    ADD CONSTRAINT [{pk_name}] PRIMARY KEY ({columns})
                """)
            except:
                pass
        
        target_conn.close()
    
    # ============================================================
    # Data Copy Methods
    # ============================================================
    
    def _copy_data_same_server(self, server: DBServer, source_db: str, target_db: str) -> int:
        """동일 서버 데이터 복제"""
        conn = self.server_service.get_connection(server, "master")
        conn.autocommit = True
        cursor = conn.cursor()
        
        count = 0
        for table in CLONE_TABLES['with_data']:
            try:
                cursor.execute(f"""
                    IF EXISTS (SELECT 1 FROM [{source_db}].INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}')
                    AND EXISTS (SELECT 1 FROM [{target_db}].INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}')
                    BEGIN
                        INSERT INTO [{target_db}].dbo.[{table}]
                        SELECT * FROM [{source_db}].dbo.[{table}]
                    END
                """)
                count += 1
            except:
                pass
        
        conn.close()
        return count
    
    def _copy_data_cross_server(
        self,
        source_server: DBServer, source_db: str,
        target_server: DBServer, target_db: str
    ) -> int:
        """크로스 서버 데이터 복제"""
        count = 0
        
        for table in CLONE_TABLES['with_data']:
            try:
                source_conn = self.server_service.get_connection(source_server, source_db)
                cursor = source_conn.cursor()
                
                cursor.execute(f"""
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'
                """)
                if not cursor.fetchone():
                    source_conn.close()
                    continue
                
                cursor.execute(f"""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION
                """)
                columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                
                cursor.execute(f"SELECT * FROM [{table}]")
                rows = cursor.fetchall()
                source_conn.close()
                
                if not rows:
                    continue
                
                target_conn = self.server_service.get_connection(target_server, target_db)
                target_conn.autocommit = True
                t_cursor = target_conn.cursor()
                
                col_list = ", ".join([f"[{c}]" for c in columns])
                placeholders = ", ".join(["?" for _ in columns])
                
                for row in rows:
                    t_cursor.execute(
                        f"INSERT INTO [{table}] ({col_list}) VALUES ({placeholders})",
                        row
                    )
                
                target_conn.close()
                count += 1
                
            except Exception as e:
                print(f"데이터 복제 실패 [{table}]: {e}")
        
        return count
    
    # ============================================================
    # DB Info Methods
    # ============================================================
    
    def get_db_tables(self, server: DBServer, db_name: str) -> List[Dict]:
        """DB 테이블 목록 조회"""
        try:
            conn = self.server_service.get_connection(server, db_name)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    t.name AS table_name,
                    SUM(p.rows) AS row_count,
                    SUM(a.total_pages) * 8.0 / 1024 AS size_mb
                FROM sys.tables t
                JOIN sys.indexes i ON t.object_id = i.object_id
                JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE t.is_ms_shipped = 0
                GROUP BY t.name
                ORDER BY t.name
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "table_name": row.table_name,
                    "row_count": row.row_count,
                    "size_mb": round(row.size_mb or 0, 2)
                })
            
            conn.close()
            return results
        except:
            return []
    
    def get_db_size(self, server: DBServer, db_name: str) -> float:
        """DB 용량 조회"""
        try:
            conn = self.server_service.get_connection(server, "master")
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT SUM(size) * 8.0 / 1024 AS size_mb
                FROM sys.master_files
                WHERE database_id = DB_ID('{db_name}')
            """)
            
            row = cursor.fetchone()
            conn.close()
            return round(row.size_mb or 0, 2) if row else 0
        except:
            return 0