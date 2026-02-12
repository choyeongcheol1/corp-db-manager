"""
DB 서버 관리 서비스
"""
from typing import List, Optional, Dict, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.database import DBServer, Corp
from app.models import (
    ServerCreate, ServerUpdate, ServerSummary,
    ServerStatus, DBStatus
)
from app.services.drivers import get_driver


class ServerService:
    """DB 서버 관리 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    # ============================================================
    # CRUD Operations
    # ============================================================
    
    def get_all_servers(self, active_only: bool = True) -> List[DBServer]:
        """전체 서버 목록 조회"""
        query = self.db.query(DBServer)
        if active_only:
            query = query.filter(DBServer.is_active == True)
        return query.order_by(DBServer.server_name).all()
    
    def get_server(self, server_id: int) -> Optional[DBServer]:
        """서버 정보 조회"""
        return self.db.query(DBServer).filter(DBServer.id == server_id).first()
    
    def get_server_by_name(self, server_name: str) -> Optional[DBServer]:
        """서버명으로 조회"""
        return self.db.query(DBServer).filter(DBServer.server_name == server_name).first()
    
    def create_server(self, server_data: ServerCreate) -> DBServer:
        """서버 등록"""
        db_server = DBServer(
            server_name=server_data.server_name,
            host=server_data.host,
            port=server_data.port,
            db_type=server_data.db_type.value,
            username=server_data.username,
            password=server_data.password,
            default_db=server_data.default_db,
            data_path=server_data.data_path,
            log_path=server_data.log_path,
            description=server_data.description,
            is_active=True
        )
        self.db.add(db_server)
        self.db.commit()
        self.db.refresh(db_server)
        return db_server
    
    def update_server(self, server_id: int, server_data: ServerUpdate) -> Optional[DBServer]:
        """서버 정보 수정"""
        db_server = self.get_server(server_id)
        if not db_server:
            return None
        
        update_data = server_data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_server, field, value)
        
        db_server.updated_at = datetime.now()
        self.db.commit()
        self.db.refresh(db_server)
        return db_server
    
    def delete_server(self, server_id: int) -> bool:
        """서버 삭제"""
        db_server = self.get_server(server_id)
        if not db_server:
            return False
        
        corp_count = self.db.query(Corp).filter(Corp.server_id == server_id).count()
        if corp_count > 0:
            raise ValueError(f"서버에 {corp_count}개의 법인 DB가 있어 삭제할 수 없습니다.")
        
        self.db.delete(db_server)
        self.db.commit()
        return True
    
    # ============================================================
    # Connection Methods (드라이버 위임)
    # ============================================================
    
    def get_connection(self, server: DBServer, database: str = None):
        """DB 연결 획득"""
        driver = get_driver(server)
        return driver.get_connection(database)
    
    def test_connection(self, server: DBServer) -> Tuple[bool, str, Optional[str]]:
        """연결 테스트"""
        driver = get_driver(server)
        return driver.test_connection()
    
    # ============================================================
    # Server Info Methods (드라이버 위임)
    # ============================================================
    
    def get_server_databases(self, server: DBServer, prefix: str = None) -> List[Dict]:
        """DB 목록 조회"""
        driver = get_driver(server)
        return driver.get_databases(prefix)
    
    def get_databases_with_disk_usage(self, server: DBServer, prefix: str = None) -> List[Dict]:
        """DB별 용량 + 디스크 사용률 조회"""
        driver = get_driver(server)
        if hasattr(driver, 'get_databases_with_disk_usage'):
            return driver.get_databases_with_disk_usage(prefix)
        # fallback: 기존 get_databases에 빈 디스크 정보 추가
        return [dict(item, disk_total_gb=0, disk_free_gb=0, disk_used_pct=0, db_disk_pct=0, drive='')
                for item in driver.get_databases(prefix)]
    
    def get_server_status(self, server: DBServer) -> ServerStatus:
        """서버 상태 확인"""
        success, _, _ = self.test_connection(server)
        return ServerStatus.ONLINE if success else ServerStatus.OFFLINE
    
    def get_file_paths(self, server: DBServer, reference_db: str = None) -> Dict[str, str]:
        """파일 경로 조회"""
        driver = get_driver(server)
        return driver.get_file_paths(reference_db)
    
    # ============================================================
    # Health Check Methods (드라이버 위임)
    # ============================================================
    
    def check_server_health(self, server: DBServer) -> Dict:
        """서버 상태 점검"""
        driver = get_driver(server)
        return driver.check_server_health()
    
    def check_database_health(self, server: DBServer, database: str) -> Dict:
        """개별 DB 상태 점검"""
        driver = get_driver(server)
        return driver.check_database_health(database)
    
    def check_all_databases_health(self, server: DBServer, prefix: str = None) -> Dict:
        """전체 DB 상태 점검"""
        driver = get_driver(server)
        return driver.check_all_databases_health(prefix)
    
    # ============================================================
    # Summary Methods
    # ============================================================
    
    def get_server_summary(self, server: DBServer) -> ServerSummary:
        """서버 요약 정보"""
        corps = self.db.query(Corp).filter(Corp.server_id == server.id).all()
        
        warning_count = sum(1 for c in corps if c.status == DBStatus.WARNING.value)
        error_count = sum(1 for c in corps if c.status == DBStatus.ERROR.value)
        
        total_size = 0
        db_count = 0
        try:
            dbs = self.get_server_databases(server)
            db_count = len(dbs)
            total_size = sum(db['size_mb'] for db in dbs)
        except:
            pass
        
        return ServerSummary(
            id=server.id,
            server_name=server.server_name,
            host=server.host,
            port=server.port,
            db_type=server.db_type or "mssql",
            default_db=server.default_db,
            username=server.username,
            description=server.description,
            status=self.get_server_status(server),
            db_count=db_count,
            total_size_mb=total_size,
            warning_count=warning_count,
            error_count=error_count
        )
    
    def get_all_server_summaries_fast(self) -> List[ServerSummary]:
        """전체 서버 요약 (연결 테스트 없이 빠른 반환)
        
        페이지 초기 로딩용. 연결 상태는 프론트에서 비동기로 개별 체크.
        """
        servers = self.get_all_servers(active_only=True)
        result = []
        for server in servers:
            corps = self.db.query(Corp).filter(Corp.server_id == server.id).all()
            warning_count = sum(1 for c in corps if c.status == DBStatus.WARNING.value)
            error_count = sum(1 for c in corps if c.status == DBStatus.ERROR.value)
            
            result.append(ServerSummary(
                id=server.id,
                server_name=server.server_name,
                host=server.host,
                port=server.port,
                db_type=server.db_type or "mssql",
                default_db=server.default_db,
                username=server.username,
                description=server.description,
                status=ServerStatus.UNKNOWN,  # 연결 테스트 생략
                db_count=0,
                total_size_mb=0,
                warning_count=warning_count,
                error_count=error_count
            ))
        return result
    
    def get_all_server_summaries(self) -> List[ServerSummary]:
        """전체 서버 요약"""
        servers = self.get_all_servers(active_only=True)
        return [self.get_server_summary(s) for s in servers]