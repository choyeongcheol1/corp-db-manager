"""
DB 드라이버 추상 클래스
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional, Any
from app.core.database import DBServer


class BaseDriver(ABC):
    """DB 드라이버 추상 클래스"""
    
    def __init__(self, server: DBServer):
        self.server = server
    
    # ============================================================
    # Connection Methods
    # ============================================================
    
    @abstractmethod
    def get_connection(self, database: str = None) -> Any:
        """DB 연결 획득"""
        pass
    
    @abstractmethod
    def test_connection(self) -> Tuple[bool, str, Optional[str]]:
        """연결 테스트 - (success, message, version)"""
        pass
    
    # ============================================================
    # Database Methods
    # ============================================================
    
    @abstractmethod
    def get_databases(self, prefix: str = None) -> List[Dict]:
        """DB 목록 조회"""
        pass
    
    @abstractmethod
    def get_tables(self, database: str) -> List[Dict]:
        """테이블 목록 조회"""
        pass

    @abstractmethod
    def get_table_columns(self, database: str, table_name: str) -> List[Dict]:
        """
        테이블 컬럼 정보 조회
        
        Returns:
            [
                {
                    "column_name": str,
                    "data_type": str,
                    "max_length": int,
                    "is_nullable": bool,
                    "is_identity": bool,
                    "is_primary_key": bool,
                    "default_value": str,
                    "description": str
                }
            ]
        """
        pass
    
    @abstractmethod
    def get_db_size(self, database: str) -> float:
        """DB 용량 조회 (MB)"""
        pass
    
    @abstractmethod
    def create_database(self, db_name: str, data_path: str = None, log_path: str = None) -> bool:
        """DB 생성"""
        pass
    
    # ============================================================
    # File Path Methods
    # ============================================================
    
    def get_file_paths(self, reference_db: str = None) -> Dict[str, str]:
        """파일 경로 조회"""
        if self.server.data_path and self.server.log_path:
            return {
                "data_path": self.server.data_path,
                "log_path": self.server.log_path
            }
        return self._get_default_paths()
    
    @abstractmethod
    def _get_default_paths(self) -> Dict[str, str]:
        """기본 파일 경로"""
        pass
    
    # ============================================================
    # Health Check Methods
    # ============================================================
    
    @abstractmethod
    def check_server_health(self) -> Dict:
        """
        서버 상태 점검
        
        Returns:
            {
                "server_id": int,
                "server_name": str,
                "status": "normal" | "warning" | "error",
                "checked_at": datetime,
                "checks": [
                    {
                        "name": str,
                        "status": "normal" | "warning" | "error",
                        "value": str,
                        "detail": str
                    }
                ],
                "issues": [str]
            }
        """
        pass
    
    @abstractmethod
    def check_database_health(self, database: str) -> Dict:
        """
        개별 DB 상태 점검
        
        Returns:
            {
                "db_name": str,
                "status": "normal" | "warning" | "error",
                "checks": [
                    {
                        "name": str,
                        "status": "normal" | "warning" | "error", 
                        "value": str,
                        "detail": str
                    }
                ],
                "issues": [str]
            }
        """
        pass
    
    def check_all_databases_health(self, prefix: str = None) -> Dict:
        """
        전체 DB 상태 점검
        
        Returns:
            {
                "total": int,
                "normal": int,
                "warning": int,
                "error": int,
                "databases": [DB 점검 결과],
                "issues": [전체 이슈 목록]
            }
        """
        from datetime import datetime
        
        databases = self.get_databases(prefix)
        results = []
        all_issues = []
        
        normal_count = 0
        warning_count = 0
        error_count = 0
        
        for db in databases:
            health = self.check_database_health(db['db_name'])
            health['db_name'] = db['db_name']
            health['size_mb'] = db.get('size_mb', 0)
            health['create_date'] = db.get('create_date')
            results.append(health)
            
            # 상태 카운트
            if health['status'] == 'error':
                error_count += 1
            elif health['status'] == 'warning':
                warning_count += 1
            else:
                normal_count += 1
            
            # 이슈 수집
            for issue in health.get('issues', []):
                all_issues.append(f"{db['db_name']}: {issue}")
        
        return {
            "checked_at": datetime.now(),
            "total": len(databases),
            "normal": normal_count,
            "warning": warning_count,
            "error": error_count,
            "databases": results,
            "issues": all_issues
        }