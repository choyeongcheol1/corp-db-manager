"""
DB 드라이버 팩토리
"""
from typing import TYPE_CHECKING
from app.models import DBType

if TYPE_CHECKING:
    from app.services.drivers.base import BaseDriver
    from app.core.database import DBServer


def get_driver(server: "DBServer") -> "BaseDriver":
    """서버 타입에 맞는 드라이버 반환"""
    db_type = server.db_type or DBType.MSSQL.value
    
    if db_type == DBType.POSTGRESQL.value:
        from app.services.drivers.postgresql import PostgreSQLDriver
        return PostgreSQLDriver(server)
    elif db_type == DBType.MYSQL.value:
        from app.services.drivers.mysql import MySQLDriver
        return MySQLDriver(server)
    elif db_type == DBType.ORACLE.value:
        from app.services.drivers.oracle import OracleDriver
        return OracleDriver(server)
    else:
        # MSSQL (기본)
        from app.services.drivers.mssql import MSSQLDriver
        return MSSQLDriver(server)