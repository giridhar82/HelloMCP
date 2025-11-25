from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class DatabaseType(str, Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQLITE = "sqlite"


class QueryType(str, Enum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DatabaseConnection(BaseModel):
    host: str
    port: int
    database: str
    username: str
    password: str
    database_type: DatabaseType
    ssl_mode: Optional[str] = None
    connection_timeout: int = 30
    pool_size: int = 5


class DatabaseQuery(BaseModel):
    query: str
    parameters: Optional[Dict[str, Any]] = None
    database_connection: DatabaseConnection
    timeout: int = 30
    max_rows: Optional[int] = None


class QueryResult(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    row_count: int = 0
    execution_time: float = 0.0
    error_message: Optional[str] = None
    columns: Optional[List[str]] = None
    query_type: Optional[QueryType] = None


class QueryRiskAssessment(BaseModel):
    risk_level: RiskLevel
    risk_score: float
    warnings: List[str] = []
    dangerous_operations: List[str] = []
    is_safe: bool
    recommendation: Optional[str] = None


class DatabaseSchema(BaseModel):
    tables: List[Dict[str, Any]]
    views: List[Dict[str, Any]] = []
    procedures: List[Dict[str, Any]] = []
    functions: List[Dict[str, Any]] = []