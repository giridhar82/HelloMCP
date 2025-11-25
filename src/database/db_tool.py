import time
import importlib
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from ..models import DatabaseConnection, DatabaseQuery, QueryResult, QueryRiskAssessment, DatabaseSchema, QueryType
from .connectors.base import BaseDatabaseConnector
from .safety.risk_checker import SQLRiskChecker


class DatabaseTool:
    def __init__(self):
        self._connectors: Dict[str, BaseDatabaseConnector] = {}
        self._risk_checker = SQLRiskChecker()
        self._connector_types = {
            "postgresql": "src.database.connectors.postgresql.PostgreSQLConnector",
            "mysql": "src.database.connectors.mysql.MySQLConnector",
            "oracle": "src.database.connectors.oracle.OracleConnector",
        }

    async def execute_query(self, query: DatabaseQuery, validate_safety: bool = True) -> QueryResult:
        start = time.time()
        try:
            if validate_safety:
                ra = await self._risk_checker.assess_query_risk(query.query)
                if not ra.is_safe:
                    return QueryResult(success=False, error_message=ra.recommendation or "Query blocked", execution_time=time.time() - start, query_type=QueryType.SELECT)
            c = await self._get_connector(query.database_connection)
            res = await c.execute_query(query.query, query.parameters)
            res.execution_time = time.time() - start
            return res
        except Exception as e:
            return QueryResult(success=False, error_message=str(e), execution_time=time.time() - start, query_type=QueryType.SELECT)

    async def get_database_schema(self, connection: DatabaseConnection) -> DatabaseSchema:
        c = await self._get_connector(connection)
        return await c.get_schema()

    async def list_tables(self, connection: DatabaseConnection, schema: Optional[str] = None) -> List[str]:
        c = await self._get_connector(connection)
        return await c.list_tables(schema)

    async def validate_query(self, query: str, connection: DatabaseConnection) -> QueryRiskAssessment:
        try:
            c = await self._get_connector(connection)
            syntax_ok = await c.validate_syntax(query)
            ra = await self._risk_checker.assess_query_risk(query)
            if not syntax_ok:
                ra.is_safe = False
                ra.recommendation = "Query has syntax errors"
            return ra
        except Exception as e:
            return QueryRiskAssessment(risk_level="high", risk_score=100.0, is_safe=False, recommendation=f"Validation failed: {str(e)}")

    async def test_connection(self, connection: DatabaseConnection) -> bool:
        try:
            c = await self._get_connector(connection)
            return await c.test_connection()
        except Exception:
            return False

    async def close_connection(self, connection: DatabaseConnection) -> None:
        key = self._conn_key(connection)
        conn = self._connectors.get(key)
        if conn:
            await conn.close()
            del self._connectors[key]

    async def close_all_connections(self) -> None:
        for key, c in list(self._connectors.items()):
            try:
                await c.close()
            finally:
                del self._connectors[key]

    async def _get_connector(self, connection: DatabaseConnection) -> BaseDatabaseConnector:
        key = self._conn_key(connection)
        conn = self._connectors.get(key)
        if not conn:
            cls_path = self._connector_types.get(connection.database_type)
            if not cls_path:
                raise ValueError(f"Unsupported database type: {connection.database_type}")
            module_path, class_name = cls_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
            conn = cls(connection)
            await conn.connect()
            self._connectors[key] = conn
        return conn

    def _conn_key(self, c: DatabaseConnection) -> str:
        return f"{c.database_type}:{c.host}:{c.port}:{c.database}:{c.username}"

    @asynccontextmanager
    async def get_connection(self, connection: DatabaseConnection):
        c = await self._get_connector(connection)
        try:
            yield c
        finally:
            pass
