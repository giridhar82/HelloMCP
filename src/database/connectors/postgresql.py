import asyncio
from typing import Any, Dict, Optional, List

import psycopg2
from psycopg2.extras import RealDictCursor

from .base import BaseDatabaseConnector
from ...models import QueryResult, DatabaseSchema, QueryType, DatabaseConnection


class PostgreSQLConnector(BaseDatabaseConnector):
    def __init__(self, connection: DatabaseConnection):
        super().__init__(connection)
        self._connection = None

    async def connect(self) -> None:
        try:
            conn_str = (
                f"host={self.connection.host} "
                f"port={self.connection.port} "
                f"dbname={self.connection.database} "
                f"user={self.connection.username} "
                f"password={self.connection.password}"
            )
            if self.connection.ssl_mode:
                conn_str += f" sslmode={self.connection.ssl_mode}"
            loop = asyncio.get_event_loop()
            self._connection = await loop.run_in_executor(
                None, lambda: psycopg2.connect(conn_str, cursor_factory=RealDictCursor)
            )
            self._is_connected = True
        except psycopg2.Error as e:
            raise ConnectionError(str(e))

    async def disconnect(self) -> None:
        if self._connection:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._connection.close)
            self._connection = None
            self._is_connected = False

    async def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        if not self._is_connected:
            raise RuntimeError("Not connected")
        loop = asyncio.get_event_loop()

        def _execute():
            with self._connection.cursor() as cursor:
                qt = self.get_query_type(query)
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                if qt == QueryType.SELECT:
                    data = cursor.fetchall()
                    cols = [d[0] for d in cursor.description] if cursor.description else []
                    return QueryResult(success=True, data=[dict(r) for r in data] if data else [], row_count=len(data), columns=cols, query_type=qt)
                rc = cursor.rowcount
                self._connection.commit()
                return QueryResult(success=True, row_count=rc, query_type=qt)

        return await loop.run_in_executor(None, _execute)

    async def get_schema(self) -> DatabaseSchema:
        if not self._is_connected:
            raise RuntimeError("Not connected")
        loop = asyncio.get_event_loop()

        def _get_schema():
            schema = DatabaseSchema(tables=[], views=[], procedures=[], functions=[])
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                        SELECT table_name, table_type, table_schema
                        FROM information_schema.tables 
                        WHERE table_schema NOT IN ('information_schema','pg_catalog')
                        ORDER BY table_name
                    """
                )
                for row in cursor.fetchall():
                    schema.tables.append({"name": row["table_name"], "type": row["table_type"], "schema": row["table_schema"]})
                for tbl in schema.tables:
                    cursor.execute(
                        """
                            SELECT column_name, data_type, is_nullable, column_default
                            FROM information_schema.columns 
                            WHERE table_name = %s
                            ORDER BY ordinal_position
                        """,
                        (tbl["name"],),
                    )
                    tbl["columns"] = [
                        {
                            "name": r["column_name"],
                            "type": r["data_type"],
                            "nullable": r["is_nullable"] == "YES",
                            "default": r["column_default"],
                        }
                        for r in cursor.fetchall()
                    ]
                cursor.execute(
                    """
                        SELECT table_name as view_name, view_definition
                        FROM information_schema.views 
                        WHERE table_schema NOT IN ('information_schema','pg_catalog')
                        ORDER BY table_name
                    """
                )
                for row in cursor.fetchall():
                    schema.views.append({"name": row["view_name"], "definition": row["view_definition"]})
                cursor.execute(
                    """
                        SELECT routine_name, routine_type, data_type
                        FROM information_schema.routines 
                        WHERE routine_schema NOT IN ('information_schema','pg_catalog')
                        ORDER BY routine_name
                    """
                )
                for row in cursor.fetchall():
                    if row["routine_type"] == "FUNCTION":
                        schema.functions.append({"name": row["routine_name"], "return_type": row["data_type"]})
                    elif row["routine_type"] == "PROCEDURE":
                        schema.procedures.append({"name": row["routine_name"]})
            return schema

        return await loop.run_in_executor(None, _get_schema)

    async def list_tables(self, schema: Optional[str] = None) -> List[str]:
        if not self._is_connected:
            raise RuntimeError("Not connected")
        loop = asyncio.get_event_loop()

        def _list():
            names: List[str] = []
            with self._connection.cursor() as cursor:
                if schema:
                    cursor.execute(
                        """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = %s
                            ORDER BY table_name
                        """,
                        (schema,),
                    )
                else:
                    cursor.execute(
                        """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema NOT IN ('information_schema','pg_catalog')
                            ORDER BY table_name
                        """
                    )
                for row in cursor.fetchall():
                    names.append(row["table_name"]) if isinstance(row, dict) else names.append(row[0])
            return names

        return await loop.run_in_executor(None, _list)

    async def validate_syntax(self, query: str) -> bool:
        if not self._is_connected:
            return False
        loop = asyncio.get_event_loop()

        def _validate():
            with self._connection.cursor() as cursor:
                cursor.execute("EXPLAIN " + query)
                return True

        try:
            await loop.run_in_executor(None, _validate)
            return True
        except psycopg2.Error:
            return False

    async def test_connection(self) -> bool:
        if not self._is_connected:
            return False
        loop = asyncio.get_event_loop()

        def _test():
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                row = cursor.fetchone()
                return bool(row)

        try:
            return await loop.run_in_executor(None, _test)
        except Exception:
            return False

    def get_query_type(self, query: str) -> QueryType:
        return self._parse_query_type(query)
