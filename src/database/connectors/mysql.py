import asyncio
from typing import Any, Dict, Optional, List

import mysql.connector
from mysql.connector import Error as MySQLError

from .base import BaseDatabaseConnector
from ...models import QueryResult, DatabaseSchema, QueryType, DatabaseConnection


class MySQLConnector(BaseDatabaseConnector):
    def __init__(self, connection: DatabaseConnection):
        super().__init__(connection)
        self._connection = None

    async def connect(self) -> None:
        try:
            cfg = {
                "host": self.connection.host,
                "port": self.connection.port,
                "database": self.connection.database,
                "user": self.connection.username,
                "password": self.connection.password,
                "connection_timeout": self.connection.connection_timeout,
            }
            if self.connection.ssl_mode:
                cfg["ssl_disabled"] = self.connection.ssl_mode == "disabled"
            loop = asyncio.get_event_loop()
            self._connection = await loop.run_in_executor(None, lambda: mysql.connector.connect(**cfg))
            self._is_connected = True
        except MySQLError as e:
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
            cur = self._connection.cursor(dictionary=True)
            try:
                qt = self.get_query_type(query)
                if parameters:
                    cur.execute(query, parameters)
                else:
                    cur.execute(query)
                if qt == QueryType.SELECT:
                    data = cur.fetchall()
                    cols = [d[0] for d in cur.description] if cur.description else []
                    return QueryResult(success=True, data=data or [], row_count=len(data or []), columns=cols, query_type=qt)
                rc = cur.rowcount
                self._connection.commit()
                return QueryResult(success=True, row_count=rc, query_type=qt)
            finally:
                cur.close()

        return await loop.run_in_executor(None, _execute)

    async def get_schema(self) -> DatabaseSchema:
        if not self._is_connected:
            raise RuntimeError("Not connected")
        loop = asyncio.get_event_loop()

        def _get_schema():
            schema = DatabaseSchema(tables=[], views=[], procedures=[], functions=[])
            cur = self._connection.cursor(dictionary=True)
            try:
                cur.execute(
                    """
                        SELECT TABLE_NAME AS table_name, TABLE_TYPE AS table_type, TABLE_SCHEMA AS table_schema
                        FROM information_schema.tables 
                        WHERE TABLE_SCHEMA = DATABASE()
                        ORDER BY TABLE_NAME
                    """
                )
                for row in cur.fetchall():
                    schema.tables.append({"name": row["table_name"], "type": row["table_type"], "schema": row["table_schema"]})
                for tbl in schema.tables:
                    cur.execute(
                        """
                            SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, IS_NULLABLE AS is_nullable,
                                   COLUMN_DEFAULT AS column_default, COLUMN_KEY AS column_key
                            FROM information_schema.columns 
                            WHERE TABLE_NAME = %s AND TABLE_SCHEMA = DATABASE()
                            ORDER BY ORDINAL_POSITION
                        """,
                        (tbl["name"],),
                    )
                    tbl["columns"] = [
                        {
                            "name": r["column_name"],
                            "type": r["data_type"],
                            "nullable": r["is_nullable"] == "YES",
                            "default": r["column_default"],
                            "key": r["column_key"],
                        }
                        for r in cur.fetchall()
                    ]
                cur.execute(
                    """
                        SELECT TABLE_NAME AS view_name, VIEW_DEFINITION AS view_definition
                        FROM information_schema.views 
                        WHERE TABLE_SCHEMA = DATABASE()
                        ORDER BY TABLE_NAME
                    """
                )
                for row in cur.fetchall():
                    schema.views.append({"name": row["view_name"], "definition": row["view_definition"]})
                cur.execute(
                    """
                        SELECT ROUTINE_NAME AS routine_name, ROUTINE_TYPE AS routine_type
                        FROM information_schema.routines 
                        WHERE ROUTINE_SCHEMA = DATABASE()
                        ORDER BY ROUTINE_NAME
                    """
                )
                for row in cur.fetchall():
                    if row["routine_type"] == "PROCEDURE":
                        schema.procedures.append({"name": row["routine_name"]})
                    elif row["routine_type"] == "FUNCTION":
                        schema.functions.append({"name": row["routine_name"]})
            finally:
                cur.close()
            return schema

        return await loop.run_in_executor(None, _get_schema)

    async def list_tables(self, schema: Optional[str] = None) -> List[str]:
        if not self._is_connected:
            raise RuntimeError("Not connected")
        loop = asyncio.get_event_loop()

        def _list():
            names: List[str] = []
            cur = self._connection.cursor(dictionary=True)
            try:
                if schema:
                    cur.execute(
                        """
                            SELECT TABLE_NAME AS table_name
                            FROM information_schema.tables
                            WHERE TABLE_SCHEMA = %s
                            ORDER BY TABLE_NAME
                        """,
                        (schema,),
                    )
                else:
                    cur.execute(
                        """
                            SELECT TABLE_NAME AS table_name
                            FROM information_schema.tables
                            WHERE TABLE_SCHEMA = DATABASE()
                            ORDER BY TABLE_NAME
                        """
                    )
                for row in cur.fetchall():
                    names.append(row["table_name"]) if isinstance(row, dict) else names.append(row[0])
            finally:
                cur.close()
            return names

        return await loop.run_in_executor(None, _list)

    async def validate_syntax(self, query: str) -> bool:
        if not self._is_connected:
            return False
        loop = asyncio.get_event_loop()

        def _validate():
            cur = self._connection.cursor()
            try:
                cur.execute("EXPLAIN " + query)
                return True
            finally:
                cur.close()

        try:
            await loop.run_in_executor(None, _validate)
            return True
        except MySQLError:
            return False

    async def test_connection(self) -> bool:
        if not self._is_connected:
            return False
        loop = asyncio.get_event_loop()

        def _test():
            cur = self._connection.cursor()
            try:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                return bool(row)
            finally:
                cur.close()

        try:
            return await loop.run_in_executor(None, _test)
        except Exception:
            return False

    def get_query_type(self, query: str) -> QueryType:
        return self._parse_query_type(query)
