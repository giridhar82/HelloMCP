import asyncio
from typing import Any, Dict, Optional, List

import cx_Oracle

from .base import BaseDatabaseConnector
from ...models import QueryResult, DatabaseSchema, QueryType, DatabaseConnection


class OracleConnector(BaseDatabaseConnector):
    def __init__(self, connection: DatabaseConnection):
        super().__init__(connection)
        self._connection = None

    async def connect(self) -> None:
        try:
            dsn = cx_Oracle.makedsn(self.connection.host, self.connection.port, service_name=self.connection.database)
            loop = asyncio.get_event_loop()
            self._connection = await loop.run_in_executor(
                None,
                lambda: cx_Oracle.connect(user=self.connection.username, password=self.connection.password, dsn=dsn, encoding="UTF-8"),
            )
            self._is_connected = True
        except cx_Oracle.Error as e:
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
            cur = self._connection.cursor()
            try:
                qt = self.get_query_type(query)
                if parameters:
                    oracle_query = query
                    for k in parameters.keys():
                        oracle_query = oracle_query.replace(f"%({k})s", f":{k}")
                    cur.execute(oracle_query, parameters)
                else:
                    cur.execute(query)
                if qt == QueryType.SELECT:
                    data = cur.fetchall()
                    cols = [d[0] for d in cur.description] if cur.description else []
                    out = []
                    for row in data or []:
                        out.append({cols[i]: row[i] for i in range(len(cols))})
                    return QueryResult(success=True, data=out, row_count=len(out), columns=cols, query_type=qt)
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
            cur = self._connection.cursor()
            try:
                cur.execute("SELECT table_name, tablespace_name, status FROM user_tables ORDER BY table_name")
                for row in cur.fetchall():
                    schema.tables.append({"name": row[0], "tablespace": row[1], "status": row[2]})
                for tbl in schema.tables:
                    cur.execute(
                        """
                            SELECT column_name, data_type, data_length, nullable, data_default
                            FROM user_tab_columns WHERE table_name = :t ORDER BY column_id
                        """,
                        {"t": tbl["name"]},
                    )
                    tbl["columns"] = [
                        {
                            "name": r[0],
                            "type": r[1],
                            "length": r[2],
                            "nullable": r[3] == "Y",
                            "default": r[4],
                        }
                        for r in cur.fetchall()
                    ]
                cur.execute("SELECT view_name, text FROM user_views ORDER BY view_name")
                for row in cur.fetchall():
                    schema.views.append({"name": row[0], "definition": row[1]})
                cur.execute(
                    "SELECT object_name, object_type, status FROM user_objects WHERE object_type IN ('PROCEDURE','FUNCTION') ORDER BY object_name"
                )
                for row in cur.fetchall():
                    if row[1] == "PROCEDURE":
                        schema.procedures.append({"name": row[0], "status": row[2]})
                    elif row[1] == "FUNCTION":
                        schema.functions.append({"name": row[0], "status": row[2]})
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
            cur = self._connection.cursor()
            try:
                if schema:
                    cur.execute("SELECT table_name FROM all_tables WHERE owner = :o ORDER BY table_name", {"o": schema})
                else:
                    cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
                for row in cur.fetchall():
                    names.append(row[0])
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
                cur.execute("EXPLAIN PLAN FOR " + query)
                self._connection.rollback()
                return True
            finally:
                cur.close()

        try:
            await loop.run_in_executor(None, _validate)
            return True
        except cx_Oracle.Error:
            return False

    async def test_connection(self) -> bool:
        if not self._is_connected:
            return False
        loop = asyncio.get_event_loop()

        def _test():
            cur = self._connection.cursor()
            try:
                cur.execute("SELECT 1 FROM dual")
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
