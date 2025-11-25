from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List

from ...models import DatabaseConnection, QueryResult, DatabaseSchema, QueryType


class BaseDatabaseConnector(ABC):
    def __init__(self, connection: DatabaseConnection):
        self.connection = connection
        self._is_connected = False

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @abstractmethod
    async def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        pass

    @abstractmethod
    async def get_schema(self) -> DatabaseSchema:
        pass

    @abstractmethod
    async def list_tables(self, schema: Optional[str] = None) -> List[str]:
        pass

    @abstractmethod
    async def validate_syntax(self, query: str) -> bool:
        pass

    @abstractmethod
    async def test_connection(self) -> bool:
        pass

    @abstractmethod
    def get_query_type(self, query: str) -> QueryType:
        pass

    async def close(self) -> None:
        if self._is_connected:
            await self.disconnect()
            self._is_connected = False

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    def _parse_query_type(self, query: str) -> QueryType:
        q = query.strip().upper()
        if q.startswith("SELECT"):
            return QueryType.SELECT
        if q.startswith("INSERT"):
            return QueryType.INSERT
        if q.startswith("UPDATE"):
            return QueryType.UPDATE
        if q.startswith("DELETE"):
            return QueryType.DELETE
        if q.startswith("CREATE"):
            return QueryType.CREATE
        if q.startswith("ALTER"):
            return QueryType.ALTER
        if q.startswith("DROP"):
            return QueryType.DROP
        return QueryType.SELECT
