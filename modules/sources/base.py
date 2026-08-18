from abc import ABC, abstractmethod

class SourceAdapter(ABC):

    def __init__(self, conn_id: str, schema: str):
        self.conn_id = conn_id
        self.schema = schema

    @abstractmethod
    def get_hook(self):
        """Airflow Hook 인스턴스를 반환."""
        ...

    @abstractmethod
    def get_tables(self) -> list[str]:
        """동기화 대상 테이블 목록을 반환."""
        ...

    @abstractmethod
    def get_columns(self, table_name: str) -> list[tuple[str, str]]:
        """(column_name, native_type) 리스트를 ordinal_position 순서로 반환."""
        ...

    @abstractmethod
    def map_type(self, native_type: str) -> str:
        """소스 DB의 타입명을 Snowflake 타입명으로 변환."""
        ...

    @abstractmethod
    def build_select_query(self, table_name: str) -> str:
        """테이블 전체를 뽑아오는 SELECT 쿼리 문자열 (SQL Injection 안전 처리 포함)."""
        ...

    @abstractmethod
    def get_row_count(self, table_name: str) -> int:
        """validate 단계에서 사용할 row count."""
        ...
