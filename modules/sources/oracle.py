from sources.base import SourceAdapter

class OracleAdapter(SourceAdapter):

    TYPE_MAP = {
        "NUMBER": "NUMBER",
        "FLOAT": "FLOAT",
        "VARCHAR2": "STRING",
        "NVARCHAR2": "STRING",
        "CHAR": "STRING",
        "CLOB": "STRING",
        "DATE": "TIMESTAMP_NTZ",
        "TIMESTAMP": "TIMESTAMP_NTZ",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
    }

    def __init__(self, conn_id: str, schema: str):
        super().__init__(conn_id, schema.upper())

    def get_hook(self):
        from airflow.providers.oracle.hooks.oracle import OracleHook
        return OracleHook(self.conn_id)

    def get_tables(self) -> list[str]:
        ora = self.get_hook()
        return [
            r[0]
            for r in ora.get_records(
                "SELECT table_name FROM all_tables WHERE owner = :1",
                parameters=(self.schema,),
            )
        ]

    def get_columns(self, table_name: str) -> list[tuple[str, str]]:
        ora = self.get_hook()
        return ora.get_records(
            """
            SELECT column_name, data_type
            FROM all_tab_columns
            WHERE table_name = :1 AND owner = :2
            ORDER BY column_id
            """,
            parameters=(table_name.upper(), self.schema),
        )

    def map_type(self, native_type: str) -> str:
        return self.TYPE_MAP.get(native_type.upper(), "STRING")

    def build_select_query(self, table_name: str) -> str:
        safe_name = table_name.replace('"', '""')
        return f'SELECT * FROM "{self.schema}"."{safe_name}"'

    def get_row_count(self, table_name: str) -> int:
        ora = self.get_hook()
        safe_name = table_name.replace('"', '""')
        return ora.get_first(f'SELECT COUNT(*) FROM "{self.schema}"."{safe_name}"')[0]
