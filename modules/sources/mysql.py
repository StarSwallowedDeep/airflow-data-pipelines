from sources.base import SourceAdapter

class MySQLAdapter(SourceAdapter):

    TYPE_MAP = {
        "tinyint": "NUMBER",
        "smallint": "NUMBER",
        "mediumint": "NUMBER",
        "int": "NUMBER",
        "bigint": "NUMBER",
        "decimal": "NUMBER",
        "float": "FLOAT",
        "double": "FLOAT",
        "varchar": "STRING",
        "char": "STRING",
        "text": "STRING",
        "longtext": "STRING",
        "date": "DATE",
        "datetime": "TIMESTAMP_NTZ",
        "timestamp": "TIMESTAMP_NTZ",
        "json": "VARIANT",
    }

    def get_hook(self):
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        return MySqlHook(self.conn_id)

    def get_tables(self) -> list[str]:
        mysql = self.get_hook()
        return [
            r[0]
            for r in mysql.get_records(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                parameters=(self.schema,),
            )
        ]

    def get_columns(self, table_name: str) -> list[tuple[str, str]]:
        mysql = self.get_hook()
        return mysql.get_records(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
            """,
            parameters=(table_name, self.schema),
        )

    def map_type(self, native_type: str) -> str:
        return self.TYPE_MAP.get(native_type.lower(), "STRING")

    def build_select_query(self, table_name: str) -> str:
        # MySQL은 backtick으로 escaping. 내부 백틱은 이중화 처리.
        safe_name = table_name.replace("`", "``")
        return f"SELECT * FROM `{safe_name}`"

    def get_row_count(self, table_name: str) -> int:
        mysql = self.get_hook()
        safe_name = table_name.replace("`", "``")
        return mysql.get_first(f"SELECT COUNT(*) FROM `{safe_name}`")[0]
