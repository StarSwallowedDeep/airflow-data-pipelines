from sources.base import SourceAdapter
from psycopg2 import sql as pg_sql

class PostgresAdapter(SourceAdapter):

    TYPE_MAP = {
        "smallint": "NUMBER",
        "integer": "NUMBER",
        "bigint": "NUMBER",
        "numeric": "NUMBER",
        "real": "FLOAT",
        "double precision": "FLOAT",
        "text": "STRING",
        "character varying": "STRING",
        "character": "STRING",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp without time zone": "TIMESTAMP_NTZ",
        "timestamp with time zone": "TIMESTAMP_TZ",
        "json": "VARIANT",
        "jsonb": "VARIANT",
        "uuid": "STRING",
    }

    def get_hook(self):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        return PostgresHook(self.conn_id)

    def get_tables(self) -> list[str]:
        pg = self.get_hook()
        return [
            r[0]
            for r in pg.get_records(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s",
                parameters=(self.schema,),
            )
        ]

    def get_columns(self, table_name: str) -> list[tuple[str, str]]:
        pg = self.get_hook()
        return pg.get_records(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
            """,
            parameters=(table_name, self.schema),
        )

    def map_type(self, native_type: str) -> str:
        return self.TYPE_MAP.get(native_type, "STRING")

    def build_select_query(self, table_name: str) -> str:
        pg = self.get_hook()
        return pg_sql.SQL("SELECT * FROM {}").format(
            pg_sql.Identifier(table_name)
        ).as_string(pg.get_conn())

    def get_row_count(self, table_name: str) -> int:
        pg = self.get_hook()
        return pg.get_first(
            pg_sql.SQL("SELECT COUNT(*) FROM {}").format(
                pg_sql.Identifier(table_name)
            ).as_string(pg.get_conn())
        )[0]
