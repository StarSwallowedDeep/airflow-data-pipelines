from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def qualified_table(schema: str, table_name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table_name)}"

def create_tables(sf: SnowflakeHook, schema: str, table_name: str, staging_name: str, ddl: str):
    sf.run(f"CREATE TABLE IF NOT EXISTS {qualified_table(schema, table_name)} ({ddl})")
    sf.run(f"CREATE OR REPLACE TABLE {qualified_table(schema, staging_name)} ({ddl})")

def upload_to_stage(sf: SnowflakeHook, stage_name: str, file_path: str):
    sf.run(f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

def copy_into_and_swap(sf: SnowflakeHook, schema: str, stage_name: str, table_name: str, staging_name: str, file_name: str):
    sf.run(f"COPY INTO {qualified_table(schema, staging_name)} FROM @{stage_name}/{file_name} FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE")
    sf.run(f"ALTER TABLE {qualified_table(schema, table_name)} SWAP WITH {qualified_table(schema, staging_name)}")

def get_row_count(sf: SnowflakeHook, schema: str, table_name: str) -> int:
    return sf.get_first(f"SELECT COUNT(*) FROM {qualified_table(schema, table_name)}")[0]

def cleanup_stage_and_staging(sf: SnowflakeHook, schema: str, stage_name: str, table_name: str, staging_name: str, file_name: str):
    sf.run(f"REMOVE @{stage_name}/{file_name}")
    sf.run(f"DROP TABLE IF EXISTS {qualified_table(schema, staging_name)}")
