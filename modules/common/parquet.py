import pandas as pd

def export_to_parquet(engine, query: str, col_names: list[str], file_path: str, chunk_size: int) -> str:
    """
    SQLAlchemy 엔진으로 쿼리를 실행해서 청크 단위로 parquet 파일에 씁니다.
    테이블이 비어있는 경우에도 컬럼 스키마를 가진 빈 파일을 생성합니다.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    writer = None
    try:
        for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
            chunk.columns = col_names
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(file_path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        pd.DataFrame(columns=col_names).to_parquet(file_path, index=False, engine="pyarrow")

    return file_path
