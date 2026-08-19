import pandas as pd

def export_to_parquet(
    engine,
    query: str,
    col_names: list[str],
    file_path: str,
    chunk_size: int,
) -> str:

    import pyarrow as pa
    import pyarrow.parquet as pq

    writer = None

    try:
        for chunk in pd.read_sql(
            query,
            engine,
            chunksize=chunk_size,
        ):
            chunk.columns = col_names

            # timestamp를 microsecond 정밀도로 통일
            for col in chunk.columns:
                if pd.api.types.is_datetime64_any_dtype(chunk[col]):
                    chunk[col] = chunk[col].astype("datetime64[us]")

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False,
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    file_path,
                    table.schema,
                )

            writer.write_table(table)

    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        pd.DataFrame(columns=col_names).to_parquet(
            file_path,
            index=False,
            engine="pyarrow",
        )

    return file_path
