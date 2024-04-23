import duckdb
import s3fs

path_s3 = "lgaliana/data/elections_example.parquet"
fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "https://minio.lab.sspcloud.fr"})

duckdb.query("""
COPY
    (SELECT * FROM read_csv_auto(
        'https://www.data.gouv.fr/fr/datasets/r/52a11762-45b4-414f-b6a2-eaa290217dc6',
        columns = {'Voix': 'VARCHAR'})
    )
    TO 'output.parquet'
    (FORMAT 'parquet');
""")

fs.put("output.parquet", path_s3)
