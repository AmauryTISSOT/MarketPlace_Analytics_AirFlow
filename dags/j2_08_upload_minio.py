from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="j2_08_upload_minio",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j2"],
)
def upload_minio():

    @task()
    def create_and_upload() -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import json

        data = [{"id": 1, "product": "Widget A"}, {"id": 2, "product": "Widget B"}]
        local_path = "/tmp/test_orders.json"
        with open(local_path, "w") as f:
            json.dump(data, f)

        hook = S3Hook(aws_conn_id="minio_local")
        s3_key = "raw/test/orders.json"
        hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name="data-lake",
            replace=True,
        )
        print(f"Uploaded to s3://data-lake/{s3_key}")
        return f"s3://data-lake/{s3_key}"

    create_and_upload()

upload_minio()
