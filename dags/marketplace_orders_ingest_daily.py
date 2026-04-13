from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="marketplace_orders_ingest_daily",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["marketplace", "elt", "j3"],
)
def marketplace_orders_ingest_daily():

    @task
    def extract_orders(ds: str) -> str:
        from hooks.marketplace_api import MarketplaceAPIHook

        hook = MarketplaceAPIHook(marketplace_api_conn_id="marketplace_api")
        orders = hook.get_orders(ds)

        local_path = f"/tmp/orders_{ds}.json"
        with open(local_path, "w", encoding="utf-8") as file:
            json.dump(orders, file, ensure_ascii=False)

        return local_path

    @task
    def upload_raw_to_minio(local_path: str, ds: str) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        bucket_name = "marketplace-raw"
        object_key = f"orders/dt={ds}/orders.json"

        s3_hook = S3Hook(aws_conn_id="minio_local")
        s3_hook.load_file(
            filename=local_path,
            key=object_key,
            bucket_name=bucket_name,
            replace=True,
        )

        return f"s3://{bucket_name}/{object_key}"

    @task
    def load_staging_orders(local_path: str) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        with open(local_path, "r", encoding="utf-8") as file:
            orders = json.load(file)

        rows = [
            (
                order["id"],
                order["seller_id"],
                order["customer_id"],
                order["product_id"],
                order["date"],
                order["quantity"],
                order["total"],
                order["status"],
            )
            for order in orders
        ]

        pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")
        pg_hook.run("CREATE SCHEMA IF NOT EXISTS staging;")

        pg_hook.insert_rows(
            table="dwh.fact_orders",
            rows=rows,
            target_fields=[
                "order_id",
                "seller_id",
                "customer_id",
                "product_id",
                "dt",
                "quantity",
                "total_amount",
                "status",
            ],
            replace=False,
        )

        return len(rows)

    local_path = extract_orders()
    raw_uploaded = upload_raw_to_minio(local_path)
    load_staging_orders(local_path) << raw_uploaded


marketplace_orders_ingest_daily()