from __future__ import annotations

from datetime import datetime
import json
import os

from airflow.decorators import dag, task


@dag(
    dag_id="marketplace_orders_ingest_daily",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["marketplace", "elt", "j3"],
)
def marketplace_orders_ingest_daily():

    @task
    def extract_orders(ds: str) -> str:
        from plugins.hooks.marketplace_api import MarketplaceAPIHook

        print("ici dag")

        hook = MarketplaceAPIHook(marketplace_api_conn_id="marketplace_api")
        print("After hook")
        orders = hook.get_orders(ds)
        print("After orders")

        local_path = f"/tmp/orders_{ds}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(orders, f, ensure_ascii=False)

        return local_path

    @task
    def upload_raw_to_minio(local_path: str, ds: str) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        bucket_name = "marketplace-raw"
        key = f"orders/dt={ds}/orders.json"

        hook = S3Hook(aws_conn_id="minio_local")
        hook.load_file(
            filename=local_path,
            key=key,
            bucket_name=bucket_name,
            replace=True,
        )

        return f"s3://{bucket_name}/{key}"

    @task
    def load_staging_orders(local_path: str) -> int:
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        with open(local_path, "r", encoding="utf-8") as f:
            orders = json.load(f)

        rows = []
        for o in orders:
            rows.append((
                o["id"],
                o["date"],
                o.get("seller_id"),
                o["customer_id"],
                o.get("product_id"),
                o.get("product"),
                o["quantity"],
                o["unit_price"],
                o["total"],
                o["status"],
            ))

        pg.run("""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.orders (
            id VARCHAR(30) PRIMARY KEY,
            date DATE NOT NULL,
            seller_id VARCHAR(20),
            customer_id VARCHAR(20) NOT NULL,
            product_id VARCHAR(20),
            product VARCHAR(100),
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            total DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) NOT NULL,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
        """)

        pg.insert_rows(
            table="staging.orders",
            rows=rows,
            target_fields=[
                "id",
                "date",
                "seller_id",
                "customer_id",
                "product_id",
                "product",
                "quantity",
                "unit_price",
                "total",
                "status",
            ],
            replace=True,
        )

        return len(rows)

    local_path = extract_orders()
    raw_uri = upload_raw_to_minio(local_path)
    load_staging_orders(local_path)


marketplace_orders_ingest_daily()