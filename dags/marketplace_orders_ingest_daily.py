from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task


def map_orders_to_staging_rows(orders):
    """transforme la liste de commandes JSON en liste de tuples pour le staging"""
    result = []
    for order in orders:
        row = (
            order["id"],
            order["seller_id"],
            order["customer_id"],
            order["product_id"],
            order["date"],
            order["quantity"],
            order["total"],
            order["status"],
        )
        result.append(row)
    return result


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
    def extract_orders(ds: str = None) -> str:
        from hooks.marketplace_api import MarketplaceAPIHook

        # si ds est pas fourni (trigger manuel) on prend aujourd'hui
        if ds is None:
            ds = datetime.now().strftime("%Y-%m-%d")

        hook = MarketplaceAPIHook(marketplace_api_conn_id="marketplace_api")
        orders = hook.get_orders(ds)
        print(f"extraction de {len(orders)} commandes pour {ds}")

        # on sauvegarde en local pour les autres tasks
        local_path = f"/tmp/orders_{ds}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(orders, f, ensure_ascii=False)

        return local_path

    @task
    def upload_raw_to_minio(local_path: str, ds: str = None) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        if ds is None:
            ds = datetime.now().strftime("%Y-%m-%d")

        bucket = "marketplace-raw"
        key = f"orders/dt={ds}/orders.json"

        s3 = S3Hook(aws_conn_id="minio_local")
        s3.load_file(
            filename=local_path,
            key=key,
            bucket_name=bucket,
            replace=True,
        )
        print(f"uploaded to s3://{bucket}/{key}")

        return f"s3://{bucket}/{key}"

    @task
    def load_staging_orders(local_path: str, ds: str = None) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if ds is None:
            ds = datetime.now().strftime("%Y-%m-%d")

        with open(local_path, "r", encoding="utf-8") as f:
            orders = json.load(f)

        rows = map_orders_to_staging_rows(orders)

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        # on supprime les donnees de la date avant de re-inserer (idempotence)
        pg.run(
            "DELETE FROM staging.orders WHERE dt = %s",
            parameters=(ds,),
        )

        pg.insert_rows(
            table="staging.orders",
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
        )
        print(f"charge {len(rows)} lignes dans staging.orders")

        return len(rows)

    @task
    def transform_staging_to_dwh(ds: str = None) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if ds is None:
            ds = datetime.now().strftime("%Y-%m-%d")

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        # pattern idempotent : on delete la partition puis on re-insere depuis staging
        pg.run(
            """
            DELETE FROM dwh.fact_orders WHERE dt = %(ds)s;
            INSERT INTO dwh.fact_orders
                (order_id, seller_id, customer_id, product_id, dt,
                 quantity, total_amount, status)
            SELECT
                order_id, seller_id, customer_id, product_id, dt,
                quantity, total_amount, status
            FROM staging.orders
            WHERE dt = %(ds)s;
            """,
            parameters={"ds": ds},
        )
        print(f"transform staging -> dwh.fact_orders done pour {ds}")

    # enchainement des tasks
    local_path = extract_orders()
    upload_raw_to_minio(local_path)
    nb_rows = load_staging_orders(local_path)
    transform_staging_to_dwh() << nb_rows


marketplace_orders_ingest_daily()
