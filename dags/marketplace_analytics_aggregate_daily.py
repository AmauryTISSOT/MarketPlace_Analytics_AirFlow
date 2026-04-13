from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import Asset



# 🎯 Asset attendu par le projet (naming cohérent avec le schéma)
DWH_ORDERS_ASSET = Asset("dwh_orders")


@dag(
    dag_id="marketplace_analytics_aggregate_daily",
    schedule=[DWH_ORDERS_ASSET],  # 🔥 déclenché par la disponibilité du DWH
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["marketplace", "analytics", "metabase"],
)
def marketplace_analytics_aggregate_daily():

    # 1. KPI globaux (CA, volume, panier moyen)
    @task
    def build_daily_metrics(ds: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        pg.run(
            """
            DELETE FROM analytics.daily_metrics WHERE dt = %(ds)s;

            INSERT INTO analytics.daily_metrics
            SELECT
                dt,
                COUNT(*) AS orders_count,
                SUM(quantity) AS total_quantity,
                SUM(total_amount) AS gmv,
                AVG(total_amount) AS avg_order_value
            FROM dwh.fact_orders
            WHERE dt = %(ds)s
            GROUP BY dt;
            """,
            parameters={"ds": ds},
        )

    # 2. Top sellers (dashboard Metabase)
    @task
    def build_seller_metrics(ds: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        pg.run(
            """
            DELETE FROM analytics.seller_metrics WHERE dt = %(ds)s;

            INSERT INTO analytics.seller_metrics
            SELECT
                o.dt,
                o.seller_id,
                s.name,
                SUM(o.total_amount) AS revenue,
                COUNT(*) AS orders_count
            FROM dwh.fact_orders o
            JOIN dwh.dim_seller s USING (seller_id)
            WHERE o.dt = %(ds)s
            GROUP BY o.dt, o.seller_id, s.name;
            """,
            parameters={"ds": ds},
        )

    # 3. Répartition par catégorie
    @task
    def build_category_metrics(ds: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        pg.run(
            """
            DELETE FROM analytics.category_metrics WHERE dt = %(ds)s;

            INSERT INTO analytics.category_metrics
            SELECT
                o.dt,
                p.category,
                SUM(o.total_amount) AS revenue,
                COUNT(*) AS orders_count
            FROM dwh.fact_orders o
            JOIN dwh.dim_product p USING (product_id)
            WHERE o.dt = %(ds)s
            GROUP BY o.dt, p.category;
            """,
            parameters={"ds": ds},
        )

    #  4. Clients actifs vs dormants
    @task
    def build_customer_metrics(ds: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        pg.run(
            """
            DELETE FROM analytics.customer_metrics WHERE dt = %(ds)s;

            INSERT INTO analytics.customer_metrics
            SELECT
                %(ds)s AS dt,
                COUNT(DISTINCT customer_id) AS active_customers
            FROM dwh.fact_orders
            WHERE dt = %(ds)s;
            """,
            parameters={"ds": ds},
        )

    #  Orchestration simple (parallélisable)
    daily = build_daily_metrics()
    seller = build_seller_metrics()
    category = build_category_metrics()
    customer = build_customer_metrics()

    daily >> [seller, category, customer]


# 🚀 instanciation
marketplace_analytics_aggregate_daily()