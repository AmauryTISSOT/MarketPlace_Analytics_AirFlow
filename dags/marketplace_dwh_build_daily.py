from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import Asset


# DAG 2 : on refresh les tables de dimensions
# declenche automatiquement quand DAG 1 produit l'asset "raw_orders"

@dag(
    dag_id="marketplace_dwh_build_daily",
    schedule=[Asset("raw_orders")],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["marketplace", "dwh", "dimensions"],
)
def marketplace_dwh_build_daily():

    @task
    def refresh_dim_seller():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from hooks.marketplace_api import MarketplaceAPIHook

        hook = MarketplaceAPIHook(marketplace_api_conn_id="marketplace_api")
        sellers = hook.get_sellers()
        print(f"on a recupere {len(sellers)} sellers")

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        # on upsert chaque seller un par un
        for s in sellers:
            pg.run(
                """
                INSERT INTO dwh.dim_seller (seller_id, name, country, joined_date)
                VALUES (%(seller_id)s, %(name)s, %(country)s, %(joined_date)s)
                ON CONFLICT (seller_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    country = EXCLUDED.country,
                    joined_date = EXCLUDED.joined_date;
                """,
                parameters={
                    "seller_id": s["id"],
                    "name": s["name"],
                    "country": s.get("country"),
                    "joined_date": s.get("joined_date"),
                },
            )
        print("sellers done")
        return len(sellers)

    @task
    def refresh_dim_customer():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from hooks.marketplace_api import MarketplaceAPIHook

        hook = MarketplaceAPIHook(marketplace_api_conn_id="marketplace_api")
        # on prend 500 customers max (c'est la limite de l'API)
        customers = hook.get_customers(limit=500)
        print(f"nb customers: {len(customers)}")

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        for c in customers:
            pg.run(
                """
                INSERT INTO dwh.dim_customer (customer_id, email, city, signup_date)
                VALUES (%(customer_id)s, %(email)s, %(city)s, %(signup_date)s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    email = EXCLUDED.email,
                    city = EXCLUDED.city,
                    signup_date = EXCLUDED.signup_date;
                """,
                parameters={
                    "customer_id": c["id"],
                    "email": c.get("email"),
                    "city": c.get("city"),
                    # attention l'API renvoie "registered_date" mais la table c'est "signup_date"
                    "signup_date": c.get("registered_date"),
                },
            )
        print("customers done")
        return len(customers)

    @task
    def refresh_dim_product():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from hooks.marketplace_api import MarketplaceAPIHook

        hook = MarketplaceAPIHook(marketplace_api_conn_id="marketplace_api")
        products = hook.get_products()
        print(f"nb products: {len(products)}")

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        for p in products:
            pg.run(
                """
                INSERT INTO dwh.dim_product (product_id, name, category, base_price)
                VALUES (%(product_id)s, %(name)s, %(category)s, %(base_price)s)
                ON CONFLICT (product_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    base_price = EXCLUDED.base_price;
                """,
                parameters={
                    "product_id": p["id"],
                    "name": p["name"],
                    "category": p.get("category"),
                    # pareil, l'API dit "price" mais nous on stocke "base_price"
                    "base_price": p.get("price"),
                },
            )
        print("products done")
        return len(products)

    @task
    def refresh_dim_date(ds: str = None):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # si ds est pas fourni (trigger manuel) on prend la date du jour
        if ds is None:
            ds = datetime.now().strftime("%Y-%m-%d")

        dt = datetime.strptime(ds, "%Y-%m-%d")
        print(f"refresh dim_date pour {ds}")

        pg = PostgresHook(postgres_conn_id="postgres_dwh")

        # on insert la date seulement si elle existe pas deja
        pg.run(
            """
            INSERT INTO dwh.dim_date (dt, year, month, day_of_week)
            VALUES (%(dt)s, %(year)s, %(month)s, %(day_of_week)s)
            ON CONFLICT (dt) DO NOTHING;
            """,
            parameters={
                "dt": ds,
                "year": dt.year,
                "month": dt.month,
                "day_of_week": dt.weekday(),
            },
        )

    @task(outlets=[Asset("dwh_orders")])
    def signal_dwh_ready():
        print("dimensions refreshed, dwh pret")

    # les 4 tasks sont independantes donc elles tournent en parallele
    # une fois toutes finies, on signal que le dwh est pret (declenche DAG 3)
    s = refresh_dim_seller()
    c = refresh_dim_customer()
    p = refresh_dim_product()
    d = refresh_dim_date()
    [s, c, p, d] >> signal_dwh_ready()


marketplace_dwh_build_daily()
