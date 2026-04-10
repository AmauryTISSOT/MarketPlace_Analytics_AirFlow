from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="j2_08_read_postgres",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j2"],
)
def read_postgres():

    @task()
    def create_table() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        hook.run("""
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE TABLE IF NOT EXISTS staging.test_orders (
                id SERIAL PRIMARY KEY,
                product VARCHAR(100),
                amount DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT NOW()
            );
            INSERT INTO staging.test_orders (product, amount)
            VALUES ('Widget A', 29.99), ('Widget B', 49.99), ('Widget C', 9.99)
            ON CONFLICT DO NOTHING;
        """)
        print("Table creee et donnees inserees")

    @task()
    def read_orders() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        records = hook.get_records("SELECT COUNT(*) FROM staging.test_orders;")
        count = records[0][0]
        print(f"Nombre de commandes : {count}")
        return count

    create_table() >> read_orders()

read_postgres()
