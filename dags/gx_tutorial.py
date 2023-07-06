"""
## Run a Great Expectations validation suite on a Postgres table

This DAG runs the Great Expectations validation suite `strawberry_suite` 
located at `include/great_expectations/expectations/strawberry_suite.json`
on a Postgres table. Needs a Postgres connection with the name `postgres_default`.
"""

from pendulum import datetime
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

POSTGRES_CONN_ID = "postgres_default"


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
)
def gx_tutorial():
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE strawberries (
                id VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100),
                amount INT
            );

            INSERT INTO strawberries (id, name, amount)
            VALUES ('001', 'Strawberry Order 1', 10),
                ('002', 'Strawberry Order 2', 5),
                ('003', 'Strawberry Order 3', 8),
                ('004', 'Strawberry Order 4', 3),
                ('005', 'Strawberry Order 5', 12);
            """,
    )

    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir="include/great_expectations",
        data_asset_name="strawberries",
        expectation_suite_name="strawberry_suite",
        return_json_dict=True,
    )

    drop_table_pg = PostgresOperator(
        task_id="drop_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE strawberries;
            """,
    )

    create_table_pg >> gx_validate_pg >> drop_table_pg


gx_tutorial()
