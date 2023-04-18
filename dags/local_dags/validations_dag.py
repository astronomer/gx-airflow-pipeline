from airflow.decorators import dag
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

import pandas as pd
from pendulum import datetime
from sklearn.linear_model import LinearRegression
import logging

task_logger = logging.getLogger("airflow.task")

IN_DATA_FILE_PATH = "include/data/energy_data.csv"
FILESYSTEM_CONN_ID = "local_filesystem_conn"
DB_CONN_ID = "snowflake_conn"
DB_SCHEMA = "TAMARAFINGERLIN"
GX_ROOT_DIR = "include/great_expectations"
GX_SUITE_NAME = "energy_suite"
REPORT_TABLE_NAME = "reporting_energy"
COUNTRIES = ["CH", "FR"]


@aql.transform
def transform_energy(in_table, countries):
    return """
            SELECT 
                RENEWABLES_CAPACITY/TOTAL_CAPACITY AS RENEWABLES_PCT,
                {{ in_table }}.*
            FROM {{ in_table }}
            WHERE TOTAL_CAPACITY > 0 AND COUNTRY IN ({{ countries }});
            """


@aql.dataframe
def analyze(df: pd.DataFrame):
    countries = df["country"].unique()

    for country in countries:
        country_data = df[df["country"] == country]
        country_data_since_2010 = country_data[country_data["year"] >= 2010]
        X = country_data_since_2010["year"].values.reshape(-1, 1)
        y_r = country_data_since_2010["renewables_pct"]
        model_r = LinearRegression()
        model_r.fit(X, y_r)
        task_logger.info(
            "Important note: this is about energy produced, not necessarily energy consumed!"
        )
        task_logger.info(
            f"{country}: renewables Linear regression coefficient: {model_r.coef_[0]}"
        )
        task_logger.info(country_data_since_2010)

    return df


def validation_failure_callback_func():
    print("Oh no!")


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
)
def validations_dag():
    extracted_data = aql.load_file(
        input_file=File(path=IN_DATA_FILE_PATH, conn_id=FILESYSTEM_CONN_ID),
        output_table=Table(conn_id=DB_CONN_ID, metadata=Metadata(schema=DB_SCHEMA)),
    )

    transformed_data = transform_energy(
        extracted_data,
        countries=COUNTRIES,
        output_table=Table(
            conn_id=DB_CONN_ID,
            metadata=Metadata(schema=DB_SCHEMA),
            name=REPORT_TABLE_NAME,
        ),
    )

    gx_validate = GreatExpectationsOperator(
        task_id="gx_validate",
        conn_id=DB_CONN_ID,
        data_context_root_dir=GX_ROOT_DIR,
        data_asset_name=f"{DB_SCHEMA}.{REPORT_TABLE_NAME}",
        expectation_suite_name=GX_SUITE_NAME,
        do_xcom_push=False,
        validation_failure_callback=validation_failure_callback_func,
    )

    transformed_data >> gx_validate

    gx_validate >> analyze(
        transformed_data,
        output_table=Table(conn_id=DB_CONN_ID, metadata=Metadata(schema=DB_SCHEMA)),
    )

    aql.cleanup()


validations_dag()
