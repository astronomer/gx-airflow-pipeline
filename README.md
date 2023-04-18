Overview
========

Welcome! This is a sample repository showing how to use the new GreatExpectationsOperator in an Apache Airflow DAG. 

How to use this repository
==========================

1. Run `git clone https://github.com/astronomer/gx-airflow-pipeline.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.
5. Create an Airflow connection to your database. The example uses a Snowflake connection called `snowflake_conn` but any database both [supported by the Astro Python SDK](https://astro-sdk-python.readthedocs.io/en/stable/supported_databases.html) as well as the expectations used in your GX suite will work!


Useful Links
============

- [Great Expectations](https://greatexpectations.io/)
- [Apache Airflow](https://airflow.apache.org/)
- [Great Expectations Provider](https://registry.astronomer-dev.io/providers/airflow-provider-great-expectations/versions/0.2.6)
- [Orchestrate Great Expectations with Airflow Guide](https://docs.astronomer.io/learn/airflow-great-expectations)


Data source
===========

The Data used is a subset of this dataset found on the Open power System Data platform: Open Power System Data. 2020. Data Package National generation capacity. Version 2020-10-01. https://doi.org/10.25832/national_generation_capacity/2020-10-01. (Primary data from various sources, for a complete list see URL).