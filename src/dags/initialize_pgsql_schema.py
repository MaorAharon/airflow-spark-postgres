import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

START_DATE = datetime(2018, 8, 20)

def get_create_schema(schema: str) -> str:
    return f"""
        DO $$
        DECLARE
            my_schema varchar := '{schema}';
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.schemata WHERE schema_name = format('%I', my_schema)
          ) THEN
            EXECUTE format('CREATE SCHEMA %I', my_schema);
          END IF;
        END $$;
           """

default_args = {
    "owner": "Maor Aharon",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["maor.hrn@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG("initialize-pgsql-schema",
         description="This DAG should be triggered manully only once in order to initialize pgsql schemas and tables.",
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         max_active_runs=1,
         ) as dag:

    create_schema_stage = PostgresOperator(
        task_id='create_schema_stage',
        postgres_conn_id='postgres_conn',
        sql=get_create_schema('stage'),
    )

    create_schema_taxi = PostgresOperator(
        task_id='create_schema_taxi',
        postgres_conn_id='postgres_conn',
        sql=get_create_schema('taxi'),
    )

    create_tbl_ny_yellow = PostgresOperator(
        task_id='create_tbl_ny_yellow',
        postgres_conn_id='postgres_conn',
        sql=f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT FROM pg_tables WHERE schemaname = 'taxi' AND tablename  = 'ny_yellow'
          ) THEN
            EXECUTE 
                ('CREATE TABLE taxi.ny_yellow (
                    vendor_id bigint, -- VendorID
                    tpep_pickup_datetime timestamp without time zone,
                    tpep_dropoff_datetime timestamp without time zone,
                    passenger_count double precision,
                    trip_distance double precision,
                    ratecode_id double precision, -- RatecodeID
                    store_and_fwd_flag text,
                    pu_location_id bigint, -- PULocationID
                    do_location_id bigint, -- DOLocationID
                    payment_type bigint,
                    fare_amount double precision,
                    extra double precision,
                    mta_tax double precision,
                    tip_amount double precision,
                    tolls_amount double precision,
                    improvement_surcharge double precision,
                    total_amount double precision,
                    congestion_surcharge double precision,
                    airport_fee integer,
                    date  date not null
                ) PARTITION BY RANGE (date)');
          END IF;
        END $$;
           """,
    )

    create_cnt_ny_yellow = PostgresOperator(
        task_id='create_cnt_ny_yellow',
        postgres_conn_id='postgres_conn',
        sql=f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT FROM pg_tables WHERE schemaname = 'taxi' AND tablename  = 'ny_yellow_cnt'
          ) THEN
            EXECUTE 
                ('CREATE TABLE taxi.ny_yellow_cnt (
                    cnt bigint,
                    date date not null) 
                    ');
          END IF;
        END $$;
           """,
    )

    create_schema_stage >> create_schema_taxi >> [create_tbl_ny_yellow, create_cnt_ny_yellow]