import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
START_DATE = datetime(2010, 1, 1)

###############################################
# Parameters
###############################################
spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark:7077"
postgres_driver_jar = "/shared-data/accessories/postgresql-42.2.6.jar"
# wget_path = "/opt/bitnami/spark/tmp/wget.py"
wget_path = "/shared-data/accessories/wget.py"

class PostgresOperatorWithOutput(PostgresOperator):
    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        self.log.info("Executing: %s", self.sql)
        output = pg_hook.get_records(self.sql, parameters=self.parameters)
        self.log.info("Output: %s", output)
        if output:
            self.xcom_push(context, key="return_value", value=1)
        else:
            self.xcom_push(context, key="return_value", value=0)

def is_stage_empty_fn():
    context = get_current_context()
    cnt = context['ti'].xcom_pull(task_ids='cnt_stage')
    if cnt > 0:
        return 'attach_partition'
    else:
        return 'drop_table'

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

with DAG("load_ny_yellow",
         description="This DAG download trip-data data and load it into taxi.ny_yellow pgsql table",
         default_args=default_args,
         schedule_interval="0 0 1 * *", # once a month
         catchup=True,
         max_active_runs=3, # execute 3 in parallel
         ) as dag:

    detach_drop_delete_table = PostgresOperator(
        task_id='detach_drop_delete_table',
        postgres_conn_id='postgres_conn',
        sql="""
        DO $$
        DECLARE
          source_table varchar := 'ny_yellow';
          my_schema varchar := 'taxi';
          partition_table_schema varchar := 'stage';
          partition_table varchar := 'yellow_tripdata_{{ logical_date.strftime('%Y_%m') }}';
          ld varchar := '{{ logical_date.format('YYYY-MM-DD') }}';
        BEGIN
          IF EXISTS (
            SELECT *
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE
              i.inhparent = format('%I.%I', my_schema, source_table)::regclass
              AND c.relname = partition_table
              AND n.nspname = partition_table_schema
          ) THEN
            EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I', my_schema, source_table, partition_table_schema, partition_table);
          END IF;
          
          IF EXISTS (
            SELECT FROM pg_tables WHERE schemaname = format('%I', partition_table_schema) AND tablename = format('%I', partition_table)
          ) THEN
            EXECUTE format('DROP TABLE %I.%I', partition_table_schema, partition_table);
          END IF;
          
          IF EXISTS (
            SELECT * FROM taxi.ny_yellow_cnt WHERE date = TO_DATE(ld, 'YYYY-MM-DD')	
          ) THEN
            EXECUTE format('DELETE FROM taxi.ny_yellow_cnt WHERE date = ''%I'' ', ld);  
          END IF;
          
        END $$;
        """,
    )

    create_stage_tbl = PostgresOperator(
        task_id='create_stage_tbl',
        postgres_conn_id='postgres_conn',
        sql="""
        CREATE TABLE stage.yellow_tripdata_{{ logical_date.strftime('%Y_%m') }} (
            LIKE taxi.ny_yellow INCLUDING CONSTRAINTS
        );
        """
    )

    process_spark_download = SparkSubmitOperator(
    task_id="process_spark_download",
    application="/usr/local/spark/applications/ny_yellow_download.py",
    name="Download-ny-yellow",
    conn_id="spark_conn",
    conf={"spark.master": spark_master,
          "spark.sql.caseSensitive": True,
          "spark.executor.memory": "1024M",
          "spark.driver.cores": "1",
          "spark.driver.memory": "1024M",
          },
    total_executor_cores=1,
    application_args=['{{ logical_date.format("YYYY-MM-DD") }}'],
    py_files=wget_path,
    )

    process_spark_load = SparkSubmitOperator(
    task_id="process_spark_load",
    application="/usr/local/spark/applications/ny_yellow_load.py",
    name="Load-ny-yellow",
    conn_id="spark_conn",
    conf={"spark.master": spark_master,
          "spark.sql.caseSensitive": True,
          "spark.executor.memory": "1024M",
          "spark.driver.cores": "1",
          "spark.driver.memory": "1024M",
          },
    total_executor_cores=1,
    application_args=['{{ logical_date.format("YYYY-MM-DD") }}'],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    )

    alter_stage_tbl = PostgresOperator(
        task_id='alter_stage_tbl',
        postgres_conn_id='postgres_conn',
        sql="""
        ALTER TABLE stage.yellow_tripdata_{{ logical_date.strftime('%Y_%m') }} ALTER COLUMN date SET NOT NULL;
        """
    )

    cnt_stage = PostgresOperatorWithOutput(
        task_id='cnt_stage',
        postgres_conn_id='postgres_conn',
        sql="""
        SELECT
            count(*)
        FROM
            stage.yellow_tripdata_{{ logical_date.strftime('%Y_%m') }};
        """,
        execution_timeout=timedelta(minutes=5),
    )
# "{{ ti.xcom_pull(task_ids='generate_bash_command') }}"
    is_stage_empty = BranchPythonOperator(
        task_id='is_stage_empty',
        python_callable=is_stage_empty_fn,
        execution_timeout=timedelta(minutes=2),
    )

    drop_table = PostgresOperator(
        task_id="drop_table",
        postgres_conn_id='postgres_conn',
        sql="""
        DO $$
        DECLARE
          partition_table_schema varchar := 'stage';
          partition_table varchar := 'yellow_tripdata_{{ logical_date.strftime('%Y_%m') }}';
        BEGIN
          IF EXISTS (
            SELECT FROM pg_tables WHERE schemaname = format('%I', partition_table_schema) AND tablename = format('%I', partition_table)
          ) THEN
            EXECUTE format('DROP TABLE %I.%I', partition_table_schema, partition_table);
          END IF;

        END $$;
        """,
        execution_timeout=timedelta(minutes=5),
    )

    attach_partition = PostgresOperator(
        task_id="attach_partition",
        postgres_conn_id='postgres_conn',
        sql="""
        DO $$
        DECLARE
          partition_table_schema varchar := 'stage';
          partition_table varchar := 'yellow_tripdata_{{ logical_date.strftime('%Y_%m') }}';
          ld varchar := '{{ logical_date.format('YYYY-MM-DD') }}';
          ld_plus_day varchar := '{{ logical_date.add(days=1).format('YYYY-MM-DD') }}';
        BEGIN
          IF EXISTS (
            SELECT * FROM pg_tables WHERE schemaname = format('%I', partition_table_schema) AND tablename = format('%I', partition_table)
          ) THEN
            EXECUTE format('ALTER TABLE taxi.ny_yellow
                            ATTACH PARTITION %I.%I 
                            FOR VALUES FROM (''%s'') TO (''%s'')',
                             partition_table_schema, partition_table, ld, ld_plus_day);
          END IF;
        END $$;
        """,
            execution_timeout=timedelta(minutes=5),
    )

    complete = DummyOperator(
        task_id="complete",
        trigger_rule=TriggerRule.ALL_DONE
    )

    detach_drop_delete_table >> create_stage_tbl >> process_spark_download >> process_spark_load >> \
    alter_stage_tbl >> cnt_stage >> is_stage_empty >> [attach_partition, drop_table] >> complete
