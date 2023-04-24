from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
import logging


def get_snowflake_hook(snowflake_conn_id: str = "snowflake_default"):
    return SnowflakeHook(snowflake_conn_id=snowflake_conn_id)


def truncate_table(table_name: str):
    snow_hook = get_snowflake_hook()
    snow_hook.run(sql=f"TRUNCATE TABLE {table_name} ", autocommit=True)
    logging.info(f"Truncated table:{table_name}")


def copy_from_stage(
    filename: str,
    file_format: str = "my_json_format",
    target_table: str = "raw_jobs_data",
    stage_name: str = "job_skills_stage",
):
    snow_hook = get_snowflake_hook()
    sql = f"""
        COPY INTO {target_table.upper()}
        FROM @{stage_name}/{filename}
        file_format = {file_format};
    """
    snow_hook.run(sql=sql, autocommit=True)


def flatten_into_new_table(
    target_table: str = "flat_jobs_data",
    source_table: str = "raw_jobs_data",
    target_json_key: str = "data",
):
    snow_hook = get_snowflake_hook()
    sql = f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT 
            value:title::String AS job_title
            , value:company_name::String AS company_name
            , value:location::String AS location
            , value:via::String AS via
            , value:description::String AS description
            , value:job_id::String AS job_id
        FROM 
            {source_table}
            , lateral flatten( input => json_data_raw:{target_json_key});
    """
    snow_hook.run(sql=sql, autocommit=True)
