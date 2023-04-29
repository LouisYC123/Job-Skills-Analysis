from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
import logging


def get_snowflake_hook(snowflake_conn_id: str = "snowflake_default"):
    return SnowflakeHook(snowflake_conn_id=snowflake_conn_id)


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
