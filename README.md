# job-skills-analysis
Visualising trends in Job skill requirements using SerpApi, Airflow, Snowflake and Dash
or  
End-to-end pipeline for analysis of trends in Job skill requirements using SerpApi, Airflow, Snowflake and Dash  
or 
Building ELT and warehouse using SerpApi, Airflow, Snowflake and Dash  

## Summary


## Interesting Features



## Prerequisites

- Working installation of Docker  
- Snowflake account
- Working installation of SnowSQL (see - https://docs.snowflake.com/en/user-guide/snowsql-install-config)
- AWS account
- IAM Role for the Snowflake Stage & Pipe (see below)

## Setup

### Snowflake External Stage (S3) creation
A Snowflake external stage points to a storage location outside of Snowflake. Here, we will be using S3. Once set up, data loaded to the staged s3 bucket will automatically become available in Snowflake.

1. Create an S3 bucket and (optional) subfolder to hold the raw json data extracted from the google_jobs engine.
2. Follow the instructions here: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
3. Create a table with a single column of type VARIANT to hold the raw jobs data:
```
CREATE TABLE raw_jobs_json_table (
	raw_data VARIANT
);

```

## Useful AWS CLI command
Create a bucket:
```
s3 mb s3://<bucket-name>
``` 