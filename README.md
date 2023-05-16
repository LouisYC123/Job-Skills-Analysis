![jobs_dlh_architecture drawio](https://user-images.githubusercontent.com/97873724/234169343-b7d7d469-980a-47a8-a8af-7ce00078772a.png)
# job-skills-analysis
Visualising trends in Job skill requirements using SerpApi, Airflow, Snowflake and Dash
Building a Data LakeHouse with AWS, Snowflake & Airflow


## Summary


## Interesting Features



## Prerequisites

- Working installation of Docker  
- Snowflake account
- Working installation of SnowSQL (see - https://docs.snowflake.com/en/user-guide/snowsql-install-config)
- AWS account
- IAM Role for the Snowflake Stage & Pipe (see below)
- Snowflake S3 stage (see below)
- dbt setup

**NOTE:** I set up dbt before spinning up container, need to think how I want this to work

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

## Setup


### Step one: Building the Docker image

Build the Docker container image using the following command:

```bash
./mwaa-local-env build-image
```
**Note**: it takes several minutes to build the Docker image locally.

### Step two: Running Apache Airflow

#### Local Dev

Runs a local Apache Airflow environment that is a close representation of MWAA by configuration.

```bash
./mwaa-local-env start
```

To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are stopped.

#### dbt init
docker exec into container and run dbt init from /dbt , follow the command line instructions
** NOTE: ** You will need your snowflake credentials here

#### add a profiles.yml
```
dbt_proj:
  target: raw_data
  outputs:
    raw_data:
      type: snowflake
      account: 

      # User/password auth
      user: 
      password: 

      role: 
      database: 
      warehouse: 
      schema: 
      threads: 
      client_session_keep_alive: False
      query_tag: 

```
### Step three: Accessing the Airflow UI

By default, the `bootstrap.sh` script creates a username and password for your local Airflow environment.

- Username: `admin`
- Password: `test`

#### Airflow UI

- Open the Apache Airlfow UI: <http://localhost:8080/>.

## Useful AWS CLI command
Create a bucket:
```
s3 mb s3://<bucket-name>
``` 
