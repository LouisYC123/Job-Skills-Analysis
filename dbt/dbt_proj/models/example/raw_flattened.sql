{{ config(materialized='table') }}


SELECT 
    value:title::String AS job_title
    , value:company_name::String AS company_name
    , value:location::String AS location
    , value:via::String AS via
    , value:description::String AS description
    , value:job_id::String AS job_id
FROM 
    raw_jobs_data
    , lateral flatten( input => json_data_raw:data)