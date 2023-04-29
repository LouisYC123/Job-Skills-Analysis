{{ config(materialized='table') }}

with source as (
    SELECT * FROM {{ source('raw_data', 'google_jobs' )}}
)


SELECT 
    value:title::String AS job_title
    , value:company_name::String AS company_name
    , value:location::String AS location
    , value:via::String AS via
    , value:description::String AS description
    , value:job_id::String AS job_id
FROM 
    source
    , lateral flatten( input => raw_data:data)