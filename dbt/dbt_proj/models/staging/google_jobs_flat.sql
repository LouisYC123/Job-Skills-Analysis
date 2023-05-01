{{ config(materialized='view') }}

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
    , value:detected_extensions.posted_at::String as listing_posted_at
    , value:detected_extensions.schedule_type::String as schedule_type
    , CURRENT_TIMESTAMP() as load_timestamp
FROM 
    source
    , lateral flatten( input => raw_data:data)