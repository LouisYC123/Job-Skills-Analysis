{{ config(materialized='view') }}

with source as (
    SELECT * FROM {{ source('raw_data', 'google_jobs_raw' )}}
)


SELECT DISTINCT
    value:title::String AS job_title
    , value:company_name::String AS company_name
    , value:location::String AS location
    , value:via::String AS posted_via
    , value:description::String AS description
    , value:job_id::String AS job_id
    , value:detected_extensions.posted_at::String as listing_posted_at
    , value:detected_extensions.schedule_type::String as job_type
    , CURRENT_TIMESTAMP() as load_timestamp
    , case 
        when 
        listing_posted_at like '%hour%' 
            then dateadd(hour, -(regexp_replace(listing_posted_at, '[^0-9]', ''):: int) * 1, CURRENT_TIMESTAMP())
        when listing_posted_at like '%day%' 
            then dateadd(hour, -(regexp_replace(listing_posted_at, '[^0-9]', ''):: int) * 24, CURRENT_TIMESTAMP())
      end as job_listing_posted_at
FROM 
    source
    , lateral flatten( input => raw_data:data)