with source as (
    SELECT * FROM {{ source('raw_data', 'google_jobs_raw' )}}
),
skills_list as (
    select * from {{ source('raw_data', 'skills_list' )}}
),

flatten_json as (
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
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['job_title', 'company_name', 'location', 'F.value']) }} as jobskill_id
    , F.value AS jobskill 
    , job_title
    , company_name
    , location
    , job_type
    , NULL as salary
    , NULL AS url
    , 'google_jobs' AS jobsite
    , job_listing_posted_at
    , load_timestamp
FROM   
    flatten_json, 
    Table(
        Flatten(
            list_filter(
                STRTOK_TO_ARRAY(
                    TRANSLATE(REPLACE(REPLACE(description, '\n', ' '),',', ' ' ),'()[]{}.!', '        ' ), ' '
                    ),
                (select ARRAY_AGG(skill) from skills_list)
            )
         )
     ) F
