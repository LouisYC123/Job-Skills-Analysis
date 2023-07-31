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
        , value:country::String AS country
        , value:via::String AS posted_via
        , value:description::String AS description
        , value:job_id::String AS job_id
        , value:detected_extensions.posted_at::String as listing_posted_at
        , value:detected_extensions.schedule_type::String as job_type
        , value:detected_extensions.posted_at:String as job_listing_posted_at
        , CURRENT_TIMESTAMP() as load_timestamp

    FROM 
        source
        , lateral flatten( input => raw_data:data)
)

SELECT 
    F.value AS jobskill 
    , job_title
    , company_name
    , country
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
