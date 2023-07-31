with source as (
    SELECT * FROM {{ source('raw_data', 'cwjobs_raw' )}}
),
skills_list as (
    select * from {{ source('raw_data', 'skills_list' )}}
),

flatten_json as (
    SELECT DISTINCT
        value:job_title::String AS job_title
        , value:url::String AS url
        , value:company_name::String AS company_name
        , value:country::String AS country
        , value:job_location::String AS location
        , value:salary::String AS salary
        , value:job_type::String AS job_type
        , value:job_description::String AS description
        , CURRENT_TIMESTAMP() as load_timestamp
        , value:date_posted::String AS job_listing_posted_at 
    FROM 
        source
        , lateral flatten( input => raw_data)
)

SELECT 
    F.value AS jobskill 
    , job_title
    , company_name
    , country
    , location
    , job_type
    , salary
    , url
    , 'cwjobs' AS jobsite
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
WHERE 
    job_title IS NOT NULL