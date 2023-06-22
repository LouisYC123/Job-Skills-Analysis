{{ config(materialized='view') }}

with source as (
    select * from {{ ref('google_jobs_flat') }}
),

skills_list as (
    select * from jobs_db.raw_data.skills_list
)


SELECT 
    get_non_skill_words(
                STRTOK_TO_ARRAY(TRANSLATE(REPLACE(REPLACE(description, '\n', ' '),',', ' ' ),'()[]{}.!', '        ' ), ' '),
                (select ARRAY_AGG(skill) from skills_list)
    ) as non_skill_words
    , job_listing_posted_at
    , load_timestamp
FROM   
    source

