{{ config(materialized='view') }}

with source as (
    select * from {{ ref('google_jobs_flat') }}
),

skills_list as (
    select * from jobs_db.raw_data.skills_list
)


SELECT 
    {{ dbt_utils.generate_surrogate_key(['job_title', 'company_name', 'location', 'F.value']) }} as jobskill_id
    , F.value AS jobskill 
    , job_title
    , company_name
    , {{ dbt_utils.generate_surrogate_key(['company_name']) }} as company_id
    , location
    , {{ dbt_utils.generate_surrogate_key(['location']) }} as location_id
    , {{ dbt_utils.generate_surrogate_key(['job_title', 'company_name', 'location']) }} as job_id
    , job_type
    , {{ dbt_utils.generate_surrogate_key(['job_type']) }} as job_type_id
    , posted_via
    , job_listing_posted_at
    , load_timestamp
FROM   
    source, 
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
