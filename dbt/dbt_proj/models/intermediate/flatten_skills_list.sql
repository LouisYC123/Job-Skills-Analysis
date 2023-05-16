{{ config(materialized='view') }}

with source as (
    select * from {{ ref('google_jobs_flat') }}
)


SELECT 
    {{ dbt_utils.generate_surrogate_key(['job_title', 'company_name', 'location', 'F.value']) }} as jobskill_id
    , F.value AS jobskill 
    , job_title
    , job_type
    , company_name
    , location
    , job_listing_posted_at
    , posted_via
    , load_timestamp
FROM   
    source, 
    Table(Flatten(list_filter(STRTOK_TO_ARRAY(description, ' ')))) F
