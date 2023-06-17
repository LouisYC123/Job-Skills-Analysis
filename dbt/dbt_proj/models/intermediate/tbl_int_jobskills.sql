{{
    config(
        materialized='incremental',
        unique_key='jobskill_id',
        merge_update_columns = [
            'job_type',
            'posted_via',
            'updated_timestamp'
            ]
    )
}}

with source as (
    select * from {{ ref('flatten_skills_list') }}
)

{% if is_incremental() %}

select
    jobskill_id
    , jobskill
    , job_title 
    , company_name
    , company_id
    , location 
    , location_id
    , job_id
    , job_type 
    , job_type_id
    , job_listing_posted_at
    , posted_via
    , CURRENT_TIMESTAMP() as load_timestamp
    , CURRENT_TIMESTAMP() as updated_timestamp
    
from source
where load_timestamp >= (select coalesce(max(load_timestamp), '2020-01-01' ) from {{ this }})
    and jobskill_id not in (select jobskill_id from {{ this }})

{% else %}

SELECT 
    *
    , CURRENT_TIMESTAMP() as updated_timestamp
FROM 
    source

{% endif %}


