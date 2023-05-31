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

{% if is_incremental() %}

select
    jobskill_id
    , jobskill
    , job_title 
    , job_type 
    , company_name
    , location 
    , job_listing_posted_at
    , posted_via
    , CURRENT_TIMESTAMP() as load_timestamp
    , CURRENT_TIMESTAMP() as updated_timestamp
    
from {{ ref('flatten_skills_list') }}
where load_timestamp >= (select coalesce(max(load_timestamp), '2020-01-01' ) from {{ this }})
    and jobskill_id not in (select jobskill_id from {{ this }})

{% else %}

SELECT 
    *
FROM 
    {{ ref('flatten_skills_list') }}

{% endif %}


