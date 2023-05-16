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


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where load_timestamp >= (select max(load_timestamp) from {{ this }})

{% endif %}


