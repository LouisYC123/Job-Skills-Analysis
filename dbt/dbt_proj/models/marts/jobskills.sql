{{
    config(
        materialized='incremental',
        unique_key='jobskill_id',
        merge_update_columns = [
            'contract_type',
            'employment_type'
            'pay_amount',
            'pay_currency',
            'pay_frequency',
            'posted_via',
            'updated_timestamp'
            ]
    )
}}

with source as (
    select * from {{ ref('union_and_clean') }}
)

{% if is_incremental() %}

select
     jobskill_id
    , jobskill
    , job_title
    , location
    , country
    , job_title_orig
    , contract_type
    , employment_type
    , company_name
    , pay_amount
    , pay_currency
    , pay_frequency 
    , jobsite
    , job_id
    , job_listing_posted_at
    , load_timestamp
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


