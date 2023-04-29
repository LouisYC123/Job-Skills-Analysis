
-- Use the `ref` function to select from other models

select *
from {{ ref('raw_flattened') }}
limit 10
