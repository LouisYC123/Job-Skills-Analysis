WITH cwjobs AS (
    SELECT * FROM {{ ref('cwjobs_flat') }}
),

google_jobs AS (
    SELECT * FROM {{ ref('google_jobs_flat') }}
),

unioned as (
    SELECT 
        *
    FROM 
        cwjobs
    UNION
    SELECT 
        *
    FROM 
        google_jobs
),
cleaned as (
    SELECT 
        job_title as job_title_orig
        , CASE
            WHEN lower(jobskill) IN ('sql', 'mysql', 'pl/sql', 'postgresql', 'postgres') THEN 'sql'
            WHEN lower(jobskill) IN ('nosql', 'no sql', 'no/sql', 'no-sql', 'no--sql') THEN 'no_sql'
            WHEN lower(jobskill) IN ('aws', 'Amazon Web Services' ) THEN 'aws'
            WHEN lower(jobskill) IN ('gcp', 'google cloud platform' ) THEN 'gcp'
            WHEN lower(jobskill) IN ('pyspark', 'py-spark', 'py/spark', 'py spark') THEN 'pyspark'
            WHEN lower(jobskill) IN ('map reduce', 'mapreduce', 'map-reduce') THEN 'map_reduce'
            WHEN lower(jobskill) IN ('modeling', 'modelling', 'data modelling', 'data modeling', 'data-modelling', 'data-modeling', 'dimensional modelling', 'dimensional modeling', 'dimensional-modelling') THEN 'data_modelling'
            WHEN lower(jobskill) IN ('warehouse', 'data warehouse', 'data-warehouse', 'warehous', 'ware house', 'ware-house', 'warehousing', 'ware-housing', 'data warehousing', 'mart', 'data mart') THEN 'data_warehousing'
            WHEN lower(jobskill) IN ('lakes', 'data lakes', 'data lake') THEN 'data_lake'
            WHEN lower(jobskill) IN ('powerbi', 'power-bi', 'power bi', 'power  bi', 'power/bi') THEN 'data_lake'
            WHEN lower(jobskill) IN ('ci/cd', 'ci/ cd', 'ci /cd', 'ci / cd', 'cicd', 'ci-cd', 'ci cd', 'continuous integration / continuous delivery', 'continuous integration/continuous deployment') THEN 'ci/cd'
            WHEN lower(jobskill) IN ('pubsub', 'pub sub', 'pub-sub', 'pub/sub') THEN 'pub/sub'
            WHEN lower(jobskill) IN ('data bricks', 'databricks', 'databrick', 'azure databricks', 'azure_databricks', 'azure-databricks') THEN 'databricks'
            ELSE lower(jobskill)
        END AS jobskill
        , CASE
            WHEN LOWER(job_title) LIKE 'data engineer%' THEN 'Data Engineer' 
            WHEN LOWER(job_title) LIKE '%data scientist%' THEN 'Data Scientist' 
            WHEN LOWER(job_title) LIKE '%junior%' THEN 'Junior Data Engineer'
            WHEN LOWER(job_title) LIKE '%senior%' THEN 'Senior Data Engineer'
            WHEN LOWER(job_title) LIKE '%lead data engineer%' THEN 'Lead Data Engineer'
            WHEN LOWER(job_title) LIKE '%principal data engineer%' THEN 'Principal Data Engineer'
            ELSE 'Data Engineer'
        END AS job_title
        , CASE
            WHEN LOWER(job_title) LIKE '%contract%' THEN 'contract' 
            WHEN LOWER(job_title) LIKE '%outside ir35%' THEN 'contract_outside_IR35' 
            WHEN LOWER(job_title) LIKE '%inside ir35%' THEN 'contract_inside_IR35' 
            WHEN LOWER(job_title) LIKE '%perm%' THEN 'permanent'
            WHEN LOWER(job_title) LIKE '%inter%' THEN 'internship'
            ELSE NULL
        END AS contract_type
        , CASE
            WHEN LOWER(job_title) LIKE 'full%' THEN 'full-time' 
            WHEN LOWER(job_title) LIKE '%part-%' THEN 'part-time'
            ELSE 'full-time'
        END AS employment_type
        ,   LOWER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRANSLATE(
                        COMPANY_NAME, 
                        '!"#$%&\\()*+,-./:;<=>?@[\\]^_`{|}~', 
                        '_______________________________'), 
                        '[^[:ascii:]]', 
                        ''), 
                        '\\s',
                        '_'
                    )
                ) AS company_name 
                
        , CASE 
            WHEN lower(salary) LIKE '%annum%' THEN 'annual'
            WHEN lower(salary) LIKE '%annual%' THEN 'annual'
            WHEN lower(salary) LIKE '%day%' THEN 'daily'
            WHEN lower(salary) LIKE '%daily%' THEN 'daily'
            else 'annual'
            end as pay_frequency
        , CASE 
            WHEN salary LIKE '%$%' THEN 'USD'
            WHEN salary LIKE '%€%' THEN 'EUR'
            WHEN salary LIKE '%£%' THEN 'GBP'
            WHEN lower(salary) LIKE '%usd%' THEN 'USD'
            WHEN lower(salary) LIKE '%eur%' THEN 'EUR'
            WHEN lower(salary) LIKE '%gbp%' THEN 'GBP'
            ELSE NULL
            END AS pay_currency
        , REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            salary, ',', ''),
                            '\\b\\d{1,2}\\b', ''),
                            ' to ', '-'),
                            '[^0-9.-]', 
                            '', 1, 0),
                            '^-', '')   
            AS amount
            , salary
            , jobsite
            , location
            , CASE 
                WHEN country IS NULL AND jobsite = 'cwjobs' THEN 'UK' 
                WHEN country IS NULL AND jobsite <> 'cwjobs' THEN TRIM(SPLIT_PART(location, ',', -1))
                WHEN country = 'United Kingdom' THEN 'UK' 
            ELSE country END AS country      
            ,
                CASE 
                    WHEN jobsite = 'google_jobs' AND job_listing_posted_at LIKE '%hour%' THEN dateadd(hour, -(regexp_replace(job_listing_posted_at, '[^0-9]', ''):: INT) * 1, CURRENT_TIMESTAMP())
                    WHEN jobsite = 'google_jobs' AND job_listing_posted_at LIKE '%day%' THEN dateadd(hour, -(regexp_replace(job_listing_posted_at, '[^0-9]', ''):: int) * 24, CURRENT_TIMESTAMP())
                    when jobsite = 'cwjobs' and lower(job_listing_posted_at) like '%today%' then CURRENT_DATE 
                    when jobsite = 'cwjobs' and lower(job_listing_posted_at) like '%yesterday%' then DATEADD(DAY, -1, CURRENT_DATE)
                    when jobsite = 'cwjobs' and lower(job_listing_posted_at) like '%recently%' then DATEADD(DAY, -2, CURRENT_DATE)
                    else DATEADD(DAY, -3, CURRENT_DATE) 
                END AS job_listing_posted_at
                , load_timestamp
    FROM 
        unioned
),
median_salary as(
    SELECT 
        job_title
        , jobskill
        , job_title_orig
        , salary
        , REGEXP_REPLACE(company_name, '__', '_') as company_name
        , pay_frequency 
        , pay_currency
        , (PARSE_JSON('[' || REPLACE(amount, '-', ',') || ']'))[0] AS salary_low
        , (PARSE_JSON('[' || REPLACE(amount, '-', ',') || ']'))[1] AS salary_high
        , CASE WHEN 
            amount <> '' THEN (
                CASE 
                    WHEN salary_low IS NOT NULL AND salary_high IS NOT NULL THEN  (salary_low + salary_high) / 2 
                    ELSE CAST(REGEXP_REPLACE(amount, '-', '') AS FLOAT) 
                END
            ) 
            ELSE NULL
        END AS median_salary
        , jobsite
        , contract_type
        , employment_type
        , location
        , country
        , job_listing_posted_at
        , load_timestamp
    FROM 
        cleaned
    WHERE 
        job_title <> 'Data Scientist'
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['job_title', 'company_name', 'location', 'jobskill' ]) }} as jobskill_id
    , jobskill
    , job_title
    , location
    , country
    , job_title_orig
    , contract_type
    , employment_type
    , company_name
    , CASE WHEN  median_salary < 101 THEN NULL ELSE median_salary END AS pay_amount
    , pay_currency
    , pay_frequency 
    , jobsite
    , {{ dbt_utils.generate_surrogate_key(['job_title', 'company_name', 'location' ]) }} as job_id
    , job_listing_posted_at
    , load_timestamp
FROM 
    median_salary

