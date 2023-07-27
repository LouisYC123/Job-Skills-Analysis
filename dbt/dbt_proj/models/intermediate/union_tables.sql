WITH cwjobs AS (
    SELECT * FROM {{ ref('cwjobs_flat') }}
),

google_jobs AS (
    SELECT * FROM {{ ref('google_jobs_flat') }}
)

""" different number of columns I think """
SELECT 
    *
FROM 
    cwjobs

UNION

SELECT 
    *
FROM 
    google_jobs