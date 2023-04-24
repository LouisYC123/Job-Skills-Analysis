
COPY INTO RAW_JOBS_DATA
FROM @job_skills_stage/jobs-20230424-065915.json
file_format = my_json_format;
