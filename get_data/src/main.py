# %%
from extract import get_query_params, query_jobs_from_api, create_jobs_json, api_targets
from modules.s3 import get_s3_client, save_json_to_s3
from datetime import datetime

now = datetime.now()

current_time = now.strftime("%Y%m%d-%H%M%S")

AWS_ACCESS_KEY = "AKIAZDTMOJ6VWVFRSWMJ"
AWS_SECRET_ACCESS_KEY = "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"
API_KEY = "d2a17d7142af0de090c4fb30e05c3d2b268acf8516725338f34a641064a638f1"
SEARCH_TERM = "data engineer"
TARGET_COUNTRIES = api_targets.COUNTRIES_LIST
MAX_NUM_PAGES_PER_COUNTRY = 1


if __name__ == "__main__":
    # Build the query params dict
    params = get_query_params(
        api_key=API_KEY, search_term=SEARCH_TERM, search_locations=TARGET_COUNTRIES
    )
    # Query the google_jobs engine
    jobs_list = query_jobs_from_api(
        max_pages_per_country=MAX_NUM_PAGES_PER_COUNTRY,
        query_params=params,
    )
    # Convert list of dictionaries to single json string
    jobs_json = create_jobs_json(jobs_list)

    # Load json to s3
    s3 = get_s3_client(
        aws_access_key=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    save_json_to_s3(
        s3,
        bucket_name="lg-job-skills-data-lake",
        json_object=jobs_json,
        timestamp=current_time,
    )

    # temp
    # ? Set up a cache??
    # with open("jobs.json", "w", encoding="utf-8") as f:
    #     json.dump(
    #         {result["job_id"]: result for result in jobs_list},
    #         f,
    #         ensure_ascii=False,
    #         indent=4,
    #     )
    print("complete")

# %%
