from extract import get_query_params, query_jobs_from_api, api_targets
from modules.s3 import get_s3_client, save_json_to_s3
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()


SEARCH_TERM = "data engineer"
TARGET_COUNTRIES = api_targets.COUNTRIES_LIST
MAX_NUM_PAGES_PER_COUNTRY = 2
API_KEY = os.getenv("API_KEY")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "lg-job-skills-data-lake"
PREFIX_NAME = "raw_jobs_data"

current_time = datetime.now().strftime("%Y%m%d-%H%M%S")

if __name__ == "__main__":
    # Build the query params dict
    params = get_query_params(
        api_key=API_KEY, search_term=SEARCH_TERM, search_locations=TARGET_COUNTRIES
    )
    # Query the google_jobs engine
    jobs_dict = query_jobs_from_api(
        max_pages_per_country=MAX_NUM_PAGES_PER_COUNTRY,
        query_params=params,
    )
    # Load json to s3
    s3 = get_s3_client(
        aws_access_key=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    save_json_to_s3(
        s3,
        bucket_name=BUCKET_NAME,
        prefix_name=PREFIX_NAME,
        data=jobs_dict,
        timestamp=current_time,
    )
