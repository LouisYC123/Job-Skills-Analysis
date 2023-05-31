from serpapi import GoogleSearch
from typing import Literal, Dict
from datetime import datetime


def get_query_params(
    api_key: str,
    search_term: str,
    location: Dict[str, str],
    date_posted: Literal["week", "month"] = "week",
) -> dict:
    """Builds a dictionary to be used as the query parameters used in GoogleSearch()

    Parameters
    ----------
    api_key : str
        API key for the the SerpApi service. Obtained from https://serpapi.com
    search_term : str
        Defines the query you want to search.
        You can use anything that you would use in a regular Google search.
    location : dict
        dict that contains country name and code params
    date_posted : week | month, optional
        defines wether to return jobs posted in the past week or month.
    """
    if not isinstance(api_key, str):
        raise TypeError("api_key must be a string")
    if not isinstance(search_term, str):
        raise TypeError("search_term must be a string")
    if not date_posted in ["week", "month"]:
        raise ValueError("date_posted must be either 'week' or 'month'")

    return {
        "api_key": api_key,
        "device": "desktop",  # desktop | tablet | mobile
        "engine": "google_jobs",  # Set parameter to google_jobs to use the Google Jobs API engine.
        "google_domain": "google.com",  # See https://serpapi.com/google-domains for list of supported domains.
        "q": search_term,
        "hl": "en",  # Defines the language to use for the Google Jobs search.
        "gl": "uk",  # Defines the country to use for the Google search.
        "location": location["country_name"],
        "chips": f"date_posted:{date_posted.lower()}",
    }


def query_jobs_from_api(
    query_params: dict,
    context: dict,
    max_num_pages: int = 10,
) -> dict:
    """Queries the google_jobs engine using serpapi.GoogleSearch().
    Runs once per target location.

    Parameters
    ----------
    query_params : dict
        Parameters for the search
    context : dict
        Airflow task and dag run information
    max_num_pages : int
        Maximum number of pages to query
        GoogleSearch() will be called {max_num_pages} number of times
    """
    # query api 1 page (10 jobs) at a time, for a maximum of {max_num_pages} pages
    jobs_list = []
    for num in range(max_num_pages):
        start = num * 10
        search = GoogleSearch(query_params)
        results = search.get_dict()
        try:
            if results["error"] == "Google hasn't returned any results for this query.":
                print(
                    f"Google hasn't returned any results for page: {start} from {query_params['location']}"
                )
                continue
        except KeyError:
            print(
                f"Getting SerpAPI data for page: {start} from {query_params['location']}"
            )
            print("\n")
            num_jobs_found = len(results["jobs_results"])
            print(f"Found {str(num_jobs_found)} ")
        else:
            continue
        # add list of jobs to jobs_list
        jobs = results["jobs_results"]

        jobs_list.append(jobs)

    return [item for sublist in jobs_list for item in sublist]
