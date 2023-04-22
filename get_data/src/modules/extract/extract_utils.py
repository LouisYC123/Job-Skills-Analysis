from serpapi import GoogleSearch
from typing import Literal, List
import json


def get_query_params(
    api_key: str,
    search_term: str,
    search_locations: List[str],
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
    search_locations : str
        list of countries to be used as search_locations
        e.g ["Germany"], ["France", "Poland"] etc
    date_posted : week | month, optional
        defines wether to return jobs posted in the past week or month.
    """
    if not isinstance(api_key, str):
        raise TypeError("api_key must be a string")
    if not isinstance(search_term, str):
        raise TypeError("search_term must be a string")
    if not isinstance(search_locations, list):
        raise TypeError("search_location must be a list")
    if not date_posted in ["week", "month"]:
        raise ValueError("date_posted must be either 'week' or 'month'")

    return {
        "api_key": api_key,
        "device": "desktop",  # desktop | tablet | mobile
        "engine": "google_jobs",  # Set parameter to google_jobs to use the Google Jobs API engine.
        "google_domain": "google.com",  # See https://serpapi.com/google-domains for list of supported Google domains.
        "q": search_term,
        "hl": "en",  # Defines the language to use for the Google Jobs search.
        "gl": "us",  # Defines the country to use for the Google search.
        "search_locations": search_locations,
        "chips": f"date_posted:{date_posted.lower()}",
    }


def query_jobs_from_api(max_pages_per_country: int, query_params: dict) -> list:
    """Queries the google_jobs engine using serpapi.GoogleSearch()

    Parameters
    ----------
    coutries_list: list
        list of countries to be used as search_locations
    max_num_pages : int
        Maximum number of pages to query
    query_params : dict
        Parameters for the search
    """
    # query api 10 pages at a time, for a maximum of {num_pages} pages
    coutries_list = query_params["search_locations"]
    query_params.pop("search_locations")
    jobs_list = []
    for country in coutries_list:
        for num in range(max_pages_per_country):
            start = num * 10
            query_params["start"] = start
            query_params["location"] = country

            search = GoogleSearch(query_params)
            results = search.get_dict()

            # check if the last search page (i.e., no results)
            try:
                if (
                    results["error"]
                    == "Google hasn't returned any results for this query."
                ):
                    print(
                        f"Google hasn't returned any results for page: {start} from {country}"
                    )
                    continue
            except KeyError:
                print(f"Getting SerpAPI data for page: {start} from {country}")
                print("\n")
                num_jobs_found = len(results["jobs_results"])
                print(f"Found {str(num_jobs_found)} ")
            else:
                continue

            # add list of jobs to
            jobs = results["jobs_results"]
            jobs_list.append(jobs)

    flat_jobs_list = [item for sublist in jobs_list for item in sublist]

    return flat_jobs_list


def create_jobs_json(jobs_list: list) -> str:
    """Turns list of dictionaries into a single json string.

    Parameters
    ----------
    jobs_list : list
        list of dictionaries, with each dictionary being a job listing

    """
    return json.dumps({result["job_id"]: result for result in jobs_list}, indent=4)
