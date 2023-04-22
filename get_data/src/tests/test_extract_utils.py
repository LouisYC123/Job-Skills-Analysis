import pytest
from extract import get_query_params


@pytest.mark.parametrize(
    "api_key, search_term, search_location",
    [
        (123, "analyst", "germany"),
        ("123", ["analyst"], "germany"),
        ("123", "analyst", {"search_location": "germany"}),
        ("123", ["analyst"], {"search_location": "germany"}),
        (123, ["analyst"], {"search_location": "germany"}),
    ],
)
def test_get_query_params_raises(api_key, search_term, search_location):
    """get_query_params() should raise an exception when wrong type params passed"""
    with pytest.raises(TypeError):
        get_query_params(api_key, search_term, search_location)


@pytest.mark.parametrize(
    "date_posted",
    [("year"), ("weekk"), ("Montth"), ({"date_posted": "week"}), (123)],
)
def test_get_query_params_wrong_date_posted_raises(date_posted):
    """get_query_params() should raise an exception when wrong type params passed"""
    with pytest.raises(ValueError):
        get_query_params("123", "analyst", "search_location", date_posted)
