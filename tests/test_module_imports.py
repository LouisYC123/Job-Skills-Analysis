"""Trivial module - used to verify modules listed in requirements.txt have been 
installed succesfully"""

import pytest


def test_has_requests():
    import requests


def test_has_dotenv():
    import dotenv


def test_has_serp_api():
    import serpapi
