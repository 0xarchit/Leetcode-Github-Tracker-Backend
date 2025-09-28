import os
import requests
from dotenv import load_dotenv
from typing import Any, Dict, List
import urllib3
from urllib3.exceptions import InsecureRequestWarning


load_dotenv()
GITHUB_API = os.getenv("GITHUB_API", "").rstrip("/")
LEETCODE_API = os.getenv("LEETCODE_API", "").rstrip("/")

# Suppress warnings because we intentionally disable verification per user request
urllib3.disable_warnings(InsecureRequestWarning)


def _session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    return s


class ApiError(Exception):
    pass


def _require_base(name: str, value: str) -> str:
    if not value:
        raise ApiError(f"Missing base URL for {name} in environment")
    return value


def get_github_summary(username: str) -> Dict[str, Any]:
    base = _require_base("GITHUB_API", GITHUB_API)
    url = f"{base}/api"
    params = {"username": username}
    with _session() as s:
        r = s.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()


def get_leetcode_profile(username: str) -> Dict[str, Any]:
    base = _require_base("LEETCODE_API", LEETCODE_API)
    with _session() as s:
        # Try lowercase path first (per endpoint.txt), then common alternates
        for path in [f"/userprofile/{username}", f"/{username}", f"/userProfile/{username}"]:
            r = s.get(base + path, timeout=30)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            return r.json()
    raise ApiError("LeetCode profile endpoint not found for provided username")


def get_leetcode_language_stats(username: str) -> Dict[str, Any]:
    base = _require_base("LEETCODE_API", LEETCODE_API)
    with _session() as s:
        # The OpenAPI shows /languageStats; endpoint.txt shows /languagestats
        for path in ["/languageStats", "/languagestats"]:
            r = s.get(base + path, params={"username": username}, timeout=30)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            return r.json()
    raise ApiError("LeetCode language stats endpoint not found")


def get_leetcode_badges(username: str) -> Dict[str, Any]:
    base = _require_base("LEETCODE_API", LEETCODE_API)
    url = f"{base}/{username}/badges"
    with _session() as s:
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.json()


def get_github_contributions(username: str) -> Dict[str, Any]:
    """Fetch daily GitHub contributions (calendar-style) for a username.
    Endpoint returns structure with weeks -> contributionDays {date, contributionCount}.
    """
    base = _require_base("GITHUB_API", GITHUB_API)
    url = f"{base}/contri"
    params = {"username": username}
    with _session() as s:
        r = s.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()


def get_leetcode_calendar(username: str) -> Dict[str, Any]:
    """Fetch LeetCode submission calendar; typically contains submissionCalendar as JSON string."""
    base = _require_base("LEETCODE_API", LEETCODE_API)
    url = f"{base}/{username}/calendar"
    with _session() as s:
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
