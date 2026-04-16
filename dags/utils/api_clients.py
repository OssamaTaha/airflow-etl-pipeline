"""
Reusable API client wrappers for the ETL pipeline.
"""
import requests
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# Suppress noisy logs from requests
logging.getLogger("urllib3").setLevel(logging.WARNING)


class ExchangeRateClient:
    """Client for open.er-api.com exchange rate API."""

    BASE_URL = "https://open.er-api.com/v6/latest"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def get_rates(self, base_currency: str = "USD") -> Dict[str, Any]:
        """Fetch latest exchange rates for a base currency."""
        url = f"{self.BASE_URL}/{base_currency}"
        logger.info(f"Fetching exchange rates from {url}")

        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()
        if data.get("result") != "success":
            raise ValueError(f"API returned non-success: {data.get('result')}")

        logger.info(f"Got {len(data.get('rates', {}))} rates for {base_currency}")
        return data


class WorldBankClient:
    """Client for World Bank Indicators API."""

    BASE_URL = "https://api.worldbank.org/v2"

    def __init__(self, timeout: int = 60):
        self.timeout = timeout

    def get_indicators(
        self,
        country_codes: List[str],
        indicator_codes: List[str],
        start_year: int = 2020,
        end_year: int = 2025,
    ) -> List[Dict[str, Any]]:
        """Fetch indicators for multiple countries and indicators."""
        all_results = []

        for indicator in indicator_codes:
            countries = ";".join(country_codes)
            url = (
                f"{self.BASE_URL}/country/{countries}/indicator/{indicator}"
                f"?date={start_year}:{end_year}&format=json&per_page=500"
            )
            logger.info(f"Fetching {indicator} for {countries}")

            page = 1
            while url:
                try:
                    response = requests.get(url, timeout=self.timeout)
                    response.raise_for_status()
                    data = response.json()

                    if len(data) < 2 or data[1] is None:
                        logger.warning(f"No data returned for {indicator}")
                        break

                    for record in data[1]:
                        all_results.append({
                            "country_code": record.get("countryiso3code", ""),
                            "country_name": record.get("country", {}).get("value", ""),
                            "indicator_code": indicator,
                            "indicator_name": record.get("indicator", {}).get("value", ""),
                            "year": int(record.get("date", 0)) if record.get("date") else None,
                            "value": record.get("value"),
                        })

                    # Pagination
                    pages = data[0].get("pages", 1)
                    if page < pages:
                        page += 1
                        url = url.replace(f"page={page-1}", f"page={page}").replace(
                            "per_page=500", f"page={page}&per_page=500"
                        ) if f"page={page-1}" in url else url + f"&page={page}"
                    else:
                        url = None

                except requests.RequestException as e:
                    logger.error(f"Error fetching {indicator}: {e}")
                    url = None

        logger.info(f"Total records fetched: {len(all_results)}")
        return all_results

    def get_countries(self) -> List[Dict[str, Any]]:
        """Get list of available countries."""
        url = f"{self.BASE_URL}/country?format=json&per_page=300"
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        return data[1] if len(data) > 1 else []


class JSONPlaceholderClient:
    """Client for JSONPlaceholder fake REST API."""

    BASE_URL = "https://jsonplaceholder.typicode.com"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def get_users(self) -> List[Dict[str, Any]]:
        """Fetch all users."""
        response = requests.get(f"{self.BASE_URL}/users", timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def health_check(self) -> bool:
        """Check if API is reachable."""
        try:
            response = requests.get(f"{self.BASE_URL}/posts/1", timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False
