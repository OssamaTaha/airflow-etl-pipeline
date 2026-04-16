"""
Tests for extraction logic — API clients and validators.
Run with: pytest tests/test_extract.py -v
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))


class TestExchangeRateValidation:
    """Tests for exchange rate response validation."""

    def test_valid_response(self):
        from utils.validators import validate_exchange_rate_response
        data = {
            "result": "success",
            "base_code": "USD",
            "rates": {"EUR": 0.92, "GBP": 0.79, "EGP": 30.9, "JPY": 150.2, "CAD": 1.36,
                       "AUD": 1.53, "CHF": 0.88, "CNY": 7.24, "INR": 83.1, "BRL": 4.97,
                       "ZAR": 18.5, "SAR": 3.75, "AED": 3.67}
        }
        assert validate_exchange_rate_response(data) is True

    def test_missing_base_code(self):
        from utils.validators import validate_exchange_rate_response
        data = {"result": "success", "rates": {"EUR": 0.92}}
        assert validate_exchange_rate_response(data) is False

    def test_missing_rates(self):
        from utils.validators import validate_exchange_rate_response
        data = {"result": "success", "base_code": "USD"}
        assert validate_exchange_rate_response(data) is False

    def test_non_success_result(self):
        from utils.validators import validate_exchange_rate_response
        data = {"result": "error", "base_code": "USD", "rates": {"EUR": 0.92}}
        assert validate_exchange_rate_response(data) is False

    def test_few_rates(self):
        from utils.validators import validate_exchange_rate_response
        data = {"result": "success", "base_code": "USD", "rates": {"EUR": 0.92, "GBP": 0.79}}
        assert validate_exchange_rate_response(data) is False


class TestWorldBankValidation:
    """Tests for World Bank record validation."""

    def test_valid_records(self):
        from utils.validators import validate_worldbank_records
        records = [
            {"country_code": "EGY", "indicator_code": "NY.GDP.MKTP.CD", "year": 2023, "value": 400000000000},
            {"country_code": "SAU", "indicator_code": "NY.GDP.MKTP.CD", "year": 2023, "value": 1000000000000},
            {"country_code": "EGY", "indicator_code": "SP.POP.TOTL", "year": 2023, "value": 104000000},
        ]
        is_valid, errors = validate_worldbank_records(records)
        assert is_valid is True
        assert len(errors) == 0

    def test_empty_records(self):
        from utils.validators import validate_worldbank_records
        is_valid, errors = validate_worldbank_records([])
        assert is_valid is False
        assert "No records returned" in errors

    def test_missing_country_code(self):
        from utils.validators import validate_worldbank_records
        records = [{"indicator_code": "NY.GDP.MKTP.CD", "year": 2023, "value": 100}]
        is_valid, errors = validate_worldbank_records(records)
        assert is_valid is False


class TestAPIFunctionality:
    """Tests for API client functionality (uses mocked responses)."""

    def test_api_clients_import(self):
        from utils.api_clients import ExchangeRateClient, WorldBankClient, JSONPlaceholderClient
        er = ExchangeRateClient()
        wb = WorldBankClient()
        jp = JSONPlaceholderClient()
        assert er is not None
        assert wb is not None
        assert jp is not None

    def test_exchange_rate_client_url(self):
        from utils.api_clients import ExchangeRateClient
        client = ExchangeRateClient()
        assert "open.er-api.com" in client.BASE_URL
