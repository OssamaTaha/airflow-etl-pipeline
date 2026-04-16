"""
Data validation helpers for the ETL pipeline.
"""
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def validate_exchange_rate_response(data: Dict[str, Any]) -> bool:
    """Validate exchange rate API response structure."""
    checks = []

    # Check required fields
    if not data.get("base_code"):
        checks.append("Missing base_code")
    if not data.get("rates"):
        checks.append("Missing rates dictionary")
    if data.get("result") != "success":
        checks.append(f"Non-success result: {data.get('result')}")

    # Check rates dictionary
    rates = data.get("rates", {})
    if not isinstance(rates, dict):
        checks.append("Rates is not a dictionary")
    elif len(rates) < 10:
        checks.append(f"Too few rates: {len(rates)} (expected 100+)")

    # Check rate values are numeric
    for currency, rate in rates.items():
        if not isinstance(rate, (int, float)):
            checks.append(f"Non-numeric rate for {currency}: {rate}")
            break

    if checks:
        for check in checks:
            logger.error(f"Validation failed: {check}")
        return False

    logger.info(f"Validation passed: {len(rates)} rates for {data['base_code']}")
    return True


def validate_worldbank_records(records: List[Dict[str, Any]]) -> tuple[bool, List[str]]:
    """Validate World Bank indicator records. Returns (is_valid, error_messages)."""
    errors = []

    if not records:
        errors.append("No records returned")
        return False, errors

    for i, record in enumerate(records):
        if not record.get("country_code"):
            errors.append(f"Record {i}: Missing country_code")
        if not record.get("indicator_code"):
            errors.append(f"Record {i}: Missing indicator_code")
        if record.get("year") is None:
            errors.append(f"Record {i}: Missing year")
        # value can be None for some years/countries — that's ok

    # Check we have data for expected indicators
    indicators_found = set(r.get("indicator_code") for r in records)
    if len(indicators_found) < 2:
        errors.append(f"Only {len(indicators_found)} distinct indicators found")

    is_valid = len(errors) == 0
    if not is_valid:
        for err in errors[:5]:  # Log first 5 errors
            logger.error(f"Validation error: {err}")

    return is_valid, errors


def validate_no_nulls(records: List[Dict], required_fields: List[str]) -> tuple[bool, List[str]]:
    """Check that required fields are not null/empty in records."""
    errors = []
    for i, record in enumerate(records):
        for field in required_fields:
            if field not in record or record[field] is None:
                errors.append(f"Record {i}: null/missing '{field}'")
    return len(errors) == 0, errors


def check_data_freshness(
    records: List[Dict],
    timestamp_field: str,
    max_age_hours: int = 48,
) -> bool:
    """Verify data is recent enough."""
    from datetime import datetime, timezone

    if not records:
        logger.warning("No records to check freshness")
        return False

    latest = max(
        (r.get(timestamp_field) for r in records if r.get(timestamp_field)),
        default=None,
    )

    if latest is None:
        logger.warning(f"No {timestamp_field} found in records")
        return False

    if isinstance(latest, str):
        latest = datetime.fromisoformat(latest.replace("Z", "+00:00"))

    age = datetime.now(timezone.utc) - latest
    if age.total_seconds() > max_age_hours * 3600:
        logger.warning(f"Data is {age} old (max: {max_age_hours}h)")
        return False

    logger.info(f"Data freshness OK: {age} old")
    return True
