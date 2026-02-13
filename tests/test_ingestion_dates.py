from datetime import date

from nba_analytics.ingestion import filter_dates


def test_filter_dates_applies_start_and_end() -> None:
    values = ["2026-01-01", "2026-01-10", "2026-01-20"]
    result = filter_dates(values, date(2026, 1, 5), date(2026, 1, 15), lookback_days=None)
    assert result == [date(2026, 1, 10)]

