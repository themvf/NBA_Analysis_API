from nba_analytics.api_client import NbaDailyApiClient


def test_records_parser_supports_multiple_shapes() -> None:
    assert NbaDailyApiClient.records({"data": [{"a": 1}, {"b": 2}]}) == [{"a": 1}, {"b": 2}]
    assert NbaDailyApiClient.records([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]
    assert NbaDailyApiClient.records({"a": 1}) == [{"a": 1}]
    assert NbaDailyApiClient.records("invalid") == []


def test_date_values_supports_list_and_wrapped() -> None:
    assert NbaDailyApiClient.date_values({"data": ["2026-01-01", "2026-01-02"]}) == ["2026-01-01", "2026-01-02"]
    assert NbaDailyApiClient.date_values(["2026-01-01"]) == ["2026-01-01"]
    assert NbaDailyApiClient.date_values({"x": 1}) == []


def test_paginated_records_accumulates_pages() -> None:
    class StubClient(NbaDailyApiClient):
        def __init__(self) -> None:
            super().__init__(base_url="http://example")
            self.calls = 0

        def get(self, path, params=None):
            self.calls += 1
            offset = params["offset"]
            limit = params["limit"]
            if offset == 0:
                return {"count": 3, "data": [{"x": 1}, {"x": 2}]}
            if offset == 2:
                return {"count": 3, "data": [{"x": 3}]}
            return {"count": 3, "data": []}

    client = StubClient()
    rows = client.paginated_records("/predictions", page_size=2)
    assert rows == [{"x": 1}, {"x": 2}, {"x": 3}]
    assert client.calls == 2

