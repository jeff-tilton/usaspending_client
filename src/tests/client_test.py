import pytest

from usaspending_client import USASpending


@pytest.fixture()
def usa():
    usa = USASpending()
    yield usa


class TestClient(object):
    def test_bulk_download_awards_filters(self, usa):
        start_date = "2019-10-01"
        end_date = "2020-09-30"
        filters = usa.bulk_download_awards(start_date=start_date, end_date=end_date)
        assert filters == {
            "date_range": {"start_date": start_date, "end_date": end_date}
        }
