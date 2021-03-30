import pytest

from usaspending_client import USASpending


@pytest.fixture()
def usa():
    usa = USASpending()
    yield usa


class TestClient(object):
    def test_bulk_download_awards_200_response(self, usa):
        filters = {
            "prime_award_types": [
                "A",
                "B",
                "C",
                "D",
                "IDV_A",
                "IDV_B",
                "IDV_B_A",
                "IDV_B_B",
                "IDV_B_C",
                "IDV_C",
                "IDV_D",
                "IDV_E",
                "02",
                "03",
                "04",
                "05",
                "10",
                "06",
                "07",
                "08",
                "09",
                "11",
            ],
            "sub_award_types": [],
            "date_type": "action_date",
            "date_range": {"start_date": "2019-10-01", "end_date": "2020-09-30"},
            "agencies": [
                {
                    "type": "funding",
                    "tier": "subtier",
                    "name": "Animal and Plant Health Inspection Service",
                    "toptier_name": "Department of Agriculture",
                }
            ],
        }

        assert usa.bulk_download_awards(filters=filters).status_code == 200
