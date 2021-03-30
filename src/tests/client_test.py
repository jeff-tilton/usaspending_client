import json

import pytest

from usaspending_client import USASpending


@pytest.fixture()
def usa():
    usa = USASpending()
    yield usa


data_tests = [
    (
        {
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
    ),
]


class TestClient(object):
    @pytest.mark.parametrize(
        "filters",
        data_tests,
    )
    def test_bulk_download_awards_200_response(self, filters, usa):
        assert usa.bulk_download_awards(filters=filters).status_code == 200

    @pytest.mark.parametrize(
        "filters",
        data_tests,
    )
    def test_bulk_download_status_200_response(self, filters, usa):

        response = usa.bulk_download_awards(filters=filters)

        assert response.status_code == 200

        data = json.loads(response.text)

        file_name = data["file_name"]

        response = usa.bulk_download_status(file_name=file_name)

        assert response.status_code == 200

    @pytest.mark.parametrize(
        "filters",
        data_tests,
    )
    def test_awards_to_df(self, filters, usa):
        df = usa.awards(filters=filters)
        assert not df.empty
