import json
import logging
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
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        # https://stackoverflow.com/a/50375022/4296857
        self._caplog = caplog

    @pytest.mark.parametrize(
        "filters",
        data_tests,
    )
    def test_bulk_download_awards_using_filters_object_200_response(self, filters, usa):
        assert usa.bulk_download_awards(filters=filters).status_code == 200

    def test_bulk_download_awards_using_filters_object_400_response(self, usa):
        assert (
            usa.bulk_download_awards(
                start_date="2019-10-01", end_date="2020-09-30"
            ).status_code
            == 400
        )
        with self._caplog.at_level(30):
            assert (
                "Missing one or more required body parameters: prime_award_types or sub_award_types"
                in self._caplog.records[-1].message
            )

    def test_bulk_download_awards_using_arguments_200_response(self, usa):

        response = usa.bulk_download_awards(
            start_date="2019-10-01", end_date="2020-09-30", prime_award_types=["A"]
        )
        if response.status_code != 200:
            print(response.text)
        assert response.status_code == 200

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
    def test_bulk_awards_to_df(self, filters, usa):
        df = usa.bulk_awards(filters=filters)
        assert not df.empty

    def test_awards_200_response(self, usa):
        response = usa.awards(award_id="CONT_AWD_12639519P0311_12K3_-NONE-_-NONE-")
        assert response.status_code == 200


# pytest --log-cli-level=10
