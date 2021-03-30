import requests


class USASpending:
    def __init__(self):
        self.BASE_URL = "https://api.usaspending.gov"

    def bulk_download_awards(
        self,
        start_date=None,
        end_date=None,
        date_type="action_date",
        agencies=[{"toptier_name": "Department of Energy"}],
        prime_award_types=[],
        place_of_performance_locations=[],
        place_of_performance_scope=None,
        recipient_locations=None,
        recipient_scope=None,
        sub_award_types=None,
        filters=None,
    ):
        url = self.BASE_URL + "/api/v2/bulk_download/awards/"

        date_range = {"start_date": start_date, "end_date": end_date}
        if not filters:
            kwargs = locals()
            filters = {}
            for kwarg, v in kwargs.items():
                if v and kwarg not in ["self", "start_date", "end_date", "url"]:
                    filters.update({kwarg: v})

        response = requests.post(url=url, json={"filters": filters})

        return response
