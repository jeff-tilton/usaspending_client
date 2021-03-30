import logging
import sys
import json

from io import BytesIO
from urllib.request import urlretrieve
from urllib.request import urlopen
import shutil
import requests
import pandas as pd
from zipfile import ZipFile

from .utils import log_decorator


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)
FORMAT = "%(levelname)s - %(asctime)s - %(name)s - %(message)s"


class USASpending:
    def __init__(self, verbosity=10):
        self.BASE_URL = "https://api.usaspending.gov"
        logging.basicConfig(stream=sys.stderr, level=verbosity, format=FORMAT)

    @LD
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
        LOGGER.debug(f"Status code: {response.status_code}")
        return response

    def bulk_download_status(self, file_name):
        url = self.BASE_URL + f"/api/v2/download/status/?file_name={file_name}"
        LOGGER.debug(f"url: {url}")
        return requests.get(url)

    @LD
    def awards(
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
        return_df=True,
        file_destination=None,
    ):

        rqst = self.bulk_download_awards(
            start_date=start_date,
            end_date=end_date,
            date_type=date_type,
            agencies=agencies,
            prime_award_types=prime_award_types,
            place_of_performance_locations=place_of_performance_locations,
            place_of_performance_scope=place_of_performance_scope,
            recipient_locations=recipient_locations,
            recipient_scope=recipient_scope,
            sub_award_types=sub_award_types,
            filters=filters,
        )
        data = json.loads(rqst.text)
        file_name = data["file_name"]
        status = "unfinished"
        attempts = 0
        while status != "finished" and attempts < 10:
            dl_status = self.bulk_download_status(file_name=file_name)
            data = json.loads(dl_status.text)
            status = data["status"]
            attempts += 1

        if status == "finished":
            file_url = data["file_url"]
            if return_df:

                try:

                    # ref: https://stackoverflow.com/a/39217788/4296857
                    # ref: https://stackoverflow.com/a/46676405/4296857

                    with requests.get(file_url, stream=True) as r:
                        zf = ZipFile(BytesIO(r.content))
                    match = [s for s in zf.namelist() if ".csv" in s][0]
                    df = pd.read_csv(zf.open(match), low_memory=False)
                    return df

                except:
                    LOGGER.error("Failed to return dataframe", exc_info=True)

        if not return_df and not file_destination:
            msg = "Need to return a pandas dataframe or provide file location for download"
            LOGGER.error(msg)
            raise ValueError(msg)

        urlretrieve(file_url, file_destination)
