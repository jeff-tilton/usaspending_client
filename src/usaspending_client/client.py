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
        """This method sends a request to the backend to begin generating a
            zipfile of award data in CSV form for download.

        Parameters
        ----------
        start_date : str
            Required start date of time period.
        end_date : str
            Required end date of time period.
        date_type : str (enum)
            options:
                - 'action_date'
                - 'last_modified_date'
        agencies : array[Agency]
            Agency: obj
                name: str
                tier: enum[str]
                    - 'toptier'
                    - 'subtier'
                type: enum[str]
                    - 'funding'
                    - 'awarding'
                toptier_name: str
                    Provided when the `name` belongs to a subtier agency.
        prime_award_types : array[enum[string]]
            options:
                - IDV_A
                - IDV_B
                - IDV_B_A
                - IDV_B_B
                - IDV_B_C
                - IDV_C
                - IDV_D
                - IDV_E
                - 02
                - 03
                - 04
                - 05
                - 06
                - 07
                - 08
                - 09
                - 10
                - 11
                - A
                - B
                - C
                - D
        place_of_performance_locations : type
            Description of parameter `place_of_performance_locations`.
        place_of_performance_scope : type
            Description of parameter `place_of_performance_scope`.
        recipient_locations : type
            Description of parameter `recipient_locations`.
        recipient_scope : type
            Description of parameter `recipient_scope`.
        sub_award_types : type
            Description of parameter `sub_award_types`.
        filters : type
            Description of parameter `filters`.

        Returns
        -------
        type
            Description of returned object.

        """
        url = self.BASE_URL + "/api/v2/bulk_download/awards/"

        date_range = {"start_date": start_date, "end_date": end_date}
        if not filters:
            start_date = pd.to_datetime(start_date).strftime("%Y-%m-%d")
            end_date = pd.to_datetime(end_date).strftime("%Y-%m-%d")
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

        """Short summary.

        Parameters
        ----------
        start_date : type
            Description of parameter `start_date`.
        end_date : type
            Description of parameter `end_date`.
        date_type : type
            Description of parameter `date_type`.
        agencies : type
            Description of parameter `agencies`.
        prime_award_types : type
            Description of parameter `prime_award_types`.
        place_of_performance_locations : type
            Description of parameter `place_of_performance_locations`.
        place_of_performance_scope : type
            Description of parameter `place_of_performance_scope`.
        recipient_locations : type
            Description of parameter `recipient_locations`.
        recipient_scope : type
            Description of parameter `recipient_scope`.
        sub_award_types : type
            Description of parameter `sub_award_types`.
        filters : type
            Description of parameter `filters`.

        Returns
        -------
        type
            Description of returned object.

        """

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
