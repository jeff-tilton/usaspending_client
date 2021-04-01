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
from .utils import flatten_dict


LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)
FORMAT = "%(levelname)s - %(asctime)s - %(name)s - %(message)s"


class USASpending:
    def __init__(self, verbosity=10):
        self.BASE_URL = "https://api.usaspending.gov"
        logging.basicConfig(stream=sys.stderr, level=verbosity, format=FORMAT)

    @staticmethod
    def _log_response_(response):
        status = response.status_code
        LOGGER.debug(f"Status code: {response.status_code}")
        if status != 200:
            LOGGER.warning(response.text)

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
            zipfile of award data in CSV form for download.  [Full documentation for endpoint](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md).

        Parameters
        ----------
        start_date : str
            Required start date of time period.
        end_date : str
            Required end date of time period.
        date_type : enum[str]
            - `'domestic'`
            - `'foreign'`
        agencies : array[Agency]


        prime_award_types : array[enum[string]]

            - `'IDV_A'`
            - `'IDV_B'`
            - `'IDV_B_A'`
            - `'IDV_B_B'`
            - `'IDV_B_C'`
            - `'IDV_C'`
            - `'IDV_D'`
            - `'IDV_E'`
            - `'02'`
            - `'03'`
            - `'04'`
            - `'05'`
            - `'06'`
            - `'07'`
            - `'08'`
            - `'09'`
            - `'10'`
            - `'11'`
            - `'A'`
            - `'B'`
            - `'C'`
            - `'D'`

        place_of_performance_locations : array[Location]

        place_of_performance_scope : enum[string]

            - `'domestic'`
            - `'foreign'`

        recipient_locations : array[Location]

        recipient_scope : enum[string]

            - `'domestic'`
            - `'foreign'`

        sub_award_types : array[enum[string]]

            - `'grant'`
            - `'procurement'`

        filters : object
            A fully built python dictionary with all filters, bypassing all other arguments.
            See [endpoint documentation](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md) for an example.


        ## Agency: object

        - name: str
        - tier: enum[str]

            -- `'toptier'`

            -- `'subtier'`

        - type: enum[str]

            -- `'funding'`

            -- `'awarding'`

        ## Location: object

        - `'country'`: str
        - `'state'`: str
        - `'county'`: str
        - `'city'`: str
        - `'district'`: str
        - `'zip'`: str

        Returns
        -------
        request.response
            Response from the USASpending /api/v2/bulk_download/awards/ endpoint.

        Examples
        -------

        ```python
        >>> #using arguments
        >>> from usaspending_client import USASpending
        >>> usa = USASpending()
        >>> response = usa.bulk_download_awards(start_date="2019-10-01", end_date="2020-09-30", prime_award_types=["A"])
        >>> #using filters object
        >>> filters = {"prime_award_types": ["A"],"sub_award_types": [],"date_type": "action_date","date_range": {"start_date": "2019-10-01","end_date": "2020-09-30"},"agencies": [{"type": "funding","tier": "subtier","name": "Animal and Plant Health Inspection Service","toptier_name": "Department of Agriculture"}]}
        >>> response = usa.bulk_download_awards(filters=filters)
        ```
        """
        url = self.BASE_URL + "/api/v2/bulk_download/awards/"

        if not filters:
            start_date = pd.to_datetime(start_date).strftime("%Y-%m-%d")
            end_date = pd.to_datetime(end_date).strftime("%Y-%m-%d")
            date_range = {"start_date": start_date, "end_date": end_date}
            kwargs = locals()
            filters = {}
            for kwarg, v in kwargs.items():
                if v and kwarg not in ["self", "start_date", "end_date", "url"]:
                    filters.update({kwarg: v})

        response = requests.post(url=url, json={"filters": filters})
        self._log_response_(response)
        return response

    @LD
    def bulk_download_status(self, file_name):
        """This method returns the current status of a download job
         that has been requested with the v2/bulk_download/awards/
         or v2/bulk_download/transaction/ endpoint that same day.
         [Full documentation for endpoint](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/download/status.md).

        Parameters
        ----------
        file_name : str
            File name returned in a bulk_download response object
        """
        url = self.BASE_URL + f"/api/v2/download/status/?file_name={file_name}"
        response = requests.get(url)
        self._log_response_(response)
        return response

    @LD
    def bulk_awards(
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
        attempts=10,
    ):

        """This method sends a request to the backend to begin generating a
            zipfile of award data in CSV form for download.  [Full documentation for endpoint](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md).

        Parameters
        ----------
        start_date : str
            Required start date of time period.
        end_date : str
            Required end date of time period.
        date_type : enum[str]
            - `'domestic'`
            - `'foreign'`
        agencies : array[Agency]


        prime_award_types : array[enum[string]]

            - `'IDV_A'`
            - `'IDV_B'`
            - `'IDV_B_A'`
            - `'IDV_B_B'`
            - `'IDV_B_C'`
            - `'IDV_C'`
            - `'IDV_D'`
            - `'IDV_E'`
            - `'02'`
            - `'03'`
            - `'04'`
            - `'05'`
            - `'06'`
            - `'07'`
            - `'08'`
            - `'09'`
            - `'10'`
            - `'11'`
            - `'A'`
            - `'B'`
            - `'C'`
            - `'D'`

        place_of_performance_locations : array[Location]

        place_of_performance_scope : enum[string]

            - `'domestic'`
            - `'foreign'`



        recipient_locations : array[Location]

        recipient_scope : enum[string]

            - `'domestic'`
            - `'foreign'`


        sub_award_types : array[enum[string]]

            - `'grant'`
            - `'procurement'`

        filters : object
            A fully built python dictionary with all filters, bypassing all other arguments.
            See [endpoint documentation](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md) for an example.

        return_df: bool
            Return a pandas dataframe

        file_destination: str
            If not a pandas dataframe, filelocation to store zipped csv's

        attempts: int
            Number of times to check if bulk download has completed.

        ## Agency: object

        - name: str
        - tier: enum[str]

            -- `'toptier'`

            -- `'subtier'`

        - type: enum[str]

            -- `'funding'`

            -- `'awarding'`

        ## Location: object

        - `'country'`: str
        - `'state'`: str
        - `'county'`: str
        - `'city'`: str
        - `'district'`: str
        - `'zip'`: str

        Returns
        -------
        pd.DataFrame or zip file
             Final response from the USASpending /api/v2/bulk_download/awards/ endpoint.

        Examples
        -------

        ```python
        >>> #using arguments
        >>> from usaspending_client import USASpending
        >>> usa = USASpending()
        >>> df = usa.awards(start_date="2019-10-01", end_date="2020-09-30", prime_award_types=["A"])
        >>> #using filters object
        >>> filters = {"prime_award_types": ["A"],"sub_award_types": [],"date_type": "action_date","date_range": {"start_date": "2019-10-01","end_date": "2020-09-30"},"agencies": [{"type": "funding","tier": "subtier","name": "Animal and Plant Health Inspection Service","toptier_name": "Department of Agriculture"}]}
        >>> df = usa.awards(filters=filters)
        ```
        """

        if not return_df and not file_destination:
            msg = "Need to return a pandas dataframe or provide file location for download"
            raise ValueError(msg)

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
        status = None
        runs = 0
        while status != "finished" and runs < attempts:
            dl_status = self.bulk_download_status(file_name=file_name)
            data = json.loads(dl_status.text)
            status = data["status"]
            runs += 1

        try:
            file_url = data["file_url"]
            LOGGER.debug(file_url)
        except KeyError:
            raise KeyError(f"Bulk download did not finish in {attempts} attempts.")

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

        urlretrieve(file_url, file_destination)

    @LD
    def awards(self, award_id, return_json=False):
        """Short summary.

        Parameters
        ----------
        award_id : type
            Description of parameter `award_id`.
        return_json : type
            Description of parameter `return_json` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        Examples
        --------
        Examples should be written in doctest format, and
        should illustrate how to use the function/class.
        >>>

        """
        url = self.BASE_URL + f"/api/v2/awards/{award_id}"
        response = requests.get(url)
        self._log_response_(response)
        if return_json:
            response = json.loads(response.text)
        return response

    @LD
    def awards_list(self, award_ids, return_json=False):
        """Short summary.

        Parameters
        ----------
        award_ids : type
            Description of parameter `award_ids`.
        return_json : type
            Description of parameter `return_json` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        Examples
        --------
        Examples should be written in doctest format, and
        should illustrate how to use the function/class.
        >>>

        """
        result = []
        for award_id in award_ids:
            try:
                response = self.awards(award_id=award_id, return_json=return_json)
            except:
                LOGGER.error(f"Error requesting award id: {award_id}")
        return result

    @LD
    def awards_df(self, award_ids):
        """Short summary.

        Parameters
        ----------
        award_ids : type
            Description of parameter `award_ids`.

        Returns
        -------
        type
            Description of returned object.

        Examples
        --------
        Examples should be written in doctest format, and
        should illustrate how to use the function/class.
        >>>

        """
        awards = self.awards_list(award_ids, return_json=True)
        flattened_awards = [flatten_dict(award) for award in awards]
        df = pd.DataFrame(awards)
        return df
