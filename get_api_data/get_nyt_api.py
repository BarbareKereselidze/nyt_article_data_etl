import requests
from datetime import datetime
from typing import Dict, Tuple, Union, Any

from utils.logger import get_logger


class APIDataRetriever:
    def __init__(self, config: Dict[str, Any], begin_date: str) -> None:
        """ initialize the APIDataRetriever class. """

        self.begin_date: str = begin_date
        self.end_date: str = datetime.now().strftime('%Y-%m-%d')

        self.api_key: str = config['API']['api_key']
        self.url: str = config['API']['api_url']

        self.logger = get_logger()

    def get_api_data(self, page: int) -> Union[None, Tuple[int, dict]]:
        """ retrieves data from the api.
            returns: a tuple containing status code and api response json, or (None, None) in case of an error. """

        try:
            params = {
                'api-key': self.api_key,
                'begin_date': self.begin_date,
                'end_date': self.end_date,
                'sort': 'oldest',  # sort by eldest date in the start and end date range
                'page': page
            }

            response = requests.get(self.url, params=params)
            response.raise_for_status()

            return response.status_code, response.json()

        except requests.exceptions.RequestException as req_error:
            self.logger.error(f" request error: {req_error}")
            return None, None
