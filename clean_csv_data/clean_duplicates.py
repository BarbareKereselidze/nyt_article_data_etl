import pandas as pd
from typing import Dict, Any

from utils.logger import get_logger


class DuplicatesRemover:
    def __init__(self, config: Dict[str, Any]) -> None:
        """ initialize the DuplicatesRemover class """

        self.output_csv_path: str = config["Paths"]["clean_csv_path"]
        self.logger = get_logger()

    def clean_duplicates(self) -> None:
        """ read csv, remove duplicates based on 'web_url', and write the cleaned data back to csv. """

        df = pd.read_csv(self.output_csv_path)
        # keep track of initial number of rows in the dataframe
        initial_rows = df.shape[0]

        df.drop_duplicates(subset=['web_url'], inplace=True)
        df.to_csv(self.output_csv_path, index=False, columns=['web_url'] + [col for col in df.columns if col != 'web_url'])

        # keep track of how many rows were duplicate for logging
        removed_rows = initial_rows - df.shape[0]

        self.logger.info(f" duplicates removed: {removed_rows}")
