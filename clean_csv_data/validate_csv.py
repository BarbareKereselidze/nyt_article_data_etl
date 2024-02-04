import csv
from typing import Dict, Any

from clean_csv_data.csv_model import CsvModel
from utils.logger import get_logger


class CsvValidator:
    def __init__(self, config: Dict[str, Any]) -> None:
        """ initializing CsvValidator class for csv validation and cleaning. """

        self.input_csv_path: str = config["Paths"]["csv_path"]
        self.output_csv_path: str = config["Paths"]["clean_csv_path"]
        self.logger = get_logger()

    def validate_csv(self) -> None:
        """ validate and clean the csv file. """

        csv.field_size_limit(3 * 1024 * 1024)  # 3mb

        # read the original csv file
        with open(self.input_csv_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            header = csv_reader.fieldnames

            # write into the new csv file
            with open(self.output_csv_path, 'w', newline='') as clean_csv:
                csv_writer = csv.DictWriter(clean_csv, fieldnames=header)
                csv_writer.writeheader()

                # validate data using the csv model
                for row in csv_reader:
                    model_instance = CsvModel(**row)

                    try:
                        model_instance.validate_web_url(model_instance.web_url)
                    except ValueError:
                        get_logger().error(f" skipping row due to invalid url: {row}")
                        continue

                    validated_row = {**row, **model_instance.dict()}
                    csv_writer.writerow(validated_row)

        self.logger.info(f" clean data is kept at: {self.output_csv_path}")
