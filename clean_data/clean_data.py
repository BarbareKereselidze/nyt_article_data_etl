import csv
import json
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

from utils.logger import get_logger


class CsvModel(BaseModel):
    web_url: Optional[str]
    print_page: Optional[float] = Field(default=None, validate_default=True)
    multimedia: Optional[str] = Field(default=None, validate_default=True)
    headline: Optional[str] = Field(default=None, validate_default=True)
    keywords: Optional[str] = Field(default=None, validate_default=True)
    word_count: Optional[float] = Field(default=None, validate_default=True)
    byline: Optional[str] = Field(default=None, validate_default=True)

    @field_validator("web_url", mode="before")
    def validate_web_url(cls, value):
        if isinstance(value, str):
            if value.startswith("https://"):
                return value
        else:
            raise ValueError("Invalid web_url")

    @field_validator("print_page", "word_count", mode="before")
    def validate_int(cls, value):
        if value is None:
            return None

        if isinstance(value, float):
            return value

        try:
            cleaned_value = float(value)
            return cleaned_value
        except ValueError:
            int_part = ''.join(filter(str.isdigit, value))
            if int_part:
                return float(int_part)
            else:
                return None

    @field_validator("pub_date", mode="before", check_fields=False)
    def validate_date(cls, value):
        if isinstance(value, str):
            try:
                new_val = datetime.strptime(value, '%Y-%m-%d %H:%M:%S%z')
                return new_val
            except ValueError:
                return None
        else:
            return None

    @field_validator("multimedia", "headline", "keywords", "byline", mode="before")
    def validate_jsonb_types(cls, value):
        if value is None:
            return None

        if isinstance(value, str):
            try:
                new_val = json.dumps(value)
                return new_val
            except ValueError:
                return None
        else:
            return None


def process_csv(config):
    input_csv_path = config["Paths"]["csv_file_path"]
    output_csv_path = config["Paths"]["clean_csv_path"]
    logger = get_logger()

    csv.field_size_limit(3 * 1024 * 1024)  # 3mb

    with open(input_csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames

        with open(output_csv_path, 'w', newline='') as clean_csv:
            csv_writer = csv.DictWriter(clean_csv, fieldnames=header)
            csv_writer.writeheader()

            for row in csv_reader:
                try:
                    model_instance = CsvModel(**row)
                except ValueError as error:
                    logger.error(f'could not validate row {row} - error: {error}')
                    continue

                try:
                    model_instance.validate_web_url(model_instance.web_url)
                except ValueError:
                    logger.error(f'Invalid web_url - {row}')
                    continue

                validated_row = {**row, **model_instance.dict()}
                csv_writer.writerow(validated_row)

    logger.info(f"clean data is kept at: {output_csv_path}")
