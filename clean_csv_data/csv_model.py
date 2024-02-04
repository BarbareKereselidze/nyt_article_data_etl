import json
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, field_validator


class CsvModel(BaseModel):
    """ initializing the CsvModel data class for later data cleaning. """

    web_url: Optional[str]
    print_page: Optional[float] = None
    multimedia: Optional[str] = None
    headline: Optional[str] = None
    keywords: Optional[str] = None
    pub_date: Optional[datetime] = None
    word_count: Optional[float] = None
    byline: Optional[str] = None

    # setting up config for the model
    class Config:
        validate_assignment = True

    @field_validator("web_url", mode="before")
    def validate_web_url(cls, value: Any) -> Optional[str]:
        if isinstance(value, str):
            if value.startswith("https://"):
                return value
        else:
            raise ValueError

    @field_validator("print_page", "word_count", mode="before")
    def validate_int(cls, value: Any) -> Optional[float]:
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
    def validate_date(cls, value: Any) -> Optional[datetime]:
        try:
            new_val = datetime.strptime(value, '%Y-%m-%d %H:%M:%S%z')
            return new_val
        except ValueError:
            return None

    @field_validator("multimedia", "headline", "keywords", "byline", mode="before")
    def validate_jsonb_types(cls, value: Any) -> Optional[str]:
        try:
            new_val = json.dumps(value)
            return new_val
        except ValueError:
            return None
