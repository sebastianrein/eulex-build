import os
from datetime import date
from pathlib import Path
from typing import Literal, Annotated, Union

import yaml
from pydantic import BaseModel, PastDate, Field, model_validator, PositiveInt, field_validator

from eulexbuild.utils import normalize_celex, is_valid_celex, is_valid_procedure_number, normalize_procedure_number


# Metadata
class Metadata(BaseModel):
    project_name: str = "EULEX-BUILD Dataset"
    author: str = ""
    description: str = "A new dataset constructed with EULEX-BUILD."
    date_created: date = date.today()
    version: str = "1.0"


# Data
class DescriptiveMode(BaseModel):
    mode: Literal["descriptive"] = "descriptive"
    document_types: set[Literal["directive", "regulation", "decision", "proposal"]] = {"directive", "regulation",
                                                                                       "decision"}
    start_date: Annotated[date, PastDate]
    end_date: Annotated[date, PastDate]
    filter_keywords: set[str] = set()
    include_corrigenda: bool = False
    include_consolidated_texts: bool = False
    include_national_transpositions: bool = False

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("Start date must be before end date.")
        return self


class FixedMode(BaseModel):
    mode: Literal["fixed"] = "fixed"
    celex_ids: set[str] = set()
    procedure_numbers: set[str] = set()

    @field_validator("celex_ids")
    @classmethod
    def validate_celex_ids(cls, ids: set[str]) -> set[str]:
        invalid = set()
        normalized = set()
        for id in ids:
            id = normalize_celex(id)
            normalized.add(id)
            if not is_valid_celex(id):
                invalid.add(id)
        if invalid:
            raise ValueError(f"Invalid CELEX ID format: {', '.join(invalid)}.")
        return normalized

    @field_validator("procedure_numbers")
    @classmethod
    def validate_procedure_numbers(cls, numbers: set[str]) -> set[str]:
        invalid = set()
        normalized = set()
        for number in numbers:
            if not is_valid_procedure_number(number):
                invalid.add(number)
            number = normalize_procedure_number(number)
            normalized.add(number)
        if invalid:
            raise ValueError(f"Invalid procedure number format: {', '.join(invalid)}.")
        return normalized

    @model_validator(mode="after")
    def validate_at_least_one_entry(self):
        if not self.celex_ids and not self.procedure_numbers:
            raise ValueError("At least one entry must be provided in either 'celex_ids' or 'procedure_numbers'.")
        return self


# Processing
class TextExtraction(BaseModel):
    include_recitals: bool = True
    include_articles: bool = True
    include_annexes: bool = True


class RelationsExtraction(BaseModel):
    include_relations: bool = True
    include_original_act_relations_for_consolidated_texts: bool = False


class Processing(BaseModel):
    enable_parallel_processing: bool = True
    max_threads: PositiveInt = os.cpu_count() - 1
    automated_mode: bool = False
    text_extraction: TextExtraction = TextExtraction()
    relations_extraction: RelationsExtraction = RelationsExtraction()

    @field_validator("max_threads")
    @classmethod
    def cap_max_threads(cls, v: int) -> int:
        cpu_count = os.cpu_count()
        if cpu_count and v > cpu_count:
            return cpu_count
        return v


# Output
class Output(BaseModel):
    include_raw_full_text: bool = False
    formats: set[Literal["csv", "parquet"]] = {"csv", "parquet"}
    output_directory: str = "./output"


# Top level
class Config(BaseModel):
    metadata: Metadata = Metadata()
    # Fixed mode or descriptive mode are mutually exclusive
    data: Annotated[Union[DescriptiveMode, FixedMode], Field(discriminator="mode")]
    processing: Processing = Processing()
    output: Output = Output()


# Functions
def validate_configuration(file_path: str | Path) -> Config:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found at {path}.")
    if not path.is_file():
        raise ValueError(f"Configuration file at {path} is not a file.")

    with path.open() as f:
        data = yaml.safe_load(f)
        return Config.model_validate(data)
