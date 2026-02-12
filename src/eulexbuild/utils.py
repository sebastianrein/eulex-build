import logging
import re

CELEX_PATTERN = re.compile(r'^[0-9CE][0-9]{4}[A-Z]{1,3}[0-9]{4,6}[A-Z]{0,3}[_\-]?[0-9]{0,9}$')
CONSOLIDATED_CELEX_PATTERN = re.compile(r'^(0)(\d{4}[RLD]\d{4})(-\d{8}$)')
PROCEDURE_NUMBER_PATTERN = re.compile(r'^[0-9]{4}/[0-9]{4}(\([a-zA-Z]{3}\)|/[a-zA-Z]{3})$')


class ForwardHandler(logging.Handler):
    def __init__(self, target_logger: logging.Logger):
        super().__init__()
        self.target_logger = target_logger
        self.setLevel(target_logger.level)

    def emit(self, record):
        message = f"{record.name}: {record.getMessage()}"
        self.target_logger.log(record.levelno, message)


def normalize_string(text: str) -> str:
    """
    Normalize a string by:
    - Stripping leading/trailing whitespace
    - Replacing all whitespace characters (tabs, newlines, etc.) with single spaces
    - Remove spaces before punctuation
    - Ensure single space after punctuation
    """
    text = re.sub(r'([,;:!?.)}\]])(\S)', r'\1 \2', text)
    text = re.sub(r'\s+([,;:!?.])', r'\1', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def normalize_celex(celex: str) -> str:
    return celex.strip().upper()


def is_valid_celex(celex: str) -> bool:
    return bool(CELEX_PATTERN.match(celex))


def is_consolidated_celex(celex: str) -> bool:
    return bool(CONSOLIDATED_CELEX_PATTERN.match(celex))


def validate_celex(celex: str) -> str:
    celex = normalize_celex(celex)
    if not is_valid_celex(celex):
        raise ValueError(f"Invalid CELEX ID format: {celex}.")
    return celex


def convert_consolidated_celex_to_original(celex: str) -> str:
    celex = normalize_celex(celex)
    if not is_consolidated_celex(celex):
        raise ValueError(f"Not a consolidated CELEX ID: {celex}. "
                         f"Expected format: 0YYYYXNNNN-YYYYMMDD (e.g., 02016R0679-20210101)")
    consolidated_match = CONSOLIDATED_CELEX_PATTERN.match(celex)
    return "3" + consolidated_match.group(2)


def normalize_procedure_number(procedure_number: str) -> str:
    procedure_number = procedure_number.strip().upper()
    procedure_number = procedure_number.replace('(', '/').replace(')', '')
    return procedure_number


def is_valid_procedure_number(procedure_number: str) -> bool:
    return bool(PROCEDURE_NUMBER_PATTERN.match(procedure_number))
