"""
DDP extract Slack
"""
from pathlib import Path
import logging

import pandas as pd

import port.unzipddp as unzipddp

from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="csv_en",
        ddp_filetype=DDPFiletype.CSV,
        language=Language.EN,
        known_files=[
        ]
    ),
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid slack CSV", message=""),
    StatusCode(id=1, description="Not a slack CSV", message=""),
]


def validate(filename: Path) -> ValidateInput:
    """
    """
    known_colnames = [
     "Date Accessed", 
     "User Agent - Simple",
     "User Agent - Full",
     "IP Address",
     "Number of Logins",
     "Last Date Accessed"
    ]

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)
    df = unzipddp.read_csv_from_file_to_df(filename)
    for known_col in known_colnames:
        if known_col not in df.columns:
            validation.set_status_code(1)
            return validation

    validation.set_status_code(0)
    return validation


def slack_logins_to_df(filename: str) -> pd.DataFrame:
    out = pd.DataFrame()
    cols_to_keep = [
     "Date Accessed", 
     "Last Date Accessed",
     "User Agent - Simple",
     "Number of Logins",
    ]
    try:
        out = unzipddp.read_csv_from_file_to_df(filename)
        out = out[cols_to_keep]

    except Exception as e:
        logger.error(e)

    return out
