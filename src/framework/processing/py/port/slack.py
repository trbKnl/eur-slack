"""
DDP extract Slack
"""
from pathlib import Path
import logging
import re

import pandas as pd


from dateutil import parser
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


def format_timestamp(timestamp, to_string = True):
    pattern = r'\(.*?\)'
    timestamp = re.sub(pattern, '', timestamp)
    if to_string:
        return parser.parse(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return parser.parse(timestamp)

def hour_diff(row):
    start_col = "Date Accessed"
    end_col = "Last Date Accessed"
    start_time = format_timestamp(row[start_col], to_string = False)
    end_time = format_timestamp(row[end_col], to_string = False)

    time_diff_hours = (end_time - start_time).total_seconds() / 3600
    return time_diff_hours


def clean_df(df) -> pd.DataFrame:
    try:
        # remove rows containing 'Google Calendar' from "User Agent - Simple"
        df = df[df["User Agent - Simple"] != "Google Calendar"].reset_index(drop=True)

        # Try to clean all timestamps
        df["Login duration in hours"] = df.apply(hour_diff, axis=1)
        df["Date Accessed"] = df["Date Accessed"].apply(lambda x: format_timestamp(x))
        df["Last Date Accessed"] = df["Last Date Accessed"].apply(lambda x: format_timestamp(x))


    except Exception as e:
        logger.error(e)
    
    return df



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
        out = clean_df(out)

    except Exception as e:
        logger.error(e)

    return out

