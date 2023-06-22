# type: ignore[attr-defined]
"""Python library to harvest target exhibit files list from sec edger database that matches given list of phrases"""

import sys
from importlib import metadata as importlib_metadata

import sec_edger_harverster.SecEdgerHarvester as SecEdgerHarvester
from harvest_insurance_contract import harverst_insurance_contract


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


version: str = get_version()
