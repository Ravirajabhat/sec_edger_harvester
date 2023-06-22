"""Tests for hello function."""
import pytest

from sec_edger_harvester import harverst_insurance_contract


@pytest.mark.parametrize(
    ("cik_list", "expected"),
    [
        (["0001018979"], "[]"),
    ],
)
def harverst_insurance_contract(cik_list,expected_result):
    result = harverst_insurance_contract(cik_list)
    print(result)
    assert result == expected_result
