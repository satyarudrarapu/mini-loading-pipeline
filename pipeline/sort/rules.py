from __future__ import annotations

import re
from typing import List

from pydantic import BaseModel, field_validator

class SortRule(BaseModel):
    
    pattern: str
    table: str
    date_group: int = 1

    @field_validator("pattern")
    @classmethod
    def pattern_must_compile(cls, v: str) -> str:
        """Reject patterns that do not compile at config load time (N2)."""
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f"Invalid regex pattern {v!r}: {exc}") from exc
        return v

    def compiled(self) -> re.Pattern:
        """Return the compiled regex (cached by Python's re module)."""
        return re.compile(self.pattern)


class MatchResult(BaseModel):

    table: str
    data_date: str   

    @field_validator("data_date")
    @classmethod
    def must_be_iso_date(cls, v: str) -> str:
        """Validate the date string is YYYY-MM-DD (N7 — UTC dates only)."""
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError(f"data_date must be YYYY-MM-DD, got: {v!r}")
        return v


class SortConfig(BaseModel):
    rules: List[SortRule]

class NoRuleMatchError(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(f"No sort rule matched key: {key!r}")
        self.key = key

SORT_RULES = SortConfig(
    rules=[
        SortRule(
            pattern=r"^raw/usgs/all_day/(\d{4}-\d{2}-\d{2})/\d{6}\.geojson$",
            table="usgs.earthquakes",
            date_group=1,
        ),
        SortRule(
            pattern=r"^raw/usgs/history/(\d{4}-\d{2}-\d{2})\.geojson$",
            table="usgs.earthquakes",
            date_group=1,
        ),
    ]
)

def match(key: str) -> MatchResult:

    for rule in SORT_RULES.rules:
        m = rule.compiled().match(key)
        if m:
            data_date = m.group(rule.date_group)
            return MatchResult(table=rule.table, data_date=data_date)

    raise NoRuleMatchError(key)
