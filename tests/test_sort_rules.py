from __future__ import annotations

import pytest

from pipeline.sort.rules import (
    NoRuleMatchError,
    MatchResult,
    SortRule,
    match,
)

class TestAllDayKeys:
    """Tests for real all_day feed keys from S3."""

    def test_match_returns_correct_table(self):
        key = "raw/usgs/all_day/2026-11-10/143000.geojson"
        result = match(key)
        assert result.table == "usgs.earthquakes"

    def test_match_returns_correct_data_date(self):
        key = "raw/usgs/all_day/2026-11-10/143000.geojson"
        result = match(key)
        assert result.data_date == "2026-11-10"

    def test_match_returns_match_result_type(self):
        key = "raw/usgs/all_day/2026-11-10/143000.geojson"
        result = match(key)
        assert isinstance(result, MatchResult)

    def test_different_date_is_captured_correctly(self):
        key = "raw/usgs/all_day/2026-11-01/000000.geojson"
        result = match(key)
        assert result.data_date == "2026-11-01"

    def test_different_time_in_key_does_not_affect_date(self):
        key = "raw/usgs/all_day/2026-11-10/235959.geojson"
        result = match(key)
        assert result.data_date == "2026-11-10"

    def test_midnight_time(self):
        key = "raw/usgs/all_day/2026-11-10/000000.geojson"
        result = match(key)
        assert result.data_date == "2026-11-10"


class TestHistoryKeys:
    """Tests for real history backfill keys from S3."""

    def test_history_match_returns_correct_table(self):
        key = "raw/usgs/history/2026-11-01.geojson"
        result = match(key)
        assert result.table == "usgs.earthquakes"

    def test_history_match_returns_correct_data_date(self):
        key = "raw/usgs/history/2026-11-01.geojson"
        result = match(key)
        assert result.data_date == "2026-11-01"

    def test_history_different_date(self):
        key = "raw/usgs/history/2026-11-07.geojson"
        result = match(key)
        assert result.data_date == "2026-11-07"

    def test_history_start_of_year(self):
        key = "raw/usgs/history/2026-01-01.geojson"
        result = match(key)
        assert result.data_date == "2026-01-01"


class TestUnmatchedKeys:
    """
    Every unmatched key must raise NoRuleMatchError.
    The sort worker catches this and does NOT delete the SQS message,
    causing it to go to the DLQ after 5 receives (F5).
    """

    def test_unknown_prefix_raises(self):
        with pytest.raises(NoRuleMatchError):
            match("raw/unknown/x.txt")

    def test_sorted_prefix_raises(self):
        """Sorted objects must not be re-sorted."""
        with pytest.raises(NoRuleMatchError):
            match("sorted/usgs.earthquakes/2026-11-10/143000.geojson")

    def test_empty_key_raises(self):
        with pytest.raises(NoRuleMatchError):
            match("")

    def test_partial_all_day_key_raises(self):
        """Key must have both date and time components."""
        with pytest.raises(NoRuleMatchError):
            match("raw/usgs/all_day/2026-11-10")

    def test_all_day_wrong_extension_raises(self):
        with pytest.raises(NoRuleMatchError):
            match("raw/usgs/all_day/2026-11-10/143000.json")

    def test_history_wrong_extension_raises(self):
        with pytest.raises(NoRuleMatchError):
            match("raw/usgs/history/2026-11-01.json")

    def test_no_rule_error_contains_key(self):
        """NoRuleMatchError message should include the offending key."""
        key = "raw/unknown/x.txt"
        with pytest.raises(NoRuleMatchError) as exc_info:
            match(key)
        assert key in str(exc_info.value)


class TestUtcDates:
    """
    data_date must always be a valid YYYY-MM-DD string.
    MatchResult.must_be_iso_date validates this at construction time.
    """

    def test_all_day_data_date_is_valid_iso(self):
        import re
        result = match("raw/usgs/all_day/2026-11-10/143000.geojson")
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", result.data_date)

    def test_history_data_date_is_valid_iso(self):
        import re
        result = match("raw/usgs/history/2026-11-01.geojson")
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", result.data_date)


class TestSortRuleValidator:
    """
    SortRule.pattern_must_compile should raise at config load time
    if a pattern is invalid — prevents silent misconfiguration (N2).
    """

    def test_invalid_regex_raises_at_construction(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            SortRule(
                pattern="[invalid(regex",
                table="test.table",
                date_group=1,
            )

    def test_valid_pattern_accepted(self):
        rule = SortRule(
            pattern=r"^raw/usgs/all_day/(\d{4}-\d{2}-\d{2})/\d{6}\.geojson$",
            table="usgs.earthquakes",
            date_group=1,
        )
        assert rule.table == "usgs.earthquakes"
