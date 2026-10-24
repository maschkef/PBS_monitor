"""Tests for the minimal PBS calendar-event parser.

A fixed ``now`` makes every case deterministic — no monkeypatching of the
system clock. Unparseable input must return ``None`` without raising so the
PBS adapter can degrade gracefully.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from alerting.sources.pbs_calendar import pbs_calendar_to_next_utc


def _utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=UTC)


class TestKeywordForms:
    def test_hourly_rounds_up_to_next_full_hour(self):
        now = _utc(2026, 3, 10, 14, 23, 47)
        assert pbs_calendar_to_next_utc("hourly", now=now) == _utc(
            2026, 3, 10, 15, 0, 0
        )

    def test_hourly_at_exact_hour_still_moves_to_next(self):
        now = _utc(2026, 3, 10, 14, 0, 0)
        assert pbs_calendar_to_next_utc("hourly", now=now) == _utc(
            2026, 3, 10, 15, 0, 0
        )

    def test_daily_returns_next_midnight(self):
        now = _utc(2026, 3, 10, 14, 23, 0)
        assert pbs_calendar_to_next_utc("daily", now=now) == _utc(
            2026, 3, 11, 0, 0, 0
        )

    def test_weekly_from_wednesday_jumps_to_next_monday(self):
        # 2026-03-11 is a Wednesday.
        now = _utc(2026, 3, 11, 10, 0, 0)
        assert pbs_calendar_to_next_utc("weekly", now=now) == _utc(
            2026, 3, 16, 0, 0, 0
        )

    def test_weekly_from_monday_jumps_a_full_week(self):
        # 2026-03-09 is a Monday.
        now = _utc(2026, 3, 9, 10, 0, 0)
        assert pbs_calendar_to_next_utc("weekly", now=now) == _utc(
            2026, 3, 16, 0, 0, 0
        )

    def test_monthly_mid_month(self):
        now = _utc(2026, 3, 15, 10, 0, 0)
        assert pbs_calendar_to_next_utc("monthly", now=now) == _utc(
            2026, 4, 1, 0, 0, 0
        )

    def test_monthly_december_rolls_year(self):
        now = _utc(2026, 12, 20, 10, 0, 0)
        assert pbs_calendar_to_next_utc("monthly", now=now) == _utc(
            2027, 1, 1, 0, 0, 0
        )


class TestWildcardDateTimeForms:
    def test_hms_before_time_today(self):
        now = _utc(2026, 3, 10, 3, 10, 0)
        assert pbs_calendar_to_next_utc(
            "*-*-* 03:14:00", now=now
        ) == _utc(2026, 3, 10, 3, 14, 0)

    def test_hms_after_time_tomorrow(self):
        now = _utc(2026, 3, 10, 4, 0, 0)
        assert pbs_calendar_to_next_utc(
            "*-*-* 03:14:00", now=now
        ) == _utc(2026, 3, 11, 3, 14, 0)

    def test_hm_without_seconds_accepted(self):
        now = _utc(2026, 3, 10, 2, 0, 0)
        assert pbs_calendar_to_next_utc(
            "*-*-* 03:14", now=now
        ) == _utc(2026, 3, 10, 3, 14, 0)

    def test_exact_match_moves_to_tomorrow(self):
        # Candidate at `now` should NOT fire now; roll one day.
        now = _utc(2026, 3, 10, 3, 14, 0)
        assert pbs_calendar_to_next_utc(
            "*-*-* 03:14:00", now=now
        ) == _utc(2026, 3, 11, 3, 14, 0)


class TestWeekdayForms:
    def test_single_weekday_on_different_day(self):
        # 2026-03-13 is a Friday.
        now = _utc(2026, 3, 13, 10, 0, 0)
        assert pbs_calendar_to_next_utc(
            "Sat *-*-* 04:00", now=now
        ) == _utc(2026, 3, 14, 4, 0, 0)

    def test_single_weekday_same_day_before_time(self):
        # 2026-03-14 is a Saturday, before 04:00.
        now = _utc(2026, 3, 14, 2, 0, 0)
        assert pbs_calendar_to_next_utc(
            "Sat *-*-* 04:00", now=now
        ) == _utc(2026, 3, 14, 4, 0, 0)

    def test_single_weekday_same_day_after_time(self):
        # 2026-03-14 is a Saturday, after 04:00.
        now = _utc(2026, 3, 14, 10, 0, 0)
        assert pbs_calendar_to_next_utc(
            "Sat *-*-* 04:00", now=now
        ) == _utc(2026, 3, 21, 4, 0, 0)

    def test_multi_weekday_picks_nearest(self):
        # Tuesday 2026-03-10; next Mon or Fri → Friday 2026-03-13.
        now = _utc(2026, 3, 10, 10, 0, 0)
        assert pbs_calendar_to_next_utc(
            "Mon,Fri *-*-* 00:00", now=now
        ) == _utc(2026, 3, 13, 0, 0, 0)


class TestUnparseable:
    @pytest.mark.parametrize(
        "spec",
        [
            None, "", "   ", "nonsense", "Sat 04:00",
            "*-*-*", "*-*-* 25:00", "*-*-* 03:70",
            "Xyz *-*-* 04:00", "daily daily",
            "2026-03-10 03:14:00",  # concrete date not supported
        ],
    )
    def test_returns_none(self, spec):
        now = _utc(2026, 3, 10, 10, 0, 0)
        assert pbs_calendar_to_next_utc(spec, now=now) is None

    def test_non_string_input_returns_none(self):
        assert pbs_calendar_to_next_utc(1234) is None  # type: ignore[arg-type]


class TestTimezoneHandling:
    def test_naive_now_treated_as_utc(self):
        naive = datetime(2026, 3, 10, 14, 0, 0)  # noqa: DTZ001
        result = pbs_calendar_to_next_utc("hourly", now=naive)
        assert result == _utc(2026, 3, 10, 15, 0, 0)

    def test_default_now_returns_future_timestamp(self):
        result = pbs_calendar_to_next_utc("daily")
        assert result is not None
        assert result > datetime.now(UTC)
