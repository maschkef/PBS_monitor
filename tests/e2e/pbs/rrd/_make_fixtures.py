"""Regenerate the six PBS RRD fixtures with synthetic, deterministic data.

Run: ``python tests/e2e/pbs/rrd/_make_fixtures.py``

The output files share the exact wire-shape of a real PBS 4.2 RRD response
(row counts, step cadence, sparse-row encoding, ``used + available ≠ total``
reservation gap) but every byte count and timestamp window is fictional.
Timestamps anchor on ``NEWEST_TS`` which is hard-coded as
``_NEWEST_FIXTURE_TS`` in ``tests/test_pbs_backfill.py``.
"""
from __future__ import annotations

import json
from pathlib import Path

NEWEST_TS = 1793036805  # must match tests/test_pbs_backfill.py::_NEWEST_FIXTURE_TS

TOTAL_BYTES = float(10 * 1024**4)       # 10 TiB
RESERVATION = float(200 * 1024**3)      # 200 GiB — produces used+available != total

USED_MIN_FRAC = 0.30
USED_MAX_FRAC = 0.70

# (name, step_seconds, total_rows, dense_count)
TIMEFRAMES: list[tuple[str, int, int, int]] = [
    ("hour",   60,     61,   61),
    ("day",    60,     1440, 1439),
    ("week",   1800,   337,  337),
    ("month",  1800,   1440, 1439),
    ("year",   21600,  1440, 112),
    ("decade", 604800, 523,  5),
]


def _dense_values(dense_index: int, dense_count: int) -> tuple[float, float]:
    """Return (used, available) for a dense row.

    `used` grows linearly from USED_MIN_FRAC*TOTAL to USED_MAX_FRAC*TOTAL
    across the dense window; `available` keeps the 200 GiB reservation gap
    so used+available = total - RESERVATION, mirroring real PBS/ZFS output.
    """
    span = max(1, dense_count - 1)
    frac = USED_MIN_FRAC + (USED_MAX_FRAC - USED_MIN_FRAC) * dense_index / span
    used = float(round(TOTAL_BYTES * frac))
    available = TOTAL_BYTES - RESERVATION - used
    return used, available


def _rows_for(step: int, total_rows: int, dense_count: int) -> list[dict]:
    """Build a timeframe's row list: sparse prefix, then dense tail."""
    sparse_count = total_rows - dense_count
    rows: list[dict] = []
    for i in range(total_rows):
        ts = NEWEST_TS - (total_rows - 1 - i) * step
        if i < sparse_count:
            rows.append({"time": ts})
        else:
            dense_index = i - sparse_count
            used, available = _dense_values(dense_index, dense_count)
            rows.append({
                "available": available,
                "time": ts,
                "total": TOTAL_BYTES,
                "used": used,
            })
    return rows


def main() -> None:
    out_dir = Path(__file__).parent
    for name, step, total_rows, dense_count in TIMEFRAMES:
        rows = _rows_for(step, total_rows, dense_count)
        payload = {"data": rows}
        path = out_dir / f"rrd_{name}.json"
        with path.open("w") as fp:
            json.dump(payload, fp)
        print(f"wrote {path.name}  rows={total_rows}  dense={dense_count}")


if __name__ == "__main__":
    main()
