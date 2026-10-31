# PBS RRD discovery fixtures

These fixtures are **synthetic**, generated deterministically by
[`_make_fixtures.py`](_make_fixtures.py). Their response shape (field names,
step cadence, sparse-row encoding, row counts per timeframe) matches a real
PBS 4.2 RRD response, but every byte count and timestamp window is fictional
(10 TiB total, 200 GiB ZFS-reservation gap, `used` grows linearly 30 %→70 %
across each timeframe's dense window).

Anchor: `NEWEST_TS = 1793036805` (also hard-coded as `_NEWEST_FIXTURE_TS` in
[`tests/test_pbs_backfill.py`](../../../test_pbs_backfill.py)).

Regenerate with: `python tests/e2e/pbs/rrd/_make_fixtures.py`

These power the unit tests for the storage-history backfill feature.

## Captured endpoints

| File | Endpoint | Notes |
|---|---|---|
| `rrd_hour.json`    | `/admin/datastore/ssd/rrd?timeframe=hour&cf=AVERAGE`    | 60s step, ~1h window |
| `rrd_day.json`     | `/admin/datastore/ssd/rrd?timeframe=day&cf=AVERAGE`     | 60s step, ~24h window |
| `rrd_week.json`    | `/admin/datastore/ssd/rrd?timeframe=week&cf=AVERAGE`    | 1800s step, ~7d window |
| `rrd_month.json`   | `/admin/datastore/ssd/rrd?timeframe=month&cf=AVERAGE`   | 1800s step, ~30d window |
| `rrd_year.json`    | `/admin/datastore/ssd/rrd?timeframe=year&cf=AVERAGE`    | 21600s step, ~1yr window |
| `rrd_decade.json`  | `/admin/datastore/ssd/rrd?timeframe=decade&cf=AVERAGE`  | 604800s step, ~10yr window |

## Confirmed facts

- **Row shape**: `{"time": <epoch>, "used": <bytes>, "total": <bytes>, "available": <bytes>}`.
  Field is `available`, **not** `avail` as in `/status`.
- **Missing data = missing keys**, not `null`. Rows with no data contain only `time`:
  `{"time": 1792950405}`. Must be dropped by the parser.
- **Step/coverage per timeframe** (unique time deltas confirmed via actual data):
  60s/hour, 60s/day, 1800s/week, 1800s/month, 21600s/year, 604800s/decade.
- **ACL**: HTTP 200 on all six timeframes with the current token — no 403.

## Tier mapping for backfill (age-based placement)

The six PBS timeframes feed the project's four storage-history tiers:

| PBS timeframe | Step | Fits tier |
|---|---|---|
| hour, day | 60s | `raw` (no snapping; 300s min-interval guard skipped for backfill) |
| week, month | 1800s | `hourly` (3600s bucket → 2 points median per bucket) |
| year | 21600s | `sixhour` (exact bucket match) |
| decade | 604800s | `daily` (sparse weekly points) |

The actual target tier for a given point is still **age-based** (compared to
each tier's retention window), not fixed by timeframe — this is important
because `raw` only holds 7d of data, so day-timeframe points older than 7d
would belong in `hourly`.

## Secret-safety

Captured responses contain no tokens, hostnames, or user names. RRD responses
have no identifiers.
