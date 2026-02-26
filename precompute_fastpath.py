import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List

from routers.dashboard import getUserSegments, getCohortRetention, getWellbeingReport
from database.firebase import db, init_error


def _resolve_recent_dates(days: int) -> List[str]:
    ist_offset = timedelta(hours=5, minutes=30)
    today_ist = (datetime.now(timezone.utc) + ist_offset).date()
    start_date = today_ist - timedelta(days=max(1, days) - 1)
    return [
        (start_date + timedelta(days=idx)).strftime("%Y-%m-%d")
        for idx in range(max(1, days))
    ]


def _parse_weeks(raw_weeks: str) -> List[int]:
    values: List[int] = []
    for token in (raw_weeks or "").split(","):
        part = token.strip()
        if not part:
            continue
        try:
            parsed = int(part)
            if parsed > 0:
                values.append(parsed)
        except ValueError:
            pass

    if not values:
        values = [8]
    return sorted(set(values))


async def _warm_preaggregates(days: int, weeks_values: List[int]):
    dates_to_warm = _resolve_recent_dates(days)
    print(f"[warmup] days={days}, dates={len(dates_to_warm)}, weeks={weeks_values}")

    for date_str in dates_to_warm:
        print(f"[warmup] user_segments date={date_str}")
        await getUserSegments(date_str)

        print(f"[warmup] wellbeing_report date={date_str}")
        await getWellbeingReport(date_str)

    for weeks in weeks_values:
        print(f"[warmup] cohort_retention weeks={weeks}")
        await getCohortRetention(weeks)

    print("[warmup] completed")


def main():
    parser = argparse.ArgumentParser(
        description="Precompute fast-path snapshots for dashboard analytics endpoints."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of recent IST dates to warm for user segments + wellbeing (default: 7).",
    )
    parser.add_argument(
        "--weeks",
        type=str,
        default="8",
        help="Comma-separated cohort retention windows (default: 8). Example: 8,12,16",
    )
    args = parser.parse_args()

    if not db:
        raise RuntimeError(f"Database connection not initialized. Error: {init_error}")

    weeks_values = _parse_weeks(args.weeks)
    asyncio.run(_warm_preaggregates(days=max(1, args.days), weeks_values=weeks_values))


if __name__ == "__main__":
    main()
