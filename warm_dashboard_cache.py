import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List

from database.firebase import db, init_error
from routers.dashboard import (
    build_user_activity_index,
    get_all_users,
    get_user_status,
    get_screentime,
    get_sessions,
    get_analytics,
    get_summary,
    get_dau_trend,
    get_stickiness,
    get_new_vs_returning,
    getGrowthFunnel,
    get_session_summary,
    getPeakUsageHeatmap,
    getSessionDurationDistribution,
    getDailyUsagePatterns,
    getTopApps,
    getUserSegments,
    getCohortRetention,
    getWellbeingReport,
)


def _resolve_ist_today() -> str:
    ist_offset = timedelta(hours=5, minutes=30)
    return (datetime.now(timezone.utc) + ist_offset).strftime("%Y-%m-%d")


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


async def _warm_all(days: int, weeks_list: List[int], top_limit: int):
    today_str = _resolve_ist_today()
    print(
        "[warm-dashboard] "
        f"today={today_str}, days={days}, weeks={weeks_list}, top_limit={top_limit}"
    )

    try:
        index_stats = build_user_activity_index()
        print(
            "[warm-dashboard] user activity index "
            f"users={index_stats.get('indexed_users', 0)}, "
            f"docs_scanned={index_stats.get('docs_scanned', 0)}"
        )
    except Exception as index_err:
        print(f"[warm-dashboard] user activity index build failed: {index_err}")

    # Core summary endpoints
    await get_summary()
    await get_analytics()
    await get_dau_trend(days)
    await get_stickiness("30d")
    await get_new_vs_returning(days)
    await getGrowthFunnel(days)
    await get_session_summary(today_str)

    # Heavy list endpoints
    await get_all_users()
    await get_user_status()
    await get_screentime(user_id=None, date=today_str)
    await get_sessions(user_id=None, date=today_str)

    # Advanced analytics endpoints
    await getPeakUsageHeatmap(days)
    await getSessionDurationDistribution(days=days, start_date=None, end_date=None)
    await getDailyUsagePatterns(days)
    await getTopApps(days=days, limit=top_limit, category="all")
    await getUserSegments(today_str)
    await getWellbeingReport(today_str)

    for weeks in weeks_list:
        await getCohortRetention(weeks)

    print("[warm-dashboard] completed")


def main():
    parser = argparse.ArgumentParser(description="Warm cache for all dashboard APIs.")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Default lookback days for day-based analytics endpoints (default: 30).",
    )
    parser.add_argument(
        "--weeks",
        type=str,
        default="8",
        help="Comma-separated cohort weeks to warm (default: 8). Example: 8,12,16",
    )
    parser.add_argument(
        "--top-limit",
        type=int,
        default=10,
        help="Top apps limit used for getTopApps warmup (default: 10).",
    )
    args = parser.parse_args()

    if not db:
        raise RuntimeError(f"Database connection not initialized. Error: {init_error}")

    days = max(1, args.days)
    top_limit = max(1, min(200, args.top_limit))
    weeks_list = _parse_weeks(args.weeks)
    asyncio.run(_warm_all(days=days, weeks_list=weeks_list, top_limit=top_limit))


if __name__ == "__main__":
    main()
