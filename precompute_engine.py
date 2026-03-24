"""
Precompute Engine - Runs daily at 1 AM IST (7:30 PM UTC)
Computes all dashboard metrics and stores them in:
  1. Firestore (permanent backup)
  2. Redis/Upstash (ultra-fast cache, 8 hour TTL)
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from database.firebase import db
from cache import set_cached_data

CACHE_TTL_HOURS = 8  # Cache lasts 8 hours (refreshed every night at 1 AM)

def log(msg: str):
    print(f"[precompute] {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - {msg}")

def format_duration(seconds: float) -> str:
    hours = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    if hours > 0:
        return f"{hours}h {mins}m"
    return f"{mins}m"

def save_to_firestore(collection: str, doc_id: str, data: dict):
    """Save precomputed data to Firestore"""
    try:
        data["_computed_at"] = datetime.utcnow().isoformat()
        db.collection(collection).document(doc_id).set(data)
        log(f"Saved to Firestore: {collection}/{doc_id}")
    except Exception as e:
        log(f"Firestore write error {collection}/{doc_id}: {e}")

def save_to_redis(key: str, data: dict):
    """Save precomputed data to Redis with 8-hour TTL"""
    try:
        result = set_cached_data(key, data, ttl_minutes=CACHE_TTL_HOURS * 60)
        if result:
            log(f"Cached in Redis: {key}")
        else:
            log(f"Redis unavailable for key: {key}")
    except Exception as e:
        log(f"Redis write error {key}: {e}")


# ─────────────────────────────────────────────────────────────
# STEP 1: Fetch all raw data once (avoid repeated scans)
# ─────────────────────────────────────────────────────────────
def fetch_all_raw_data(days: int = 30) -> Dict[str, Any]:
    """Fetch raw screentime data once for all computations"""
    log(f"Fetching raw data for last {days} days...")

    today_dt = datetime.utcnow()
    cutoff_str = (today_dt - timedelta(days=days)).strftime("%Y-%m-%d")
    today_str = today_dt.strftime("%Y-%m-%d")

    docs = db.collection_group("dates").stream()

    # Aggregation structures
    by_date: Dict[str, Dict] = {}          # date -> { userIds, sessions, screentime }
    by_user: Dict[str, Dict] = {}          # userId -> { firstSeen, lastSeen, dates, apps }
    app_totals: Dict[str, Dict] = {}       # appName -> { totalScreentime, sessions, category }
    category_totals: Dict[str, Dict] = {}  # category -> { totalScreentime, sessions }
    hourly_data: Dict[int, Dict] = {h: {"users": set(), "sessions": 0} for h in range(24)}

    for doc in docs:
        date_str = doc.id
        user_id = doc.reference.parent.parent.id
        data = doc.to_dict() or {}
        apps = data.get("apps", [])
        if isinstance(apps, dict):
            apps = list(apps.values())

        # Track user first/last seen
        if user_id not in by_user:
            by_user[user_id] = {
                "firstSeen": date_str,
                "lastSeen": date_str,
                "allDates": set(),
                "recentDates": set(),
                "totalScreentime": 0,
                "totalSessions": 0,
                "recentScreentime": 0,
                "recentSessions": 0,
                "recentApps": set()
            }
        u = by_user[user_id]
        if date_str < u["firstSeen"]:
            u["firstSeen"] = date_str
        if date_str > u["lastSeen"]:
            u["lastSeen"] = date_str
        u["allDates"].add(date_str)

        is_recent = date_str >= cutoff_str
        if is_recent:
            u["recentDates"].add(date_str)

        # Daily aggregation
        if date_str not in by_date:
            by_date[date_str] = {"userIds": set(), "sessions": 0, "screentime": 0}
        by_date[date_str]["userIds"].add(user_id)

        for app in apps:
            app_name = app.get("appName", "Unknown")
            category = app.get("category", "Other")
            sc = app.get("totalScreenTime", 0)
            sess = app.get("sessionCount", 0)
            last_used = app.get("lastUsedTime")

            u["totalScreentime"] += sc
            u["totalSessions"] += sess
            by_date[date_str]["sessions"] += sess
            by_date[date_str]["screentime"] += sc

            if is_recent:
                u["recentScreentime"] += sc
                u["recentSessions"] += sess
                u["recentApps"].add(app_name)

                if app_name not in app_totals:
                    app_totals[app_name] = {"totalScreentime": 0, "sessions": 0, "category": category}
                app_totals[app_name]["totalScreentime"] += sc
                app_totals[app_name]["sessions"] += sess

                if category not in category_totals:
                    category_totals[category] = {"totalScreentime": 0, "sessions": 0}
                category_totals[category]["totalScreentime"] += sc
                category_totals[category]["sessions"] += sess

                # Hourly data from lastUsedTime
                try:
                    if isinstance(last_used, (int, float)):
                        hr = datetime.utcfromtimestamp(last_used / 1000.0).hour
                        hourly_data[hr]["users"].add(user_id)
                        hourly_data[hr]["sessions"] += sess
                except Exception:
                    pass

    log(f"Fetched {len(by_user)} users, {len(by_date)} date documents")
    return {
        "by_date": by_date,
        "by_user": by_user,
        "app_totals": app_totals,
        "category_totals": category_totals,
        "hourly_data": hourly_data,
        "cutoff_str": cutoff_str,
        "today_str": today_str,
        "today_dt": today_dt
    }


# ─────────────────────────────────────────────────────────────
# STEP 2: Compute Daily Metrics (DAU, WAU, MAU, Stickiness)
# ─────────────────────────────────────────────────────────────
def compute_daily_metrics(raw: Dict) -> Dict:
    log("Computing daily metrics (DAU/WAU/MAU/Stickiness)...")
    by_date = raw["by_date"]
    today_str = raw["today_str"]
    today_dt = raw["today_dt"]
    cutoff_str = raw["cutoff_str"]

    recent_dates = sorted([d for d in by_date.keys() if d >= cutoff_str])
    
    if not recent_dates:
        return {}

    dau_values = [len(by_date[d]["userIds"]) for d in recent_dates]
    dau_avg = int(sum(dau_values) / len(dau_values)) if dau_values else 0
    dau_today = len(by_date.get(today_str, {}).get("userIds", set()))

    # WAU (last 7 days)
    wau_cutoff = (today_dt - timedelta(days=7)).strftime("%Y-%m-%d")
    wau_users = set()
    for d in recent_dates:
        if d >= wau_cutoff:
            wau_users.update(by_date[d]["userIds"])
    wau = len(wau_users)

    # MAU (last 30 days)
    mau_users = set()
    for d in recent_dates:
        mau_users.update(by_date[d]["userIds"])
    mau = len(mau_users)

    stickiness = round((dau_avg / mau * 100), 1) if mau > 0 else 0.0

    # DAU trend (last 30 days)
    dau_trend = []
    for d in recent_dates:
        dau_trend.append({"date": d, "dau": len(by_date[d]["userIds"])})

    result = {
        "dau": dau_today,
        "dau_avg_30d": dau_avg,
        "wau": wau,
        "mau": mau,
        "stickiness": stickiness,
        "dau_trend": dau_trend,
        "date": today_str,
        "period_days": len(recent_dates)
    }

    save_to_firestore("analytics_precomputed", f"dailyMetrics_{today_str}", result)
    save_to_redis("dashboard:daily_metrics", result)
    return result


# ─────────────────────────────────────────────────────────────
# STEP 3: Compute Top Apps
# ─────────────────────────────────────────────────────────────
def compute_top_apps(raw: Dict) -> Dict:
    log("Computing top apps...")
    app_totals = raw["app_totals"]
    today_str = raw["today_str"]

    if not app_totals:
        return {}

    ranked = sorted(app_totals.items(), key=lambda x: x[1]["totalScreentime"], reverse=True)
    top_apps_list = []
    for rank, (app_name, stats) in enumerate(ranked[:20], 1):
        top_apps_list.append({
            "rank": rank,
            "app_name": app_name,
            "category": stats["category"],
            "total_screentime_seconds": int(stats["totalScreentime"] / 1000),
            "total_screentime_formatted": format_duration(stats["totalScreentime"] / 1000),
            "total_sessions": stats["sessions"]
        })

    result = {
        "date": today_str,
        "top_apps": top_apps_list,
        "total_apps_tracked": len(app_totals)
    }

    save_to_firestore("analytics_precomputed", f"topApps_{today_str}", result)
    save_to_redis("dashboard:top_apps_30d", result)
    return result


# ─────────────────────────────────────────────────────────────
# STEP 4: Compute User Segments
# ─────────────────────────────────────────────────────────────
def compute_user_segments(raw: Dict) -> Dict:
    log("Computing user segments...")
    by_user = raw["by_user"]
    today_str = raw["today_str"]
    today_dt = raw["today_dt"]

    power = regular = casual = at_risk = 0
    power_list = at_risk_list = []
    power_list, at_risk_list = [], []

    for user_id, u in by_user.items():
        if not u["recentDates"]:
            continue

        days_active = len(u["recentDates"])
        avg_sc_sec = (u["recentScreentime"] / 1000) / days_active if days_active > 0 else 0

        if avg_sc_sec > 4 * 3600:
            power += 1
        elif avg_sc_sec > 1 * 3600:
            regular += 1
        elif avg_sc_sec > 0:
            casual += 1

        # At-risk: had activity before but inactive last 7 days
        last_seen_dt = datetime.strptime(u["lastSeen"], "%Y-%m-%d")
        days_inactive = (today_dt - last_seen_dt).days
        if 7 < days_inactive <= 30:
            at_risk += 1

    total = power + regular + casual
    result = {
        "date": today_str,
        "power_users": power,
        "regular_users": regular,
        "casual_users": casual,
        "at_risk_users": at_risk,
        "total_active_users": total,
        "power_percent": round(power / total * 100, 1) if total > 0 else 0,
        "regular_percent": round(regular / total * 100, 1) if total > 0 else 0,
        "casual_percent": round(casual / total * 100, 1) if total > 0 else 0,
    }

    save_to_firestore("analytics_precomputed", f"userSegments_{today_str}", result)
    save_to_redis("dashboard:user_segments", result)
    return result


# ─────────────────────────────────────────────────────────────
# STEP 5: Compute Dashboard Summary (The Main Fast-Read Doc)
# ─────────────────────────────────────────────────────────────
def compute_dashboard_summary(daily: Dict, top_apps: Dict, segments: Dict, raw: Dict) -> Dict:
    log("Computing dashboard_summary...")
    app_totals = raw["app_totals"]
    category_totals = raw["category_totals"]
    by_date = raw["by_date"]
    cutoff_str = raw["cutoff_str"]
    today_str = raw["today_str"]

    recent_dates = [d for d in by_date.keys() if d >= cutoff_str]
    total_sessions = sum(by_date[d]["sessions"] for d in recent_dates)
    total_screentime = sum(by_date[d]["screentime"] for d in recent_dates)
    total_days = len(recent_dates) or 1

    avg_session_sec = int(total_screentime / 1000 / total_sessions) if total_sessions > 0 else 0

    top_category = "N/A"
    if category_totals:
        top_category = max(category_totals.items(), key=lambda x: x[1]["totalScreentime"])[0]

    top_app = top_apps.get("top_apps", [{}])[0].get("app_name", "N/A") if top_apps.get("top_apps") else "N/A"

    result = {
        "generated_at": datetime.utcnow().isoformat(),
        "date": today_str,
        "period": "last_30_days",

        # Core metrics
        "dau": daily.get("dau", 0),
        "dau_avg_30d": daily.get("dau_avg_30d", 0),
        "wau": daily.get("wau", 0),
        "mau": daily.get("mau", 0),
        "stickiness": daily.get("stickiness", 0),
        "dau_trend": daily.get("dau_trend", []),

        # Sessions
        "total_sessions_30d": total_sessions,
        "avg_session_seconds": avg_session_sec,
        "avg_session_formatted": format_duration(avg_session_sec),

        # Top content
        "top_app": top_app,
        "top_category": top_category,
        "top_apps": top_apps.get("top_apps", [])[:10],

        # Segments
        "power_users": segments.get("power_users", 0),
        "regular_users": segments.get("regular_users", 0),
        "casual_users": segments.get("casual_users", 0),
        "at_risk_users": segments.get("at_risk_users", 0),
    }

    save_to_firestore("analytics_precomputed", "dashboard_summary", result)
    save_to_redis("dashboard:summary", result)
    log("Dashboard summary saved!")
    return result


# ─────────────────────────────────────────────────────────────
# MAIN RUNNER - Called by Scheduler at 1 AM
# ─────────────────────────────────────────────────────────────
def run_full_precompute():
    log("=" * 50)
    log("Starting full precompute job...")
    start = datetime.utcnow()

    try:
        # Fetch data once
        raw = fetch_all_raw_data(days=30)

        # Run all computations using the shared raw data
        daily = compute_daily_metrics(raw)
        top_apps = compute_top_apps(raw)
        segments = compute_user_segments(raw)
        summary = compute_dashboard_summary(daily, top_apps, segments, raw)

        elapsed = (datetime.utcnow() - start).total_seconds()
        log(f"Precompute COMPLETE in {elapsed:.1f}s")
        log("=" * 50)

        return {
            "status": "success",
            "elapsed_seconds": elapsed,
            "computed_at": start.isoformat()
        }

    except Exception as e:
        import traceback
        log(f"PRECOMPUTE FAILED: {e}")
        traceback.print_exc()
        return {"status": "error", "error": str(e)}
