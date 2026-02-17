from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import Optional, List, Dict, Any, Tuple
from database.firebase import db, init_error
from datetime import datetime, timedelta, timezone
from firebase_admin import firestore
from google.cloud.firestore_v1.field_path import FieldPath
from middleware.auth import verify_api_key
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

try:
    from cache import get_cached_data, set_cached_data
except Exception as cache_import_error:
    print(f"[cache] import unavailable: {cache_import_error}")

    def get_cached_data(_key: str):
        return None

    def set_cached_data(_key: str, _data: Any, _ttl_minutes: int = 5) -> bool:
        return False

router = APIRouter(
    prefix="/api/dashboard",
    tags=["Dashboard"],
    dependencies=[Depends(verify_api_key)]
)

_DASHBOARD_CACHE_TTL_MIN = {
    "users": 10,
    "users_status": 5,
    "screentime": 3,
    "sessions": 3,
    "analytics": 5,
    "summary": 5,
    "dau_trend": 10,
    "stickiness": 10,
    "new_vs_returning": 10,
    "session_summary": 5,
    "peak_usage_heatmap": 10,
    "session_duration_distribution": 10,
    "daily_usage_patterns": 10,
}


def _normalize_cache_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_normalize_cache_value(v) for v in value]
    if isinstance(value, dict):
        return {
            str(k): _normalize_cache_value(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    return str(value)


def _build_cache_key(endpoint_name: str, params: Optional[Dict[str, Any]] = None) -> str:
    normalized_params = _normalize_cache_value(params or {})
    params_json = json.dumps(normalized_params, sort_keys=True, separators=(",", ":"))
    return f"dashboard:{endpoint_name}:{params_json}"


def _cache_get(cache_key: str):
    try:
        cached = get_cached_data(cache_key)
        if cached is not None:
            print(f"[cache] HIT {cache_key}")
        return cached
    except Exception as cache_err:
        print(f"[cache] read error key={cache_key}: {cache_err}")
        return None


def _cache_set(cache_key: str, payload: Any, ttl_minutes: int):
    try:
        cached_ok = set_cached_data(cache_key, payload, ttl_minutes=ttl_minutes)
        if cached_ok:
            print(f"[cache] SET {cache_key} ttl={ttl_minutes}m")
    except Exception as cache_err:
        print(f"[cache] write error key={cache_key}: {cache_err}")

def process_user_stats(doc) -> Dict[str, Any]:
    """Helper function to process a single user's stats for /users endpoint"""
    try:
        user_data = doc.to_dict()
        user_id = doc.id
        user_data["id"] = user_id

        # Aggregate statistics from screentime/{userId}/dates
        dates_ref = db.collection("screentime").document(user_id).collection("dates")
        date_docs = dates_ref.stream()

        total_screentime = 0
        total_sessions = 0
        active_days = 0
        all_apps_set = set()

        for date_doc in date_docs:
            data = date_doc.to_dict()
            active_days += 1
            
            apps_data = data.get("apps", [])
            apps_list = []
            
            if isinstance(apps_data, list):
                apps_list = apps_data
            elif isinstance(apps_data, dict):
                apps_list = list(apps_data.values())
            
            if isinstance(apps_list, list):
                for app in apps_list:
                    if not isinstance(app, dict): continue
                    
                    st_raw = app.get("totalScreenTime", 0)
                    sess_raw = app.get("sessionCount", 0)
                    name = app.get("appName")
                    
                    try:
                        st = float(str(st_raw).replace(',', ''))
                        total_screentime += st
                    except (ValueError, TypeError):
                        pass
                    
                    try:
                        sess = float(str(sess_raw).replace(',', ''))
                        total_sessions += sess
                    except (ValueError, TypeError):
                        pass
                        
                    if name:
                        all_apps_set.add(name)
        
        avg_session_duration = 0
        if total_sessions > 0:
            avg_session_duration = total_screentime / total_sessions

        user_data["totalScreentime"] = total_screentime
        user_data["totalSessions"] = total_sessions
        user_data["averageSessionDuration"] = round(avg_session_duration, 2)
        user_data["activeDays"] = active_days
        user_data["allAppsUsed"] = list(all_apps_set)
        
        return user_data
    except Exception as e:
        print(f"Error processing user {doc.id}: {e}")
        # Return partial data or re-raise? 
        # Better to return basic data so one failure doesn't crash the whole request
        return {
            "id": doc.id,
            "error": str(e),
            **doc.to_dict()
        }

@router.get("/users", response_model=Dict[str, Any])
async def get_all_users():
    """
    Fetch all users from the users collection with aggregated usage statistics.
    Optimized with ThreadPoolExecutor for concurrent subcollection fetching.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("users")
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        users_ref = db.collection("users")
        # specific to Python 3.7+ used in run_command tool?
        # stream() returns a generator, we need list to iterate or pass to executor
        # but generator cannot be pickled? No, stream yields DocumentSnapshots which are fine.
        # It's safer to materialize the list of user docs first.
        docs = list(users_ref.stream())

        users = []
        
        # Use ThreadPoolExecutor for parallel processing
        # Max workers: 10-20 is usually a sweet spot for network I/O
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_user = {executor.submit(process_user_stats, doc): doc for doc in docs}
            
            for future in as_completed(future_to_user):
                try:
                    user_data = future.result()
                    users.append(user_data)
                except Exception as exc:
                    print(f"User processing generated an exception: {exc}")

        response_payload = {
            "success": True,
            "data": users,
            "count": len(users)
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["users"])
        return response_payload
    except Exception as e:
        print(f"❌ Error fetching users: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/users/status", response_model=Dict[str, Any])
async def get_user_status():
    """
    Fetch all users and determine their active status.
    Active: Synced screentime data in the last 5 days.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("users_status")
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        users_ref = db.collection("users")
        docs = users_ref.stream() # Keep sequential as it's lighter (limit(1) query)
        # But actually, limit(1) query is still a query. 147 of them sequentially is slow.
        # Let's optimize this too if we can, but main focus is users/analytics.
        # For now, keep as is to minimize regression risk, unless user asked for it. 
        # User asked for /analytics timeout primarily.

        users_status = []
        today = datetime.now()
        five_days_ago = (today - timedelta(days=5)).strftime("%Y-%m-%d")

        for doc in docs:
            user_data = doc.to_dict()
            user_id = doc.id
            user_data["id"] = user_id
            
            screentime_dates_ref = db.collection("screentime").document(user_id).collection("dates")
            start_date_doc_ref = screentime_dates_ref.document(five_days_ago)

            recent_activity = screentime_dates_ref.where(
                "__name__", ">=", start_date_doc_ref
            ).limit(1).stream()

            is_active = False
            for _ in recent_activity:
                is_active = True
                break
            
            status_str = "Active" if is_active else "Inactive"
            user_data["status"] = status_str
            users_status.append(user_data)

        response_payload = {
            "success": True,
            "data": users_status,
            "count": len(users_status)
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["users_status"])
        return response_payload
    except Exception as e:
        print(f"❌ Error fetching user status: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/screentime")
async def get_screentime(
    user_id: Optional[str] = Query(None, alias="userId", description="Filter by specific user ID"),
    date: Optional[str] = Query(None, description="Filter by specific date (YYYY-MM-DD)")
):
    """
    Fetch screentime data from nested Firestore collections.
    Structure: screentime/{userId}/dates/{date}
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database connection not initialized")

        cache_key = _build_cache_key("screentime", {"userId": user_id, "date": date})
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        screentime_data = []
        
        if user_id:
            user_ref = db.collection("screentime").document(user_id).collection("dates")
            
            if date:
                doc_ref = user_ref.document(date)
                doc = doc_ref.get()
                if doc.exists:
                    data = doc.to_dict()
                    data["userId"] = user_id
                    data["date"] = date
                    screentime_data.append(data)
            else:
                docs = user_ref.stream()
                for doc in docs:
                    data = doc.to_dict()
                    data["userId"] = user_id
                    data["date"] = doc.id
                    screentime_data.append(data)
        else:
            users_ref = db.collection("screentime")
            users_snapshot = users_ref.stream()
            for user_doc in users_snapshot:
                current_user_id = user_doc.id
                dates_ref = users_ref.document(current_user_id).collection("dates")
                
                if date:
                    doc_ref = dates_ref.document(date)
                    doc = doc_ref.get()
                    if doc.exists:
                        data = doc.to_dict()
                        data["userId"] = current_user_id
                        data["date"] = date
                        screentime_data.append(data)
                else:
                    date_docs = dates_ref.stream()
                    for d_doc in date_docs:
                        data = d_doc.to_dict()
                        data["userId"] = current_user_id
                        data["date"] = d_doc.id
                        screentime_data.append(data)

        response_payload = {
            "success": True,
            "data": screentime_data,
            "count": len(screentime_data),
            "filters": {
                "userId": user_id or "all",
                "date": date or "all"
            }
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["screentime"])
        return response_payload

    except Exception as e:
        print(f"❌ Error fetching screentime: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/sessions")
async def get_sessions(
    user_id: Optional[str] = Query(None, alias="userId", description="Filter by specific user ID"),
    date: Optional[str] = Query(None, description="Filter by specific date (YYYY-MM-DD)")
):
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database connection not initialized")

        cache_key = _build_cache_key("sessions", {"userId": user_id, "date": date})
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        sessions_data = []

        if user_id:
            user_ref = db.collection("sessions").document(user_id).collection("dates")
            
            if date:
                doc_ref = user_ref.document(date)
                doc = doc_ref.get()
                if doc.exists:
                    data = doc.to_dict()
                    data["userId"] = user_id
                    data["date"] = date
                    sessions_data.append(data)
            else:
                docs = user_ref.stream()
                for doc in docs:
                    data = doc.to_dict()
                    data["userId"] = user_id
                    data["date"] = doc.id
                    sessions_data.append(data)
        else:
            users_ref = db.collection("sessions")
            users_snapshot = users_ref.stream()
            for user_doc in users_snapshot:
                current_user_id = user_doc.id
                dates_ref = users_ref.document(current_user_id).collection("dates")
                
                if date:
                    doc_ref = dates_ref.document(date)
                    doc = doc_ref.get()
                    if doc.exists:
                        data = doc.to_dict()
                        data["userId"] = current_user_id
                        data["date"] = date
                        sessions_data.append(data)
                else:
                    date_docs = dates_ref.stream()
                    for d_doc in date_docs:
                        data = d_doc.to_dict()
                        data["userId"] = current_user_id
                        data["date"] = d_doc.id
                        sessions_data.append(data)

        response_payload = {
            "success": True,
            "data": sessions_data,
            "count": len(sessions_data),
            "filters": {
                "userId": user_id or "all",
                "date": date or "all"
            }
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["sessions"])
        return response_payload

    except Exception as e:
        print(f"❌ Error fetching sessions: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

def process_user_analytics(doc, today_str, week_ago_str, month_ago_str):
    """
    Helper function to process a single user's analytics.
    Returns a dictionary with stats for this user contributed to global totals.
    """
    # Initialize per-user counters
    user_stats = {
        "users_today": 0, "users_week": 0, "users_month": 0, "user_has_data": False,
        "total_sessions_all": 0, "total_screentime_all": 0,
        "total_sessions_today": 0, "total_screentime_today": 0,
        "total_sessions_week": 0, "total_screentime_week": 0,
        "total_sessions_month": 0, "total_screentime_month": 0,
        "new_users_today": 0,
        "synced_sessions_today": 0, "synced_screentime_today": 0
    }

    try:
        user_id = doc.id
        
        # Define Dates in Local Time (IST +5:30)
        now_utc = datetime.now(timezone.utc)
        ist_offset = timedelta(hours=5, minutes=30)
        today_ist_start = (now_utc + ist_offset).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Consistent Date Windows (Inclusive Rolling)
        week_ist_start = today_ist_start - timedelta(days=6)
        month_ist_start = today_ist_start - timedelta(days=29)

        # Check if user is NEW (Created Today)
        # Check if user is NEW and update Active flags based on creation date
        user_created_utc = doc.create_time
        new_user_today = False
        new_user_week = False
        new_user_month = False
        
        if user_created_utc:
            user_created_ist = user_created_utc + ist_offset
            if user_created_ist >= today_ist_start:
                user_stats["new_users_today"] = 1
                new_user_today = True
            if user_created_ist >= week_ist_start:
                new_user_week = True
            if user_created_ist >= month_ist_start:
                new_user_month = True

        # 1. Fetch Screentime Data
        st_ref = db.collection("screentime").document(user_id).collection("dates")
        st_docs = st_ref.stream()
        
        # 2. Fetch Sessions Data (Correct Source)
        sess_ref = db.collection("sessions").document(user_id).collection("dates")
        sess_docs = sess_ref.stream()
        
        # 3. Merge Data by Date
        date_map = {}
        
        # Process Screentime
        for doc in st_docs:
            date_id = doc.id
            data = doc.to_dict()
            
            apps_data = data.get("apps", [])
            apps_list = []
            if isinstance(apps_data, list):
                apps_list = apps_data
            elif isinstance(apps_data, dict):
                apps_list = list(apps_data.values())
                
            date_st = 0
            for app in apps_list:
                if not isinstance(app, dict): continue
                st_raw = app.get("totalScreenTime", 0)
                try:
                    date_st += float(str(st_raw).replace(',', ''))
                except (ValueError, TypeError):
                    pass
            
            if date_id not in date_map:
                date_map[date_id] = {"st": 0, "sess": 0}
            date_map[date_id]["st"] = date_st

        # Process Sessions
        for doc in sess_docs:
            date_id = doc.id
            data = doc.to_dict()
            
            sessions_list = data.get("sessions", [])
            sess_count = 0
            if isinstance(sessions_list, list):
                sess_count = len(sessions_list)
            
            if date_id not in date_map:
                date_map[date_id] = {"st": 0, "sess": 0}
            date_map[date_id]["sess"] = sess_count
            
        # 4. Aggregate Stats
        user_active_today = False
        user_active_week = False
        user_active_month = False
        
        for date_id, metrics in date_map.items():
            date_st = metrics["st"]
            date_sess = metrics["sess"]
            
            # All Time
            user_stats["total_sessions_all"] += date_sess
            user_stats["total_screentime_all"] += date_st
            
            if date_sess > 0 or date_st > 0:
                user_stats["user_has_data"] = True
                
            # Today
            if date_id == today_str:
                user_stats["total_sessions_today"] += date_sess
                user_stats["total_screentime_today"] += date_st
                
                if date_sess > 0 or date_st > 0:
                    user_active_today = True
            
            # Week
            if date_id >= week_ago_str:
                user_stats["total_sessions_week"] += date_sess
                user_stats["total_screentime_week"] += date_st
                if date_sess > 0 or date_st > 0:
                    user_active_week = True
            
            # Month
            if date_id >= month_ago_str:
                user_stats["total_sessions_month"] += date_sess
                user_stats["total_screentime_month"] += date_st
                if date_sess > 0 or date_st > 0:
                    user_active_month = True
        
        # Consistent Active User Logic: Data OR New User
        if new_user_today:
            user_active_today = True
        if new_user_week:
            user_active_week = True
        if new_user_month:
            user_active_month = True

        if user_active_today:
            user_stats["users_today"] = 1
        if user_active_week:
            user_stats["users_week"] = 1
        if user_active_month:
            user_stats["users_month"] = 1
            
        return user_stats

    except Exception as e:
        print(f"Error processing analytics for user {doc.id}: {e}")
        return user_stats # Return empty stats on error

@router.get("/analytics", response_model=Dict[str, Any])
async def get_analytics():
    """
    Get comprehensive analytics for the dashboard.
    Optimized with ThreadPoolExecutor.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("analytics")
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        # Calculate date ranges
        # Use simple date string comparison for efficiency.
        now = datetime.now()
        today_str = now.strftime("%Y-%m-%d")
        
        # "Today" is inclusive.
        # Week = Today + 6 past days (Total 7 days).
        week_ago_str = (now - timedelta(days=6)).strftime("%Y-%m-%d")
        
        # Month = Today + 29 past days (Total 30 days).
        month_ago_str = (now - timedelta(days=29)).strftime("%Y-%m-%d")
        
        # Initialize Global Counters
        total_users = 0
        total_sessions_all = 0
        total_screentime_all = 0
        
        total_sessions_today = 0
        total_screentime_today = 0
        users_today = 0
        
        total_sessions_week = 0
        total_screentime_week = 0
        users_week = 0
        
        total_sessions_month = 0
        total_screentime_month = 0
        users_month = 0
        
        new_users_today = 0
        
        users_all_time_active_count = 0

        # Fetch ALL users
        users_ref = db.collection("users")
        docs = list(users_ref.stream())
        total_users = len(docs)
        
        # Parallel Execution
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Pass read-only date strings to avoid datetime sharing issues
            future_to_user = {
                executor.submit(process_user_analytics, doc, today_str, week_ago_str, month_ago_str): doc 
                for doc in docs
            }
            
            for future in as_completed(future_to_user):
                try:
                    stats = future.result()
                    
                    # Aggregation
                    total_sessions_all += stats["total_sessions_all"]
                    total_screentime_all += stats["total_screentime_all"]
                    if stats["user_has_data"]:
                        users_all_time_active_count += 1
                        
                    total_sessions_today += stats["total_sessions_today"]
                    total_screentime_today += stats["total_screentime_today"]
                    users_today += stats["users_today"]
                    
                    total_sessions_week += stats["total_sessions_week"]
                    total_screentime_week += stats["total_screentime_week"]
                    users_week += stats["users_week"]
                    
                    total_sessions_month += stats["total_sessions_month"]
                    total_screentime_month += stats["total_screentime_month"]
                    users_month += stats["users_month"]
                    
                    new_users_today += stats["new_users_today"]
                    
                except Exception as exc:
                    print(f"Analytics processing exception: {exc}")

        # Convert milliseconds to hours
        hours_all = total_screentime_all / (1000 * 60 * 60) if total_screentime_all > 0 else 0
        hours_today = total_screentime_today / (1000 * 60 * 60) if total_screentime_today > 0 else 0
        hours_week = total_screentime_week / (1000 * 60 * 60) if total_screentime_week > 0 else 0
        hours_month = total_screentime_month / (1000 * 60 * 60) if total_screentime_month > 0 else 0
        hours_month = total_screentime_month / (1000 * 60 * 60) if total_screentime_month > 0 else 0
        
        response_payload = {
            "success": True,
            "data": {
                "allTime": {
                    "totalSessions": total_sessions_all,
                    "totalUsers": total_users,  # ALL users in database
                    "activeUsers": users_all_time_active_count, # Added for completeness
                    "totalScreentime": round(total_screentime_all, 2),  # milliseconds
                    "activeHours": round(hours_all, 2)
                },
                "today": {
                    "totalSessions": total_sessions_today,
                    "activeUsers": users_today,
                    "activeHours": round(hours_today, 2),
                    "newUsers": new_users_today
                },
                "lastWeek": {
                    "totalSessions": total_sessions_week,
                    "activeUsers": users_week,
                    "activeHours": round(hours_week, 2)
                },
                "lastMonth": {
                    "totalSessions": total_sessions_month,
                    "activeUsers": users_month,
                    "activeHours": round(hours_month, 2)
                }
            },
            "metadata": {
                "today": today_str,
                "generatedAt": now.isoformat()
            }
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["analytics"])
        return response_payload
    
    except Exception as e:
        print(f"❌ Error fetching analytics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/summary", response_model=Dict[str, Any])
async def get_summary():
    """
    Get summary statistics for the dashboard including:
    - Total users and growth
    - DAU (Daily Active Users) with change and percentage
    - WAU (Weekly Active Users) with change and percentage
    - MAU (Monthly Active Users) with change and percentage
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("summary")
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        # Define Dates in IST timezone
        now_utc = datetime.now(timezone.utc)
        ist_offset = timedelta(hours=5, minutes=30)
        now_ist = now_utc + ist_offset
        
        # Current periods
        today_str = now_ist.strftime("%Y-%m-%d")
        week_start_str = (now_ist - timedelta(days=6)).strftime("%Y-%m-%d")
        month_start_str = (now_ist - timedelta(days=29)).strftime("%Y-%m-%d")
        
        # Previous periods for comparison
        yesterday_str = (now_ist - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_week_start_str = (now_ist - timedelta(days=13)).strftime("%Y-%m-%d")
        prev_week_end_str = (now_ist - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_month_start_str = (now_ist - timedelta(days=59)).strftime("%Y-%m-%d")
        prev_month_end_str = (now_ist - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Get all users
        users_ref = db.collection("users")
        all_users = list(users_ref.stream())
        current_total_users = len(all_users)
        
        # Calculate previous total users (users created before today)
        previous_total_users = 0
        for user_doc in all_users:
            user_created_utc = user_doc.create_time
            if user_created_utc:
                user_created_ist = user_created_utc + ist_offset
                if user_created_ist.strftime("%Y-%m-%d") < today_str:
                    previous_total_users += 1
        
        # Calculate total users growth
        total_users_growth = 0
        if previous_total_users > 0:
            total_users_growth = ((current_total_users - previous_total_users) / previous_total_users) * 100
        
        # Initialize counters
        dau_today = 0
        dau_yesterday = 0
        wau_current = 0
        wau_previous = 0
        mau_current = 0
        mau_previous = 0
        
        # Process each user
        for user_doc in all_users:
            user_id = user_doc.id
            
            # Get screentime data
            st_ref = db.collection("screentime").document(user_id).collection("dates")
            st_docs = list(st_ref.stream())
            
            # Create a set of dates with activity
            active_dates = set()
            for date_doc in st_docs:
                date_id = date_doc.id
                data = date_doc.to_dict()
                
                # Check if there's actual data
                apps_data = data.get("apps", [])
                if apps_data:
                    active_dates.add(date_id)
            
            # DAU - Today
            if today_str in active_dates:
                dau_today += 1
            
            # DAU - Yesterday
            if yesterday_str in active_dates:
                dau_yesterday += 1
            
            # WAU - Current week (last 7 days)
            has_current_week_activity = False
            for date_id in active_dates:
                if week_start_str <= date_id <= today_str:
                    has_current_week_activity = True
                    break
            if has_current_week_activity:
                wau_current += 1
            
            # WAU - Previous week (7 days before current week)
            has_previous_week_activity = False
            for date_id in active_dates:
                if prev_week_start_str <= date_id <= prev_week_end_str:
                    has_previous_week_activity = True
                    break
            if has_previous_week_activity:
                wau_previous += 1
            
            # MAU - Current month (last 30 days)
            has_current_month_activity = False
            for date_id in active_dates:
                if month_start_str <= date_id <= today_str:
                    has_current_month_activity = True
                    break
            if has_current_month_activity:
                mau_current += 1
            
            # MAU - Previous month (30 days before current month)
            has_previous_month_activity = False
            for date_id in active_dates:
                if prev_month_start_str <= date_id <= prev_month_end_str:
                    has_previous_month_activity = True
                    break
            if has_previous_month_activity:
                mau_previous += 1
        
        # Calculate changes
        dau_change = dau_today - dau_yesterday
        dau_change_percent = 0
        if dau_yesterday > 0:
            dau_change_percent = ((dau_today - dau_yesterday) / dau_yesterday) * 100
        
        wau_change = wau_current - wau_previous
        wau_change_percent = 0
        if wau_previous > 0:
            wau_change_percent = ((wau_current - wau_previous) / wau_previous) * 100
        
        mau_change = mau_current - mau_previous
        mau_change_percent = 0
        if mau_previous > 0:
            mau_change_percent = ((mau_current - mau_previous) / mau_previous) * 100
        
        response_payload = {
            "success": True,
            "data": {
                "total_users": current_total_users,
                "total_users_growth": round(total_users_growth, 2),
                "dau": dau_today,
                "dau_change": dau_change,
                "dau_change_percent": round(dau_change_percent, 2),
                "wau": wau_current,
                "wau_change": wau_change,
                "wau_change_percent": round(wau_change_percent, 2),
                "mau": mau_current,
                "mau_change": mau_change,
                "mau_change_percent": round(mau_change_percent, 2),
                "last_updated": now_ist.isoformat()
            }
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["summary"])
        return response_payload
    
    except Exception as e:
        print(f"❌ Error fetching summary: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/dau-trend", response_model=Dict[str, Any])
async def get_dau_trend(
    days: int = Query(30, description="Number of days to look back (default 30)")
):
    """
    Calculate Daily Active Users (DAU) trend.
    DAU = count of unique userId values that have a document for that date.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("dau_trend", {"days": days})
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        # Define Dates in IST timezone
        now_utc = datetime.now(timezone.utc)
        ist_offset = timedelta(hours=5, minutes=30)
        now_ist = now_utc + ist_offset
        today = now_ist.date()
        
        # Calculate date range
        end_date = today
        start_date = end_date - timedelta(days=days-1) # Inclusive of today, so days-1
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # Initialize storage for daily unique users
        # Map: date_string -> set of user_ids
        daily_users: Dict[str, set] = {}
        
        # Pre-populate dates to ensure all days are present even if 0 DAU
        # And to filter later
        target_dates = []
        for i in range(days):
            d = start_date + timedelta(days=i)
            d_str = d.strftime("%Y-%m-%d")
            daily_users[d_str] = set()
            target_dates.append(d_str)

        # Iterate all users to get their activity
        # Note: This might be slow for very large user bases. 
        # For scalability, we'd want a collection group query or pre-aggregated stats.
        # Given the requirements and current structure, we iterate users -> dates.
        users_ref = db.collection("users")
        # Use simple stream, maybe limit fields if possible but we need IDs
        user_docs = users_ref.stream()
        
        # Use ThreadPoolExecutor for concurrency if needed, but let's try sequential first 
        # or reuse the pattern from other endpoints if performance is a concern.
        # For aggregation, simpler is often better to start.
        
        # Let's collect all date docs relevant to the period
        # Optimization: Use Collection Group Query?
        # db.collection_group("dates") matches ALL collections named "dates".
        # Structure is screentime/{userId}/dates/{date}
        # If there are other "dates" collections, this might be risky.
        # But efficiently: db.collection_group("dates").where("__name__", ">=", start_date_str).stream()
        # This would give us ALL date docs from ALL users in one go.
        # The parent of the date doc is the user's "dates" collection. 
        # The parent of that is the user doc? No, screentime/{userId}. 
        # So date_doc.reference.parent.parent.id would be the userId.
        
        # SAFE APPROACH described in Plan: Iterate users -> subcollections
        # But to be faster, let's try the Collection Group query if we can filter by path? 
        # Firestore doesn't easily filter by path in queries.
        
        # Let's stick to the reliable User iteration for now, matching `get_summary` logic.
        
        all_users = list(user_docs)
        
        for user_doc in all_users:
            user_id = user_doc.id
            
            # Reference to screentime dates
            dates_ref = db.collection("screentime").document(user_id).collection("dates")
            
            # Query only dates within range
            # We can use document ID range queries on __name__
            # This is very efficient.
            start_date_doc = dates_ref.document(start_date_str)
            end_date_doc = dates_ref.document(end_date_str)
            query = dates_ref.where("__name__", ">=", start_date_doc).where("__name__", "<=", end_date_doc)
            
            # Projection: We only need the document ID (the date), no data.
            # .select([]) extracts only the ID and creation/update time metadata, minimal bandwidth.
            date_docs = query.select([]).stream()
            
            for date_doc in date_docs:
                date_str = date_doc.id
                if date_str in daily_users:
                    daily_users[date_str].add(user_id)
        
        # Construct the response data
        trend_data = []
        previous_dau = 0
        
        # Summary stats tracking
        total_dau_sum = 0
        peak_dau = -1
        peak_day_obj = None
        lowest_dau = float('inf')
        lowest_day_obj = None
        
        # Iterate in chronological order
        for date_str in target_dates:
            user_set = daily_users[date_str]
            current_dau = len(user_set)
            
            # Date object for formatting
            d_obj = datetime.strptime(date_str, "%Y-%m-%d")
            
            # Day of week (Python: Monday, Tuesday...)
            day_of_week = d_obj.strftime("%A")
            
            # Is weekend? (Saturday=5, Sunday=6)
            is_weekend = d_obj.weekday() >= 5
            
            # Change stats
            change_from_prev = None
            change_percent = None
            
            # We can calculate change for all except the very first day in the list
            # Note: The very first day of the range has no "previous" in the range. 
            # If we wanted strict change for the first day, we'd need to fetch day-1.
            # Requirement says: "For each date in the last 30 days... This count = DAU"
            # And "change_from_previous: Current day DAU - Previous day DAU"
            # It implies using the previous day *in the series* or actual previous day?
            # Usually in these charts, the first point has null change.
            if date_str != start_date_str:
                change_from_prev = current_dau - previous_dau
                if previous_dau > 0:
                    change_percent = (change_from_prev / previous_dau) * 100
                else:
                    change_percent = 0 if current_dau == 0 else 100.0 # 0 -> X is 100% growth effectively or infinite
            
            day_data = {
                "date": date_str,
                "dau": current_dau,
                "day_of_week": day_of_week,
                "change_from_previous": change_from_prev,
                "change_percent": round(change_percent, 2) if change_percent is not None else None,
                "is_weekend": is_weekend
            }
            trend_data.append(day_data)
            
            # Summary updates
            total_dau_sum += current_dau
            
            if current_dau > peak_dau:
                peak_dau = current_dau
                peak_day_obj = {"date": date_str, "dau": current_dau, "day_of_week": day_of_week}
            
            if current_dau < lowest_dau:
                lowest_dau = current_dau
                lowest_day_obj = {"date": date_str, "dau": current_dau, "day_of_week": day_of_week}
                
            previous_dau = current_dau

        # Final Summary
        avg_dau = total_dau_sum / days if days > 0 else 0
        
        response_payload = {
            "period": {
                "start": start_date_str,
                "end": end_date_str,
                "total_days": days
            },
            "data": trend_data,
            "summary": {
                "avg_dau": round(avg_dau, 2),
                "peak_day": peak_day_obj,
                "lowest_day": lowest_day_obj
            }
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["dau_trend"])
        return response_payload

    except Exception as e:
        print(f"❌ Error fetching DAU trend: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/stickiness", response_model=Dict[str, Any])
async def get_stickiness(
    period: str = Query("30d", description="Period for historical trend (7d, 30d, 90d)")
):
    """
    Calculate Stickiness Ratio (DAU/MAU * 100).
    - DAU: Daily Active Users (Unique users with a date doc today)
    - MAU: Monthly Active Users (Unique users with ANY date doc in last 30 days)
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("stickiness", {"period": period})
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        # Parse period
        period_days = 30
        if period == "7d": period_days = 7
        elif period == "90d": period_days = 90
        
        # Define Dates (IST)
        now_utc = datetime.now(timezone.utc)
        ist_offset = timedelta(hours=5, minutes=30)
        now_ist = now_utc + ist_offset
        today = now_ist.date()
        today_str = today.strftime("%Y-%m-%d")
        
        # Historical dates needs
        # We need historical comparison points:
        # - 7 days ago
        # - 30 days ago
        # - Array of points based on period
        
        # Calculate maximum lookback needed
        # To calculate MAU for a date D, we need data from D-30 to D.
        # Max historical point is today - period_days.
        # Max lookback from there is (today - period_days) - 30.
        # So overall window start: today - period_days - 30.
        
        # Actually, let's just fetch ALL dates for users and process in memory.
        # It's cleaner given we iterate users anyway.
        
        users_ref = db.collection("users")
        user_docs = users_ref.stream()
        
        # Map: date_string -> set of user_ids
        daily_users: Dict[str, set] = {}
        all_user_ids = set()
        
        # Iterate users
        for user_doc in user_docs:
            user_id = user_doc.id
            all_user_ids.add(user_id)
            
            dates_ref = db.collection("screentime").document(user_id).collection("dates")
            
            # Fetch ALL dates. 
            # Optimization: We could limit to dates >= (today - period_days - 60) 
            # to cover 30d ago trend MAU window (30+30=60).
            # Max lookback: 90d period -> 90 + 30 = 120 days.
            lookback_days = period_days + 60 # Safe buffer for trends
            min_date = today - timedelta(days=lookback_days)
            min_date_doc = dates_ref.document(min_date.strftime("%Y-%m-%d"))
            
            try:
                # Use document ID range query
                date_docs = dates_ref.where("__name__", ">=", min_date_doc).select([]).stream()
                
                for d in date_docs:
                    d_str = d.id
                    if d_str not in daily_users:
                        daily_users[d_str] = set()
                    daily_users[d_str].add(user_id)
            except Exception as e:
                print(f"Error fetching dates for user {user_id}: {e}")
                continue

        # Helper to calculate Stickiness for a specific date
        def calculate_stickiness_for_date(target_date_str):
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
            
            # DAU: Users active ON target_date
            dau_set = daily_users.get(target_date_str, set())
            dau_count = len(dau_set)
            
            # MAU: Users active in [target_date - 29 days, target_date] (30 days total)
            mau_set = set()
            start_mau = target_date - timedelta(days=29)
            
            # Iterate dates in range to collect unique users
            # Optimization: iterate 30 days
            for i in range(30):
                d = start_mau + timedelta(days=i)
                d_str = d.strftime("%Y-%m-%d")
                if d_str in daily_users:
                    mau_set.update(daily_users[d_str])
            
            mau_count = len(mau_set)
            
            ratio = 0.0
            if mau_count > 0:
                ratio = (dau_count / mau_count) * 100
                
            return {
                "date": target_date_str,
                "dau": dau_count,
                "mau": mau_count,
                "ratio": round(ratio, 1)
            }

        # 1. Current Stickiness
        current_stats = calculate_stickiness_for_date(today_str)
        current_ratio = current_stats["ratio"]
        
        # 2. Trend: 7 days ago
        date_7d_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        stats_7d = calculate_stickiness_for_date(date_7d_ago)
        trend_7d = round(current_ratio - stats_7d["ratio"], 1)
        
        # 3. Trend: 30 days ago
        date_30d_ago = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        stats_30d = calculate_stickiness_for_date(date_30d_ago)
        trend_30d = round(current_ratio - stats_30d["ratio"], 1)
        
        # 4. Historical Data
        historical_data = []
        
        # Determine interval based on period
        step = 1 # Daily
        if period == "30d": step = 7 # Weekly
        if period == "90d": step = 7 # Weekly
        
        # Generate points
        # For 30d/90d: every 7th day backwards from today
        # For 7d: every day matches requirements?
        # Req: "For 30d period: return ~4-5 data points (weekly intervals)"
        
        dates_to_plot = []
        if period == "7d":
            for i in range(7):
                d = today - timedelta(days=6-i) # Last 7 days including today
                dates_to_plot.append(d.strftime("%Y-%m-%d"))
        else:
            # Weekly snapshots
            # Start from today and go back 'period_days' with step 7
            # Actually, charts usually go Left->Right (Old->New)
            # So start from (today - period) up to today?
            # Or simplified: User said "return weekly snapshots (every 7th day)"
            # Let's do: today, today-7, today-14... then reverse
            curr = today
            min_date = today - timedelta(days=period_days)
            while curr > min_date:
                dates_to_plot.append(curr.strftime("%Y-%m-%d"))
                curr = curr - timedelta(days=7)
            dates_to_plot.reverse() # Ascending order
            
        for d_str in dates_to_plot:
            stats = calculate_stickiness_for_date(d_str)
            historical_data.append({
                "date": d_str,
                "ratio": stats["ratio"]
            })
            
        # 5. Status
        status_str = "low_engagement"
        if current_ratio >= 20: status_str = "high_engagement"
        elif current_ratio >= 10: status_str = "moderate_engagement"
        
        response_payload = {
            "current_ratio": current_ratio,
            "target_ratio": 20.0,
            "status": status_str,
            "trend_7d": trend_7d,
            "trend_30d": trend_30d,
            "historical_data": historical_data,
            "details": {
                "dau": current_stats["dau"],
                "mau": current_stats["mau"],
                "calculation": f"{current_ratio}% = ({current_stats['dau']:,} / {current_stats['mau']:,}) × 100",
                "period": period,
                "generated_at": now_ist.isoformat()
            }
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["stickiness"])
        return response_payload

    except Exception as e:
        print(f"❌ Error fetching Stickiness: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/new-vs-returning", response_model=Dict[str, Any])
async def get_new_vs_returning(
    days: int = Query(7, description="Number of days to analyze (e.g. 7, 14, 30)")
):
    """
    Calculate New vs Returning Users breakdown.
    - NEW USER: First ever activity date is TODAY.
    - RETURNING USER: Active today, but first activity was BEFORE today.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("new_vs_returning", {"days": days})
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        # Define Dates (IST)
        now_utc = datetime.now(timezone.utc)
        ist_offset = timedelta(hours=5, minutes=30)
        now_ist = now_utc + ist_offset
        today = now_ist.date()
        today_str = today.strftime("%Y-%m-%d")
        
        # Calculate date range
        end_date = today
        start_date = end_date - timedelta(days=days-1) # Inclusive
        
        # Step 1: Build User History Map & Daily Activity
        # Efficiently query ALL dates using collection group query
        # We need to know the *first* date for every user.
        # Collection Group: "dates"
        print(f"Fetching all date docs for New/Returning analysis...")
        
        # Optimization: We check if we can select only empty fields to save bandwidth
        # We need: doc.id (date), parent.parent.id (userId)
        # Note: In some SDKs/backends parent access might need document fetch.
        # But usually doc ref is available from metadata.
        all_date_docs = db.collection_group("dates").select([]).stream()
        
        user_first_seen: Dict[str, str] = {}
        daily_active_users: Dict[str, set] = {}
        
        for doc in all_date_docs:
            date_str = doc.id
            # Parent of 'dates' collection is the user document
            # Path: screentime/{userId}/dates/{date}
            # doc.reference.parent = collection "dates"
            # doc.reference.parent.parent = document "{userId}"
            user_id = doc.reference.parent.parent.id
            
            # Track first seen
            if user_id not in user_first_seen:
                user_first_seen[user_id] = date_str
            else:
                if date_str < user_first_seen[user_id]:
                    user_first_seen[user_id] = date_str
            
            # Track daily activity (only relevant for our query range, but building all is fine/fast for IDs)
            if date_str not in daily_active_users:
                daily_active_users[date_str] = set()
            daily_active_users[date_str].add(user_id)
            
        print(f"Processed {len(user_first_seen)} users history.")

        # Step 2: Calculate Daily Breakdown
        breakdown_data = []
        
        total_new_sum = 0
        total_returning_sum = 0
        total_new_percent_sum = 0.0
        days_with_data = 0
        
        # Iterate from start_date to end_date (inclusive)
        # Sort descending (newest first) as per example? 
        # Requirement example shows descending dates.
        
        target_dates = []
        for i in range(days):
            d = start_date + timedelta(days=i)
            target_dates.append(d)
        
        # Process in chronological order for trend calculation, then reverse for response?
        # Let's process chronological first.
        chronological_data = []
        
        for d in target_dates:
            d_str = d.strftime("%Y-%m-%d")
            day_of_week = d.strftime("%A")
            
            active_users = daily_active_users.get(d_str, set())
            total_active = len(active_users)
            
            new_count = 0
            returning_count = 0
            
            if total_active > 0:
                for uid in active_users:
                    first_date = user_first_seen.get(uid)
                    if first_date == d_str:
                        new_count += 1
                    elif first_date < d_str:
                        returning_count += 1
                    else:
                        # Should not happen if logic is correct (active today means first_seen <= today)
                        pass
            
            new_percent = 0.0
            returning_percent = 0.0
            
            if total_active > 0:
                new_percent = round((new_count / total_active) * 100, 1)
                returning_percent = round((returning_count / total_active) * 100, 1)
                # Ensure 100% total if needed, but rounding might make it 99.9 or 100.1. 
                # Let's trust the calc or force returning = 100 - new?
                # User req: "Ensure new_percent + returning_percent = 100.0"
                if new_percent + returning_percent != 100.0:
                    returning_percent = round(100.0 - new_percent, 1)
            
            # Accumulate for summary
            total_new_sum += new_count
            total_returning_sum += returning_count
            # Only count active days for average percentages? 
            # Or all days including 0? Usually all days in period.
            total_new_percent_sum += new_percent
            days_with_data += 1
            
            entry = {
                "date": d_str,
                "total_users": total_active,
                "new_users": new_count,
                "new_users_percent": new_percent,
                "returning_users": returning_count,
                "returning_users_percent": returning_percent,
                "day_of_week": day_of_week
            }
            chronological_data.append(entry)

        # Step 3: Summary & Trends
        avg_new_percent = 0.0
        if days_with_data > 0:
            avg_new_percent = round(total_new_percent_sum / days_with_data, 1)
        avg_returning_percent = round(100.0 - avg_new_percent, 1)
        
        # Trend: First half vs Second half
        start_idx = 0
        mid_idx = days // 2
        
        first_half = chronological_data[0:mid_idx]
        second_half = chronological_data[mid_idx:]
        
        avg_first = 0.0
        if first_half:
            avg_first = sum(x["new_users_percent"] for x in first_half) / len(first_half)
            
        avg_second = 0.0
        if second_half:
            avg_second = sum(x["new_users_percent"] for x in second_half) / len(second_half)
            
        trend = "stable"
        if avg_second > avg_first * 1.05: # 5% margin
            trend = "increasing"
        elif avg_second < avg_first * 0.95:
            trend = "decreasing"
            
        # Sort data descending (newest first)
        breakdown_data = sorted(chronological_data, key=lambda x: x["date"], reverse=True)
        
        response_payload = {
            "period": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d")
            },
            "data": breakdown_data,
            "summary": {
                "avg_new_percent": avg_new_percent,
                "avg_returning_percent": avg_returning_percent,
                "new_user_trend": trend,
                "total_new_users": total_new_sum,
                "total_returning_users": total_returning_sum
            }
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["new_vs_returning"])
        return response_payload

    except Exception as e:
        print(f"❌ Error fetching New vs Returning: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/session-summary")
async def get_session_summary(
    target_date: Optional[str] = Query(None, alias="date", description="Target date (YYYY-MM-DD), defaults to today")
):
    """
    Calculate session behavior summary metrics:
    1. Average sessions per user per day
    2. Average session duration
    3. Session length distribution
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database connection not initialized")

        # Define IST timezone offset
        ist_offset = timedelta(hours=5, minutes=30)
        now_ist = datetime.now(timezone.utc) + ist_offset
        
        if not target_date:
            target_date = now_ist.strftime("%Y-%m-%d")

        cache_key = _build_cache_key("session_summary", {"date": target_date})
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response
            
        # Parse target date and get yesterday
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        yesterday_str = (target_dt - timedelta(days=1)).strftime("%Y-%m-%d")

        def get_metrics_for_date(date_str):
            users_ref = db.collection("users")
            user_docs = list(users_ref.stream())
            
            total_users = 0
            total_sessions = 0
            total_screentime = 0
            session_durations = []

            def process_user_date(user_doc):
                user_id = user_doc.id
                
                # 1. Fetch Screentime
                st_ref = db.collection("screentime").document(user_id).collection("dates").document(date_str)
                st_doc = st_ref.get()
                
                # 2. Fetch Sessions for accurate count
                sess_ref = db.collection("sessions").document(user_id).collection("dates").document(date_str)
                sess_doc = sess_ref.get()
                
                user_sessions = 0
                user_screentime = 0
                
                if st_doc.exists:
                    st_data = st_doc.to_dict()
                    # Root screentime is reliable
                    user_screentime = float(str(st_data.get("totalScreenTime", 0)).replace(',', ''))
                    
                    # Try to get sessions from sessions collection first (more reliable)
                    if sess_doc.exists:
                        sess_data = sess_doc.to_dict()
                        sessions_list = sess_data.get("sessions", [])
                        if isinstance(sessions_list, list):
                            user_sessions = len(sessions_list)
                    
                    # Fallback to totalSessions field in ST doc if still 0
                    if user_sessions == 0:
                        user_sessions = float(str(st_data.get("totalSessions", 0)).replace(',', ''))
                        
                    # Final fallback: sum apps if both are somehow 0 but apps exist
                    if user_sessions == 0 or user_screentime == 0:
                        apps = st_data.get("apps", [])
                        if isinstance(apps, dict): apps = list(apps.values())
                        if isinstance(apps, list):
                            temp_sess = 0
                            temp_st = 0
                            for app in apps:
                                if not isinstance(app, dict): continue
                                try:
                                    temp_sess += float(str(app.get("sessionCount", 0)).replace(',', ''))
                                    temp_st += float(str(app.get("totalScreenTime", 0)).replace(',', ''))
                                except (ValueError, TypeError): pass
                            if user_sessions == 0: user_sessions = temp_sess
                            if user_screentime == 0: user_screentime = temp_st

                    if user_sessions > 0:
                        return {
                            "sessions": user_sessions,
                            "screentime": user_screentime,
                            "avg_duration": user_screentime / user_sessions
                        }
                return None

            with ThreadPoolExecutor(max_workers=20) as executor:
                results = list(executor.map(process_user_date, user_docs))

            for res in results:
                if res:
                    total_users += 1
                    total_sessions += res["sessions"]
                    total_screentime += res["screentime"]
                    session_durations.append(res["avg_duration"])

            if total_users == 0:
                return None

            avg_sessions_per_user = total_sessions / total_users
            avg_duration_ms = total_screentime / total_sessions
            avg_duration_seconds = avg_duration_ms / 1000

            # Distribution
            short, medium, long, power = 0, 0, 0, 0
            for d in session_durations:
                if d < 60000: short += 1
                elif d < 300000: medium += 1
                elif d < 900000: long += 1
                else: power += 1

            distribution = {
                "short_0_1min": round((short / total_users * 100), 1),
                "medium_1_5min": round((medium / total_users * 100), 1),
                "long_5_15min": round((long / total_users * 100), 1),
                "power_15plus": round((power / total_users * 100), 1)
            }

            return {
                "avg_sessions_per_user": avg_sessions_per_user,
                "avg_duration_seconds": avg_duration_seconds,
                "distribution": distribution,
                "total_sessions": total_sessions,
                "total_users": total_users
            }

        # Calculate for Today (Target)
        today_metrics = get_metrics_for_date(target_date)
        if not today_metrics:
            response_payload = {
                "success": True,
                "message": f"No data found for {target_date}",
                "date": target_date,
                "data": None
            }
            _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["session_summary"])
            return response_payload

        # Calculate for Yesterday
        yesterday_metrics = get_metrics_for_date(yesterday_str)

        # Calculate changes
        sessions_change = 0
        duration_change = 0
        if yesterday_metrics:
            sessions_change = today_metrics["avg_sessions_per_user"] - yesterday_metrics["avg_sessions_per_user"]
            duration_change = today_metrics["avg_duration_seconds"] - yesterday_metrics["avg_duration_seconds"]

        def format_duration(seconds):
            seconds = int(round(seconds))
            minutes = seconds // 60
            secs = seconds % 60
            return f"{minutes}m {secs}s"

        response_payload = {
            "date": target_date,
            "avg_sessions_per_user": round(today_metrics["avg_sessions_per_user"], 1),
            "avg_sessions_per_user_change": round(sessions_change, 2),
            "avg_session_duration_seconds": int(round(today_metrics["avg_duration_seconds"])),
            "avg_session_duration_formatted": format_duration(today_metrics["avg_duration_seconds"]),
            "avg_duration_change_seconds": int(round(duration_change)),
            "session_distribution": today_metrics["distribution"],
            "total_sessions_today": today_metrics["total_sessions"],
            "total_users_today": today_metrics["total_users"]
        }
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["session_summary"])
        return response_payload

    except Exception as e:
        print(f"ERROR: Error in get_session_summary: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


def _format_hour(hour: int) -> str:
    hour = hour % 24
    if hour == 0:
        return "12am"
    if hour == 12:
        return "12pm"
    if hour < 12:
        return f"{hour}am"
    return f"{hour - 12}pm"


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(float(str(value).replace(",", "")))
    except (ValueError, TypeError):
        return default


def _get_intensity(sessions: int, max_sessions: int) -> str:
    if max_sessions <= 0:
        return "low"

    percent = sessions / max_sessions
    if percent < 0.2:
        return "low"
    if percent < 0.5:
        return "medium"
    if percent < 0.8:
        return "high"
    return "peak"


def _build_intensity_thresholds(max_sessions: int) -> Dict[str, Dict[str, Optional[int]]]:
    if max_sessions <= 0:
        return {
            "low": {"min": 0, "max": 0},
            "medium": {"min": 1, "max": 0},
            "high": {"min": 1, "max": 0},
            "peak": {"min": 1, "max": None},
        }

    low_max = int(max_sessions * 0.2)
    medium_max = int(max_sessions * 0.5)
    high_max = int(max_sessions * 0.8)

    return {
        "low": {"min": 0, "max": low_max},
        "medium": {"min": low_max + 1, "max": medium_max},
        "high": {"min": medium_max + 1, "max": high_max},
        "peak": {"min": high_max + 1, "max": None},
    }


def _init_heatmap_matrix() -> Dict[int, Dict[int, int]]:
    return {
        day: {hour: 0 for hour in range(24)}
        for day in range(7)
    }


def _fetch_user_dates_in_range(user_id: str, start_date_str: str, end_date_str: str):
    """
    Fetch date docs for a single user from screentime/{userId}/dates in date-id range.
    Falls back to in-memory filtering per user if doc-id range query is unsupported.
    """
    user_dates_ref = db.collection("screentime").document(user_id).collection("dates")
    start_doc_ref = user_dates_ref.document(start_date_str)
    end_doc_ref = user_dates_ref.document(end_date_str)

    try:
        user_dates_query = (
            user_dates_ref
            .where(
                filter=firestore.FieldFilter(
                    "__name__", ">=", start_doc_ref
                )
            )
            .where(
                filter=firestore.FieldFilter(
                    "__name__", "<=", end_doc_ref
                )
            )
        )
    except TypeError:
        # Fallback for older Firestore clients
        user_dates_query = (
            user_dates_ref
            .where("__name__", ">=", start_doc_ref)
            .where("__name__", "<=", end_doc_ref)
        )

    try:
        return list(user_dates_query.stream())
    except Exception as range_query_err:
        print(
            "[getPeakUsageHeatmap] user-range query failed; using user-level in-memory "
            f"filter for user={user_id}, error={range_query_err}"
        )
        return [
            doc
            for doc in user_dates_ref.stream()
            if start_date_str <= doc.id <= end_date_str
        ]


def _aggregate_user_heatmap(
    user_id: str,
    start_date_str: str,
    end_date_str: str
) -> Tuple[Dict[int, Dict[int, int]], int, int]:
    """
    Aggregate heatmap contributions for one user.
    Returns: (matrix, processed_date_docs, processed_app_rows)
    """
    user_matrix = _init_heatmap_matrix()
    processed_docs = 0
    processed_app_rows = 0

    try:
        date_docs = _fetch_user_dates_in_range(user_id, start_date_str, end_date_str)

        for doc in date_docs:
            processed_docs += 1
            payload = doc.to_dict() or {}
            apps = payload.get("apps", [])

            # Handle missing/non-list apps gracefully
            if isinstance(apps, dict):
                apps = list(apps.values())
            if not isinstance(apps, list):
                continue

            for app in apps:
                if not isinstance(app, dict):
                    continue

                last_used_time = app.get("lastUsedTime")
                if last_used_time in (None, ""):
                    continue

                ts_ms = _safe_int(last_used_time, default=-1)
                if ts_ms < 0:
                    continue

                dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                day_of_week = int(dt.strftime("%w"))  # 0=Sunday
                hour = dt.hour

                # sessionCount default = 1 when missing
                sessions = _safe_int(app.get("sessionCount"), default=1)
                if sessions < 0:
                    sessions = 0

                user_matrix[day_of_week][hour] += sessions
                processed_app_rows += 1

    except Exception as user_err:
        print(
            f"[getPeakUsageHeatmap] failed user aggregation user={user_id}, error={user_err}"
        )

    return user_matrix, processed_docs, processed_app_rows


@router.get("/getPeakUsageHeatmap", response_model=Dict[str, Any])
async def getPeakUsageHeatmap(
    days: int = Query(30, description="Number of days to analyze", ge=1, le=365)
):
    """
    Generate 7x24 heatmap of average sessions per day/hour from:
    screentime/{userId}/dates/{dateString}
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        cache_key = _build_cache_key("peak_usage_heatmap", {"days": days})
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        # Step 1: Calculate date range (inclusive)
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days - 1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        print(
            f"[getPeakUsageHeatmap] days={days}, start_date={start_date_str}, end_date={end_date_str}"
        )

        # Step 2: Count weekday occurrences in the period (0=Sunday)
        day_counts = {i: 0 for i in range(7)}
        for i in range(days):
            d = end_date - timedelta(days=i)
            day_counts[int(d.strftime("%w"))] += 1
        print(f"[getPeakUsageHeatmap] day_counts={day_counts}")

        # Step 3: Initialize 7x24 matrix
        matrix = _init_heatmap_matrix()

        # Step 4: Discover user ids from users collection and query each user's
        # screentime/date range in parallel. We cannot rely on root screentime docs
        # because subcollections may exist under missing parent documents.
        try:
            user_docs = list(db.collection("users").select([]).stream())
        except Exception:
            user_docs = list(db.collection("users").stream())

        user_ids = [doc.id for doc in user_docs]

        # Fallback: derive user ids from screentime date-doc parents if users collection
        # is empty/unavailable.
        if not user_ids:
            print("[getPeakUsageHeatmap] users collection empty; deriving user ids from dates")
            user_ids = list({
                doc.reference.parent.parent.id
                for doc in db.collection_group("dates").stream()
                if doc.reference.parent.parent
                and doc.reference.parent.parent.parent
                and doc.reference.parent.parent.parent.id == "screentime"
            })

        print(f"[getPeakUsageHeatmap] candidate users={len(user_ids)}")

        processed_screentime_docs = 0
        processed_app_rows = 0

        # Step 5: Aggregate sessions by day/hour
        if user_ids:
            max_workers = min(24, max(4, len(user_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        _aggregate_user_heatmap,
                        user_id,
                        start_date_str,
                        end_date_str
                    )
                    for user_id in user_ids
                ]

                for future in as_completed(futures):
                    user_matrix, user_doc_count, user_app_rows = future.result()
                    processed_screentime_docs += user_doc_count
                    processed_app_rows += user_app_rows

                    for day in range(7):
                        for hour in range(24):
                            matrix[day][hour] += user_matrix[day][hour]

        print(
            f"[getPeakUsageHeatmap] processed_screentime_docs={processed_screentime_docs}, processed_app_rows={processed_app_rows}"
        )

        # Step 6: Calculate averages by weekday occurrence count
        for day in range(7):
            for hour in range(24):
                matrix[day][hour] = (
                    round(matrix[day][hour] / day_counts[day])
                    if day_counts[day] > 0
                    else 0
                )

        # Step 7: Find max for intensity calculation
        max_value = 0
        for day in range(7):
            for hour in range(24):
                if matrix[day][hour] > max_value:
                    max_value = matrix[day][hour]
        print(f"[getPeakUsageHeatmap] max_avg_sessions={max_value}")

        # Step 8 + 9: Build data structure
        day_names = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ]
        data = []
        all_slots = []

        for day in range(7):
            hourly_sessions = []
            for hour in range(24):
                avg_sessions = matrix[day][hour]
                hourly_sessions.append(
                    {
                        "hour": hour,
                        "avg_sessions": avg_sessions,
                        "intensity": _get_intensity(avg_sessions, max_value),
                    }
                )
                all_slots.append(
                    {
                        "day": day_names[day],
                        "hour": hour,
                        "avg_sessions": avg_sessions,
                    }
                )

            data.append(
                {
                    "day_of_week": day_names[day],
                    "day_index": day,
                    "hourly_sessions": hourly_sessions,
                }
            )

        # Step 10: Top 5 peak slots
        top_slots = sorted(all_slots, key=lambda x: x["avg_sessions"], reverse=True)[:5]
        peak_times = [
            {
                "day": slot["day"],
                "hour": slot["hour"],
                "avg_sessions": slot["avg_sessions"],
                "formatted": (
                    f"{slot['day']} "
                    f"{_format_hour(slot['hour'])}-{_format_hour(slot['hour'] + 1)}"
                ),
            }
            for slot in top_slots
        ]

        # Step 11: Bottom slots (for avoid-time recommendations)
        bottom_slots = sorted(all_slots, key=lambda x: x["avg_sessions"])[:3]
        avoid_slots = [
            {
                "day": slot["day"],
                "hour": slot["hour"],
                "avg_sessions": slot["avg_sessions"],
                "formatted": (
                    f"{slot['day']} "
                    f"{_format_hour(slot['hour'])}-{_format_hour(slot['hour'] + 1)}"
                ),
            }
            for slot in bottom_slots
        ]

        # Recommendation text blocks (same output shape requested)
        best_times = [
            f"{slot['day']} {_format_hour(slot['hour'])}-{_format_hour(slot['hour'] + 2)}"
            for slot in top_slots[:2]
        ]

        hour_totals = {h: sum(matrix[d][h] for d in range(7)) for h in range(24)}
        lowest_daily_hour = min(hour_totals, key=hour_totals.get) if hour_totals else 0
        avoid_times = [
            f"Daily {_format_hour(lowest_daily_hour)}-{_format_hour(lowest_daily_hour + 3)}"
        ]
        if avoid_slots:
            avoid_times.append(
                f"{avoid_slots[0]['day']} "
                f"{_format_hour(avoid_slots[0]['hour'])}-{_format_hour(avoid_slots[0]['hour'] + 1)}"
            )

        response_payload = {
            "period": f"last_{days}_days",
            "data": data,
            "peak_times": peak_times,
            "intensity_thresholds": _build_intensity_thresholds(max_value),
            "notification_recommendation": {
                "best_times": best_times,
                "avoid_times": avoid_times,
            },
        }

        print("[getPeakUsageHeatmap] response generated successfully")
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["peak_usage_heatmap"])
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error in getPeakUsageHeatmap: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


def _parse_yyyy_mm_dd(date_str: str, field_name: str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field_name}. Expected format YYYY-MM-DD."
        )


def _avg_array_seconds(arr: List[float]) -> int:
    if not arr:
        return 0
    return int(round(sum(arr) / len(arr)))


def _format_duration(seconds: float) -> str:
    total_seconds = int(round(seconds))
    mins = total_seconds // 60
    secs = total_seconds % 60
    if mins == 0:
        return f"{secs}s"
    return f"{mins}m {secs}s"


def _aggregate_user_session_duration_distribution(
    user_id: str,
    start_date_str: str,
    end_date_str: str
) -> Tuple[List[float], Dict[str, List[float]], int]:
    durations: List[float] = []
    bucket_durations: Dict[str, List[float]] = {
        "short": [],
        "medium": [],
        "long": [],
        "power": []
    }
    valid_user_days = 0

    try:
        date_docs = _fetch_user_dates_in_range(user_id, start_date_str, end_date_str)

        for doc in date_docs:
            payload = doc.to_dict() or {}
            apps = payload.get("apps", [])

            if isinstance(apps, dict):
                apps = list(apps.values())
            if not isinstance(apps, list):
                continue

            total_screen_time_ms = 0
            total_sessions = 0

            for app in apps:
                if not isinstance(app, dict):
                    continue

                screen_time = _safe_int(app.get("totalScreenTime"), default=0)
                session_count = _safe_int(app.get("sessionCount"), default=0)

                if screen_time < 0:
                    screen_time = 0
                if session_count < 0:
                    session_count = 0

                total_screen_time_ms += screen_time
                total_sessions += session_count

            # Skip user-day with no sessions
            if total_sessions == 0:
                continue

            avg_duration_seconds = (total_screen_time_ms / total_sessions) / 1000.0
            durations.append(avg_duration_seconds)
            valid_user_days += 1

            if avg_duration_seconds < 60:
                bucket_durations["short"].append(avg_duration_seconds)
            elif avg_duration_seconds < 300:
                bucket_durations["medium"].append(avg_duration_seconds)
            elif avg_duration_seconds < 900:
                bucket_durations["long"].append(avg_duration_seconds)
            else:
                bucket_durations["power"].append(avg_duration_seconds)

    except Exception as user_err:
        print(
            "[getSessionDurationDistribution] failed user aggregation "
            f"user={user_id}, error={user_err}"
        )

    return durations, bucket_durations, valid_user_days


def _aggregate_user_daily_usage_patterns(
    user_id: str,
    start_date_str: str,
    end_date_str: str
) -> List[Tuple[str, int, int]]:
    """
    Returns per-date aggregates for a user:
    [(date_str, total_sessions, total_screentime_ms), ...]
    """
    rows: List[Tuple[str, int, int]] = []

    try:
        date_docs = _fetch_user_dates_in_range(user_id, start_date_str, end_date_str)

        for doc in date_docs:
            payload = doc.to_dict() or {}
            apps = payload.get("apps", [])

            if isinstance(apps, dict):
                apps = list(apps.values())
            if not isinstance(apps, list):
                apps = []

            total_sessions = 0
            total_screentime_ms = 0

            for app in apps:
                if not isinstance(app, dict):
                    continue

                sessions = _safe_int(app.get("sessionCount"), default=0)
                screentime_ms = _safe_int(app.get("totalScreenTime"), default=0)

                if sessions < 0:
                    sessions = 0
                if screentime_ms < 0:
                    screentime_ms = 0

                total_sessions += sessions
                total_screentime_ms += screentime_ms

            rows.append((doc.id, total_sessions, total_screentime_ms))

    except Exception as user_err:
        print(
            "[getDailyUsagePatterns] failed user aggregation "
            f"user={user_id}, error={user_err}"
        )

    return rows


@router.get("/getSessionDurationDistribution", response_model=Dict[str, Any])
async def getSessionDurationDistribution(
    days: int = Query(30, description="Number of days to analyze", ge=1, le=3650),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD), defaults to today")
):
    """
    Calculate session duration distribution from:
    screentime/{userId}/dates/{dateString}
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        # Step 1: Calculate date range
        resolved_end_date = (
            _parse_yyyy_mm_dd(end_date, "end_date")
            if end_date
            else datetime.now(timezone.utc).date()
        )
        resolved_start_date = (
            _parse_yyyy_mm_dd(start_date, "start_date")
            if start_date
            else (resolved_end_date - timedelta(days=days))
        )

        if resolved_start_date > resolved_end_date:
            raise HTTPException(
                status_code=400,
                detail="start_date must be on or before end_date."
            )

        start_date_str = resolved_start_date.strftime("%Y-%m-%d")
        end_date_str = resolved_end_date.strftime("%Y-%m-%d")
        total_days = (resolved_end_date - resolved_start_date).days

        cache_key = _build_cache_key(
            "session_duration_distribution",
            {
                "start_date": start_date_str,
                "end_date": end_date_str,
                "days": days,
            }
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        print(
            "[getSessionDurationDistribution] "
            f"start={start_date_str}, end={end_date_str}, days_param={days}, total_days={total_days}"
        )

        # Discover candidate users from users collection first.
        try:
            user_docs = list(db.collection("users").select([]).stream())
        except Exception:
            user_docs = list(db.collection("users").stream())

        user_ids = [doc.id for doc in user_docs]

        # Fallback if users collection is empty.
        if not user_ids:
            print(
                "[getSessionDurationDistribution] users collection empty; deriving user ids from dates"
            )
            user_ids = list({
                doc.reference.parent.parent.id
                for doc in db.collection_group("dates").stream()
                if doc.reference.parent.parent
                and doc.reference.parent.parent.parent
                and doc.reference.parent.parent.parent.id == "screentime"
            })

        print(f"[getSessionDurationDistribution] candidate users={len(user_ids)}")

        # Step 2+3: Query docs in range and compute user-day average durations.
        all_durations: List[float] = []
        bucket_durations: Dict[str, List[float]] = {
            "short": [],
            "medium": [],
            "long": [],
            "power": []
        }
        total_user_days = 0

        if user_ids:
            max_workers = min(24, max(4, len(user_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        _aggregate_user_session_duration_distribution,
                        user_id,
                        start_date_str,
                        end_date_str
                    )
                    for user_id in user_ids
                ]

                for future in as_completed(futures):
                    user_durations, user_buckets, user_days = future.result()
                    all_durations.extend(user_durations)
                    total_user_days += user_days

                    for key in bucket_durations:
                        bucket_durations[key].extend(user_buckets[key])

        print(
            "[getSessionDurationDistribution] "
            f"total_user_days={total_user_days}, durations_collected={len(all_durations)}"
        )

        # Step 4: Calculate totals and percentages
        counts = {
            "short": len(bucket_durations["short"]),
            "medium": len(bucket_durations["medium"]),
            "long": len(bucket_durations["long"]),
            "power": len(bucket_durations["power"])
        }

        def calc_percent(count: int) -> float:
            if total_user_days == 0:
                return 0.0
            return round((count / total_user_days) * 100, 1)

        percents = {
            "short": calc_percent(counts["short"]),
            "medium": calc_percent(counts["medium"]),
            "long": calc_percent(counts["long"]),
            "power": calc_percent(counts["power"])
        }

        # Step 5: Average duration per bucket
        bucket_avgs = {
            "short": _avg_array_seconds(bucket_durations["short"]),
            "medium": _avg_array_seconds(bucket_durations["medium"]),
            "long": _avg_array_seconds(bucket_durations["long"]),
            "power": _avg_array_seconds(bucket_durations["power"])
        }

        # Step 6: Overall median and average
        if all_durations:
            sorted_durations = sorted(all_durations)
            mid = len(sorted_durations) // 2
            if len(sorted_durations) % 2 == 0:
                median_seconds = (sorted_durations[mid - 1] + sorted_durations[mid]) / 2
            else:
                median_seconds = sorted_durations[mid]
            avg_seconds = sum(all_durations) / len(all_durations)
        else:
            median_seconds = 0.0
            avg_seconds = 0.0

        median_seconds_rounded = int(round(median_seconds))
        avg_seconds_rounded = int(round(avg_seconds))

        # Step 8: Insights
        insights = []
        if percents["short"] > 40:
            insights.append({
                "type": "warning",
                "message": f"{percents['short']}% are quick sessions - optimize app loading speed"
            })

        if percents["power"] < 10:
            insights.append({
                "type": "info",
                "message": f"Only {percents['power']}% power sessions - consider adding deeper features"
            })

        if percents["medium"] > 30:
            insights.append({
                "type": "positive",
                "message": f"{percents['medium']}% focused sessions shows strong task completion"
            })

        if percents["power"] > 20:
            insights.append({
                "type": "positive",
                "message": f"{percents['power']}% power users - strong deep engagement!"
            })

        # Step 9: App type
        app_type = "balanced"
        if percents["short"] > 50:
            app_type = "quick_check"
        elif percents["power"] > 20:
            app_type = "deep_engagement"
        elif percents["medium"] > 40:
            app_type = "utility"

        response_payload = {
            "date_range": {
                "start": start_date_str,
                "end": end_date_str,
                "total_days": total_days
            },
            "total_user_days_analyzed": total_user_days,
            "distribution": [
                {
                    "bucket": "0-1min",
                    "count": counts["short"],
                    "percent": percents["short"],
                    "label": "Quick Check",
                    "description": "Just checking notifications or quick task",
                    "avg_duration_seconds": bucket_avgs["short"],
                    "color": "#DBEAFE"
                },
                {
                    "bucket": "1-5min",
                    "count": counts["medium"],
                    "percent": percents["medium"],
                    "label": "Focused Use",
                    "description": "Completing a specific task or goal",
                    "avg_duration_seconds": bucket_avgs["medium"],
                    "color": "#93C5FD"
                },
                {
                    "bucket": "5-15min",
                    "count": counts["long"],
                    "percent": percents["long"],
                    "label": "Deep Browse",
                    "description": "Exploring content or discovery mode",
                    "avg_duration_seconds": bucket_avgs["long"],
                    "color": "#3B82F6"
                },
                {
                    "bucket": "15+min",
                    "count": counts["power"],
                    "percent": percents["power"],
                    "label": "Power Users",
                    "description": "Heavy engagement or content creation",
                    "avg_duration_seconds": bucket_avgs["power"],
                    "color": "#1D4ED8"
                }
            ],
            "overall": {
                "median_duration_seconds": median_seconds_rounded,
                "median_duration_formatted": _format_duration(median_seconds_rounded),
                "avg_duration_seconds": avg_seconds_rounded,
                "avg_duration_formatted": _format_duration(avg_seconds_rounded)
            },
            "insights": insights,
            "app_type": app_type
        }

        print("[getSessionDurationDistribution] response generated successfully")
        _cache_set(
            cache_key,
            response_payload,
            _DASHBOARD_CACHE_TTL_MIN["session_duration_distribution"]
        )
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error in getSessionDurationDistribution: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/getDailyUsagePatterns", response_model=Dict[str, Any])
async def getDailyUsagePatterns(
    days: int = Query(30, description="Number of days to analyze", ge=1, le=3650)
):
    """
    Analyze usage patterns by day of week from:
    screentime/{userId}/dates/{dateString}
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        # Step 1: Calculate date range
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days - 1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        cache_key = _build_cache_key(
            "daily_usage_patterns",
            {"days": days, "start_date": start_date_str, "end_date": end_date_str}
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        print(
            "[getDailyUsagePatterns] "
            f"days={days}, start={start_date_str}, end={end_date_str}"
        )

        # Step 2+3: Build per-date aggregate first
        by_date: Dict[str, Dict[str, Any]] = {}

        try:
            user_docs = list(db.collection("users").select([]).stream())
        except Exception:
            user_docs = list(db.collection("users").stream())

        user_ids = [doc.id for doc in user_docs]
        if not user_ids:
            print("[getDailyUsagePatterns] users collection empty; deriving user ids from dates")
            user_ids = list({
                doc.reference.parent.parent.id
                for doc in db.collection_group("dates").stream()
                if doc.reference.parent.parent
                and doc.reference.parent.parent.parent
                and doc.reference.parent.parent.parent.id == "screentime"
            })

        print(f"[getDailyUsagePatterns] candidate users={len(user_ids)}")

        if user_ids:
            max_workers = min(24, max(4, len(user_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_user = {
                    executor.submit(
                        _aggregate_user_daily_usage_patterns,
                        user_id,
                        start_date_str,
                        end_date_str
                    ): user_id
                    for user_id in user_ids
                }

                for future in as_completed(future_to_user):
                    rows = future.result()
                    user_id = future_to_user[future]

                    for date_str, sessions, screentime_ms in rows:
                        if date_str not in by_date:
                            by_date[date_str] = {
                                "users": set(),
                                "totalSessions": 0,
                                "totalScreenTime": 0
                            }

                        by_date[date_str]["users"].add(user_id)
                        by_date[date_str]["totalSessions"] += sessions
                        by_date[date_str]["totalScreenTime"] += screentime_ms

        print(f"[getDailyUsagePatterns] unique_dates={len(by_date)}")

        # Step 4: Initialize grouped structure
        day_names = [
            "Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday"
        ]
        grouped: Dict[int, Dict[str, Any]] = {
            i: {"name": day_names[i], "index": i, "dateEntries": []}
            for i in range(7)
        }

        # Step 5: Add dates to day groups
        for date_str, data in by_date.items():
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            day_of_week = (date_obj.weekday() + 1) % 7  # convert to JS-like 0=Sunday
            grouped[day_of_week]["dateEntries"].append({
                "date": date_str,
                "sessions": data["totalSessions"],
                "users": len(data["users"]),
                "screentime": data["totalScreenTime"]
            })

        # Step 6: Calculate per-day averages
        results: List[Dict[str, Any]] = []

        for i in range(7):
            day = grouped[i]
            entries = day["dateEntries"]

            if len(entries) == 0:
                results.append({
                    "day_of_week": day["name"],
                    "day_index": i,
                    "avg_sessions": 0,
                    "avg_dau": 0,
                    "sessions_per_user": 0,
                    "avg_duration_seconds": 0,
                    "avg_duration_formatted": "0s",
                    "occurrences": 0,
                    "is_peak": False,
                    "vs_avg_percent": 0
                })
                continue

            count = len(entries)
            avg_sessions = round(sum(e["sessions"] for e in entries) / count)
            avg_users = round(sum(e["users"] for e in entries) / count)

            total_screentime_ms = sum(e["screentime"] for e in entries)
            total_sessions_all = sum(e["sessions"] for e in entries)

            avg_duration_sec = (
                round((total_screentime_ms / 1000) / total_sessions_all)
                if total_sessions_all > 0
                else 0
            )

            sessions_per_user = (
                round(avg_sessions / avg_users, 2)
                if avg_users > 0
                else 0
            )

            results.append({
                "day_of_week": day["name"],
                "day_index": i,
                "avg_sessions": avg_sessions,
                "avg_dau": avg_users,
                "sessions_per_user": sessions_per_user,
                "avg_duration_seconds": avg_duration_sec,
                "avg_duration_formatted": _format_duration(avg_duration_sec),
                "occurrences": count,
                "is_peak": False,
                "vs_avg_percent": 0
            })

        # Step 7: Overall average and vs_avg_percent
        days_with_data = [d for d in results if d["avg_sessions"] > 0]
        overall_avg = (
            round(sum(d["avg_sessions"] for d in days_with_data) / len(days_with_data))
            if days_with_data
            else 0
        )

        for day in results:
            if day["avg_sessions"] > 0 and overall_avg > 0:
                day["vs_avg_percent"] = round(
                    ((day["avg_sessions"] - overall_avg) / overall_avg) * 100,
                    1
                )

        # Step 8: Peak and lowest days
        if days_with_data:
            sorted_by_session = sorted(days_with_data, key=lambda x: x["avg_sessions"], reverse=True)
            peak_day = sorted_by_session[0]
            lowest_day = sorted_by_session[-1]

            for day in results:
                if day["day_of_week"] == peak_day["day_of_week"]:
                    day["is_peak"] = True
                    break
        else:
            peak_day = {"day_of_week": "Sunday", "avg_sessions": 0, "day_index": 0}
            lowest_day = {"day_of_week": "Sunday", "avg_sessions": 0, "day_index": 0}

        # Step 9: Weekday vs weekend
        weekdays = [d for d in results if 1 <= d["day_index"] <= 5]
        weekends = [d for d in results if d["day_index"] in (0, 6)]

        weekday_avg = round(sum(d["avg_sessions"] for d in weekdays) / len(weekdays)) if weekdays else 0
        weekend_avg = round(sum(d["avg_sessions"] for d in weekends) / len(weekends)) if weekends else 0
        weekday_higher_percent = (
            round(((weekday_avg - weekend_avg) / weekend_avg) * 100, 1)
            if weekend_avg > 0
            else 0
        )

        # Step 10: Insights
        insights: List[Dict[str, str]] = []
        insights.append({
            "type": "positive",
            "message": f"{peak_day['day_of_week']} is peak day - best day to launch new features"
        })
        insights.append({
            "type": "info",
            "message": f"Weekday engagement {weekday_higher_percent}% higher than weekends"
        })

        weekday_duration = round(
            sum(d["avg_duration_seconds"] for d in weekdays) / len(weekdays)
        ) if weekdays else 0
        weekend_duration = round(
            sum(d["avg_duration_seconds"] for d in weekends) / len(weekends)
        ) if weekends else 0

        if weekend_duration > weekday_duration:
            insights.append({
                "type": "info",
                "message": "Weekend sessions are longer - users have more free time"
            })

        response_payload = {
            "period": f"last_{days}_days",
            "total_days_analyzed": days,
            "data": results,
            "summary": {
                "overall_avg_sessions": overall_avg,
                "peak_day": {
                    "day": peak_day["day_of_week"],
                    "avg_sessions": peak_day["avg_sessions"],
                    "day_index": peak_day["day_index"]
                },
                "lowest_day": {
                    "day": lowest_day["day_of_week"],
                    "avg_sessions": lowest_day["avg_sessions"],
                    "day_index": lowest_day["day_index"]
                },
                "weekday_vs_weekend": {
                    "weekday_avg_sessions": weekday_avg,
                    "weekend_avg_sessions": weekend_avg,
                    "weekday_higher_percent": weekday_higher_percent
                }
            },
            "insights": insights
        }

        print("[getDailyUsagePatterns] response generated successfully")
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["daily_usage_patterns"])
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error in getDailyUsagePatterns: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")
