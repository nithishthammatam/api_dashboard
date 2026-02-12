from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import Optional, List, Dict, Any, Tuple
from database.firebase import db, init_error
from datetime import datetime, timedelta, timezone
from firebase_admin import firestore
from middleware.auth import verify_api_key
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter(
    prefix="/api/dashboard",
    tags=["Dashboard"],
    dependencies=[Depends(verify_api_key)]
)

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

        return {
            "success": True,
            "data": users,
            "count": len(users)
        }
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

        return {
            "success": True,
            "data": users_status,
            "count": len(users_status)
        }
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

        return {
            "success": True,
            "data": screentime_data,
            "count": len(screentime_data),
            "filters": {
                "userId": user_id or "all",
                "date": date or "all"
            }
        }

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

        return {
            "success": True,
            "data": sessions_data,
            "count": len(sessions_data),
            "filters": {
                "userId": user_id or "all",
                "date": date or "all"
            }
        }

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
        
        return {
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
        
        return {
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
        
        return {
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
        
        return {
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
        
        return {
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

    except Exception as e:
        print(f"❌ Error fetching New vs Returning: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")
