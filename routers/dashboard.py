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
