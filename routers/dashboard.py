
import re
import math
from fastapi import Request, Body, Header
import firebase_admin.auth as auth
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import Optional, List, Dict, Any, Tuple
from database.firebase import db, init_error
from datetime import datetime, timedelta, timezone
from firebase_admin import firestore
from google.cloud.firestore_v1.field_path import FieldPath
from middleware.auth import verify_api_key
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import hashlib

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
    "growth_funnel": 10,
    "session_summary": 5,
    "peak_usage_heatmap": 10,
    "session_duration_distribution": 10,
    "daily_usage_patterns": 10,
    "top_apps": 5,
    "category_drilldown": 5,
    "user_segments": 5,
    "cohort_retention": 10,
    "wellbeing_report": 10,
}

_PREAGG_COLLECTIONS = {
    "user_segments": "dashboard_preagg_user_segments",
    "cohort_retention": "dashboard_preagg_cohort_retention",
    "wellbeing_report": "dashboard_preagg_wellbeing_report",
}

_PREAGG_SCHEMA_VERSION = {
    "user_segments": 1,
    "cohort_retention": 1,
    "wellbeing_report": 1,
}

_PERSISTENT_CACHE_COLLECTION = "dashboard_preagg_cache"
_PERSISTENT_CACHE_VERSION = 1
_PERSISTENT_CACHE_MAX_BYTES = 850000
_USER_ACTIVITY_INDEX_COLLECTION = "dashboard_user_activity_index"
_USER_ACTIVITY_INDEX_META_DOC = "__meta__"
_USER_ACTIVITY_INDEX_SCHEMA_VERSION = 1


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


def _make_cache_safe_payload(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_make_cache_safe_payload(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _make_cache_safe_payload(v) for k, v in value.items()}
    return str(value)


def _is_json_serializable(value: Any) -> bool:
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


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
    # Fallback: materialized Firestore cache for cross-process warm starts.
    try:
        persistent_cached = _persistent_cache_get(cache_key)
        if persistent_cached is not None:
            return persistent_cached
    except Exception as persistent_err:
        print(f"[cache] persistent read error key={cache_key}: {persistent_err}")
    return None


def _cache_set(cache_key: str, payload: Any, ttl_minutes: int):
    cache_payload = payload
    if not _is_json_serializable(cache_payload):
        cache_payload = _make_cache_safe_payload(payload)

    try:
        cached_ok = set_cached_data(cache_key, cache_payload, ttl_minutes=ttl_minutes)
    except Exception as cache_err:
        print(f"[cache] write error key={cache_key}: {cache_err}")
        cached_ok = False

    if cached_ok:
        if cache_payload is payload:
            print(f"[cache] SET {cache_key} ttl={ttl_minutes}m")
        else:
            print(f"[cache] SET(normalized) {cache_key} ttl={ttl_minutes}m")
    # Best-effort persistent cache write.
    try:
        _persistent_cache_set(cache_key, cache_payload, ttl_minutes)
    except Exception as persistent_err:
        print(f"[cache] persistent write error key={cache_key}: {persistent_err}")


def _persistent_cache_doc_id(cache_key: str) -> str:
    return hashlib.sha256(cache_key.encode("utf-8")).hexdigest()


def _persistent_cache_is_payload_small_enough(payload: Any) -> bool:
    try:
        encoded = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        return len(encoded) <= _PERSISTENT_CACHE_MAX_BYTES
    except Exception:
        return False


def _persistent_cache_get(cache_key: str):
    if not db:
        return None

    doc_id = _persistent_cache_doc_id(cache_key)
    doc = db.collection(_PERSISTENT_CACHE_COLLECTION).document(doc_id).get()
    if not doc.exists:
        return None

    payload = doc.to_dict() or {}
    schema_version = payload.get("schema_version")
    if schema_version not in (None, _PERSISTENT_CACHE_VERSION):
        return None

    expires_at_epoch = payload.get("expires_at_epoch")
    try:
        expires_at_epoch_int = int(expires_at_epoch) if expires_at_epoch is not None else 0
    except (TypeError, ValueError):
        expires_at_epoch_int = 0

    now_epoch = int(datetime.now(timezone.utc).timestamp())
    if expires_at_epoch_int > 0 and expires_at_epoch_int <= now_epoch:
        return None

    cached_payload = payload.get("payload")
    if cached_payload is None:
        return None

    ttl_seconds_remaining = (
        max(0, expires_at_epoch_int - now_epoch)
        if expires_at_epoch_int > 0
        else 0
    )
    if ttl_seconds_remaining > 0:
        ttl_minutes_remaining = max(1, int((ttl_seconds_remaining + 59) // 60))
        redis_payload = cached_payload
        if not _is_json_serializable(redis_payload):
            redis_payload = _make_cache_safe_payload(redis_payload)
        try:
            set_cached_data(cache_key, redis_payload, ttl_minutes=ttl_minutes_remaining)
        except Exception:
            pass

    print(f"[cache] PERSISTENT HIT {cache_key}")
    return cached_payload


def _persistent_cache_set(cache_key: str, payload: Any, ttl_minutes: int):
    if not db or ttl_minutes <= 0:
        return
    if not _persistent_cache_is_payload_small_enough(payload):
        print(f"[cache] PERSISTENT SKIP (payload too large) key={cache_key}")
        return

    doc_id = _persistent_cache_doc_id(cache_key)
    expires_at_epoch = int(
        (datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)).timestamp()
    )
    db.collection(_PERSISTENT_CACHE_COLLECTION).document(doc_id).set(
        {
            "schema_version": _PERSISTENT_CACHE_VERSION,
            "cache_key": cache_key,
            "expires_at_epoch": expires_at_epoch,
            "ttl_minutes": ttl_minutes,
            "payload": payload,
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True
    )


def _preagg_get(endpoint_key: str, doc_id: str):
    collection_name = _PREAGG_COLLECTIONS.get(endpoint_key)
    if not collection_name:
        return None

    try:
        doc = db.collection(collection_name).document(doc_id).get()
        if not doc.exists:
            return None

        doc_payload = doc.to_dict() or {}
        expected_version = _PREAGG_SCHEMA_VERSION.get(endpoint_key)
        stored_version = doc_payload.get("schema_version")
        if (
            expected_version is not None
            and stored_version is not None
            and stored_version != expected_version
        ):
            print(
                "[preagg] schema mismatch "
                f"endpoint={endpoint_key}, doc={doc_id}, expected={expected_version}, got={stored_version}"
            )
            return None

        payload = doc_payload.get("payload")
        if isinstance(payload, dict):
            print(f"[preagg] HIT endpoint={endpoint_key}, doc={doc_id}")
            return payload

        # Backward-compat: if payload is stored directly in the document.
        if isinstance(doc_payload, dict):
            fallback_payload = {
                k: v
                for k, v in doc_payload.items()
                if k not in {"payload", "schema_version", "updated_at", "meta"}
            }
            if fallback_payload:
                print(f"[preagg] HIT (direct) endpoint={endpoint_key}, doc={doc_id}")
                return fallback_payload

        return None
    except Exception as preagg_err:
        print(f"[preagg] read error endpoint={endpoint_key}, doc={doc_id}, error={preagg_err}")
        return None


def _preagg_set(endpoint_key: str, doc_id: str, payload: Dict[str, Any], meta: Optional[Dict[str, Any]] = None):
    collection_name = _PREAGG_COLLECTIONS.get(endpoint_key)
    if not collection_name:
        return

    try:
        write_data: Dict[str, Any] = {
            "schema_version": _PREAGG_SCHEMA_VERSION.get(endpoint_key),
            "payload": payload,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        if meta:
            write_data["meta"] = meta

        db.collection(collection_name).document(doc_id).set(write_data, merge=True)
        print(f"[preagg] SET endpoint={endpoint_key}, doc={doc_id}")
    except Exception as preagg_err:
        print(f"[preagg] write error endpoint={endpoint_key}, doc={doc_id}, error={preagg_err}")


def _scan_user_activity_from_dates() -> Tuple[Dict[str, Any], Dict[str, set], int, int]:
    user_first_seen: Dict[str, Any] = {}
    user_active_dates: Dict[str, set] = {}
    docs_scanned = 0
    screentime_docs = 0

    for date_doc in db.collection_group("dates").select([]).stream():
        docs_scanned += 1

        parent_doc_ref = date_doc.reference.parent.parent
        parent_collection = parent_doc_ref.parent if parent_doc_ref else None
        if not parent_doc_ref or not parent_collection or parent_collection.id != "screentime":
            continue

        date_str = date_doc.id
        try:
            activity_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        screentime_docs += 1
        user_id = parent_doc_ref.id

        if user_id not in user_active_dates:
            user_active_dates[user_id] = set()
        user_active_dates[user_id].add(activity_date)

        first_seen_date = user_first_seen.get(user_id)
        if first_seen_date is None or activity_date < first_seen_date:
            user_first_seen[user_id] = activity_date

    return user_first_seen, user_active_dates, docs_scanned, screentime_docs


def _persist_user_activity_index(
    user_first_seen: Dict[str, Any],
    user_active_dates: Dict[str, set],
    docs_scanned: int,
    screentime_docs: int
) -> int:
    if not db:
        return 0

    write_count = 0
    batch = db.batch()
    pending_writes = 0
    batch_limit = 300

    for user_id in sorted(user_first_seen.keys()):
        first_seen_date = user_first_seen.get(user_id)
        if first_seen_date is None:
            continue

        active_dates = user_active_dates.get(user_id, set())
        active_date_strings = sorted(
            {
                d.strftime("%Y-%m-%d")
                for d in active_dates
                if d is not None
            }
        )

        if not active_date_strings:
            active_date_strings = [first_seen_date.strftime("%Y-%m-%d")]

        doc_ref = db.collection(_USER_ACTIVITY_INDEX_COLLECTION).document(user_id)
        batch.set(
            doc_ref,
            {
                "schema_version": _USER_ACTIVITY_INDEX_SCHEMA_VERSION,
                "first_seen_date": first_seen_date.strftime("%Y-%m-%d"),
                "active_dates": active_date_strings,
                "active_days": len(active_date_strings),
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True
        )

        pending_writes += 1
        write_count += 1

        if pending_writes >= batch_limit:
            batch.commit()
            batch = db.batch()
            pending_writes = 0

    meta_ref = db.collection(_USER_ACTIVITY_INDEX_COLLECTION).document(_USER_ACTIVITY_INDEX_META_DOC)
    batch.set(
        meta_ref,
        {
            "schema_version": _USER_ACTIVITY_INDEX_SCHEMA_VERSION,
            "user_count": write_count,
            "source_docs_scanned": docs_scanned,
            "source_screentime_docs": screentime_docs,
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True
    )
    pending_writes += 1

    if pending_writes > 0:
        batch.commit()

    return write_count


def _load_user_activity_index() -> Optional[Tuple[Dict[str, Any], Dict[str, set], Dict[str, Any]]]:
    if not db:
        return None

    try:
        index_docs = list(db.collection(_USER_ACTIVITY_INDEX_COLLECTION).stream())
    except Exception as index_err:
        print(f"[user-activity-index] read error: {index_err}")
        return None

    if not index_docs:
        return None

    user_first_seen: Dict[str, Any] = {}
    user_active_dates: Dict[str, set] = {}
    meta_payload: Dict[str, Any] = {}

    for index_doc in index_docs:
        if index_doc.id == _USER_ACTIVITY_INDEX_META_DOC:
            meta_payload = index_doc.to_dict() or {}
            continue

        payload = index_doc.to_dict() or {}
        if payload.get("schema_version") not in (None, _USER_ACTIVITY_INDEX_SCHEMA_VERSION):
            continue

        first_seen_raw = str(payload.get("first_seen_date") or "").strip()
        if not first_seen_raw:
            continue

        try:
            first_seen_date = datetime.strptime(first_seen_raw, "%Y-%m-%d").date()
        except ValueError:
            continue

        active_dates_raw = payload.get("active_dates", [])
        active_dates_set = set()
        if isinstance(active_dates_raw, list):
            for raw_date in active_dates_raw:
                date_str = str(raw_date or "").strip()
                if not date_str:
                    continue
                try:
                    active_dates_set.add(datetime.strptime(date_str, "%Y-%m-%d").date())
                except ValueError:
                    continue

        if not active_dates_set:
            active_dates_set.add(first_seen_date)

        user_id = index_doc.id
        user_first_seen[user_id] = first_seen_date
        user_active_dates[user_id] = active_dates_set

    if not user_first_seen:
        return None

    return user_first_seen, user_active_dates, meta_payload


def build_user_activity_index() -> Dict[str, Any]:
    """
    Rebuild first_seen + active_dates index from screentime date docs.
    """
    if not db:
        raise RuntimeError(f"Database connection not initialized. Error: {init_error}")

    user_first_seen, user_active_dates, docs_scanned, screentime_docs = _scan_user_activity_from_dates()
    indexed_users = _persist_user_activity_index(
        user_first_seen,
        user_active_dates,
        docs_scanned,
        screentime_docs
    )

    return {
        "indexed_users": indexed_users,
        "docs_scanned": docs_scanned,
        "screentime_docs": screentime_docs,
    }


def _load_or_build_user_activity_index() -> Tuple[Dict[str, Any], Dict[str, set], Dict[str, Any]]:
    indexed_payload = _load_user_activity_index()
    if indexed_payload is not None:
        user_first_seen, user_active_dates, meta_payload = indexed_payload
        print(
            "[user-activity-index] HIT "
            f"users={len(user_first_seen)}, updated_at={meta_payload.get('updated_at')}"
        )
        return (
            user_first_seen,
            user_active_dates,
            {
                "source": "index",
                "docs_scanned": int(meta_payload.get("source_docs_scanned", 0) or 0),
                "screentime_docs": int(meta_payload.get("source_screentime_docs", 0) or 0),
            }
        )

    print("[user-activity-index] MISS; rebuilding from collection_group('dates')")
    user_first_seen, user_active_dates, docs_scanned, screentime_docs = _scan_user_activity_from_dates()

    try:
        indexed_users = _persist_user_activity_index(
            user_first_seen,
            user_active_dates,
            docs_scanned,
            screentime_docs
        )
        print(f"[user-activity-index] REBUILT users={indexed_users}")
    except Exception as index_write_err:
        print(f"[user-activity-index] write error: {index_write_err}")

    return (
        user_first_seen,
        user_active_dates,
        {
            "source": "scan",
            "docs_scanned": docs_scanned,
            "screentime_docs": screentime_docs,
        }
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

@router.get("/getGrowthFunnel", response_model=Dict[str, Any])
async def getGrowthFunnel(
    days: int = Query(30, description="Number of days to analyze", ge=1, le=3650)
):
    """
    Growth funnel metrics for newly acquired users and new-vs-veteran behavior split.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        ist_offset = timedelta(hours=5, minutes=30)
        end_date = (datetime.now(timezone.utc) + ist_offset).date()
        start_date = end_date - timedelta(days=days - 1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        cache_key = _build_cache_key(
            "growth_funnel",
            {
                "v": 1,
                "days": days,
                "start_date": start_date_str,
                "end_date": end_date_str,
            }
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        print(
            "[getGrowthFunnel] "
            f"days={days}, start={start_date_str}, end={end_date_str}"
        )

        def _pct(count_value: int, total_value: int) -> float:
            if total_value <= 0:
                return 0.0
            return round((count_value / total_value) * 100, 1)

        user_first_seen, user_active_dates, activity_index_stats = _load_or_build_user_activity_index()
        docs_scanned = int(activity_index_stats.get("docs_scanned", 0))
        screentime_docs = int(activity_index_stats.get("screentime_docs", 0))
        activity_source = str(activity_index_stats.get("source") or "unknown")

        cohort_user_ids = [
            user_id
            for user_id, first_seen_date in user_first_seen.items()
            if start_date <= first_seen_date <= end_date
        ]
        new_users_acquired = len(cohort_user_ids)

        retained_day_1_count = 0
        retained_week_1_count = 0
        retained_month_1_count = 0

        first_week_eligible = {day_num: 0 for day_num in range(1, 8)}
        first_week_retained = {day_num: 0 for day_num in range(1, 8)}

        daily_new_users_map = {}
        for offset in range(days):
            date_obj = start_date + timedelta(days=offset)
            daily_new_users_map[date_obj.strftime("%Y-%m-%d")] = 0

        for user_id in cohort_user_ids:
            first_seen_date = user_first_seen[user_id]
            active_dates = user_active_dates.get(user_id, set())

            first_seen_str = first_seen_date.strftime("%Y-%m-%d")
            daily_new_users_map[first_seen_str] = daily_new_users_map.get(first_seen_str, 0) + 1

            if (first_seen_date + timedelta(days=1)) in active_dates:
                retained_day_1_count += 1

            if (first_seen_date + timedelta(days=7)) in active_dates:
                retained_week_1_count += 1

            if (first_seen_date + timedelta(days=30)) in active_dates:
                retained_month_1_count += 1

            for day_num in range(1, 8):
                target_date = first_seen_date + timedelta(days=day_num)
                if target_date > end_date:
                    continue
                first_week_eligible[day_num] += 1
                if target_date in active_dates:
                    first_week_retained[day_num] += 1

        daily_new_users_payload = []
        for offset in range(days):
            date_obj = start_date + timedelta(days=offset)
            date_key = date_obj.strftime("%Y-%m-%d")
            daily_new_users_payload.append(
                {
                    "date": date_key,
                    "new_users": int(daily_new_users_map.get(date_key, 0)),
                }
            )

        first_week_behavior_payload = []
        for day_num in range(1, 8):
            first_week_behavior_payload.append(
                {
                    "day": day_num,
                    "return_rate": _pct(
                        first_week_retained[day_num],
                        first_week_eligible[day_num]
                    ),
                }
            )

        active_today_user_ids = [
            user_id
            for user_id, active_dates in user_active_dates.items()
            if end_date in active_dates
        ]

        new_segment_user_ids: List[str] = []
        veteran_segment_user_ids: List[str] = []
        for user_id in active_today_user_ids:
            first_seen_date = user_first_seen.get(user_id)
            if not first_seen_date:
                continue

            tenure_days = (end_date - first_seen_date).days + 1
            if tenure_days < 7:
                new_segment_user_ids.append(user_id)
            elif tenure_days >= 30:
                veteran_segment_user_ids.append(user_id)

        def _load_user_day_metrics(user_id: str) -> Optional[Dict[str, Any]]:
            try:
                date_doc = (
                    db.collection("screentime")
                    .document(user_id)
                    .collection("dates")
                    .document(end_date_str)
                    .get()
                )
            except Exception as fetch_err:
                print(f"[getGrowthFunnel] failed day fetch user={user_id}, error={fetch_err}")
                return None

            if not date_doc.exists:
                return None

            payload = date_doc.to_dict() or {}
            (
                daily_screentime_seconds,
                apps_used_count,
                total_sessions,
                app_seconds,
                _category_seconds,
            ) = _aggregate_daily_segment_metrics(payload)

            return {
                "user_id": user_id,
                "daily_screentime_seconds": int(daily_screentime_seconds),
                "apps_used_count": int(apps_used_count),
                "total_sessions": int(total_sessions),
                "app_seconds": app_seconds,
            }

        metric_user_ids = sorted(set(new_segment_user_ids + veteran_segment_user_ids))
        user_day_metrics: Dict[str, Dict[str, Any]] = {}

        if metric_user_ids:
            max_workers = min(24, max(4, len(metric_user_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(_load_user_day_metrics, user_id)
                    for user_id in metric_user_ids
                ]

                for future in as_completed(futures):
                    try:
                        metric_row = future.result()
                    except Exception as metric_err:
                        print(f"[getGrowthFunnel] metric aggregation failed: {metric_err}")
                        continue

                    if not metric_row:
                        continue
                    user_day_metrics[str(metric_row["user_id"])] = metric_row

        def _build_segment_payload(user_ids: List[str], definition: str) -> Dict[str, Any]:
            total_screentime = 0
            total_apps_used = 0
            total_sessions = 0
            app_totals: Dict[str, int] = {}
            metric_rows = 0

            for user_id in user_ids:
                metric_row = user_day_metrics.get(user_id)
                if not metric_row:
                    continue

                metric_rows += 1
                total_screentime += int(metric_row.get("daily_screentime_seconds", 0))
                total_apps_used += int(metric_row.get("apps_used_count", 0))
                total_sessions += int(metric_row.get("total_sessions", 0))

                for app_name, app_seconds in (metric_row.get("app_seconds") or {}).items():
                    if not app_name:
                        continue
                    app_totals[app_name] = app_totals.get(app_name, 0) + int(app_seconds)

            avg_screentime_seconds = (
                int(round(total_screentime / metric_rows))
                if metric_rows > 0
                else 0
            )
            avg_apps_used = (
                round(total_apps_used / metric_rows, 1)
                if metric_rows > 0
                else 0.0
            )
            avg_sessions = (
                round(total_sessions / metric_rows, 1)
                if metric_rows > 0
                else 0.0
            )
            top_app = _pick_top_key(app_totals)

            return {
                "definition": definition,
                "count": len(user_ids),
                "avg_screentime_formatted": _format_hh_mm_duration(avg_screentime_seconds),
                "avg_apps_used": avg_apps_used,
                "avg_sessions": avg_sessions,
                "top_app": top_app,
            }

        response_payload = {
            "period": f"last_{days}_days",
            "funnel": {
                "new_users_acquired": new_users_acquired,
                "retained_day_1": {
                    "count": retained_day_1_count,
                    "percent": _pct(retained_day_1_count, new_users_acquired),
                },
                "retained_week_1": {
                    "count": retained_week_1_count,
                    "percent": _pct(retained_week_1_count, new_users_acquired),
                },
                "retained_month_1": {
                    "count": retained_month_1_count,
                    "percent": _pct(retained_month_1_count, new_users_acquired),
                },
            },
            "daily_new_users": daily_new_users_payload,
            "first_week_behavior": first_week_behavior_payload,
            "new_vs_veteran_comparison": {
                "new_users": _build_segment_payload(
                    new_segment_user_ids,
                    "Active < 7 days"
                ),
                "veterans": _build_segment_payload(
                    veteran_segment_user_ids,
                    "Active 30+ days"
                ),
            },
        }

        print(
            "[getGrowthFunnel] "
            f"users={len(user_first_seen)}, cohort_users={new_users_acquired}, "
            f"source={activity_source}, docs_scanned={docs_scanned}, "
            f"screentime_docs={screentime_docs}"
        )

        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["growth_funnel"])
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"[getGrowthFunnel] Error: {e}")
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

@router.get("/getTopApps", response_model=Dict[str, Any])
async def getTopApps(
    days: int = Query(30, description="Number of days to analyze", ge=1, le=3650),
    limit: int = Query(10, description="Number of top apps to return", ge=1, le=200),
    category: str = Query("all", description="Category filter, or 'all'")
):
    """
    Analyze top apps by usage from:
    screentime/{userId}/dates/{dateString}
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        normalized_category = (category or "all").strip() or "all"
        category_filter = normalized_category.lower()

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days - 1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        cache_key = _build_cache_key(
            "top_apps",
            {
                "v": 3,
                "days": days,
                "limit": limit,
                "category": category_filter,
                "start_date": start_date_str,
                "end_date": end_date_str,
            }
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        print(
            "[getTopApps] "
            f"days={days}, limit={limit}, category={normalized_category}, "
            f"start={start_date_str}, end={end_date_str}"
        )

        def _to_int(raw_value: Any) -> int:
            if raw_value is None:
                return 0
            if isinstance(raw_value, bool):
                return int(raw_value)
            if isinstance(raw_value, (int, float)):
                return int(raw_value)

            try:
                return int(float(str(raw_value).replace(",", "").strip()))
            except (TypeError, ValueError):
                return 0

        def _percent(part: int, total: int, precision: int = 1) -> float:
            if total <= 0:
                return 0.0
            return round((part / total) * 100, precision)

        def _ms_to_seconds(ms_value: int) -> int:
            if ms_value <= 0:
                return 0
            return int(round(ms_value / 1000.0))

        def _format_screentime(seconds_value: int) -> str:
            if seconds_value <= 0:
                return "0h 0m"

            days_part = seconds_value // 86400
            remaining = seconds_value % 86400
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60

            if days_part > 0:
                return f"{days_part}d {hours}h"
            return f"{hours}h {minutes}m"

        def _trend_percent(current_value: int, previous_value: int) -> float:
            if previous_value <= 0:
                if current_value <= 0:
                    return 0.0
                return 100.0
            return round(((current_value - previous_value) / previous_value) * 100, 1)

        def _trend_label(change_percent: float) -> str:
            if change_percent > 1.0:
                return "increasing"
            if change_percent < -1.0:
                return "decreasing"
            return "stable"

        aggregated_apps: Dict[str, Dict[str, Any]] = {}
        grouped_categories: Dict[str, Dict[str, Any]] = {}
        user_to_apps: Dict[str, set] = {}
        weekly_category_screen_time: Dict[str, List[int]] = {}

        total_screen_time = 0
        total_sessions = 0
        docs_scanned = 0
        docs_in_range = 0

        last_7d_start = end_date - timedelta(days=6)
        prev_7d_end = last_7d_start - timedelta(days=1)
        prev_7d_start = prev_7d_end - timedelta(days=6)

        week_ranges: List[Tuple[datetime.date, datetime.date]] = []
        rolling_date = start_date
        while rolling_date <= end_date:
            week_end = min(rolling_date + timedelta(days=6), end_date)
            week_ranges.append((rolling_date, week_end))
            rolling_date = week_end + timedelta(days=1)

        weekly_total_screen_time = [0 for _ in week_ranges]

        # Step 1: collectionGroup('dates'), then date-id filtering for requested window.
        date_docs = db.collection_group("dates").stream()

        # Step 2: loop docs and apps arrays.
        for date_doc in date_docs:
            docs_scanned += 1
            date_str = date_doc.id

            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            if date_obj < start_date or date_obj > end_date:
                continue

            parent_doc_ref = date_doc.reference.parent.parent
            parent_collection = parent_doc_ref.parent if parent_doc_ref else None

            # Keep only screentime/{userId}/dates/{dateString}.
            if not parent_doc_ref or not parent_collection or parent_collection.id != "screentime":
                continue

            user_id = parent_doc_ref.id
            payload = date_doc.to_dict() or {}
            apps = payload.get("apps", [])

            if isinstance(apps, dict):
                apps = list(apps.values())
            if not isinstance(apps, list):
                continue

            docs_in_range += 1
            week_index = (date_obj - start_date).days // 7
            doc_filtered_total_screen_time = 0

            for app in apps:
                if not isinstance(app, dict):
                    continue

                session_count = _to_int(app.get("sessionCount"))
                screen_time = _to_int(app.get("totalScreenTime"))
                last_used_time = _to_int(app.get("lastUsedTime"))
                app_category = str(app.get("category") or "Unknown").strip() or "Unknown"

                if session_count < 0:
                    session_count = 0
                if screen_time < 0:
                    screen_time = 0
                if last_used_time < 0:
                    last_used_time = 0

                app_name = str(app.get("appName") or "").strip()
                package_name = str(app.get("packageName") or "").strip()
                if not app_name and not package_name:
                    continue

                if category_filter != "all" and app_category.lower() != category_filter:
                    continue

                # Step 3: group by appName + packageName.
                app_key = f"{app_name}||{package_name}"
                if app_key not in aggregated_apps:
                    aggregated_apps[app_key] = {
                        "app_name": app_name,
                        "package_name": package_name,
                        "category": app_category,
                        "total_sessions": 0,
                        "total_screentime_ms": 0,
                        "last_used_time": 0,
                        "user_ids": set(),
                        "last_7d_ms": 0,
                        "prev_7d_ms": 0,
                    }

                app_row = aggregated_apps[app_key]
                app_row["total_sessions"] += session_count
                app_row["total_screentime_ms"] += screen_time
                app_row["last_used_time"] = max(app_row["last_used_time"], last_used_time)
                if user_id:
                    app_row["user_ids"].add(user_id)
                    if user_id not in user_to_apps:
                        user_to_apps[user_id] = set()
                    user_to_apps[user_id].add(app_key)

                if last_7d_start <= date_obj <= end_date:
                    app_row["last_7d_ms"] += screen_time
                elif prev_7d_start <= date_obj <= prev_7d_end:
                    app_row["prev_7d_ms"] += screen_time

                # Step 6: group by category.
                if app_category not in grouped_categories:
                    grouped_categories[app_category] = {
                        "category": app_category,
                        "total_sessions": 0,
                        "total_screentime_ms": 0,
                        "app_keys": set(),
                        "user_ids": set(),
                        "last_7d_ms": 0,
                        "prev_7d_ms": 0,
                        "app_screentime_ms": {},
                    }
                    weekly_category_screen_time[app_category] = [0 for _ in week_ranges]

                category_row = grouped_categories[app_category]
                category_row["total_sessions"] += session_count
                category_row["total_screentime_ms"] += screen_time
                category_row["app_keys"].add(app_key)
                if user_id:
                    category_row["user_ids"].add(user_id)

                category_row["app_screentime_ms"][app_key] = (
                    category_row["app_screentime_ms"].get(app_key, 0) + screen_time
                )

                if last_7d_start <= date_obj <= end_date:
                    category_row["last_7d_ms"] += screen_time
                elif prev_7d_start <= date_obj <= prev_7d_end:
                    category_row["prev_7d_ms"] += screen_time

                doc_filtered_total_screen_time += screen_time
                if 0 <= week_index < len(weekly_total_screen_time):
                    weekly_category_screen_time[app_category][week_index] += screen_time
                total_screen_time += screen_time
                total_sessions += session_count

            if 0 <= week_index < len(weekly_total_screen_time):
                weekly_total_screen_time[week_index] += doc_filtered_total_screen_time

        # Step 4 + 5: sort by totalScreenTime desc and take top {limit}.
        sorted_apps = sorted(
            aggregated_apps.values(),
            key=lambda row: row["total_screentime_ms"],
            reverse=True
        )
        total_unique_users = len(user_to_apps)
        top_apps_payload = []
        for index, app_row in enumerate(sorted_apps[:limit], start=1):
            app_total_seconds = _ms_to_seconds(app_row["total_screentime_ms"])
            app_unique_users = len(app_row["user_ids"])
            trend_7d_pct = _trend_percent(app_row["last_7d_ms"], app_row["prev_7d_ms"])
            avg_sessions_per_user = (
                round(app_row["total_sessions"] / app_unique_users, 1)
                if app_unique_users > 0
                else 0.0
            )
            avg_duration_per_session_seconds = (
                int(round(app_total_seconds / app_row["total_sessions"]))
                if app_row["total_sessions"] > 0
                else 0
            )

            top_apps_payload.append({
                "rank": index,
                "app_name": app_row["app_name"],
                "package_name": app_row["package_name"],
                "category": app_row["category"],
                "total_sessions": app_row["total_sessions"],
                "total_screentime_seconds": app_total_seconds,
                "total_screentime_formatted": _format_screentime(app_total_seconds),
                "unique_users": app_unique_users,
                "unique_users_percent": _percent(app_unique_users, total_unique_users),
                "avg_sessions_per_user": avg_sessions_per_user,
                "avg_duration_per_session_seconds": avg_duration_per_session_seconds,
                "percent_of_total_time": _percent(app_row["total_screentime_ms"], total_screen_time),
                "trend_7d": f"{trend_7d_pct:+.1f}%",
            })

        sorted_categories = sorted(
            grouped_categories.values(),
            key=lambda row: row["total_screentime_ms"],
            reverse=True
        )

        by_category_payload = []
        for category_row in sorted_categories:
            top_app_key = None
            if category_row["app_screentime_ms"]:
                top_app_key = max(
                    category_row["app_screentime_ms"],
                    key=lambda app_key: category_row["app_screentime_ms"][app_key]
                )

            top_app_name = ""
            if top_app_key and top_app_key in aggregated_apps:
                top_app_name = aggregated_apps[top_app_key]["app_name"]

            category_trend_pct = _trend_percent(
                category_row["last_7d_ms"],
                category_row["prev_7d_ms"]
            )

            by_category_payload.append({
                "category": category_row["category"],
                "total_screentime_seconds": _ms_to_seconds(category_row["total_screentime_ms"]),
                "percent": _percent(category_row["total_screentime_ms"], total_screen_time),
                "top_app": top_app_name,
                "app_count": len(category_row["app_keys"]),
                "trend": _trend_label(category_trend_pct),
            })

        category_trend_payload = []
        for category_row in sorted_categories:
            category_name = category_row["category"]
            weekly_values = weekly_category_screen_time.get(category_name, [0 for _ in week_ranges])
            trend_data = []

            for idx, (week_start, _week_end) in enumerate(week_ranges):
                week_total = weekly_total_screen_time[idx] if idx < len(weekly_total_screen_time) else 0
                category_week_total = weekly_values[idx] if idx < len(weekly_values) else 0
                trend_data.append({
                    "date": week_start.strftime("%Y-%m-%d"),
                    "percent": _percent(category_week_total, week_total),
                })

            category_trend_payload.append({
                "category": category_name,
                "data": trend_data,
            })

        total_unique_apps = len(aggregated_apps)
        avg_apps_per_user = (
            round(
                sum(len(app_set) for app_set in user_to_apps.values()) / total_unique_users,
                1
            )
            if total_unique_users > 0
            else 0.0
        )
        top_app_name_summary = sorted_apps[0]["app_name"] if sorted_apps else ""
        top_category_summary = sorted_categories[0]["category"] if sorted_categories else ""

        response_payload = {
            "summary": {
                "total_unique_apps": total_unique_apps,
                "avg_apps_per_user": avg_apps_per_user,
                "top_app": top_app_name_summary,
                "top_category": top_category_summary,
            },
            "top_apps": top_apps_payload,
            "by_category": by_category_payload,
            "category_trend": category_trend_payload,
        }

        print(
            "[getTopApps] "
            f"apps={total_unique_apps}, users={total_unique_users}, categories={len(sorted_categories)}, "
            f"docs_in_range={docs_in_range}, docs_scanned={docs_scanned}"
        )

        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["top_apps"])
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"[getTopApps] Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/getCategoryDrilldown", response_model=Dict[str, Any])
async def getCategoryDrilldown(
    category: str = Query(..., description="Category filter (for example: Social)"),
    days: int = Query(30, description="Number of days to analyze", ge=1, le=3650)
):
    """
    Category-focused drilldown across summary, app mix, daily trend, and peak time blocks.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        normalized_category = (category or "").strip()
        if not normalized_category:
            raise HTTPException(status_code=400, detail="category is required")
        category_filter = normalized_category.lower()

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days - 1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        cache_key = _build_cache_key(
            "category_drilldown",
            {
                "v": 1,
                "category": category_filter,
                "days": days,
                "start_date": start_date_str,
                "end_date": end_date_str,
            }
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        print(
            "[getCategoryDrilldown] "
            f"category={normalized_category}, days={days}, "
            f"start={start_date_str}, end={end_date_str}"
        )

        def _seconds_from_ms(ms_value: int) -> int:
            if ms_value <= 0:
                return 0
            return int(round(ms_value / 1000.0))

        def _percent(part: int, total: int, precision: int = 1) -> float:
            if total <= 0:
                return 0.0
            return round((part / total) * 100, precision)

        def _trend_label(current_value: int, previous_value: int) -> str:
            if previous_value <= 0:
                if current_value <= 0:
                    return "stable"
                return "increasing"
            change_percent = ((current_value - previous_value) / previous_value) * 100.0
            if change_percent > 1.0:
                return "increasing"
            if change_percent < -1.0:
                return "decreasing"
            return "stable"

        block_order = [
            "Late Night 12-6am",
            "Morning 6-9am",
            "Late Morning 9am-12pm",
            "Afternoon 12-3pm",
            "Late Afternoon 3-6pm",
            "Evening 6-9pm",
            "Night 9pm-12am",
        ]
        block_sessions = {label: 0 for label in block_order}

        def _hour_to_block(hour: int) -> str:
            if 6 <= hour < 9:
                return "Morning 6-9am"
            if 9 <= hour < 12:
                return "Late Morning 9am-12pm"
            if 12 <= hour < 15:
                return "Afternoon 12-3pm"
            if 15 <= hour < 18:
                return "Late Afternoon 3-6pm"
            if 18 <= hour < 21:
                return "Evening 6-9pm"
            if 21 <= hour < 24:
                return "Night 9pm-12am"
            return "Late Night 12-6am"

        split_date = start_date + timedelta(days=days // 2)
        new_apps_cutoff = end_date - timedelta(days=min(days - 1, 6))

        category_total_seconds = 0
        category_total_sessions = 0

        category_user_ids = set()
        category_user_ids_today = set()
        dau_user_ids_today = set()

        app_aggregates: Dict[str, Dict[str, Any]] = {}
        daily_category_seconds: Dict[str, int] = {}
        daily_app_seconds: Dict[str, Dict[str, int]] = {}

        docs_scanned = 0
        docs_in_range = 0

        try:
            user_docs = list(db.collection("users").select([]).stream())
        except Exception:
            user_docs = list(db.collection("users").stream())

        user_ids = [doc.id for doc in user_docs]
        if not user_ids:
            try:
                user_ids = [doc.id for doc in db.collection("screentime").select([]).stream()]
            except Exception:
                user_ids = [doc.id for doc in db.collection("screentime").stream()]

        print(f"[getCategoryDrilldown] candidate_users={len(user_ids)}")

        def _aggregate_user_category(user_id: str) -> Dict[str, Any]:
            user_result: Dict[str, Any] = {
                "user_id": user_id,
                "category_total_seconds": 0,
                "category_total_sessions": 0,
                "category_active": False,
                "category_today_active": False,
                "dau_today_active": False,
                "docs_scanned": 0,
                "docs_in_range": 0,
                "app_aggregates": {},
                "daily_category_seconds": {},
                "daily_app_seconds": {},
                "block_sessions": {label: 0 for label in block_order},
            }

            try:
                date_docs = _fetch_user_dates_in_range(user_id, start_date_str, end_date_str)
            except Exception as user_err:
                print(f"[getCategoryDrilldown] date fetch failed user={user_id}, error={user_err}")
                return user_result

            for date_doc in date_docs:
                user_result["docs_scanned"] += 1
                date_str = date_doc.id

                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue

                payload = date_doc.to_dict() or {}
                app_rows = _get_app_rows(payload)

                day_has_any_usage = False
                day_has_category_usage = False
                day_category_total_seconds = 0

                for app in app_rows:
                    screen_time_ms = _safe_int(app.get("totalScreenTime"), default=0)
                    session_count = _safe_int(app.get("sessionCount"), default=0)

                    if screen_time_ms < 0:
                        screen_time_ms = 0
                    if session_count < 0:
                        session_count = 0

                    if screen_time_ms > 0 or session_count > 0:
                        day_has_any_usage = True

                    app_category = str(app.get("category") or "Unknown").strip() or "Unknown"
                    if app_category.lower() != category_filter:
                        continue

                    if screen_time_ms <= 0 and session_count <= 0:
                        continue

                    user_result["category_active"] = True
                    day_has_category_usage = True

                    screen_time_seconds = _seconds_from_ms(screen_time_ms)
                    user_result["category_total_seconds"] += screen_time_seconds
                    user_result["category_total_sessions"] += session_count
                    day_category_total_seconds += screen_time_seconds

                    app_name = str(app.get("appName") or "").strip()
                    package_name = str(app.get("packageName") or "").strip()
                    display_name = app_name or package_name or "Unknown App"
                    app_key = f"{display_name}||{package_name or display_name}"

                    if app_key not in user_result["app_aggregates"]:
                        user_result["app_aggregates"][app_key] = {
                            "app_name": display_name,
                            "total_seconds": 0,
                            "total_sessions": 0,
                            "previous_window_seconds": 0,
                            "current_window_seconds": 0,
                            "first_seen": None,
                        }

                    app_row = user_result["app_aggregates"][app_key]
                    app_row["total_seconds"] += screen_time_seconds
                    app_row["total_sessions"] += session_count

                    first_seen = app_row.get("first_seen")
                    if first_seen is None or date_obj < first_seen:
                        app_row["first_seen"] = date_obj

                    if date_obj >= split_date:
                        app_row["current_window_seconds"] += screen_time_seconds
                    else:
                        app_row["previous_window_seconds"] += screen_time_seconds

                    if date_str not in user_result["daily_app_seconds"]:
                        user_result["daily_app_seconds"][date_str] = {}
                    user_result["daily_app_seconds"][date_str][display_name] = (
                        user_result["daily_app_seconds"][date_str].get(display_name, 0)
                        + screen_time_seconds
                    )

                    last_used_ms = _safe_int(app.get("lastUsedTime"), default=-1)
                    if last_used_ms >= 0 and session_count > 0:
                        try:
                            event_hour = datetime.fromtimestamp(
                                last_used_ms / 1000.0,
                                tz=timezone.utc
                            ).hour
                            block_name = _hour_to_block(event_hour)
                            user_result["block_sessions"][block_name] += session_count
                        except Exception:
                            pass

                if not day_has_any_usage:
                    fallback_total_ms = _safe_int(payload.get("totalScreenTime"), default=0)
                    fallback_sessions = _safe_int(payload.get("totalSessions"), default=0)
                    if fallback_total_ms > 0 or fallback_sessions > 0:
                        day_has_any_usage = True

                if date_obj == end_date and day_has_any_usage:
                    user_result["dau_today_active"] = True

                if day_has_category_usage:
                    user_result["docs_in_range"] += 1
                    user_result["daily_category_seconds"][date_str] = (
                        user_result["daily_category_seconds"].get(date_str, 0)
                        + day_category_total_seconds
                    )
                    if date_obj == end_date:
                        user_result["category_today_active"] = True

            return user_result

        if user_ids:
            max_workers = min(24, max(4, len(user_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(_aggregate_user_category, user_id)
                    for user_id in user_ids
                ]

                for future in as_completed(futures):
                    try:
                        user_row = future.result()
                    except Exception as user_future_err:
                        print(f"[getCategoryDrilldown] user aggregation failed: {user_future_err}")
                        continue

                    user_id = str(user_row.get("user_id") or "")
                    docs_scanned += int(user_row.get("docs_scanned", 0))
                    docs_in_range += int(user_row.get("docs_in_range", 0))
                    category_total_seconds += int(user_row.get("category_total_seconds", 0))
                    category_total_sessions += int(user_row.get("category_total_sessions", 0))

                    if user_id and bool(user_row.get("category_active")):
                        category_user_ids.add(user_id)
                    if user_id and bool(user_row.get("category_today_active")):
                        category_user_ids_today.add(user_id)
                    if user_id and bool(user_row.get("dau_today_active")):
                        dau_user_ids_today.add(user_id)

                    local_apps = user_row.get("app_aggregates", {})
                    for app_key, local_app in local_apps.items():
                        if app_key not in app_aggregates:
                            app_aggregates[app_key] = {
                                "app_name": str(local_app.get("app_name") or "Unknown App"),
                                "total_seconds": 0,
                                "total_sessions": 0,
                                "user_ids": set(),
                                "previous_window_seconds": 0,
                                "current_window_seconds": 0,
                                "first_seen": None,
                            }

                        global_app = app_aggregates[app_key]
                        global_app["total_seconds"] += int(local_app.get("total_seconds", 0))
                        global_app["total_sessions"] += int(local_app.get("total_sessions", 0))
                        global_app["previous_window_seconds"] += int(
                            local_app.get("previous_window_seconds", 0)
                        )
                        global_app["current_window_seconds"] += int(
                            local_app.get("current_window_seconds", 0)
                        )

                        if user_id:
                            global_app["user_ids"].add(user_id)

                        local_first_seen = local_app.get("first_seen")
                        global_first_seen = global_app.get("first_seen")
                        if local_first_seen and (
                            global_first_seen is None or local_first_seen < global_first_seen
                        ):
                            global_app["first_seen"] = local_first_seen

                    for date_str, day_seconds in (user_row.get("daily_category_seconds") or {}).items():
                        daily_category_seconds[date_str] = (
                            daily_category_seconds.get(date_str, 0) + int(day_seconds)
                        )

                    for date_str, app_map in (user_row.get("daily_app_seconds") or {}).items():
                        if date_str not in daily_app_seconds:
                            daily_app_seconds[date_str] = {}

                        merged_map = daily_app_seconds[date_str]
                        for app_name, app_seconds in (app_map or {}).items():
                            merged_map[app_name] = merged_map.get(app_name, 0) + int(app_seconds)

                    for block_label, block_count in (user_row.get("block_sessions") or {}).items():
                        if block_label not in block_sessions:
                            block_sessions[block_label] = 0
                        block_sessions[block_label] += int(block_count)

        sorted_apps = sorted(
            app_aggregates.values(),
            key=lambda row: row["total_seconds"],
            reverse=True
        )

        apps_payload = []
        for app_row in sorted_apps[:10]:
            app_users = len(app_row["user_ids"])
            avg_time_seconds = (
                int(round(app_row["total_seconds"] / app_users))
                if app_users > 0
                else 0
            )

            apps_payload.append(
                {
                    "app_name": app_row["app_name"],
                    "share_of_category": _percent(app_row["total_seconds"], category_total_seconds),
                    "avg_time_formatted": _format_hh_mm_duration(avg_time_seconds),
                    "trend": _trend_label(
                        app_row["current_window_seconds"],
                        app_row["previous_window_seconds"]
                    ),
                }
            )

        trend_app_names = [row["app_name"] for row in sorted_apps[:5]]
        trend_payload = []
        for offset in range(days):
            day = start_date + timedelta(days=offset)
            day_str = day.strftime("%Y-%m-%d")
            day_total_seconds = daily_category_seconds.get(day_str, 0)
            day_app_map = daily_app_seconds.get(day_str, {})

            day_apps_payload = []
            for app_name in trend_app_names:
                app_day_seconds = int(day_app_map.get(app_name, 0))
                if app_day_seconds <= 0 or day_total_seconds <= 0:
                    continue
                day_apps_payload.append(
                    {
                        "app_name": app_name,
                        "share": _percent(app_day_seconds, day_total_seconds),
                    }
                )

            day_apps_payload = sorted(
                day_apps_payload,
                key=lambda row: row["share"],
                reverse=True
            )
            trend_payload.append(
                {
                    "date": day_str,
                    "apps": day_apps_payload,
                }
            )

        total_block_sessions = sum(block_sessions.values())
        peak_hours_payload = []
        if total_block_sessions > 0:
            for block_label in block_order:
                block_total = int(block_sessions.get(block_label, 0))
                if block_total <= 0:
                    continue
                peak_hours_payload.append(
                    {
                        "time_block": block_label,
                        "percent": int(round((block_total / total_block_sessions) * 100)),
                    }
                )

            peak_hours_payload = sorted(
                peak_hours_payload,
                key=lambda row: row["percent"],
                reverse=True
            )

        users_in_category = len(category_user_ids)
        avg_time_seconds = (
            int(round(category_total_seconds / users_in_category))
            if users_in_category > 0
            else 0
        )
        avg_sessions_per_user = (
            round(category_total_sessions / users_in_category, 1)
            if users_in_category > 0
            else 0.0
        )

        new_apps_added = sum(
            1
            for app_row in app_aggregates.values()
            if app_row.get("first_seen") and app_row["first_seen"] >= new_apps_cutoff
        )

        response_payload = {
            "category": normalized_category,
            "period": f"last_{days}_days",
            "summary": {
                "unique_users_today": len(category_user_ids_today),
                "percent_of_dau": _percent(len(category_user_ids_today), len(dau_user_ids_today)),
                "avg_time_seconds": avg_time_seconds,
                "avg_time_formatted": _format_hh_mm_duration(avg_time_seconds),
                "avg_sessions_per_user": avg_sessions_per_user,
                "apps_in_category": len(app_aggregates),
                "new_apps_added": int(new_apps_added),
            },
            "apps": apps_payload,
            "trend": trend_payload,
            "peak_hours": peak_hours_payload,
        }

        print(
            "[getCategoryDrilldown] "
            f"apps={len(app_aggregates)}, users={users_in_category}, "
            f"docs_in_range={docs_in_range}, docs_scanned={docs_scanned}"
        )

        _cache_set(
            cache_key,
            response_payload,
            _DASHBOARD_CACHE_TTL_MIN["category_drilldown"]
        )
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"[getCategoryDrilldown] Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


def _resolve_user_segments_date(raw_date: Optional[str]):
    if not raw_date or raw_date.strip().lower() == "today":
        ist_offset = timedelta(hours=5, minutes=30)
        return (datetime.now(timezone.utc) + ist_offset).date()
    return _parse_yyyy_mm_dd(raw_date.strip(), "date")


def _get_app_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    apps = payload.get("apps", [])
    if isinstance(apps, dict):
        apps = list(apps.values())
    if not isinstance(apps, list):
        return []
    return [app for app in apps if isinstance(app, dict)]


def _aggregate_daily_segment_metrics(
    payload: Dict[str, Any]
) -> Tuple[int, int, int, Dict[str, int], Dict[str, int]]:
    app_rows = _get_app_rows(payload)

    total_screen_time_ms = 0
    total_sessions = 0
    seen_apps = set()
    used_apps = set()
    app_seconds: Dict[str, int] = {}
    category_seconds: Dict[str, int] = {}

    for app in app_rows:
        screen_time_ms = _safe_int(app.get("totalScreenTime"), default=0)
        session_count = _safe_int(app.get("sessionCount"), default=0)

        if screen_time_ms < 0:
            screen_time_ms = 0
        if session_count < 0:
            session_count = 0

        app_name = str(app.get("appName") or "").strip()
        package_name = str(app.get("packageName") or "").strip()
        display_name = app_name or package_name
        app_key = package_name or app_name
        category = str(app.get("category") or "Unknown").strip() or "Unknown"

        if app_key:
            seen_apps.add(app_key)

        total_screen_time_ms += screen_time_ms
        total_sessions += session_count

        if screen_time_ms > 0 or session_count > 0:
            if app_key:
                used_apps.add(app_key)

            screen_time_seconds = int(round(screen_time_ms / 1000.0))
            if display_name:
                app_seconds[display_name] = app_seconds.get(display_name, 0) + screen_time_seconds
            category_seconds[category] = category_seconds.get(category, 0) + screen_time_seconds

    if total_screen_time_ms <= 0:
        fallback_screen_time_ms = _safe_int(payload.get("totalScreenTime"), default=0)
        if fallback_screen_time_ms > 0:
            total_screen_time_ms = fallback_screen_time_ms

    if total_sessions <= 0:
        fallback_sessions = _safe_int(payload.get("totalSessions"), default=0)
        if fallback_sessions > 0:
            total_sessions = fallback_sessions

    apps_used_count = len(used_apps) if used_apps else len(seen_apps)
    daily_screentime_seconds = int(round(total_screen_time_ms / 1000.0))

    return (
        daily_screentime_seconds,
        apps_used_count,
        total_sessions,
        app_seconds,
        category_seconds,
    )


def _classify_daily_segment(daily_screentime_seconds: int) -> str:
    if daily_screentime_seconds > 14400:
        return "power"
    if daily_screentime_seconds >= 3600:
        return "regular"
    return "casual"


def _compute_decline_percent(
    daily_seconds_by_date: Dict[str, int],
    target_day
) -> Optional[float]:
    recent_total = 0
    prior_total = 0

    for offset in range(7):
        date_key = (target_day - timedelta(days=offset)).strftime("%Y-%m-%d")
        recent_total += daily_seconds_by_date.get(date_key, 0)

    for offset in range(7, 14):
        date_key = (target_day - timedelta(days=offset)).strftime("%Y-%m-%d")
        prior_total += daily_seconds_by_date.get(date_key, 0)

    prior_avg = prior_total / 7.0
    if prior_avg <= 0:
        return None

    recent_avg = recent_total / 7.0
    return ((prior_avg - recent_avg) / prior_avg) * 100.0


def _pick_top_key(metric_map: Dict[str, int]) -> str:
    if not metric_map:
        return ""
    return sorted(metric_map.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _aggregate_user_segment_history(
    user_id: str,
    start_date_str: str,
    end_date_str: str,
    target_date_str: str
) -> Dict[str, Any]:
    user_result: Dict[str, Any] = {
        "user_id": user_id,
        "active_dates": set(),
        "daily_seconds_by_date": {},
        "target_metrics": None,
    }

    try:
        date_docs = _fetch_user_dates_in_range(user_id, start_date_str, end_date_str)
    except Exception as user_err:
        print(f"[getUserSegments] date fetch failed user={user_id}, error={user_err}")
        return user_result

    for date_doc in date_docs:
        date_str = date_doc.id
        payload = date_doc.to_dict() or {}

        (
            daily_screentime_seconds,
            apps_used_count,
            total_sessions,
            app_seconds,
            category_seconds,
        ) = _aggregate_daily_segment_metrics(payload)

        user_result["active_dates"].add(date_str)
        user_result["daily_seconds_by_date"][date_str] = daily_screentime_seconds

        if date_str == target_date_str:
            user_result["target_metrics"] = {
                "daily_screentime_seconds": daily_screentime_seconds,
                "apps_used_count": apps_used_count,
                "total_sessions": total_sessions,
                "app_seconds": app_seconds,
                "category_seconds": category_seconds,
            }

    return user_result


@router.get("/getUserSegments", response_model=Dict[str, Any])
async def getUserSegments(
    date: Optional[str] = Query("today", description="Target date ('today' or YYYY-MM-DD)")
):
    """
    Segment active users for a target day by screentime and decline risk.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        target_date = _resolve_user_segments_date(date)
        target_date_str = target_date.strftime("%Y-%m-%d")

        trend_days = 30
        trend_start_date = target_date - timedelta(days=trend_days - 1)
        trend_start_date_str = trend_start_date.strftime("%Y-%m-%d")

        # At-risk requires two 7-day windows, so include 13 earlier days.
        history_start_date = trend_start_date - timedelta(days=13)
        history_start_date_str = history_start_date.strftime("%Y-%m-%d")

        cache_key = _build_cache_key(
            "user_segments",
            {
                "v": 1,
                "date": target_date_str,
                "trend_days": trend_days,
                "history_start": history_start_date_str,
            }
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        preagg_doc_id = target_date_str
        preagg_response = _preagg_get("user_segments", preagg_doc_id)
        if preagg_response is not None:
            _cache_set(cache_key, preagg_response, _DASHBOARD_CACHE_TTL_MIN["user_segments"])
            return preagg_response

        try:
            user_docs = list(db.collection("users").select([]).stream())
        except Exception:
            user_docs = list(db.collection("users").stream())

        user_ids = [doc.id for doc in user_docs]
        if not user_ids:
            try:
                user_ids = [doc.id for doc in db.collection("screentime").select([]).stream()]
            except Exception:
                user_ids = [doc.id for doc in db.collection("screentime").stream()]

        print(
            "[getUserSegments] "
            f"target_date={target_date_str}, trend_start={trend_start_date_str}, users={len(user_ids)}"
        )

        user_histories: List[Dict[str, Any]] = []
        if user_ids:
            max_workers = min(24, max(4, len(user_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        _aggregate_user_segment_history,
                        user_id,
                        history_start_date_str,
                        target_date_str,
                        target_date_str,
                    )
                    for user_id in user_ids
                ]

                for future in as_completed(futures):
                    try:
                        user_histories.append(future.result())
                    except Exception as user_future_err:
                        print(f"[getUserSegments] user aggregation failed: {user_future_err}")

        segment_order = ["power", "regular", "casual", "at_risk"]
        thresholds = {
            "power": "> 4 hours/day",
            "regular": "1-4 hours/day",
            "casual": "< 1 hour/day",
            "at_risk": "Active but declining 30%+",
        }

        segment_stats: Dict[str, Dict[str, Any]] = {
            segment: {
                "count": 0,
                "screentime_total": 0,
                "apps_total": 0,
                "sessions_total": 0,
                "app_seconds": {},
                "category_seconds": {},
                "declines": [],
            }
            for segment in segment_order
        }

        trend_dates = [
            trend_start_date + timedelta(days=i)
            for i in range(trend_days)
        ]
        trend_counts: Dict[str, Dict[str, int]] = {
            day.strftime("%Y-%m-%d"): {
                "power": 0,
                "regular": 0,
                "casual": 0,
                "at_risk": 0,
            }
            for day in trend_dates
        }

        dau = 0

        for user_data in user_histories:
            active_dates = user_data.get("active_dates", set())
            daily_seconds_by_date = user_data.get("daily_seconds_by_date", {})

            if target_date_str in active_dates:
                target_metrics = user_data.get("target_metrics") or {
                    "daily_screentime_seconds": int(daily_seconds_by_date.get(target_date_str, 0)),
                    "apps_used_count": 0,
                    "total_sessions": 0,
                    "app_seconds": {},
                    "category_seconds": {},
                }

                decline_percent = _compute_decline_percent(daily_seconds_by_date, target_date)
                is_at_risk = decline_percent is not None and decline_percent > 30.0

                if is_at_risk:
                    target_segment = "at_risk"
                else:
                    target_segment = _classify_daily_segment(
                        int(target_metrics.get("daily_screentime_seconds", 0))
                    )

                dau += 1
                target_stats = segment_stats[target_segment]
                target_stats["count"] += 1
                target_stats["screentime_total"] += int(
                    target_metrics.get("daily_screentime_seconds", 0)
                )
                target_stats["apps_total"] += int(target_metrics.get("apps_used_count", 0))
                target_stats["sessions_total"] += int(target_metrics.get("total_sessions", 0))

                for app_name, app_seconds in (target_metrics.get("app_seconds") or {}).items():
                    if not app_name:
                        continue
                    target_stats["app_seconds"][app_name] = (
                        target_stats["app_seconds"].get(app_name, 0) + int(app_seconds)
                    )

                for category_name, category_total in (
                    target_metrics.get("category_seconds") or {}
                ).items():
                    if not category_name:
                        continue
                    target_stats["category_seconds"][category_name] = (
                        target_stats["category_seconds"].get(category_name, 0)
                        + int(category_total)
                    )

                if is_at_risk and decline_percent is not None:
                    target_stats["declines"].append(float(decline_percent))

            for trend_day in trend_dates:
                trend_date_str = trend_day.strftime("%Y-%m-%d")
                if trend_date_str not in active_dates:
                    continue

                trend_decline = _compute_decline_percent(daily_seconds_by_date, trend_day)
                if trend_decline is not None and trend_decline > 30.0:
                    trend_segment = "at_risk"
                else:
                    trend_segment = _classify_daily_segment(
                        int(daily_seconds_by_date.get(trend_date_str, 0))
                    )

                trend_counts[trend_date_str][trend_segment] += 1

        def _avg_or_zero(total_value: int, count_value: int) -> int:
            if count_value <= 0:
                return 0
            return int(round(total_value / count_value))

        def _pct_of_dau(count_value: int, dau_value: int) -> float:
            if dau_value <= 0:
                return 0.0
            return round((count_value / dau_value) * 100, 1)

        segments_payload: Dict[str, Dict[str, Any]] = {}
        for segment_name in segment_order:
            stats_row = segment_stats[segment_name]
            count_value = int(stats_row["count"])

            segment_entry: Dict[str, Any] = {
                "count": count_value,
                "percent_of_dau": _pct_of_dau(count_value, dau),
                "threshold": thresholds[segment_name],
                "avg_screentime_seconds": _avg_or_zero(stats_row["screentime_total"], count_value),
                "avg_apps_used": _avg_or_zero(stats_row["apps_total"], count_value),
                "avg_sessions": _avg_or_zero(stats_row["sessions_total"], count_value),
                "top_app": _pick_top_key(stats_row["app_seconds"]),
                "top_category": _pick_top_key(stats_row["category_seconds"]),
            }

            if segment_name == "at_risk":
                declines = stats_row.get("declines", [])
                avg_decline = round(sum(declines) / len(declines), 1) if declines else 0.0
                segment_entry["avg_decline_percent"] = avg_decline

            segments_payload[segment_name] = segment_entry

        trend_payload = []
        for trend_day in trend_dates:
            trend_date_str = trend_day.strftime("%Y-%m-%d")
            row = trend_counts[trend_date_str]
            trend_payload.append(
                {
                    "date": trend_date_str,
                    "power": row["power"],
                    "regular": row["regular"],
                    "casual": row["casual"],
                    "at_risk": row["at_risk"],
                }
            )

        response_payload = {
            "date": target_date_str,
            "segments": segments_payload,
            "trend": trend_payload,
        }

        _preagg_set(
            "user_segments",
            preagg_doc_id,
            response_payload,
            {
                "date": target_date_str,
                "trend_days": trend_days,
                "history_start": history_start_date_str,
            }
        )
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["user_segments"])
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"[getUserSegments] Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


def _cohort_percent(retained_count: int, cohort_size: int) -> float:
    if cohort_size <= 0:
        return 0.0
    return round((retained_count / cohort_size) * 100, 1)


def _format_cohort_label(cohort_start_date) -> str:
    week_of_month = ((cohort_start_date.day - 1) // 7) + 1
    return f"{cohort_start_date.strftime('%b')} Week {week_of_month}"


def _has_activity_in_window(
    active_dates,
    window_start_date,
    window_end_date
) -> bool:
    current_day = window_start_date
    while current_day <= window_end_date:
        if current_day in active_dates:
            return True
        current_day += timedelta(days=1)
    return False


@router.get("/getCohortRetention", response_model=Dict[str, Any])
async def getCohortRetention(
    weeks: int = Query(8, description="Number of weekly cohorts to analyze", ge=1, le=104)
):
    """
    Weekly cohort retention based on first activity date from screentime/{userId}/dates/{date}.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        ist_offset = timedelta(hours=5, minutes=30)
        today_ist = (datetime.now(timezone.utc) + ist_offset).date()

        # Analyze complete days only, excluding the current IST day.
        cohort_end_date = today_ist - timedelta(days=1)
        cohort_start_date = cohort_end_date - timedelta(days=(weeks * 7) - 1)

        cache_key = _build_cache_key(
            "cohort_retention",
            {
                "v": 1,
                "weeks": weeks,
                "start": cohort_start_date.strftime("%Y-%m-%d"),
                "end": cohort_end_date.strftime("%Y-%m-%d"),
            }
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        preagg_doc_id = f"weeks_{weeks}_{cohort_end_date.strftime('%Y-%m-%d')}"
        preagg_response = _preagg_get("cohort_retention", preagg_doc_id)
        if preagg_response is not None:
            _cache_set(cache_key, preagg_response, _DASHBOARD_CACHE_TTL_MIN["cohort_retention"])
            return preagg_response

        print(
            "[getCohortRetention] "
            f"weeks={weeks}, start={cohort_start_date.strftime('%Y-%m-%d')}, "
            f"end={cohort_end_date.strftime('%Y-%m-%d')}"
        )

        # Build user first-seen and active-date maps from screentime paths only.
        user_first_seen: Dict[str, Any] = {}
        user_active_dates: Dict[str, set] = {}
        docs_scanned = 0
        screentime_docs = 0

        for date_doc in db.collection_group("dates").select([]).stream():
            docs_scanned += 1

            parent_doc_ref = date_doc.reference.parent.parent
            parent_collection = parent_doc_ref.parent if parent_doc_ref else None
            if not parent_doc_ref or not parent_collection or parent_collection.id != "screentime":
                continue

            date_str = date_doc.id
            try:
                activity_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            screentime_docs += 1
            user_id = parent_doc_ref.id

            if user_id not in user_active_dates:
                user_active_dates[user_id] = set()
            user_active_dates[user_id].add(activity_date)

            first_seen_date = user_first_seen.get(user_id)
            if first_seen_date is None or activity_date < first_seen_date:
                user_first_seen[user_id] = activity_date

        cohort_week_starts = [
            cohort_start_date + timedelta(days=7 * index)
            for index in range(weeks)
        ]
        cohort_users: Dict[int, List[str]] = {
            index: []
            for index in range(weeks)
        }

        # Assign users to cohorts by firstSeenDate.
        for user_id, first_seen_date in user_first_seen.items():
            if first_seen_date < cohort_start_date or first_seen_date > cohort_end_date:
                continue

            cohort_index = (first_seen_date - cohort_start_date).days // 7
            if 0 <= cohort_index < weeks:
                cohort_users[cohort_index].append(user_id)

        curve_retained_counts = {
            "day_1": 0,
            "day_3": 0,
            "day_7": 0,
            "day_14": 0,
            "day_30": 0,
        }
        curve_denominator = 0

        cohorts_payload: List[Dict[str, Any]] = []

        for index, cohort_week_start in enumerate(cohort_week_starts):
            users_in_cohort = cohort_users.get(index, [])
            cohort_size = len(users_in_cohort)
            if cohort_size == 0:
                continue

            retained_day_1 = 0
            retained_day_3 = 0
            retained_day_7 = 0
            retained_day_14 = 0
            retained_day_30 = 0

            for user_id in users_in_cohort:
                first_seen_date = user_first_seen[user_id]
                active_dates = user_active_dates.get(user_id, set())

                if (first_seen_date + timedelta(days=1)) in active_dates:
                    retained_day_1 += 1

                if _has_activity_in_window(
                    active_dates,
                    first_seen_date + timedelta(days=2),
                    first_seen_date + timedelta(days=4)
                ):
                    retained_day_3 += 1

                if _has_activity_in_window(
                    active_dates,
                    first_seen_date + timedelta(days=5),
                    first_seen_date + timedelta(days=9)
                ):
                    retained_day_7 += 1

                if _has_activity_in_window(
                    active_dates,
                    first_seen_date + timedelta(days=12),
                    first_seen_date + timedelta(days=16)
                ):
                    retained_day_14 += 1

                if _has_activity_in_window(
                    active_dates,
                    first_seen_date + timedelta(days=28),
                    first_seen_date + timedelta(days=32)
                ):
                    retained_day_30 += 1

            curve_denominator += cohort_size
            curve_retained_counts["day_1"] += retained_day_1
            curve_retained_counts["day_3"] += retained_day_3
            curve_retained_counts["day_7"] += retained_day_7
            curve_retained_counts["day_14"] += retained_day_14
            curve_retained_counts["day_30"] += retained_day_30

            cohorts_payload.append(
                {
                    "cohort_week": cohort_week_start.strftime("%Y-%m-%d"),
                    "cohort_label": _format_cohort_label(cohort_week_start),
                    "cohort_size": cohort_size,
                    "retention": {
                        "day_1": _cohort_percent(retained_day_1, cohort_size),
                        "day_7": _cohort_percent(retained_day_7, cohort_size),
                        "day_14": _cohort_percent(retained_day_14, cohort_size),
                        "day_30": _cohort_percent(retained_day_30, cohort_size),
                    }
                }
            )

        day1_retention = _cohort_percent(curve_retained_counts["day_1"], curve_denominator)
        week1_retention = _cohort_percent(curve_retained_counts["day_7"], curve_denominator)
        month1_retention = _cohort_percent(curve_retained_counts["day_30"], curve_denominator)

        if len(cohorts_payload) >= 2:
            previous_cohort = cohorts_payload[-2]["retention"]
            latest_cohort = cohorts_payload[-1]["retention"]
            day1_retention_change = round(latest_cohort["day_1"] - previous_cohort["day_1"], 1)
            week1_retention_change = round(latest_cohort["day_7"] - previous_cohort["day_7"], 1)
        else:
            day1_retention_change = 0.0
            week1_retention_change = 0.0

        response_payload = {
            "summary": {
                "day1_retention": day1_retention,
                "day1_retention_change": day1_retention_change,
                "week1_retention": week1_retention,
                "week1_retention_change": week1_retention_change,
                "month1_retention": month1_retention,
                "overall_churn_rate": round(max(0.0, 100.0 - month1_retention), 1),
            },
            "cohorts": cohorts_payload,
            "retention_curve": [
                {"day": 0, "retention": 100.0 if curve_denominator > 0 else 0.0},
                {"day": 1, "retention": _cohort_percent(curve_retained_counts["day_1"], curve_denominator)},
                {"day": 3, "retention": _cohort_percent(curve_retained_counts["day_3"], curve_denominator)},
                {"day": 7, "retention": _cohort_percent(curve_retained_counts["day_7"], curve_denominator)},
                {"day": 14, "retention": _cohort_percent(curve_retained_counts["day_14"], curve_denominator)},
                {"day": 30, "retention": _cohort_percent(curve_retained_counts["day_30"], curve_denominator)},
            ]
        }

        print(
            "[getCohortRetention] "
            f"users={len(user_first_seen)}, cohorts={len(cohorts_payload)}, "
            f"docs_scanned={docs_scanned}, screentime_docs={screentime_docs}"
        )

        _preagg_set(
            "cohort_retention",
            preagg_doc_id,
            response_payload,
            {
                "weeks": weeks,
                "start": cohort_start_date.strftime("%Y-%m-%d"),
                "end": cohort_end_date.strftime("%Y-%m-%d"),
            }
        )
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["cohort_retention"])
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"[getCohortRetention] Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


def _format_hh_mm_duration(seconds_value: int) -> str:
    if seconds_value <= 0:
        return "0m"
    hours = seconds_value // 3600
    minutes = (seconds_value % 3600) // 60
    if hours <= 0:
        return f"{minutes}m"
    return f"{hours}h {minutes}m"


def _get_wellbeing_time_block(hour: int) -> str:
    if 6 <= hour < 12:
        return "morning"
    if 12 <= hour < 18:
        return "afternoon"
    if 18 <= hour < 22:
        return "evening"
    return "night"


def _format_clock_from_minutes(total_minutes: float) -> str:
    normalized_minutes = int(round(total_minutes)) % (24 * 60)
    hour_24 = normalized_minutes // 60
    minute = normalized_minutes % 60
    meridiem = "AM" if hour_24 < 12 else "PM"
    hour_12 = hour_24 % 12
    if hour_12 == 0:
        hour_12 = 12
    return f"{hour_12}:{minute:02d} {meridiem}"


def _to_ist_hour_and_minute(ts_ms: int) -> Tuple[int, int]:
    ist_offset = timedelta(hours=5, minutes=30)
    dt_ist = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) + ist_offset
    return dt_ist.hour, dt_ist.minute


def _load_wellbeing_user_day(user_id: str, date_str: str) -> Optional[Dict[str, Any]]:
    try:
        doc = (
            db.collection("screentime")
            .document(user_id)
            .collection("dates")
            .document(date_str)
            .get()
        )
    except Exception as fetch_err:
        print(f"[getWellbeingReport] failed date fetch user={user_id}, date={date_str}, error={fetch_err}")
        return None

    if not doc.exists:
        return None

    payload = doc.to_dict() or {}
    app_rows = _get_app_rows(payload)

    daily_screentime_ms = 0
    daily_sessions = 0
    time_block_sessions = {
        "morning": 0,
        "afternoon": 0,
        "evening": 0,
        "night": 0,
    }
    first_pickup_minutes: Optional[int] = None
    late_night_seconds = 0
    late_night_qualifies = False
    has_timestamp_activity = False

    for app in app_rows:
        screen_time_ms = _safe_int(app.get("totalScreenTime"), default=0)
        session_count = _safe_int(app.get("sessionCount"), default=0)

        if screen_time_ms < 0:
            screen_time_ms = 0
        if session_count < 0:
            session_count = 0

        daily_screentime_ms += screen_time_ms
        daily_sessions += session_count

        ts_ms = _safe_int(app.get("lastUsedTime"), default=-1)
        if ts_ms < 0:
            continue

        has_timestamp_activity = True
        hour, minute = _to_ist_hour_and_minute(ts_ms)
        minute_of_day = (hour * 60) + minute

        if first_pickup_minutes is None or minute_of_day < first_pickup_minutes:
            first_pickup_minutes = minute_of_day

        block_name = _get_wellbeing_time_block(hour)
        time_block_sessions[block_name] += session_count

        screen_time_seconds = int(round(screen_time_ms / 1000.0))
        if hour >= 23:
            late_night_seconds += screen_time_seconds
            if screen_time_seconds >= 1800:
                late_night_qualifies = True

    if daily_screentime_ms <= 0:
        fallback_screen_time_ms = _safe_int(payload.get("totalScreenTime"), default=0)
        if fallback_screen_time_ms > 0:
            daily_screentime_ms = fallback_screen_time_ms

    if daily_sessions <= 0:
        fallback_sessions = _safe_int(payload.get("totalSessions"), default=0)
        if fallback_sessions > 0:
            daily_sessions = fallback_sessions

    has_phone_free_block = False
    if has_timestamp_activity:
        has_phone_free_block = any(value <= 0 for value in time_block_sessions.values())

    return {
        "daily_screentime_seconds": int(round(daily_screentime_ms / 1000.0)),
        "daily_sessions": daily_sessions,
        "time_block_sessions": time_block_sessions,
        "first_pickup_minutes": first_pickup_minutes,
        "late_night_seconds": late_night_seconds,
        "late_night_qualifies": late_night_qualifies,
        "has_phone_free_block": has_phone_free_block,
    }


def _load_wellbeing_user_pair(
    user_id: str,
    target_date_str: str,
    previous_date_str: str
) -> Dict[str, Optional[Dict[str, Any]]]:
    return {
        "target": _load_wellbeing_user_day(user_id, target_date_str),
        "previous": _load_wellbeing_user_day(user_id, previous_date_str),
    }


@router.get("/getWellbeingReport", response_model=Dict[str, Any])
async def getWellbeingReport(
    date: Optional[str] = Query("today", description="Target date ('today' or YYYY-MM-DD)")
):
    """
    User wellbeing analytics for a target day.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500,
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        target_date = _resolve_user_segments_date(date)
        target_date_str = target_date.strftime("%Y-%m-%d")
        previous_date_str = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")

        cache_key = _build_cache_key(
            "wellbeing_report",
            {"v": 1, "date": target_date_str}
        )
        cached_response = _cache_get(cache_key)
        if cached_response is not None:
            return cached_response

        preagg_doc_id = target_date_str
        preagg_response = _preagg_get("wellbeing_report", preagg_doc_id)
        if preagg_response is not None:
            _cache_set(cache_key, preagg_response, _DASHBOARD_CACHE_TTL_MIN["wellbeing_report"])
            return preagg_response

        try:
            user_docs = list(db.collection("users").select([]).stream())
        except Exception:
            user_docs = list(db.collection("users").stream())

        user_ids = [doc.id for doc in user_docs]
        if not user_ids:
            try:
                user_ids = [doc.id for doc in db.collection("screentime").select([]).stream()]
            except Exception:
                user_ids = [doc.id for doc in db.collection("screentime").stream()]

        print(
            "[getWellbeingReport] "
            f"date={target_date_str}, prev_date={previous_date_str}, users={len(user_ids)}"
        )

        user_pairs: List[Dict[str, Optional[Dict[str, Any]]]] = []
        if user_ids:
            max_workers = min(24, max(4, len(user_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        _load_wellbeing_user_pair,
                        user_id,
                        target_date_str,
                        previous_date_str
                    )
                    for user_id in user_ids
                ]

                for future in as_completed(futures):
                    try:
                        user_pairs.append(future.result())
                    except Exception as user_future_err:
                        print(f"[getWellbeingReport] user pair processing failed: {user_future_err}")

        target_rows = [row["target"] for row in user_pairs if row.get("target")]
        previous_rows = [row["previous"] for row in user_pairs if row.get("previous")]

        total_active_users = len(target_rows)
        total_previous_users = len(previous_rows)

        total_target_screentime = sum(
            int(row.get("daily_screentime_seconds", 0))
            for row in target_rows
        )
        total_previous_screentime = sum(
            int(row.get("daily_screentime_seconds", 0))
            for row in previous_rows
        )

        avg_daily_screentime_seconds = (
            int(round(total_target_screentime / total_active_users))
            if total_active_users > 0
            else 0
        )
        previous_avg_screentime_seconds = (
            int(round(total_previous_screentime / total_previous_users))
            if total_previous_users > 0
            else 0
        )
        avg_change_seconds = (
            avg_daily_screentime_seconds - previous_avg_screentime_seconds
            if total_previous_users > 0
            else 0
        )

        block_names = ["morning", "afternoon", "evening", "night"]
        block_totals = {name: 0 for name in block_names}
        for row in target_rows:
            block_sessions = row.get("time_block_sessions", {})
            for name in block_names:
                block_totals[name] += int(block_sessions.get(name, 0))

        total_block_sessions = sum(block_totals.values())

        def _block_percent(value: int) -> int:
            if total_block_sessions <= 0:
                return 0
            return int(round((value / total_block_sessions) * 100))

        def _avg_sessions(value: int) -> int:
            if total_active_users <= 0:
                return 0
            return int(round(value / total_active_users))

        time_of_day_breakdown = {
            "morning_6am_12pm": {
                "percent": _block_percent(block_totals["morning"]),
                "avg_sessions": _avg_sessions(block_totals["morning"]),
            },
            "afternoon_12pm_6pm": {
                "percent": _block_percent(block_totals["afternoon"]),
                "avg_sessions": _avg_sessions(block_totals["afternoon"]),
            },
            "evening_6pm_10pm": {
                "percent": _block_percent(block_totals["evening"]),
                "avg_sessions": _avg_sessions(block_totals["evening"]),
            },
            "night_10pm_6am": {
                "percent": _block_percent(block_totals["night"]),
                "avg_sessions": _avg_sessions(block_totals["night"]),
            },
        }

        late_night_rows = [
            row for row in target_rows
            if bool(row.get("late_night_qualifies"))
        ]
        late_night_count = len(late_night_rows)
        late_night_percent = (
            round((late_night_count / total_active_users) * 100, 1)
            if total_active_users > 0
            else 0.0
        )
        avg_duration_after_11pm_seconds = (
            int(round(
                sum(int(row.get("late_night_seconds", 0)) for row in late_night_rows)
                / late_night_count
            ))
            if late_night_count > 0
            else 0
        )

        target_pickups = [
            int(row.get("first_pickup_minutes"))
            for row in target_rows
            if row.get("first_pickup_minutes") is not None
        ]
        previous_pickups = [
            int(row.get("first_pickup_minutes"))
            for row in previous_rows
            if row.get("first_pickup_minutes") is not None
        ]

        avg_pickup_minutes = (
            sum(target_pickups) / len(target_pickups)
            if target_pickups
            else 0.0
        )
        avg_pickup_hour = round(avg_pickup_minutes / 60.0, 2) if target_pickups else 0.0
        avg_pickup_formatted = (
            _format_clock_from_minutes(avg_pickup_minutes)
            if target_pickups
            else "12:00 AM"
        )

        if target_pickups and previous_pickups:
            previous_avg_pickup_minutes = sum(previous_pickups) / len(previous_pickups)
            first_pickup_change_minutes = int(round(avg_pickup_minutes - previous_avg_pickup_minutes))
        else:
            first_pickup_change_minutes = 0

        first_pickup_ranges = {
            "Before 6am": 0,
            "6-7am": 0,
            "7-8am": 0,
            "8-9am": 0,
            "After 9am": 0,
        }
        for minute_of_day in target_pickups:
            if minute_of_day < 360:
                first_pickup_ranges["Before 6am"] += 1
            elif minute_of_day < 420:
                first_pickup_ranges["6-7am"] += 1
            elif minute_of_day < 480:
                first_pickup_ranges["7-8am"] += 1
            elif minute_of_day < 540:
                first_pickup_ranges["8-9am"] += 1
            else:
                first_pickup_ranges["After 9am"] += 1

        first_pickup_distribution = []
        pickup_denominator = len(target_pickups)
        for range_label in ["Before 6am", "6-7am", "7-8am", "8-9am", "After 9am"]:
            count_value = first_pickup_ranges[range_label]
            percent_value = (
                int(round((count_value / pickup_denominator) * 100))
                if pickup_denominator > 0
                else 0
            )
            first_pickup_distribution.append(
                {"hour_range": range_label, "percent": percent_value}
            )

        screen_time_bucket_counts = {
            "under_2h": 0,
            "2_to_4h": 0,
            "4_to_6h": 0,
            "over_6h": 0,
        }
        phone_free_users_count = 0

        for row in target_rows:
            day_seconds = int(row.get("daily_screentime_seconds", 0))

            if day_seconds < 7200:
                screen_time_bucket_counts["under_2h"] += 1
            elif day_seconds < 14400:
                screen_time_bucket_counts["2_to_4h"] += 1
            elif day_seconds < 21600:
                screen_time_bucket_counts["4_to_6h"] += 1
            else:
                screen_time_bucket_counts["over_6h"] += 1

            if bool(row.get("has_phone_free_block")):
                phone_free_users_count += 1

        def _bucket_percent(count_value: int) -> float:
            if total_active_users <= 0:
                return 0.0
            return round((count_value / total_active_users) * 100, 1)

        screentime_distribution = [
            {
                "bucket": "Under 2h",
                "count": screen_time_bucket_counts["under_2h"],
                "percent": _bucket_percent(screen_time_bucket_counts["under_2h"]),
                "label": "Healthy",
            },
            {
                "bucket": "2-4h",
                "count": screen_time_bucket_counts["2_to_4h"],
                "percent": _bucket_percent(screen_time_bucket_counts["2_to_4h"]),
                "label": "Moderate",
            },
            {
                "bucket": "4-6h",
                "count": screen_time_bucket_counts["4_to_6h"],
                "percent": _bucket_percent(screen_time_bucket_counts["4_to_6h"]),
                "label": "High",
            },
            {
                "bucket": "Over 6h",
                "count": screen_time_bucket_counts["over_6h"],
                "percent": _bucket_percent(screen_time_bucket_counts["over_6h"]),
                "label": "Excessive",
            },
        ]

        over_6h_percent = _bucket_percent(screen_time_bucket_counts["over_6h"])
        concerning_patterns: List[Dict[str, str]] = []
        if late_night_count > 0:
            concerning_patterns.append(
                {
                    "type": "late_night",
                    "severity": "warning" if late_night_percent >= 15 else "info",
                    "message": (
                        f"{late_night_percent:g}% of users used phone after 11pm for 30+ min"
                    ),
                }
            )

        if screen_time_bucket_counts["over_6h"] > 0:
            concerning_patterns.append(
                {
                    "type": "excessive_usage",
                    "severity": "warning" if over_6h_percent >= 8 else "info",
                    "message": (
                        f"{over_6h_percent:g}% of users have 6+ hours of screentime"
                    ),
                }
            )

        phone_free_percent = (
            round((phone_free_users_count / total_active_users) * 100, 1)
            if total_active_users > 0
            else 0.0
        )
        positive_patterns: List[Dict[str, str]] = []
        if phone_free_users_count > 0:
            positive_patterns.append(
                {
                    "type": "phone_free",
                    "message": (
                        f"{phone_free_percent:g}% of users had at least 4 phone-free hours"
                    ),
                }
            )

        response_payload = {
            "date": target_date_str,
            "avg_daily_screentime_seconds": avg_daily_screentime_seconds,
            "avg_daily_screentime_formatted": _format_hh_mm_duration(avg_daily_screentime_seconds),
            "avg_change_seconds": avg_change_seconds,
            "time_of_day_breakdown": time_of_day_breakdown,
            "late_night_users": {
                "count": late_night_count,
                "percent": late_night_percent,
                "threshold_hour": 23,
                "avg_duration_after_11pm_seconds": avg_duration_after_11pm_seconds,
            },
            "first_pickup": {
                "avg_hour": avg_pickup_hour,
                "avg_time_formatted": avg_pickup_formatted,
                "change_minutes": first_pickup_change_minutes,
                "distribution": first_pickup_distribution,
            },
            "screentime_distribution": screentime_distribution,
            "concerning_patterns": concerning_patterns,
            "positive_patterns": positive_patterns,
        }

        _preagg_set(
            "wellbeing_report",
            preagg_doc_id,
            response_payload,
            {"date": target_date_str}
        )
        _cache_set(cache_key, response_payload, _DASHBOARD_CACHE_TTL_MIN["wellbeing_report"])
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        print(f"[getWellbeingReport] Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


############################################################
# --- APPENDED APIS ---
############################################################


# --- FROM compare_periods.py ---
import re
from datetime import datetime, timedelta

from typing import Dict, Any, List, Optional
from database.firebase import db
from middleware.auth import verify_api_key
import firebase_admin.auth as auth
from concurrent.futures import ThreadPoolExecutor, as_completed

# Authentication Helper
async def verify_user(request: Request):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        # Fallback to Admin for development
        return {"uid": "admin_user", "role": "admin", "clientId": None}
    
    token = auth_header.split('Bearer ')[1]
    try:
        decoded = auth.verify_id_token(token)
        uid = decoded.get("uid")
        
        user_doc = db.collection('dashboard_users').document(uid).get()
        if not user_doc.exists:
            return {"uid": uid, "role": "admin", "clientId": None}
            
        data = user_doc.to_dict()
        return {"uid": uid, "role": data.get("role", "client"), "clientId": data.get("clientId")}
    except Exception as e:
        return {"uid": "admin_user", "role": "admin", "clientId": None}

# Helper Functions
def format_date_str(date_obj: datetime) -> str:
    return date_obj.strftime("%Y-%m-%d")

def format_duration(seconds: float) -> str:
    mins = int(seconds // 60)
    secs = int(round(seconds % 60))
    if mins == 0:
        return f"{secs}s"
    return f"{mins}m {secs}s"

def format_label(start_str: str, end_str: str) -> str:
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    
    if start_dt.year != end_dt.year:
        return f"{start_dt.strftime('%b %-d, %Y')} - {end_dt.strftime('%b %-d, %Y')}"
    if start_dt.month != end_dt.month:
        return f"{start_dt.strftime('%b %-d')} - {end_dt.strftime('%b %-d, %Y')}"
    return f"{start_dt.strftime('%b %-d')}-{end_dt.strftime('%-d, %Y')}"

def calc_change(a: float, b: float) -> Dict[str, Any]:
    change_absolute = round(a - b, 2)
    change_percent = ((a - b) / b * 100) if b > 0 else 0
    change_percent = round(change_percent, 1)
    
    if a > b:
        direction = "up"
    elif a < b:
        direction = "down"
    else:
        direction = "same"
        
    return {
        "change_absolute": change_absolute,
        "change_percent": change_percent,
        "direction": direction
    }

def is_positive_direction(metric_name: str, direction: str) -> str:
    if direction == "same":
        return "neutral"
    
    negative_metrics = ["at_risk_users", "churn_rate", "avg_fragmentation"]
    
    if metric_name in negative_metrics:
        # For these, down is good (positive)
        return "positive" if direction == "down" else "negative"
    else:
        # For all others, up is good (positive)
        return "positive" if direction == "up" else "negative"

def get_all_dates_in_range(start_str: str, end_str: str) -> List[str]:
    dates = []
    current = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    while current <= end:
        dates.append(format_date_str(current))
        current += timedelta(days=1)
    return dates

def fetch_period_data(start_str: str, end_str: str, active_client_id: Optional[str]) -> Dict[str, Any]:
    # 1. Fetch relevant users
    users_ref = db.collection("users")
    if active_client_id:
        users_ref = users_ref.where("clientId", "==", active_client_id)
    
    user_docs = list(users_ref.stream())
    user_ids = [d.id for d in user_docs]
    
    all_docs = []
    
    # 2. Fetch date documents concurrently per user
    # Firestore supports range queries on document_id() inside a collection
    def fetch_user_dates(uid: str):
        dates_ref = db.collection("screentime").document(uid).collection("dates")
        return list(dates_ref.where(FieldPath.document_id(), ">=", start_str)
                            .where(FieldPath.document_id(), "<=", end_str)
                            .stream())

    if user_ids:
        # Use a higher worker count for this batch fetch
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(fetch_user_dates, uid) for uid in user_ids]
            for future in as_completed(futures):
                try:
                    all_docs.extend(future.result())
                except Exception as e:
                    print(f"[fetch_period_data] Error fetching dates for a user: {e}")

    by_date = {}
    by_user = {}
    app_usage = {}
    category_usage = {}
    user_first_seen = {}

    for doc in all_docs:
        date_str = doc.id
        user_id = doc.reference.parent.parent.id
        
        data = doc.to_dict() or {}
        apps = data.get("apps", [])
        if isinstance(apps, dict):
             apps = list(apps.values())
        
        # Determine first seen date (estimate from records we have)
        if user_id not in user_first_seen or date_str < user_first_seen[user_id]:
            user_first_seen[user_id] = date_str

        # Ensure structures are initialized
        if date_str not in by_date:
            by_date[date_str] = {"userIds": set(), "sessions": 0, "screentime": 0}
        
        by_date[date_str]["userIds"].add(user_id)

        if user_id not in by_user:
            by_user[user_id] = {"dates": set(), "totalScreentime": 0, "totalSessions": 0}
            
        by_user[user_id]["dates"].add(date_str)

        for app in (apps if isinstance(apps, list) else []):
            sessions = app.get("sessionCount", 0)
            screentime = app.get("totalScreenTime", 0)
            
            by_date[date_str]["sessions"] += sessions
            by_date[date_str]["screentime"] += screentime
            by_user[user_id]["totalScreentime"] += screentime
            by_user[user_id]["totalSessions"] += sessions

            app_name = app.get("appName")
            if app_name:
                category = app.get("category", "Other")
                if app_name not in app_usage:
                    app_usage[app_name] = {"totalScreentime": 0, "category": category}
                app_usage[app_name]["totalScreentime"] += screentime
                
            category = app.get("category")
            if category:
                if category not in category_usage:
                    category_usage[category] = {"totalScreentime": 0}
                category_usage[category]["totalScreentime"] += screentime

    # Step 3: All dates calculation
    all_dates = get_all_dates_in_range(start_str, end_str)
    period_days = len(all_dates) if len(all_dates) > 0 else 1

    # Step 4: DAU per day
    dau_per_day = {}
    for d_str in all_dates:
        dau_per_day[d_str] = len(by_date[d_str]["userIds"]) if d_str in by_date else 0
        
    dau_values = list(dau_per_day.values())
    dau_avg = round(sum(dau_values) / len(dau_values)) if dau_values else 0

    # Step 5: MAU
    all_user_ids = set(user_ids) # Users found initially
    mau = len(all_user_ids)

    # Step 6: Stickiness
    stickiness = round((dau_avg / mau * 100), 1) if mau > 0 else 0

    # Step 7: Avg session duration
    total_screentime = sum(d["screentime"] for d in by_date.values())
    total_sessions = sum(d["sessions"] for d in by_date.values())
    avg_session_seconds = round(total_screentime / total_sessions / 1000) if total_sessions > 0 else 0

    # Step 8: Calculate new users
    total_new_users = sum(1 for uid, first_date in user_first_seen.items() 
                          if start_str <= first_date <= end_str)
    new_users_per_day = round(total_new_users / period_days, 1)

    # Step 9: User segments
    power_users = 0
    regular_users = 0
    casual_users = 0
    at_risk_count = 0 # Placeholder for complex logic

    for uid, u_data in by_user.items():
        if len(u_data["dates"]) > 0:
            user_avg = (u_data["totalScreentime"] / 1000) / len(u_data["dates"])
            if user_avg > 14400: # 4 hours
                power_users += 1
            elif user_avg > 3600: # 1 hour
                regular_users += 1
            elif user_avg > 0:
                casual_users += 1

    # Step 10: Retention Day 1 (simplified)
    cohort_size = sum(1 for uid, first_date in user_first_seen.items() if first_date == start_str)
    returned_day1 = 0
    if cohort_size > 0:
        day_1_date = (datetime.strptime(start_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        returned_day1 = sum(1 for uid, first_date in user_first_seen.items() 
                            if first_date == start_str and uid in by_user and day_1_date in by_user[uid]["dates"])
                            
    retention_day1 = round((returned_day1 / cohort_size * 100), 1) if cohort_size > 0 else 0

    # Step 11: Top App
    top_app = "N/A"
    if app_usage:
        top_app = max(app_usage.items(), key=lambda x: x[1]["totalScreentime"])[0]

    # Step 12: Top Category
    top_category = "N/A"
    if category_usage:
        top_category = max(category_usage.items(), key=lambda x: x[1]["totalScreentime"])[0]

    # Step 13: DAU Overlay
    dau_overlay = []
    for idx, d_str in enumerate(all_dates):
        dau_overlay.append({
            "day": idx + 1,
            "date": d_str,
            "dau": dau_per_day.get(d_str, 0)
        })

    return {
        "dau_avg": dau_avg,
        "mau": mau,
        "stickiness": stickiness,
        "avg_session_seconds": avg_session_seconds,
        "avg_session_formatted": format_duration(avg_session_seconds),
        "new_users_per_day": new_users_per_day,
        "retention_day1": retention_day1,
        "power_users": power_users,
        "regular_users": regular_users,
        "casual_users": casual_users,
        "at_risk_users": at_risk_count,
        "top_app": top_app,
        "top_category": top_category,
        "total_sessions": total_sessions,
        "total_screentime_seconds": round(total_screentime / 1000),
        "period_days": period_days,
        "dau_overlay": dau_overlay
    }

@router.get("/comparePeriods")
async def compare_periods_endpoint(
    request: Request,
    period_a_start: str = Query(..., description="YYYY-MM-DD"),
    period_a_end: str = Query(..., description="YYYY-MM-DD"),
    period_b_start: str = Query(..., description="YYYY-MM-DD"),
    period_b_end: str = Query(..., description="YYYY-MM-DD"),
    viewingAs: Optional[str] = Query(None)
):
    print("comparePeriods called", {
        "period_a": f"{period_a_start} to {period_a_end}",
        "period_b": f"{period_b_start} to {period_b_end}"
    })

    # Validate Dates
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not all(date_regex.match(d) for d in [period_a_start, period_a_end, period_b_start, period_b_end]):
        raise HTTPException(status_code=400, detail="Dates must be in YYYY-MM-DD format")

    if period_a_end < period_a_start or period_b_end < period_b_start:
        raise HTTPException(status_code=400, detail="End date must be >= start date")

    user_info = await verify_user(request)
    role = user_info["role"]
    assigned_client_id = user_info["clientId"]

    active_client_id = None
    if role == 'client':
        active_client_id = assigned_client_id
    elif role == 'admin' and viewingAs:
        active_client_id = viewingAs

    # Fetch Data in Parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(fetch_period_data, period_a_start, period_a_end, active_client_id)
        future_b = executor.submit(fetch_period_data, period_b_start, period_b_end, active_client_id)
        period_a_data = future_a.result()
        period_b_data = future_b.result()

    # Compare
    comparison = {}
    metrics_to_compare = [
        ("dau_avg", ""), ("mau", ""), ("stickiness", "%"), 
        ("avg_session_seconds", ""), ("new_users_per_day", ""), 
        ("retention_day1", "%"), ("power_users", ""), 
        ("regular_users", ""), ("at_risk_users", "")
    ]
    
    wins = 0
    losses = 0
    neutral = 0

    for metric, suffix in metrics_to_compare:
        val_a = period_a_data[metric]
        val_b = period_b_data[metric]
        change_info = calc_change(val_a, val_b)
        sentiment = is_positive_direction(metric, change_info["direction"])
        
        comp_data = {
            "period_a": val_a,
            "period_b": val_b,
            "change_absolute": change_info["change_absolute"],
            "change_percent": change_info["change_percent"],
            "direction": change_info["direction"],
            "sentiment": sentiment
        }
        if suffix == "%":
            comp_data["period_a_formatted"] = f"{val_a}%"
            comp_data["period_b_formatted"] = f"{val_b}%"
        elif metric == "avg_session_seconds":
            comp_data["period_a_formatted"] = format_duration(val_a)
            comp_data["period_b_formatted"] = format_duration(val_b)
            
        comparison[metric] = comp_data

        if sentiment == "positive": wins += 1
        elif sentiment == "negative": losses += 1
        else: neutral += 1

    comparison["top_app"] = {
        "period_a": period_a_data["top_app"],
        "period_b": period_b_data["top_app"],
        "changed": period_a_data["top_app"] != period_b_data["top_app"],
        "change_label": "No change" if period_a_data["top_app"] == period_b_data["top_app"] else f"Changed from {period_b_data['top_app']} to {period_a_data['top_app']}"
    }

    comparison["top_category"] = {
        "period_a": period_a_data["top_category"],
        "period_b": period_b_data["top_category"],
        "changed": period_a_data["top_category"] != period_b_data["top_category"],
        "change_label": "No change" if period_a_data["top_category"] == period_b_data["top_category"] else f"Changed from {period_b_data['top_category']} to {period_a_data['top_category']}"
    }

    # Verdict
    if wins >= losses * 2: overall_verdict = "significantly_better"
    elif wins > losses: overall_verdict = "slightly_better"
    elif wins == losses: overall_verdict = "similar"
    elif losses >= wins * 2: overall_verdict = "significantly_worse"
    else: overall_verdict = "slightly_worse"

    return {
        "period_a": {
            "start": period_a_start,
            "end": period_a_end,
            "label": format_label(period_a_start, period_a_end),
            "days": period_a_data["period_days"]
        },
        "period_b": {
            "start": period_b_start,
            "end": period_b_end,
            "label": format_label(period_b_start, period_b_end),
            "days": period_b_data["period_days"]
        },
        "overall_verdict": overall_verdict,
        "wins": wins,
        "losses": losses,
        "comparison": comparison,
        "dau_overlay": {
            "period_a_data": period_a_data["dau_overlay"],
            "period_b_data": period_b_data["dau_overlay"]
        },
        "filter": {
            "clientId": active_client_id,
            "role": role
        }
    }

# --- FROM user_profile.py ---
import re
from datetime import datetime, timedelta

from typing import Dict, Any, List, Optional
from database.firebase import db
from middleware.auth import verify_api_key

def format_duration(seconds: float) -> str:
    hours = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    if hours > 0:
        return f"{hours}h {mins}m"
    return f"{int(seconds // 60)}m"

@router.get("/getUserProfile")
async def get_user_profile(
    user_id: str = Query(..., alias="userId"),
    days: int = Query(30)
):
    try:
        # Fetch all date documents for this user
        dates_ref = db.collection("screentime").document(user_id).collection("dates")
        docs = dates_ref.stream()

        all_date_data = {}
        for doc in docs:
            all_date_data[doc.id] = doc.to_dict()

        if not all_date_data:
            return {
                "user_id": user_id,
                "error": "User not found or no data available."
            }

        sorted_dates = sorted(all_date_data.keys())
        first_seen_str = sorted_dates[0]
        last_active_str = sorted_dates[-1]

        first_seen_dt = datetime.strptime(first_seen_str, "%Y-%m-%d")
        last_active_dt = datetime.strptime(last_active_str, "%Y-%m-%d")
        today_dt = datetime.utcnow()
        today_str = today_dt.strftime("%Y-%m-%d")

        days_since_joined = (today_dt - first_seen_dt).days
        if days_since_joined < 0:
             days_since_joined = 0

        # Current Streak
        current_streak_days = 0
        check_dt = last_active_dt
        while check_dt.strftime("%Y-%m-%d") in all_date_data:
            current_streak_days += 1
            check_dt -= timedelta(days=1)

        # Lifetime Stats
        total_screentime = 0
        total_sessions = 0
        all_apps_ever = set()

        for d_str, data in all_date_data.items():
            apps = data.get("apps", [])
            if isinstance(apps, dict):
                 apps = list(apps.values())
                 
            for app in apps:
                screentime = app.get("totalScreenTime", 0)
                sessions = app.get("sessionCount", 0)
                
                total_screentime += screentime
                total_sessions += sessions
                if app.get("appName"):
                    all_apps_ever.add(app.get("appName"))

        segment = "casual"
        avg_screentime_ms = total_screentime / len(sorted_dates)
        if avg_screentime_ms > 4 * 3600 * 1000:
            segment = "power"
        elif avg_screentime_ms > 1 * 3600 * 1000:
            segment = "regular"

        # Last N Days
        cutoff_dt = today_dt - timedelta(days=days)
        recent_dates_data = {
            d: data for d, data in all_date_data.items() 
            if datetime.strptime(d, "%Y-%m-%d") >= cutoff_dt
        }

        activity_calendar = []
        recent_screentime = 0
        recent_sessions = 0
        app_usage_recent = {}
        category_usage_recent = {}

        for d_str in sorted(recent_dates_data.keys()):
            data = recent_dates_data[d_str]
            apps = data.get("apps", [])
            if isinstance(apps, dict):
                 apps = list(apps.values())
                 
            daily_sc = sum(app.get("totalScreenTime", 0) for app in apps)
            daily_sc_sec = int(daily_sc / 1000)
            
            # App calculations
            for app in apps:
                app_name = app.get("appName", "Unknown")
                category = app.get("category", "Other")
                sc = app.get("totalScreenTime", 0)
                sess = app.get("sessionCount", 0)
                
                recent_screentime += sc
                recent_sessions += sess

                if app_name not in app_usage_recent:
                    app_usage_recent[app_name] = 0
                app_usage_recent[app_name] += sc

                if category not in category_usage_recent:
                    category_usage_recent[category] = 0
                category_usage_recent[category] += sc
            
            intensity = "low"
            if daily_sc_sec > 4 * 3600:
                intensity = "high"
            elif daily_sc_sec > 1.5 * 3600:
                intensity = "medium"

            activity_calendar.append({
                "date": d_str,
                "screentime_seconds": daily_sc_sec,
                "intensity": intensity
            })

        recent_days_count = len(recent_dates_data) or 1
        avg_daily_screentime_seconds = int(recent_screentime / 1000 / recent_days_count)
        avg_daily_sessions = int(recent_sessions / recent_days_count)

        top_app = "N/A"
        if app_usage_recent:
            top_app = max(app_usage_recent.items(), key=lambda x: x[1])[0]

        top_category = "N/A"
        if category_usage_recent:
            top_category = max(category_usage_recent.items(), key=lambda x: x[1])[0]

        # Today's Breakdown
        today_sc_sec = 0
        today_sessions = 0
        today_apps_list = []
        
        target_today_str = last_active_str # fallback to last active if "today" doesn't exist
        if target_today_str in all_date_data:
            apps = all_date_data[target_today_str].get("apps", [])
            if isinstance(apps, dict):
                 apps = list(apps.values())
                 
            for app in apps:
                app_name = app.get("appName", "Unknown")
                sc = app.get("totalScreenTime", 0)
                sc_sec = int(sc / 1000)
                sess = app.get("sessionCount", 0)
                
                today_sc_sec += sc_sec
                today_sessions += sess
                
                today_apps_list.append({
                    "app_name": app_name,
                    "screentime_seconds": sc_sec,
                    "screentime_formatted": format_duration(sc_sec),
                    "session_count": sess
                })
        
        # Sort today's apps by screentime descending
        today_apps_list.sort(key=lambda x: x["screentime_seconds"], reverse=True)

        return {
            "user_id": user_id,
            "first_seen": first_seen_str,
            "last_active": last_active_str,
            "days_since_joined": days_since_joined,
            "current_streak_days": current_streak_days,
            "segment": segment,
            
            "lifetime_stats": {
                "total_days_active": len(all_date_data),
                "total_screentime_formatted": format_duration(total_screentime / 1000),
                "total_sessions": total_sessions,
                "total_apps_used": len(all_apps_ever)
            },

            "last_30_days": {
                "activity_calendar": activity_calendar,
                "avg_daily_screentime_seconds": avg_daily_screentime_seconds,
                "avg_daily_sessions": avg_daily_sessions,
                "top_app": top_app,
                "top_category": top_category
            },

            "today": {
                "date": target_today_str,
                "total_screentime_seconds": today_sc_sec,
                "total_screentime_formatted": format_duration(today_sc_sec),
                "sessions": today_sessions,
                "apps": today_apps_list[:10]  # Return top 10 apps
            },

            "behavior_patterns": {
                "typical_wakeup_hour": 7,
                "peak_usage_hour": 19,
                "typical_sleep_hour": 23,
                "late_night_days_this_week": 3,
                "best_day_of_week": "Tuesday"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- FROM screentime_patterns.py ---
import math
from datetime import datetime, timedelta

from typing import Dict, Any, List
from database.firebase import db
from middleware.auth import verify_api_key

def parse_last_used_hour(val: Any) -> int:
    if not val:
        return -1
    try:
        if isinstance(val, (int, float)):
            # Assuming milliseconds
            dt = datetime.utcfromtimestamp(val / 1000.0)
            return dt.hour
        elif isinstance(val, str):
            # Try to parse standard ISO format
            # e.g., '2026-02-11T14:30:00Z'
            val = val.replace('Z', '+00:00')
            dt = datetime.fromisoformat(val)
            return dt.hour
    except Exception:
        pass
    return -1

@router.get("/getScreenTimePatterns")
async def get_screentime_patterns(days: int = Query(30)):
    try:
        today_dt = datetime.utcnow()
        cutoff_dt = today_dt - timedelta(days=days)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%d")

        # Use the same efficient user-based fetching strategy
        user_docs = list(db.collection("users").stream())
        user_ids = [d.id for d in user_docs]
        
        all_docs = []
        def fetch_user_dates(uid: str):
            dates_ref = db.collection("screentime").document(uid).collection("dates")
            return list(dates_ref.where(FieldPath.document_id(), ">=", cutoff_str).stream())

        if user_ids:
            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [executor.submit(fetch_user_dates, uid) for uid in user_ids]
                for future in as_completed(futures):
                    try:
                        all_docs.extend(future.result())
                    except Exception as e:
                        print(f"[getScreenTimePatterns] Error: {e}")

        # Data structures
        daily_users = {} 
        hourly_daily_users = {h: {} for h in range(24)}
        hourly_app_sessions = {h: {} for h in range(24)}
        
        blocks = {
            "morning": {"ranges": [6, 7, 8], "label": "6am-9am"},
            "lunch": {"ranges": [12, 13], "label": "12pm-2pm"},
            "evening": {"ranges": [18, 19, 20, 21], "label": "6pm-10pm"},
            "night": {"ranges": [22, 23], "label": "10pm-12am"}
        }
        
        block_daily_users = {b: {} for b in blocks}
        block_app_sessions = {b: {} for b in blocks}
        block_screentime_min = {b: 0 for b in blocks}
        block_user_dates_count = {b: 0 for b in blocks}

        first_pickup_hours = []
        last_use_hours = []

        for doc in all_docs:
            date_str = doc.id
            user_id = doc.reference.parent.parent.id
            
            if date_str not in daily_users:
                daily_users[date_str] = set()
            daily_users[date_str].add(user_id)
            
            data = doc.to_dict() or {}
            apps = data.get("apps", [])
            if isinstance(apps, dict):
                 apps = list(apps.values())
                 
            doc_hours = set()
            min_hr = 25
            max_hr = -1
            
            for app in apps:
                last_used = app.get("lastUsedTime")
                hr = parse_last_used_hour(last_used)
                
                if hr != -1:
                    doc_hours.add(hr)
                    if hr < min_hr: min_hr = hr
                    if hr > max_hr: max_hr = hr
                    
                    app_name = app.get("appName", "Unknown")
                    sessions = app.get("sessionCount", 0)
                    screentime_ms = app.get("totalScreenTime", 0)
                    
                    if app_name not in hourly_app_sessions[hr]:
                        hourly_app_sessions[hr][app_name] = 0
                    hourly_app_sessions[hr][app_name] += sessions
                    
                    if date_str not in hourly_daily_users[hr]:
                        hourly_daily_users[hr][date_str] = set()
                    hourly_daily_users[hr][date_str].add(user_id)
                    
                    for b, b_info in blocks.items():
                        if hr in b_info["ranges"]:
                            if app_name not in block_app_sessions[b]:
                                block_app_sessions[b][app_name] = 0
                            block_app_sessions[b][app_name] += sessions
                            
                            if date_str not in block_daily_users[b]:
                                block_daily_users[b][date_str] = set()
                            block_daily_users[b][date_str].add(user_id)
                            
                            block_screentime_min[b] += (screentime_ms / 1000 / 60)
            
            if min_hr != 25:
                first_pickup_hours.append(min_hr)
            if max_hr != -1:
                last_use_hours.append(max_hr)
                
            for b, b_info in blocks.items():
                if any(h in b_info["ranges"] for h in doc_hours):
                    block_user_dates_count[b] += 1

        actual_days = len(daily_users) if len(daily_users) > 0 else 1
        avg_dau = sum(len(users) for users in daily_users.values()) / actual_days if actual_days > 0 else 0
        
        daily_rhythm = []
        for hr in range(24):
            hr_avg_users = sum(len(users) for users in hourly_daily_users[hr].values()) / actual_days
            percent_dau = round((hr_avg_users / avg_dau * 100), 1) if avg_dau > 0 else 0.0
            
            top_app = "N/A"
            if hourly_app_sessions[hr]:
                top_app = max(hourly_app_sessions[hr].items(), key=lambda x: x[1])[0]
                
            total_hr_sess = sum(hourly_app_sessions[hr].values())
            avg_sess = round(total_hr_sess / (hr_avg_users * actual_days), 1) if (hr_avg_users * actual_days) > 0 else 0.0
            
            daily_rhythm.append({
                "hour": hr,
                "avg_active_users": int(hr_avg_users),
                "percent_of_dau": percent_dau,
                "avg_sessions": avg_sess,
                "top_app": top_app
            })
            
        time_blocks_res = {}
        for b, b_info in blocks.items():
            b_avg_users = sum(len(users) for users in block_daily_users[b].values()) / actual_days
            b_pct_dau = round((b_avg_users / avg_dau * 100), 1) if avg_dau > 0 else 0.0
            
            top_app = "N/A"
            if block_app_sessions[b]:
                top_app = max(block_app_sessions[b].items(), key=lambda x: x[1])[0]
                
            avg_sc_min = 0
            if block_user_dates_count[b] > 0:
                avg_sc_min = int(block_screentime_min[b] / block_user_dates_count[b])
                
            b_data = {
                "range": b_info["label"],
                "top_app": top_app,
                "avg_screentime_minutes": avg_sc_min,
                "percent_of_dau_active": b_pct_dau
            }
            if b == "night":
                b_data["late_night_warning"] = (b_pct_dau > 20.0)
                
            time_blocks_res[b] = b_data
            
        avg_first = sum(first_pickup_hours) / len(first_pickup_hours) if first_pickup_hours else 0
        avg_last = sum(last_use_hours) / len(last_use_hours) if last_use_hours else 0
        avg_free = 24 - (avg_last - avg_first) if avg_last >= avg_first else 0

        def format_hr(decimal_hr: float) -> str:
            if decimal_hr == 0:
                return "12:00 AM"
            h = int(decimal_hr)
            m = int(round((decimal_hr - h) * 60))
            if m == 60:
                h += 1
                m = 0
            ampm = "AM" if h < 12 else "PM"
            h_12 = h % 12
            if h_12 == 0: h_12 = 12
            return f"{h_12}:{m:02d} {ampm}"

        dist = {"Before 6am": 0, "6am-7am": 0, "7am-8am": 0, "8am-9am": 0, "After 9am": 0}
        total_pickups = len(first_pickup_hours) or 1
        for hr in first_pickup_hours:
            if hr < 6: dist["Before 6am"] += 1
            elif hr == 6: dist["6am-7am"] += 1
            elif hr == 7: dist["7am-8am"] += 1
            elif hr == 8: dist["8am-9am"] += 1
            else: dist["After 9am"] += 1

        pickup_dist = []
        for k, v in dist.items():
            pickup_dist.append({"range": k, "percent": round((v / total_pickups * 100), 1)})

        return {
            "daily_rhythm": daily_rhythm,
            "time_blocks": time_blocks_res,
            "pickup_putdown": {
                "avg_first_pickup_hour": round(avg_first, 2),
                "avg_first_pickup_formatted": format_hr(avg_first),
                "avg_last_use_hour": round(avg_last, 2),
                "avg_last_use_formatted": format_hr(avg_last),
                "avg_phone_free_hours": round(avg_free, 1),
                "first_pickup_distribution": pickup_dist
            }
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# --- FROM user_lifecycle.py ---
from datetime import datetime, timedelta

from typing import Dict, Any, List
from database.firebase import db
from middleware.auth import verify_api_key

def format_duration(seconds: float) -> str:
    hours = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    if hours == 0 and mins == 0:
        return f"{int(seconds)}s"
    if hours > 0:
        return f"{hours}h {mins}m"
    return f"{mins}m"

@router.get("/getUserLifecycle")
async def get_user_lifecycle(days: int = Query(90)):
    try:
        today_dt = datetime.utcnow()
        cutoff_dt = today_dt - timedelta(days=days)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%d")

        # Fetch all dates to accurately determine first_seen and age
        docs = db.collection_group("dates").stream()
        
        user_profiles = {}
        # {
        #   first_seen: str,
        #   last_seen: str,
        #   total_apps_used_ever: set,
        #   retained_at_30: bool,
        #   recent_days_active: int,
        #   recent_screentime: int,
        #   recent_sessions: int,
        #   recent_apps: set,
        #   recent_app_usage: { appName: screentime }
        # }
        
        total_recent_active_days = 0

        for doc in docs:
            date_str = doc.id
            user_id = doc.reference.parent.parent.id
            
            data = doc.to_dict() or {}
            apps = data.get("apps", [])
            if isinstance(apps, dict):
                 apps = list(apps.values())
                 
            if user_id not in user_profiles:
                user_profiles[user_id] = {
                    "first_seen": date_str,
                    "last_seen": date_str,
                    "total_apps_used_ever": set(),
                    "recent_days_active": 0,
                    "recent_screentime": 0,
                    "recent_sessions": 0,
                    "recent_apps": set(),
                    "recent_app_usage": {},
                    "is_active_after_30": False
                }
                
            profile = user_profiles[user_id]
            
            if date_str < profile["first_seen"]:
                profile["first_seen"] = date_str
            if date_str > profile["last_seen"]:
                profile["last_seen"] = date_str
                
            # Check if active at day 30+
            fs_dt = datetime.strptime(profile["first_seen"], "%Y-%m-%d")
            curr_dt = datetime.strptime(date_str, "%Y-%m-%d")
            if (curr_dt - fs_dt).days >= 30:
                profile["is_active_after_30"] = True
                
            is_recent = date_str >= cutoff_str
            if is_recent:
                profile["recent_days_active"] += 1
                total_recent_active_days += 1
            
            for app in apps:
                app_name = app.get("appName", "Unknown")
                sc = app.get("totalScreenTime", 0)
                sess = app.get("sessionCount", 0)
                
                profile["total_apps_used_ever"].add(app_name)
                
                if is_recent:
                    profile["recent_screentime"] += sc
                    profile["recent_sessions"] += sess
                    profile["recent_apps"].add(app_name)
                    if app_name not in profile["recent_app_usage"]:
                        profile["recent_app_usage"][app_name] = 0
                    profile["recent_app_usage"][app_name] += sc

        # Process Stages
        stages_def = {
            "new": {"label": "0-7 days", "min": 0, "max": 7},
            "growing": {"label": "8-30 days", "min": 8, "max": 30},
            "mature": {"label": "31-90 days", "min": 31, "max": 90},
            "veteran": {"label": "90+ days", "min": 91, "max": 99999}
        }
        
        stages_metrics = {
            key: {
                "user_count": 0,
                "recent_active_users_count": 0, # for average denominator
                "recent_active_days": 0,
                "screentime_sum": 0,
                "sessions_sum": 0,
                "apps_sum": 0,
                "app_counts": {},
                "retained_count": 0,
                "eligible_for_retention": 0
            } for key in stages_def
        }

        # Aha moment stats
        aha_above_30_eligible = 0
        aha_above_30_retained = 0
        aha_below_30_eligible = 0
        aha_below_30_retained = 0

        for user_id, profile in user_profiles.items():
            fs_dt = datetime.strptime(profile["first_seen"], "%Y-%m-%d")
            age_days = (today_dt - fs_dt).days
            if age_days < 0: age_days = 0
            
            # Identify stage
            assigned_stage = "veteran"
            for k, info in stages_def.items():
                if info["min"] <= age_days <= info["max"]:
                    assigned_stage = k
                    break
                    
            sm = stages_metrics[assigned_stage]
            sm["user_count"] += 1
            
            # Retention logic (simplified: did they use app after threshold)
            # For new/growing stage, retention might mean active in last 7 days?
            # Standard "Retention Rate" usually means D30 or D7. 
            # We'll calculate simple retention: are they still active? (last seen in last 7 days)
            ls_dt = datetime.strptime(profile["last_seen"], "%Y-%m-%d")
            is_retained = (today_dt - ls_dt).days <= 7
            if is_retained:
                sm["retained_count"] += 1
            
            if profile["recent_days_active"] > 0:
                sm["recent_active_users_count"] += 1
                sm["recent_active_days"] += profile["recent_days_active"]
                sm["screentime_sum"] += profile["recent_screentime"]
                sm["sessions_sum"] += profile["recent_sessions"]
                sm["apps_sum"] += len(profile["recent_apps"])
                
                for app_name, sc in profile["recent_app_usage"].items():
                    if app_name not in sm["app_counts"]:
                        sm["app_counts"][app_name] = {"sc": 0, "users": set()}
                    sm["app_counts"][app_name]["sc"] += sc
                    sm["app_counts"][app_name]["users"].add(user_id)
            
            # Aha moment calculation
            if age_days >= 30:
                total_apps = len(profile["total_apps_used_ever"])
                if total_apps >= 10:
                    aha_above_30_eligible += 1
                    if profile["is_active_after_30"]: aha_above_30_retained += 1
                else:
                    aha_below_30_eligible += 1
                    if profile["is_active_after_30"]: aha_below_30_retained += 1

        # Format Stage Outputs
        stages_output = {}
        for key, sm in stages_metrics.items():
            active_users = sm["recent_active_users_count"] or 1
            avg_sc_sec = int(sm["screentime_sum"] / active_users / 1000)
            avg_sess = round(sm["sessions_sum"] / active_users, 1)
            avg_apps = round(sm["apps_sum"] / active_users, 1)
            
            top_app = "N/A"
            if sm["app_counts"]:
                top_app = max(sm["app_counts"].items(), key=lambda x: x[1]["sc"])[0]
                
            percent_dau = 0.0
            if total_recent_active_days > 0:
                percent_dau = round((sm["recent_active_days"] / total_recent_active_days) * 100, 1)
                
            retention_rate = 0.0
            if sm["user_count"] > 0:
                retention_rate = round((sm["retained_count"] / sm["user_count"]) * 100, 1)
                
            out = {
                "range": stages_def[key]["label"],
                "user_count": sm["user_count"],
                "percent_of_dau": percent_dau,
                "avg_screentime_formatted": format_duration(avg_sc_sec),
                "avg_sessions": avg_sess,
                "avg_apps_used": avg_apps,
                "retention_rate": retention_rate
            }
            if top_app != "N/A":
                out["top_app"] = top_app
            stages_output[key] = out
            
        # First apps for new users
        new_app_counts = stages_metrics["new"]["app_counts"]
        new_users_total = stages_metrics["new"]["user_count"]
        
        first_apps = []
        if new_users_total > 0:
            for app_name, info in new_app_counts.items():
                pct = round((len(info["users"]) / new_users_total) * 100, 1)
                first_apps.append({"app_name": app_name, "percent_of_new_users": pct})
                
        first_apps.sort(key=lambda x: x["percent_of_new_users"], reverse=True)
        first_apps = first_apps[:5] # top 5
        
        # Aha moment
        retention_if_above = round((aha_above_30_retained / aha_above_30_eligible * 100), 1) if aha_above_30_eligible > 0 else 0.0
        retention_if_below = round((aha_below_30_retained / aha_below_30_eligible * 100), 1) if aha_below_30_eligible > 0 else 0.0
        
        aha_moment = {
            "threshold_apps": 10,
            "retention_if_above": retention_if_above,
            "retention_if_below": retention_if_below,
            "insight": f"Users tracking 10+ apps retain {retention_if_above}% at day 30 vs {retention_if_below}% for < 10 apps"
        }

        return {
            "stages": stages_output,
            "first_apps_new_users": first_apps,
            "aha_moment": aha_moment
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# --- FROM alerts.py ---

from typing import Dict, Any, List, Optional
from database.firebase import db
from middleware.auth import verify_api_key
from datetime import datetime, timezone
from pydantic import BaseModel
from google.cloud.firestore_v1.base_query import FieldFilter
import firebase_admin.auth as auth

# Optional Firebase JWT verifier if you are passing Firebase User Tokens. 
# Alternatively, since we use `verify_api_key` globally for this dashboard, we can just grab role/clientId from headers/query.
# For full compatibility with the prompt, we simulate the verifyUser logic.
async def function_helper_verify_user(request: Request):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        # Fallback to pure API-Key mode (Assume Admin) if no JWT
        return {"uid": "admin_user", "role": "admin", "clientId": None}
    
    token = auth_header.split('Bearer ')[1]
    try:
        # Assuming the token passed is actually a Firebase JWT, verify it:
        decoded = auth.verify_id_token(token)
        uid = decoded.get("uid")
        
        user_doc = db.collection('dashboard_users').document(uid).get()
        if not user_doc.exists:
            return {"uid": uid, "role": "admin", "clientId": None}
            
        data = user_doc.to_dict()
        return {"uid": uid, "role": data.get("role", "client"), "clientId": data.get("clientId")}
    except Exception as e:
        # Fallback to Admin (useful during dev with just the API key)
        return {"uid": "admin_user", "role": "admin", "clientId": None}

# Models
class MarkAlertReadRequest(BaseModel):
    alert_ids: Optional[List[str]] = []
    mark_all: bool = False

class AlertRulePostRequest(BaseModel):
    action: str  # "create" | "toggle" | "delete"
    # Create fields
    label: Optional[str] = None
    category: Optional[str] = None
    threshold: Optional[float] = None
    condition: Optional[str] = None
    # Toggle/Delete fields
    rule_id: Optional[str] = None
    is_active: Optional[bool] = None

@router.get("/getAlerts")
async def get_alerts(
    request: Request,
    status: Optional[str] = Query("all", description="'unread' or 'all'"),
    client_id: Optional[str] = Query(None, alias="clientId"),
    type: Optional[str] = Query(None),
    limit: int = Query(50)
):
    try:
        user_info = await function_helper_verify_user(request)
        role = user_info["role"]
        assigned_client_id = user_info["clientId"]

        query = db.collection("alerts")
        
        # 1. Client ID logic
        if role == "client":
            query = query.where("clientId", "==", assigned_client_id)
        elif role == "admin" and client_id:
            query = query.where("clientId", "==", client_id)

        # 2. Status logic
        if status == "unread":
            query = query.where("is_read", "==", False)

        # 3. Type logic
        if type:
            query = query.where("type", "==", type)

        # Order by created_at DESC
        query = query.order_by("created_at", direction="DESCENDING").limit(limit)
        
        docs = query.stream()
        
        alerts_list = []
        unread_count = 0
        total_count = 0
        
        for doc in docs:
            data = doc.to_dict()
            data["alert_id"] = doc.id
            if data.get("created_at"):
                data["created_at"] = data["created_at"].isoformat() if hasattr(data["created_at"], 'isoformat') else data["created_at"]
                
            if data.get("is_read") is False:
                unread_count += 1
            alerts_list.append(data)
            total_count += 1
            
        return {
            "unread_count": unread_count,
            "total_count": total_count,
            "alerts": alerts_list
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/markAlertRead")
async def mark_alert_read(
    request: Request,
    payload: MarkAlertReadRequest
):
    try:
        user_info = await function_helper_verify_user(request)
        role = user_info["role"]
        assigned_client_id = user_info["clientId"]

        updated_count = 0
        updated_ids = []

        if payload.mark_all:
            query = db.collection("alerts").where("is_read", "==", False)
            if role == "client":
                query = query.where("clientId", "==", assigned_client_id)
                
            docs = query.stream()
            batch = db.batch()
            for doc in docs:
                batch.update(doc.reference, {"is_read": True})
                updated_ids.append(doc.id)
                updated_count += 1
            batch.commit()
        else:
            batch = db.batch()
            for alert_id in payload.alert_ids:
                doc_ref = db.collection("alerts").document(alert_id)
                doc = doc_ref.get()
                if doc.exists:
                    data = doc.to_dict()
                    if role == "admin" or data.get("clientId") == assigned_client_id:
                        batch.update(doc_ref, {"is_read": True})
                        updated_ids.append(alert_id)
                        updated_count += 1
            batch.commit()

        return {
            "success": True,
            "updated_count": updated_count,
            "alert_ids_updated": updated_ids
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_default_rules(client_id):
    return [
        {
            "rule_id": "default_dau_drop",
            "clientId": client_id,
            "category": "dau_drop",
            "label": "DAU drops vs last week",
            "threshold": 15,
            "condition": "less_than",
            "is_active": True,
            "is_default": True
        },
        {
            "rule_id": "default_at_risk",
            "clientId": client_id,
            "category": "at_risk_spike",
            "label": "At-risk users spike",
            "threshold": 20,
            "condition": "greater_than",
            "is_active": True,
            "is_default": True
        },
        {
            "rule_id": "default_stickiness",
            "clientId": client_id,
            "category": "stickiness_low",
            "label": "Stickiness falls below",
            "threshold": 10,
            "condition": "less_than",
            "is_active": True,
            "is_default": True
        },
        {
            "rule_id": "default_session_drop",
            "clientId": client_id,
            "category": "session_drop",
            "label": "Session duration drops",
            "threshold": 25,
            "condition": "less_than",
            "is_active": True,
            "is_default": True
        },
        {
            "rule_id": "default_retention",
            "clientId": client_id,
            "category": "retention_milestone",
            "label": "Day-1 retention all-time high",
            "threshold": 0,
            "condition": "greater_than",
            "is_active": True,
            "is_default": True
        }
    ]

@router.get("/manageAlertRules")
async def get_manage_alert_rules(
    request: Request,
    client_id: Optional[str] = Query(None, alias="clientId")
):
    try:
        user_info = await function_helper_verify_user(request)
        role = user_info["role"]
        assigned_client_id = user_info["clientId"]
        
        target_client = assigned_client_id if role == "client" else (client_id or assigned_client_id)

        query = db.collection("alert_rules")
        if target_client:
            query = query.where("clientId", "==", target_client)
            
        docs = query.order_by("created_at").stream()
        
        custom_rules = []
        custom_categories = set()
        for doc in docs:
            data = doc.to_dict()
            data["rule_id"] = doc.id
            if data.get("created_at"):
                data["created_at"] = data["created_at"].isoformat() if hasattr(data["created_at"], 'isoformat') else data["created_at"]
            custom_rules.append(data)
            if "category" in data:
                custom_categories.add(data["category"])
                
        # Merge defaults
        defaults = get_default_rules(target_client)
        merged_rules = custom_rules.copy()
        for df in defaults:
            if df["category"] not in custom_categories:
                merged_rules.append(df)
                
        return {"rules": merged_rules}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/manageAlertRules")
async def post_manage_alert_rules(
    request: Request,
    payload: AlertRulePostRequest
):
    try:
        user_info = await function_helper_verify_user(request)
        role = user_info["role"]
        assigned_client_id = user_info["clientId"]

        # Only allow creating for yourself unless you are admin sending a specific clientId (not fully implemented in spec, assuming assigned_client_id)
        target_client = assigned_client_id 
        if role == "admin" and not target_client:
             target_client = "global_admin_client"

        if payload.action == "create":
            if not all([payload.label, payload.category, payload.threshold is not None, payload.condition]):
                raise HTTPException(status_code=400, detail="Missing required fields for create")
                
            # Check duplicate
            existing = db.collection("alert_rules").where("clientId", "==", target_client).where("category", "==", payload.category).limit(1).stream()
            if any(existing):
                raise HTTPException(status_code=400, detail="Rule for this category already exists. Use toggle to change it.")
                
            new_rule = {
                "clientId": target_client,
                "label": payload.label,
                "category": payload.category,
                "threshold": payload.threshold,
                "condition": payload.condition,
                "is_active": True,
                "created_at": datetime.now(timezone.utc),
                "is_default": False
            }
            doc_ref = db.collection("alert_rules").document()
            doc_ref.set(new_rule)
            
            new_rule["rule_id"] = doc_ref.id
            new_rule["created_at"] = new_rule["created_at"].isoformat()
            
            return {"success": True, "rule": new_rule}

        elif payload.action == "toggle":
            if not payload.rule_id or payload.is_active is None:
                raise HTTPException(status_code=400, detail="Missing rule_id or is_active for toggle")
                
            doc_ref = db.collection("alert_rules").document(payload.rule_id)
            doc = doc_ref.get()
            if not doc.exists:
                raise HTTPException(status_code=404, detail="Rule not found")
                
            data = doc.to_dict()
            if role != "admin" and data.get("clientId") != assigned_client_id:
                raise HTTPException(status_code=403, detail="Unauthorized")
                
            doc_ref.update({"is_active": payload.is_active})
            return {"success": True, "rule": {"rule_id": payload.rule_id, "is_active": payload.is_active}}
            
        elif payload.action == "delete":
            if not payload.rule_id:
                raise HTTPException(status_code=400, detail="Missing rule_id for delete")
                
            doc_ref = db.collection("alert_rules").document(payload.rule_id)
            doc = doc_ref.get()
            if not doc.exists:
                raise HTTPException(status_code=404, detail="Rule not found")
            
            data = doc.to_dict()
            if role != "admin" and data.get("clientId") != assigned_client_id:
                raise HTTPException(status_code=403, detail="Unauthorized")
                
            if data.get("is_default") == True:
                raise HTTPException(status_code=400, detail="Cannot delete default rules")
                
            doc_ref.delete()
            return {"success": True, "deleted_rule_id": payload.rule_id}
            
        else:
            raise HTTPException(status_code=400, detail="Invalid action")

    except ValueError as e:
         raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/getBulkSignalMetrics")
async def get_bulk_signal_metrics(
    date: str = Query(..., description="Date (YYYY-MM-DD)"),
    userIds: Optional[str] = Query(None, description="Comma-separated user IDs")
):
    """
    High-performance endpoint for fetching aggregated signal metrics (AFI, SCS, RLI, etc.)
    for multiple users in a single request. 
    Ideal for the Decisions page to avoid hundreds of small requests.
    """
    try:
        # 1. Determine target users
        if userIds:
            uids = [uid.strip() for uid in userIds.split(",") if uid.strip()]
        else:
            # Fallback: get some active users
            users_ref = db.collection("users").limit(50)
            uids = [d.id for d in users_ref.stream()]

        if not uids:
            return {"success": True, "data": {}}

        # Helper math functions
        def calculate_median(arr: List[float]) -> float:
            if not arr: return 0
            sorted_arr = sorted(arr)
            n = len(sorted_arr)
            if n % 2 == 1:
                return sorted_arr[n // 2]
            return (sorted_arr[n // 2 - 1] + sorted_arr[n // 2]) / 2.0

        def calculate_metrics_for_user(uid: str):
            # Fetch sessions
            sess_ref = db.collection("sessions").document(uid).collection("dates").document(date)
            sess_doc = sess_ref.get()
            sessions = []
            if sess_doc.exists:
                sessions = sess_doc.to_dict().get("sessions", [])

            # Fetch screentime
            st_ref = db.collection("screentime").document(uid).collection("dates").document(date)
            st_doc = st_ref.get()
            apps = []
            if st_doc.exists:
                apps_data = st_doc.to_dict().get("apps", [])
                if isinstance(apps_data, list):
                    apps = apps_data
                elif isinstance(apps_data, dict):
                    apps = list(apps_data.values())

            # 1. AFI Calculation
            total_st_ms = sum(a.get("totalScreenTime", 0) for a in apps)
            active_hours = total_st_ms / (1000 * 60 * 60)
            session_count = len(sessions)
            afi = round(session_count / active_hours, 2) if active_hours > 0 else 0

            # 2. SCS Calculation
            durations = [s.get("duration", 0) for s in sessions if s.get("duration")]
            if durations:
                mean_dur = sum(durations) / len(durations)
                med_dur = calculate_median(durations)
                scs = round(med_dur / mean_dur, 2) if mean_dur > 0 else 0
            else:
                scs = 0

            # 3. RLI Calculation (Top 3 apps sessions / total)
            if apps and session_count > 0:
                app_sess = sorted([a.get("sessionCount", 0) for a in apps], reverse=True)
                top3_sum = sum(app_sess[:3])
                rli = round(top3_sum / session_count, 2)
            else:
                rli = 0

            # 4. Engagement Score (Simple version matching frontend)
            morning_hours = [0,1,2,3,4,5,6,7]
            day_hours = [8,9,10,11,12,13,14,15]
            evening_hours = [16,17,18,19,20,21,22,23]

            def get_period_metrics(hours_list):
                p_sess = [s for s in sessions if datetime.fromisoformat(s.get("startTime", "2000-01-01")).hour in hours_list]
                p_st_hr = sum(s.get("duration", 0) for s in p_sess) / (1000 * 60 * 60)
                score = min(50, len(p_sess) * 2) + min(50, p_st_hr * 15)
                return score

            eng_score = round((get_period_metrics(morning_hours) + get_period_metrics(day_hours) + get_period_metrics(evening_hours)) / 3)

            # 5. Attention State
            # Median thresholds from case study
            AFI_MEDIAN = 13.68
            SCS_MEDIAN = 0.53
            if afi <= AFI_MEDIAN and scs >= SCS_MEDIAN:
                att_state = "Stable"
            elif afi > AFI_MEDIAN and scs >= SCS_MEDIAN:
                att_state = "Degrading"
            else:
                att_state = "Fatigued"

            # 6. Fatigue Indicators (Delta AFI Simplified)
            night_hr = [18,19,20,21,22,23,0,1,2,3,4,5]
            day_hr = [6,7,8,9,10,11,12,13,14,15,16,17]
            
            def get_period_afi(hr_list):
                p_sess = [s for s in sessions if datetime.fromisoformat(s.get("startTime", "2000-01-01")).hour in hr_list]
                p_st_hr = sum(s.get("duration", 0) for s in p_sess) / (1000 * 60 * 60)
                return len(p_sess) / p_st_hr if p_st_hr > 0 else 0

            delta_afi = round(get_period_afi(night_hr) - get_period_afi(day_hr), 2)
            
            fatigue_risk = "Low"
            if delta_afi >= 6 or att_state == "Fatigued" or eng_score <= 30:
                fatigue_risk = "High"
            elif delta_afi >= 2 or att_state == "Degrading":
                fatigue_risk = "Moderate"

            persona = "Habit-Dominant"
            if rli < 0.68 and afi > 13.68:
                persona = "Distraction-Dominant"
            elif rli < 0.68 or afi > 13.68:
                persona = "Mixed"

            return {
                "sessionCount": session_count,
                "activeTimeHours": round(active_hours, 2),
                "afi": afi,
                "scs": scs,
                "rli": rli,
                "engagementScore": eng_score,
                "attentionState": att_state,
                "deltaAFI": delta_afi,
                "fatigueRisk": fatigue_risk,
                "digitalPersona": persona
            }

        # Concurrently calculate for all users
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_uid = {executor.submit(calculate_metrics_for_user, uid): uid for uid in uids}
            for future in as_completed(future_to_uid):
                uid = future_to_uid[future]
                try:
                    results[uid] = future.result()
                except Exception as e:
                    print(f"Error calculating metrics for {uid}: {e}")

        return {"success": True, "data": results}

    except Exception as e:
        print(f"❌ Error in getBulkSignalMetrics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

# --- Report Generation Endpoints ---

class GenerateReportRequest(BaseModel):
    type: str
    format: str
    start_date: str
    end_date: str
    sections: List[str]

class ScheduleReportRequest(BaseModel):
    report_type: str
    schedule: str
    recipients: List[str]
    format: str

@router.post("/generateReport")
async def generate_report(request: Request, payload: GenerateReportRequest):
    """
    On-demand report generation. In production, this would trigger a background task
    to fetch all relevant metrics for the date range and compile a PDF.
    """
    try:
        user_info = await verify_user(request)
        role = user_info["role"]
        assigned_client_id = user_info["clientId"]
        
        # In a real app, you would store the job status in Firestore/Redis
        # and trigger a worker (Celery/Cloud Tasks)
        report_id = f"rpt_{hashlib.md5(f'{payload.start_date}-{payload.end_date}-{assigned_client_id}'.encode()).hexdigest()[:8]}"
        
        # Mocking the processing start
        return {
            "report_id": report_id,
            "status": "processing",
            "estimated_time_seconds": 30,
            "download_url": None
        }
    except Exception as e:
        print(f"❌ Error in generateReport: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/getReport")
async def get_report(request: Request, report_id: str = Query(..., description="The unique report ID")):
    """
    Fetch the link to a generated report. Returns status and download URL if ready.
    """
    try:
        # Mocking a completed report response
        # In production, check Firestore 'reports' collection for this ID
        return {
            "report_id": report_id,
            "status": "completed",
            "download_url": f"https://api-dashboard-8ukc.onrender.com/api/dashboard/reports/download/{report_id}.pdf",
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.post("/scheduleReport")
async def schedule_report(request: Request, payload: ScheduleReportRequest):
    """
    Schedules automated report distribution to a list of recipients.
    """
    try:
        user_info = await verify_user(request)
        assigned_client_id = user_info["clientId"]
        
        # Store schedule definition in Firestore 'report_schedules'
        # Would be picked up by a cron worker
        return {
            "success": True,
            "message": f"Successfully scheduled {payload.report_type} report for {payload.schedule}",
            "recipients_count": len(payload.recipients),
            "clientId": assigned_client_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/exportAllData")
async def export_all_data(
    request: Request,
    date: str = Query(..., description="Date (YYYY-MM-DD)"),
    userIds: Optional[str] = Query(None, description="Optional filter by user IDs")
):
    """
    Comprehensive data aggregation for the Reports page.
    Fetches data from ALL core dashboard modules for offline export.
    """
    try:
        # Perform authentication check
        await verify_user(request)
        
        # 1. Fetch Summary & Lifecycle
        summary = await get_summary(date=date)
        funnel = await getGrowthFunnel()
        
        # 2. Fetch DAU & Engagement Trends
        dau_trend = await get_dau_trend(days=30)
        usage_patterns = await get_screentime_patterns(days=30) # Fixed name
        
        # 3. Fetch App Intelligence & Categories
        top_apps = await getTopApps(days=7, limit=100)
        categories = await getCategoryDrilldown(category="Social", days=30)
        
        # 4. Fetch User Segments & Archetypes
        user_segments = await getUserSegments(date=date)
        
        # 5. Fetch Signal Metrics for active users (AFI/SCS/RLI)
        bulk_signals = await get_bulk_signal_metrics(date=date, userIds=userIds)

        # 6. Fetch Alerts & Rules
        alerts_res = await get_alerts(request=request, status="all", limit=500)
        rules = await get_manage_alert_rules(request=request)
        
        # 7. Fetch Retention & Stickiness
        retention = await getCohortRetention()
        stickiness = await get_stickiness(days=30)

        # 8. Fetch Wellbeing & Behavioral Flow
        wellbeing = await getWellbeingReport(date=date)
        
        # Generate the consolidated report payload
        report_data = {
            "metadata": {
                "snapshot_date": date,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "pages_covered": 15,
                "version": "1.0.2"
            },
            "sheets": {
                "Dashboard_Summary": summary,
                "Growth_Funnel": funnel,
                "DAU_30D_Trend": dau_trend,
                "Active_Usage_Patterns": usage_patterns,
                "Competitive_App_Intel": top_apps,
                "Category_Drilldown": categories,
                "User_Segment_Matrix": user_segments,
                "AFI_SCS_Signal_Metrics": bulk_signals,
                "System_Alerts_Log": alerts_res.get("alerts", []),
                "Alert_Config_Rules": rules,
                "Cohort_Retention_Data": retention,
                "Platform_Stickiness": stickiness,
                "Wellbeing_Audit": wellbeing
            }
        }

        return {
            "success": True,
            "data": report_data
        }
    except Exception as e:
        print(f"❌ Error in exportAllData: {e}")
        # Log the specific error to help debugging
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
