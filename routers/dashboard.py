from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import Optional, List, Dict, Any
from database.firebase import db, init_error
from datetime import datetime, timedelta
from firebase_admin import firestore
from middleware.auth import verify_api_key

router = APIRouter(
    prefix="/api/dashboard",
    tags=["Dashboard"],
    dependencies=[Depends(verify_api_key)]
)

@router.get("/users", response_model=Dict[str, Any])
async def get_all_users():
    """
    Fetch all users from the users collection.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        users_ref = db.collection("users")
        docs = users_ref.stream()

        users = []
        for doc in docs:
            user_data = doc.to_dict()
            user_data["id"] = doc.id
            users.append(user_data)

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
    Inactive: No data synced in the last 5 days.
    """
    try:
        if not db:
            raise HTTPException(
                status_code=500, 
                detail=f"Database connection not initialized. Error: {init_error}"
            )

        users_ref = db.collection("users")
        docs = users_ref.stream()

        users_status = []
        
        # Calculate 5 days ago date string
        today = datetime.now()
        five_days_ago = (today - timedelta(days=5)).strftime("%Y-%m-%d")

        for doc in docs:
            user_data = doc.to_dict()
            user_id = doc.id
            user_data["id"] = user_id
            
            # Check for recent activity
            # Query screentime/{userId}/dates where document ID (date) >= five_days_ago
            # We use limit(1) because finding even one record is enough to mark as active
            screentime_dates_ref = db.collection("screentime").document(user_id).collection("dates")
            
            # Note: Firestore queries on document IDs are done via FieldPath.document_id()
            # We use "__name__" string which represents the document ID field.
            # However, when filtering by __name__, the value must be a full document path reference if we use the client SDKs in some contexts,
            # OR we can just check string if we don't treat it as a reference.
            # A common workaround that is robust verify simple string ID:
            # We want to check if any document exists with ID >= five_days_ago.
            # But ">= string" on __name__ often requires a Doc Ref.
            # Let's try matching the exact path reference logic.
            
            # Construct a reference to a theoretical document at the start date
            start_date_doc_ref = screentime_dates_ref.document(five_days_ago)

            recent_activity = screentime_dates_ref.where(
                "__name__", ">=", start_date_doc_ref
            ).limit(1).stream()

            # stream() returns a generator. We check if it yields any items.
            is_active = False
            for _ in recent_activity:
                is_active = True
                break
            
            user_data["status"] = "Active" if is_active else "Inactive"
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
        
        # Scenario 1: Specific User
        if user_id:
            user_ref = db.collection("screentime").document(user_id).collection("dates")
            
            if date:
                # Specific Date, Specific User
                doc_ref = user_ref.document(date)
                doc = doc_ref.get()
                if doc.exists:
                    data = doc.to_dict()
                    data["userId"] = user_id
                    data["date"] = date
                    screentime_data.append(data)
            else:
                # All Dates, Specific User
                docs = user_ref.stream()
                for doc in docs:
                    data = doc.to_dict()
                    data["userId"] = user_id
                    data["date"] = doc.id
                    screentime_data.append(data)
        
        # Scenario 2: All Users
        else:
            users_ref = db.collection("screentime")
            # Note: Fetching all subcollections for all users is expensive in Firestore.
            # In Node.js, it did `db.collection('screentime').get()` then iterated docs.
            # But 'screentime' documents (users) might not have data themselves, only subcollections.
            # However, the Node.js code assumes `db.collection('screentime').get()` returns user docs.
            # We follow the same pattern.
            
            users_snapshot = users_ref.stream()
            for user_doc in users_snapshot:
                current_user_id = user_doc.id
                dates_ref = users_ref.document(current_user_id).collection("dates")
                
                if date:
                    # Specific Date, All Users
                    doc_ref = dates_ref.document(date)
                    doc = doc_ref.get()
                    if doc.exists:
                        data = doc.to_dict()
                        data["userId"] = current_user_id
                        data["date"] = date
                        screentime_data.append(data)
                else:
                    # All Dates, All Users
                    # This is potentially VERY large. Replicating existing logic.
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
    """
    Fetch sessions data from nested Firestore collections.
    Structure: sessions/{userId}/dates/{date}
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database connection not initialized")

        sessions_data = []

        # Logic mirrors get_screentime but for 'sessions' collection
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
