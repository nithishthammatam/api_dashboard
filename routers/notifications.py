from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Dict, Any, List, Optional
from database.firebase import db
from middleware.auth import verify_api_key
from datetime import datetime, timezone
from pydantic import BaseModel

router = APIRouter(
    prefix="/api/notifications",
    tags=["Notifications"],
    dependencies=[Depends(verify_api_key)]
)

class NotificationRequest(BaseModel):
    title: str
    body: str
    image_url: Optional[str] = None
    data: Optional[Dict[str, str]] = None

@router.post("/send-all")
async def send_to_all_users(notification: NotificationRequest):
    """
    Send a notification to all users who have an 'fcmToken' field.
    Sends in batches of 500 (FCM limit).
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database connection not initialized")

        # 1. Create notification record (Status: sending)
        doc_ref = db.collection("notifications").document()
        # Default data payload should be strings for FCM
        data_payload = notification.data or {}
        # Ensure all values are strings
        data_payload = {k: str(v) for k, v in data_payload.items()}
        
        notification_data = {
            "id": doc_ref.id,
            "title": notification.title,
            "body": notification.body,
            "image_url": notification.image_url,
            "data": data_payload,
            "status": "sending",
            "created_at": datetime.now(timezone.utc),
            "sent_at": None,
            "success_count": 0,
            "failure_count": 0,
            "target": "all_users"
        }
        
        doc_ref.set(notification_data)
        
        # 2. Fetch Users and Tokens
        print(f"🔍 Fetching users from Firestore...")
        users_ref = db.collection("users")
        docs = users_ref.select(["fcmToken"]).stream()
        
        tokens = []
        for doc in docs:
            user_data = doc.to_dict()
            token = user_data.get("fcmToken")
            if token:
                tokens.append(token)
                
        total_tokens = len(tokens)
        print(f"📊 Found {total_tokens} users with tokens out of 156 total.")
        
        if total_tokens == 0:
            # Update status to sent (but 0 recipients)
            doc_ref.update({
                "status": "sent",
                "sent_at": datetime.now(timezone.utc),
                "success_count": 0,
                "failure_count": 0,
                "note": "No users with fcmToken found"
            })
            return {
                "success": True, 
                "message": "Notification record created, but no user tokens found.", 
                "id": doc_ref.id, 
                "recipient_count": 0
            }

        # 3. Send in batches of 500
        from firebase_admin import messaging
        
        success_count = 0
        failure_count = 0
        
        # Helper to chunk list
        def chunk_list(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
                
        batches = list(chunk_list(tokens, 500))
        
        for batch_tokens in batches:
            try:
                print(f"🚀 Sending batch of {len(batch_tokens)} tokens...")
                message = messaging.MulticastMessage(
                    notification=messaging.Notification(
                        title=notification.title,
                        body=notification.body,
                        image=notification.image_url
                    ),
                    data=data_payload,
                    tokens=batch_tokens
                )
                response = messaging.send_each_for_multicast(message)
                print(f"✅ Batch completed. Success: {response.success_count}, Failure: {response.failure_count}")
                success_count += response.success_count
                failure_count += response.failure_count
            except Exception as batch_error:
                print(f"❌ Batch send error: {type(batch_error).__name__}: {batch_error}")
                import traceback
                traceback.print_exc()
                failure_count += len(batch_tokens)

        # 4. Update status
        update_data = {
            "status": "sent",
            "sent_at": datetime.now(timezone.utc),
            "success_count": success_count,
            "failure_count": failure_count
        }
        doc_ref.update(update_data)
        
        return {
            "success": True,
            "message": f"Notification sent. Success: {success_count}, Failed: {failure_count}",
            "id": doc_ref.id,
            "recipient_count": total_tokens,
            "success_count": success_count,
            "failure_count": failure_count
        }

    except Exception as e:
        print(f"❌ Error sending notification: {e}")
        # Try to update doc with error
        try:
             doc_ref.update({"status": "failed", "error": str(e)})
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history")
async def get_notification_history():
    """
    Get history of sent notifications.
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database connection not initialized")
            
        notifications_ref = db.collection("notifications")
        query = notifications_ref.order_by("created_at", direction="DESCENDING").limit(50)
        docs = query.stream()
        
        history = []
        for doc in docs:
            data = doc.to_dict()
            # Convert timestamps to ISO strings
            if data.get("created_at"):
                data["created_at"] = data["created_at"].isoformat()
            if data.get("sent_at"):
                data["sent_at"] = data["sent_at"].isoformat()
            history.append(data)
            
        return {
            "success": True,
            "data": history
        }

    except Exception as e:
        print(f"❌ Error fetching history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
