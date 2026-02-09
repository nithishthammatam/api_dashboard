
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json

load_dotenv()

def inspect_sessions_collection():
    print("Inspecting 'sessions' collection for Top User...")
    
    private_key = os.getenv("FIREBASE_PRIVATE_KEY")
    client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
    project_id = os.getenv("FIREBASE_PROJECT_ID")
    
    if "\\n" in private_key:
        private_key = private_key.replace("\\n", "\n")
        
    cred = credentials.Certificate({
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    })
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    user_id = "YQICD6"
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Target User: {user_id}")
    print(f"Target Date: {today_str}")

    # Check sessions/{userId}/{date}
    # Or sessions/{userId}/dates/{date} ? Standardize on structure.
    # Previous check was vague. Let's try standard paths.
    
    paths_to_check = [
        f"sessions/{user_id}/dates/{today_str}",
        f"sessions/{user_id}/{today_str}",
        f"sessions/{user_id}/all_sessions",
        f"sessions/{user_id}/daily_sessions/{today_str}"
    ]
    
    found_data = False
    for path in paths_to_check:
        print(f"Checking Path: {path}")
        # Try as document
        try:
            doc = db.document(path).get()
            if doc.exists:
                print(f"  [FOUND DOC] Data: {json.dumps(doc.to_dict(), default=str)[:200]}")
                found_data = True
        except:
            pass
            
        # Try as collection
        try:
            coll = db.collection(path).limit(5).stream()
            count = 0
            for d in coll:
                count += 1
                print(f"  [FOUND COLL ITEM] {d.id}: {json.dumps(d.to_dict(), default=str)[:100]}")
            if count > 0:
                print(f"  Found {count} items in collection.")
                found_data = True
        except:
            pass
            
    if not found_data:
        print("No data found in standard 'sessions' paths.")

if __name__ == "__main__":
    inspect_sessions_collection()
