
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import json

load_dotenv()

def inspect_users_deep():
    print("Inspecting Users and Screentime...")
    
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
    
    # Check a user with 'lastLogin' or similar
    users_ref = db.collection("users").limit(5)
    for doc in users_ref.stream():
        print(f"\nUser {doc.id}:")
        data = doc.to_dict()
        print(json.dumps(data, indent=2, default=str)) # Check for timestamp fields
        
    # Check screentime structure for sessions
    # We know screentime/{userId}/dates/{date} exists.
    # Let's see if we can find a user with data and inspect one date doc.
    print("\n--- Screentime Data Check ---")
    st_ref = db.collection("screentime").limit(1)
    for doc in st_ref.stream():
        user_id = doc.id
        print(f"Checking screentime for user: {user_id}")
        dates_ref = db.collection("screentime").document(user_id).collection("dates").limit(1)
        for date_doc in dates_ref.stream():
            print(f"  Date: {date_doc.id}")
            print(json.dumps(date_doc.to_dict(), indent=2, default=str))

if __name__ == "__main__":
    inspect_users_deep()
