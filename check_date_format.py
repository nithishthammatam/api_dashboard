import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def initialize_firebase():
    if firebase_admin._apps:
        return firestore.client()
    
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
    
    firebase_admin.initialize_app(cred)
    return firestore.client()

db = initialize_firebase()

print("=== CHECKING ACTUAL DATE DOCUMENT IDs ===\n")

# Get first user
users_ref = db.collection("users")
user_docs = users_ref.limit(1).stream()

for user_doc in user_docs:
    user_id = user_doc.id
    print(f"User ID: {user_id}\n")
    
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    date_docs = dates_ref.limit(20).stream()
    
    print("Date Document IDs found:")
    date_ids = []
    for date_doc in date_docs:
        date_id = date_doc.id
        date_ids.append(date_id)
        print(f"  - {date_id}")
    
    print(f"\nTotal dates for this user: {len(date_ids)}")
    if date_ids:
        print(f"Latest date: {max(date_ids)}")
        print(f"Earliest date: {min(date_ids)}")
    
    # Check today's date
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\nToday's date string: {today}")
    print(f"Is today in database? {today in date_ids}")
    
    break
