import firebase_admin
from firebase_admin import credentials, firestore
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def initialize_firebase():
    if firebase_admin._apps:
        return firestore.client()
    
    private_key = os.getenv("FIREBASE_PRIVATE_KEY")
    client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
    project_id = os.getenv("FIREBASE_PROJECT_ID")
    
    if private_key and "\\n" in private_key:
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

def check_user_activity(days_list=[7, 14, 30, 90]):
    print(f"Checking data presence for different windows...")
    
    # Get a sample user
    users_ref = db.collection("screentime")
    user_docs = list(users_ref.limit(5).stream())
    
    for udoc in user_docs:
        user_id = udoc.id
        print(f"\nUser: {user_id}")
        
        dates_ref = users_ref.document(user_id).collection("dates")
        all_date_ids = [d.id for d in dates_ref.stream()]
        print(f"  Total dates in DB for this user: {len(all_date_ids)}")
        
        if not all_date_ids:
            continue
            
        all_date_ids.sort(reverse=True)
        print(f"  Latest date: {all_date_ids[0]}")
        print(f"  Earliest date: {all_date_ids[-1]}")
        
        latest_date = datetime.strptime(all_date_ids[0], "%Y-%m-%d")
        
        for d in days_list:
            cutoff = (latest_date - timedelta(days=d)).strftime("%Y-%m-%d")
            count_in_window = len([dt for dt in all_date_ids if dt >= cutoff])
            print(f"  Dates in last {d} days starting from {all_date_ids[0]}: {count_in_window}")

if __name__ == "__main__":
    check_user_activity()
