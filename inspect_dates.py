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

print("\n=== INSPECTING SCREENTIME DATA ===\n")

# Get first user
users = db.collection("screentime").limit(1).stream()
for user_doc in users:
    user_id = user_doc.id
    print(f"User ID: {user_id}")
    
    # Get dates for this user
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    date_docs = dates_ref.limit(5).stream()
    
    print(f"\nDate documents found:")
    for date_doc in date_docs:
        date_id = date_doc.id
        data = date_doc.to_dict()
        
        print(f"\n  Date ID: {date_id}")
        print(f"  Keys in document: {list(data.keys())}")
        
        # Check apps array
        apps = data.get("apps", [])
        print(f"  Number of apps: {len(apps)}")
        
        if apps and len(apps) > 0:
            print(f"  First app sample: {apps[0]}")
            
            # Calculate totals for this date
            total_screentime = 0
            total_sessions = 0
            
            for app in apps:
                st = app.get("totalScreenTime", 0)
                sess = app.get("sessionCount", 0)
                
                if isinstance(st, (int, float)):
                    total_screentime += st
                
                if isinstance(sess, (int, float)):
                    total_sessions += sess
            
            print(f"  Total screentime (ms): {total_screentime}")
            print(f"  Total sessions: {total_sessions}")
            print(f"  Active hours: {total_screentime / (1000 * 60 * 60):.2f}")

# Now check date ranges
print("\n\n=== DATE RANGE ANALYSIS ===\n")
now = datetime.now()
today_str = now.strftime("%Y-%m-%d")
week_ago_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
month_ago_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")

print(f"Today: {today_str}")
print(f"Week ago: {week_ago_str}")
print(f"Month ago: {month_ago_str}")

# Get all unique dates across all users
print("\n\n=== ALL DATES IN DATABASE ===\n")
all_dates = set()
screentime_ref = db.collection("screentime")
user_docs = screentime_ref.limit(10).stream()

for user_doc in user_docs:
    user_id = user_doc.id
    dates_ref = screentime_ref.document(user_id).collection("dates")
    date_docs = dates_ref.stream()
    
    for date_doc in date_docs:
        all_dates.add(date_doc.id)

print(f"Unique dates found: {sorted(all_dates)}")
print(f"\nTotal unique dates: {len(all_dates)}")
