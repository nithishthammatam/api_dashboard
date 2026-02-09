import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict

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

print("\n=== ANALYZING DATABASE DATES ===\n")

# Calculate date ranges
now = datetime.now()
today_str = now.strftime("%Y-%m-%d")
week_ago_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
month_ago_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")

print(f"Current date: {today_str}")
print(f"Week ago: {week_ago_str}")
print(f"Month ago: {month_ago_str}")

# Collect all dates and their data
date_stats = defaultdict(lambda: {"users": set(), "sessions": 0, "screentime": 0})
all_dates = set()

users_ref = db.collection("users")
user_docs = users_ref.limit(10).stream()  # Sample first 10 users

print("\n=== Sampling First 10 Users ===\n")

for user_doc in user_docs:
    user_id = user_doc.id
    
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    date_docs = dates_ref.stream()
    
    user_dates = []
    for date_doc in date_docs:
        date_id = date_doc.id
        all_dates.add(date_id)
        user_dates.append(date_id)
        
        data = date_doc.to_dict()
        apps_list = data.get("apps", [])
        
        date_screentime = 0
        date_sessions = 0
        
        if isinstance(apps_list, list):
            for app in apps_list:
                st = app.get("totalScreenTime", 0)
                sess = app.get("sessionCount", 0)
                
                if isinstance(st, (int, float)):
                    date_screentime += st
                
                if isinstance(sess, (int, float)):
                    date_sessions += sess
        
        date_stats[date_id]["users"].add(user_id)
        date_stats[date_id]["sessions"] += date_sessions
        date_stats[date_id]["screentime"] += date_screentime
    
    if user_dates:
        print(f"User {user_id}: {len(user_dates)} dates - Latest: {max(user_dates)}, Earliest: {min(user_dates)}")

print(f"\n=== All Unique Dates Found (sorted) ===\n")
sorted_dates = sorted(all_dates)
print(f"Total unique dates: {len(sorted_dates)}")
print(f"Date range: {sorted_dates[0] if sorted_dates else 'N/A'} to {sorted_dates[-1] if sorted_dates else 'N/A'}")

print(f"\n=== Dates Breakdown ===\n")
for date in sorted(date_stats.keys(), reverse=True)[:10]:  # Show latest 10 dates
    stats = date_stats[date]
    hours = stats["screentime"] / (1000 * 60 * 60)
    print(f"{date}: {len(stats['users'])} users, {stats['sessions']} sessions, {hours:.2f} hours")

print(f"\n=== Today's Data Check ===")
if today_str in date_stats:
    stats = date_stats[today_str]
    print(f"Date: {today_str}")
    print(f"Users: {len(stats['users'])}")
    print(f"Sessions: {stats['sessions']}")
    print(f"Hours: {stats['screentime'] / (1000 * 60 * 60):.2f}")
else:
    print(f"No data found for today ({today_str})")
