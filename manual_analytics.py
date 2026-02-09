"""
Manual calculation of what analytics SHOULD return based on actual database dates
"""
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

print("=== MANUAL ANALYTICS CALCULATION ===\n")

now = datetime.now()
today_str = now.strftime("%Y-%m-%d")
week_ago_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
month_ago_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")

print(f"Today: {today_str}")
print(f"Week ago: {week_ago_str}")  
print(f"Month ago: {month_ago_str}\n")

# Counters
today_sessions = 0
today_users = set()
week_sessions = 0
week_users = set()
month_sessions = 0
month_users = set()
all_sessions = 0
all_users = set()

# Get all users
users_ref = db.collection("users")
user_docs = users_ref.stream()

dates_found = []

for user_doc in user_docs:
    user_id = user_doc.id
    
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    date_docs = dates_ref.stream()
    
    for date_doc in date_docs:
        date_id = date_doc.id
        dates_found.append(date_id)
        data = date_doc.to_dict()
        
        apps_list = data.get("apps", [])
        
        date_sessions = 0
        
        if isinstance(apps_list, list):
            for app in apps_list:
                sess = app.get("sessionCount", 0)
                if isinstance(sess, (int, float)):
                    date_sessions += sess
        
        # Add to all time
        all_sessions += date_sessions
        if date_sessions > 0:
            all_users.add(user_id)
        
        # Check if matches today
        if date_id == today_str:
            print(f"FOUND TODAY DATA: {date_id} - User: {user_id}, Sessions: {date_sessions}")
            today_sessions += date_sessions
            today_users.add(user_id)
        
        # Check if in last week
        if date_id >= week_ago_str:
            week_sessions += date_sessions
            week_users.add(user_id)
        
        # Check if in last month
        if date_id >= month_ago_str:
            month_sessions += date_sessions
            month_users.add(user_id)

print(f"\n=== RESULTS ===\n")
print(f"ALL TIME:")
print(f"  Sessions: {all_sessions}")
print(f"  Users: {len(all_users)}\n")

print(f"TODAY ({today_str}):")
print(f"  Sessions: {today_sessions}")
print(f"  Users: {len(today_users)}\n")

print(f"LAST WEEK (>= {week_ago_str}):")
print(f"  Sessions: {week_sessions}")
print(f"  Users: {len(week_users)}\n")

print(f"LAST MONTH (>= {month_ago_str}):")
print(f"  Sessions: {month_sessions}")
print(f"  Users: {len(month_users)}\n")

print(f"=== DATE RANGE IN DATABASE ===")
unique_dates = sorted(set(dates_found))
if unique_dates:
    print(f"Earliest: {unique_dates[0]}")
    print(f"Latest: {unique_dates[-1]}")
    print(f"Total unique dates: {len(unique_dates)}")
