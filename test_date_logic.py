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

print("=== TESTING DATE COMPARISONS ===\n")

now = datetime.now()
today_str = now.strftime("%Y-%m-%d")
week_ago_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
month_ago_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")

print(f"Today: {today_str}")
print(f"Week ago: {week_ago_str}")
print(f"Month ago: {month_ago_str}\n")

# Test dates from database
test_dates = ["2026-01-23", "2026-01-15", "2026-01-09", "2025-12-13", "2026-02-08"]

for date_id in test_dates:
    print(f"\nDate: {date_id}")
    print(f"  == today ({today_str})? {date_id == today_str}")
    print(f"  >= week_ago ({week_ago_str})? {date_id >= week_ago_str}")
    print(f"  >= month_ago ({month_ago_str})? {date_id >= month_ago_str}")
    
    # Categorize
    categories = []
    if date_id == today_str:
        categories.append("TODAY")
    if date_id >= week_ago_str:
        categories.append("LAST_WEEK")
    if date_id >= month_ago_str:
        categories.append("LAST_MONTH")
    categories.append("ALL_TIME")
    
    print(f"  Categories: {', '.join(categories)}")
