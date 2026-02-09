"""
Debug script to check what dates are being categorized into each time period
"""
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

print("=== CHECKING DATE CATEGORIZATION ===\n")

now = datetime.now()
today_str = now.strftime("%Y-%m-%d")
week_ago_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
month_ago_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")

print(f"Current time: {now}")
print(f"Today: {today_str}")
print(f"Week ago (>= {week_ago_str})")
print(f"Month ago (>= {month_ago_str})\n")

# Collect all unique dates
all_dates = set()
users_ref = db.collection("users")
user_docs = users_ref.stream()

for user_doc in user_docs:
    user_id = user_doc.id
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    date_docs = dates_ref.stream()
    
    for date_doc in date_docs:
        all_dates.add(date_doc.id)

sorted_dates = sorted(all_dates)

print(f"Total unique dates in database: {len(sorted_dates)}")
print(f"Date range: {sorted_dates[0]} to {sorted_dates[-1]}\n")

# Categorize dates
today_dates = []
week_dates = []
month_dates = []

for date_id in sorted_dates:
    if date_id == today_str:
        today_dates.append(date_id)
    if date_id >= week_ago_str:
        week_dates.append(date_id)
    if date_id >= month_ago_str:
        month_dates.append(date_id)

print(f"=== CATEGORIZATION RESULTS ===\n")
print(f"TODAY ({today_str}):")
print(f"  Dates: {today_dates}")
print(f"  Count: {len(today_dates)}\n")

print(f"LAST WEEK (>= {week_ago_str}):")
print(f"  Dates: {week_dates}")
print(f"  Count: {len(week_dates)}\n")

print(f"LAST MONTH (>= {month_ago_str}):")
print(f"  Dates: {month_dates}")
print(f"  Count: {len(month_dates)}\n")

print(f"=== SAMPLE DATES ===")
print(f"Last 10 dates in database:")
for date in sorted_dates[-10:]:
    categories = []
    if date == today_str:
        categories.append("TODAY")
    if date >= week_ago_str:
        categories.append("WEEK")
    if date >= month_ago_str:
        categories.append("MONTH")
    print(f"  {date} -> {', '.join(categories) if categories else 'NONE'}")
