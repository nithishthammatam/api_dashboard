"""
Diagnostic script to compare Strict vs Loose data aggregation
Strict = Current API logic (only int/float)
Loose = Robust logic (casts strings to float, handles dicts)
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

print("=== DIAGNOSTIC REPORT: STRICT VS LOOSE AGGREGATION ===\n")

now = datetime.now()
today_str = now.strftime("%Y-%m-%d")

# Counters
strict_sessions = 0
loose_sessions = 0
strict_screentime = 0
loose_screentime = 0

issues = {
    "string_sessions": 0,
    "string_screentime": 0,
    "apps_as_dict": 0,
    "apps_missing": 0
}

users_ref = db.collection("users")
docs = users_ref.stream()

for doc in docs:
    user_id = doc.id
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    date_docs = dates_ref.stream()
    
    for date_doc in date_docs:
        data = date_doc.to_dict()
        apps = data.get("apps", [])
        
        # Normalize to list
        apps_list = []
        if isinstance(apps, list):
            apps_list = apps
        elif isinstance(apps, dict):
            issues["apps_as_dict"] += 1
            # Try to handle dict - maybe values are app objects, or it's a single app?
            # For diagnosis, let's assume values are apps if dict
            try:
                apps_list = list(apps.values())
            except:
                pass
        elif apps is None:
             issues["apps_missing"] += 1
        
        for app in apps_list:
            if not isinstance(app, dict): continue
            
            # Check Sessions
            s_val = app.get("sessionCount", 0)
            
            # Strict
            if isinstance(s_val, (int, float)):
                strict_sessions += s_val
            
            # Loose
            try:
                loose_sessions += float(s_val)
                if isinstance(s_val, str):
                    issues["string_sessions"] += 1
            except:
                pass
                
            # Check ScreenTime
            st_val = app.get("totalScreenTime", 0)
            
            # Strict
            if isinstance(st_val, (int, float)):
                strict_screentime += st_val
            
            # Loose
            try:
                loose_screentime += float(st_val)
                if isinstance(st_val, str):
                    issues["string_screentime"] += 1
            except:
                pass

print(f"STRICT (Current API):")
print(f"  Sessions: {strict_sessions}")
print(f"  ScreenTime: {strict_screentime}\n")

print(f"LOOSE (Proposed Fix):")
print(f"  Sessions: {loose_sessions}")
print(f"  ScreenTime: {loose_screentime}\n")

print(f"=== ISSUES FOUND ===")
print(f"Sessions as String: {issues['string_sessions']}")
print(f"ScreenTime as String: {issues['string_screentime']}")
print(f"Apps as Dict: {issues['apps_as_dict']}")
print(f"Apps Missing: {issues['apps_missing']}")

if loose_sessions > strict_sessions:
    print(f"\n⚠️  MISMATCH FOUND! Fix is required.")
else:
    print(f"\n✅ NO MISMATCH. Data types are clean.")
