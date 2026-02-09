"""
Script to check for schema variations in screentime data
"""
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

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

print("=== CHECKING SCHEMA VARIATIONS ===\n")

users_ref = db.collection("users")
docs = users_ref.stream()

data_types = {
    "apps_list": 0,
    "apps_dict": 0,
    "apps_missing": 0,
    "root_keys": 0
}

sample_apps_dict = None
sample_root_keys = None

for doc in docs:
    user_id = doc.id
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    date_docs = dates_ref.stream()
    
    for date_doc in date_docs:
        data = date_doc.to_dict()
        
        has_apps = "apps" in data
        apps = data.get("apps")
        
        if not has_apps:
            data_types["apps_missing"] += 1
            # Check for root keys
            if "totalScreenTime" in data or "sessionCount" in data:
                data_types["root_keys"] += 1
                if not sample_root_keys:
                    sample_root_keys = {k: v for k, v in data.items() if k in ["totalScreenTime", "sessionCount"]}
        
        elif isinstance(apps, list):
            data_types["apps_list"] += 1
        
        elif isinstance(apps, dict):
            data_types["apps_dict"] += 1
            if not sample_apps_dict:
                sample_apps_dict = apps
        else:
            print(f"Unknown type for 'apps': {type(apps)}")

print("=== RESULTS ===")
print(f"Apps as List: {data_types['apps_list']}")
print(f"Apps as Dict: {data_types['apps_dict']}")
print(f"Apps Missing: {data_types['apps_missing']}")
print(f"Root Keys Found (in missing apps): {data_types['root_keys']}")

if sample_apps_dict:
    print(f"\nSample Apps Dict: {sample_apps_dict}")
if sample_root_keys:
    print(f"\nSample Root Keys: {sample_root_keys}")
