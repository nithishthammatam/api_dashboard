
import asyncio
import os
import sys
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore

load_dotenv()

def initialize_firebase():
    if firebase_admin._apps:
        return firestore.client()
    cred = None
    service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
    if service_account_path and os.path.exists(service_account_path):
        cred = credentials.Certificate(service_account_path)
    else:
        private_key = os.getenv("FIREBASE_PRIVATE_KEY")
        if private_key and "\\n" in private_key:
             private_key = private_key.replace("\\n", "\n")
        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key": private_key,
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "token_uri": "https://oauth2.googleapis.com/token",
        })
    firebase_admin.initialize_app(cred)
    return firestore.client()

db = initialize_firebase()

async def debug():
    print("START TARGETED INSPECTION")
    
    # Target User
    user_id = "230T86"
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    docs = dates_ref.limit(3).stream()
    
    for d in docs:
        print(f"\n--- Doc: {d.id} ---")
        data = d.to_dict()
        keys = list(data.keys())
        print(f"All Keys: {keys}")
        
        # Check specific fields
        print(f"sessionCount: {data.get('sessionCount')} (Type: {type(data.get('sessionCount'))})")
        
        # Check for screentime variants
        for k in ['screentime', 'totalScreentime', 'duration', 'time', 'usage']:
            if k in data:
                print(f"FOUND '{k}': {data[k]} (Type: {type(data[k])})")
        
        # Check apps
        apps = data.get('apps')
        print(f"apps: {type(apps)}")
        if isinstance(apps, dict):
            print(f"  apps keys (first 3): {list(apps.keys())[:3]}")
            # Check value type
            if apps:
                first_val = list(apps.values())[0]
                print(f"  apps first value: {first_val} (Type: {type(first_val)})")
        elif isinstance(apps, list):
             print(f"  apps is LIST! Sample: {apps[:1]}")

    print("END TARGETED INSPECTION")

if __name__ == "__main__":
    asyncio.run(debug())
