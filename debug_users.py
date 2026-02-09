
import asyncio
import os
import sys
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore

# Load environment variables
load_dotenv()

def initialize_firebase():
    if firebase_admin._apps:
        return firestore.client()
    
    cred = None
    service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
    
    if service_account_path and os.path.exists(service_account_path):
        cred = credentials.Certificate(service_account_path)
    else:
        # Fallback for dev
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
    print("🚀 Starting Advanced Debug...", flush=True)

    print("\n--- 1. Screentime Collection Analysis ---", flush=True)
    screentime_docs = db.collection("screentime").limit(5).stream()
    
    screentime_ids = []
    
    for doc in screentime_docs:
        print(f"Screentime Doc ID: {doc.id}", flush=True)
        screentime_ids.append(doc.id)
        
        # Check subcollections
        cols = doc.reference.collections()
        col_names = [c.id for c in cols]
        print(f"  Subcollections: {col_names}", flush=True)
        
        if "installed" in col_names:
            print("  ✅ HAS 'installed'!", flush=True)
            # Sample it
            inst = doc.reference.collection("installed").limit(1).stream()
            for i in inst:
                print(f"  Sample installed doc data: {i.to_dict().keys()}", flush=True)
        else:
            print("  ❌ NO 'installed'", flush=True)

    print("\n--- 2. Users Collection Analysis ---", flush=True)
    users_docs = db.collection("users").limit(5).stream()
    users_ids = []
    for doc in users_docs:
        users_ids.append(doc.id)
        print(f"User Doc ID: {doc.id}", flush=True)

    print("\n--- 3. Mismatch Check ---", flush=True)
    common = set(screentime_ids) & set(users_ids)
    print(f"Common IDs: {common}", flush=True)
    
    only_screentime = set(screentime_ids) - set(users_ids)
    print(f"IDs in Screentime but NOT Users: {only_screentime}", flush=True)

    only_users = set(users_ids) - set(screentime_ids)
    print(f"IDs in Users but NOT Screentime: {only_users}", flush=True)

if __name__ == "__main__":
    asyncio.run(debug())
