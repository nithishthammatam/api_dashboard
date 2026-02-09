
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
    print("START ROOT CHECK")
    
    users = db.collection("users").stream()
    for u in users:
        print(f"Checking User: {u.id}")
        # Check installed/{userId} collections
        ref = db.collection("installed").document(u.id)
        cols = ref.collections()
        found = False
        for c in cols:
            found = True
            print(f"  Found subcollection in installed/{u.id}: {c.id}")
            # Sample it
            docs = c.limit(1).stream()
            for d in docs:
                print(f"    Sample doc ({d.id}): {d.to_dict()}")
        
        if not found:
             print(f"  No subcollections in installed/{u.id}")
        
        # Also check if installed/{userId} is a document itself
        snap = ref.get()
        if snap.exists:
             print(f"  installed/{u.id} IS a document: {snap.to_dict()}")

    print("END ROOT CHECK")

if __name__ == "__main__":
    asyncio.run(debug())
