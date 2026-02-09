
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
    print("START DEBUG")
    
    # Check Screentime
    screentime_ref = db.collection("screentime")
    st_docs = screentime_ref.stream()
    st_ids = []
    print("--- Screentime Docs ---")
    for doc in st_docs:
        print(f"ID: {doc.id}")
        st_ids.append(doc.id)
        cols = [c.id for c in doc.reference.collections()]
        print(f"  Cols: {cols}")
        if "installed" in cols:
            inst = doc.reference.collection("installed").limit(1).stream()
            count = 0
            for i in inst:
                count += 1
                if count == 1:
                    print(f"  INSTALLED DATA: {i.to_dict()}")
            print(f"  Installed count limit 1: {count}")

    # Check Users
    users_ref = db.collection("users")
    u_docs = users_ref.stream()
    u_ids = []
    print("--- User Docs ---")
    for doc in u_docs:
        print(f"ID: {doc.id}")
        u_ids.append(doc.id)
    
    print("--- Comparison ---")
    print(f"Screentime IDs: {st_ids}")
    print(f"User IDs: {u_ids}")
    
    print("END DEBUG")

if __name__ == "__main__":
    asyncio.run(debug())
