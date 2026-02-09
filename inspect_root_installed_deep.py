
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
    print("START DEEP INSPECTION")
    
    # Target User
    user_id = "05DTSD"
    print(f"Target User: {user_id}")
    
    ref = db.collection("installed").document(user_id)
    cols = ref.collections()
    
    for c in cols:
        print(f"Found Subcollection: {c.id}")
        docs = c.stream() # Get ALL docs in subcollection
        count = 0
        for d in docs:
            count += 1
            print(f"  Doc ID: {d.id}")
            data = d.to_dict()
            print(f"  Data Keys: {list(data.keys())}")
            # print sample data logic
            if count == 1:
                print(f"  SAMPLE DATA: {data}")
        print(f"Total docs in {c.id}: {count}")

    print("END DEEP INSPECTION")

if __name__ == "__main__":
    asyncio.run(debug())
