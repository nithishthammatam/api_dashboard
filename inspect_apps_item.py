
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
    try:
        user_id = "230T86"
        docs = db.collection("screentime").document(user_id).collection("dates").limit(1).stream()
        
        for d in docs:
            data = d.to_dict()
            apps = data.get('apps')
            if isinstance(apps, list) and len(apps) > 0:
                item = apps[0]
                print("APP ITEM KEYS:", flush=True)
                for k in item.keys():
                    print(f"- {k}", flush=True)
                print("APP ITEM DATA:", flush=True)
                print(item, flush=True)
                
    except Exception as e:
        print(e)

if __name__ == "__main__":
    asyncio.run(debug())
