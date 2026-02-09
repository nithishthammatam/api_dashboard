
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import json

load_dotenv()

def find_session_collections():
    print("Searching for any collection with 'session' in name...")
    
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
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    collections = db.collections()
    for coll in collections:
        print(f"Collection Found: {coll.id}")
        
    # Check users/{userId}/sessions
    print("\nChecking users/{userId}/sessions subcollection...")
    users = db.collection("users").limit(3).stream()
    for user in users:
        print(f"User: {user.id}")
        subcolls = user.reference.collections()
        for sub in subcolls:
            print(f"  Subcollection: {sub.id}")
            if "session" in sub.id.lower():
                docs = sub.limit(3).stream()
                for d in docs:
                    print(f"    Doc: {d.id} -> {json.dumps(d.to_dict(), default=str)}")

if __name__ == "__main__":
    find_session_collections()
