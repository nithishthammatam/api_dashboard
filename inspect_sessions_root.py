
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import json

load_dotenv()

def inspect_sessions_root():
    print("Inspecting Root of 'sessions' collection...")
    
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
    
    # List first 10 docs in 'sessions'
    sessions_ref = db.collection("sessions").limit(10)
    docs = list(sessions_ref.stream())
    
    print(f"Found {len(docs)} documents in 'sessions' root.")
    
    for doc in docs:
        print(f"\nDoc ID: {doc.id}")
        print(f"Data: {json.dumps(doc.to_dict(), default=str)}")
        
        # Check for subcollections
        collections = doc.reference.collections()
        for coll in collections:
            print(f"  Subcollection: {coll.id}")
            # Peek into subcollection
            sub_docs = list(coll.limit(3).stream())
            for sd in sub_docs:
                print(f"    Sub-Doc ID: {sd.id}")
                print(f"    Sub-Data: {json.dumps(sd.to_dict(), default=str)[:100]}...")

if __name__ == "__main__":
    inspect_sessions_root()
