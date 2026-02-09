
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import json

load_dotenv()

def inspect_screentime_structure():
    print("Inspecting 'screentime' document structure...")
    
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
    
    # Check top users
    top_users = ["1MD8QR", "YQICD6"] 
    
    for uid in top_users:
        print(f"\n--- User: {uid} ---")
        doc_ref = db.collection("screentime").document(uid)
        
        # Check subcollections
        collections = doc_ref.collections()
        for coll in collections:
            print(f"  Subcollection Found: {coll.id}")
            # List docs in subcollection
            docs = list(coll.limit(3).stream())
            for d in docs:
                print(f"    Doc: {d.id}")
                print(f"    Data Keys: {list(d.to_dict().keys())}")
                if "session" in str(d.to_dict()).lower():
                     print(f"    * Contains 'session' data")

if __name__ == "__main__":
    inspect_screentime_structure()
