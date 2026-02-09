
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import json

load_dotenv()

def inspect_structure():
    print("Inspecting DB Structure...")
    
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
    
    # 1. Inspect 'users' collection for login fields
    print("\n--- Users Collection Sample ---")
    users_ref = db.collection("users").limit(1)
    for doc in users_ref.stream():
        print(json.dumps(doc.to_dict(), indent=2, default=str))

    # 2. Inspect 'sessions' collection structure
    print("\n--- Sessions Collection Check ---")
    sessions_ref = db.collection("sessions").limit(1)
    session_docs = list(sessions_ref.stream())
    
    if not session_docs:
        print("No documents found in 'sessions' collection root.")
    else:
        for doc in session_docs:
            print(f"Session Doc ID: {doc.id}")
            # Check subcollections
            collections = doc.reference.collections()
            for coll in collections:
                print(f"  Subcollection: {coll.id}")
                sub_docs = coll.limit(1).stream()
                for sub_doc in sub_docs:
                    print(f"    Sample Doc ({sub_doc.id}):")
                    print(json.dumps(sub_doc.to_dict(), indent=2, default=str))

if __name__ == "__main__":
    inspect_structure()
