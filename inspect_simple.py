
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

def initialize_firebase():
    if firebase_admin._apps:
        return firestore.client()
    
    # Defaults
    cred = None
    service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
    
    if service_account_path and os.path.exists(service_account_path):
        cred = credentials.Certificate(service_account_path)
    else:
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

    firebase_admin.initialize_app(cred)
    return firestore.client()

try:
    db = initialize_firebase()
    print("DB Initialized", flush=True)

    users = db.collection("users").stream()
    user_ids = []
    for u in users:
        user_ids.append(u.id)
    
    print(f"User IDs: {user_ids}", flush=True)

    if user_ids:
        uid = user_ids[0]
        print(f"Checking screentime for user: {uid}", flush=True)
        
        # Check installed
        inst_ref = db.collection("screentime").document(uid).collection("installed")
        docs = inst_ref.limit(1).stream()
        
        found = False
        for doc in docs:
            found = True
            print("Found installed doc!", flush=True)
            print(f"Data: {doc.to_dict()}", flush=True)
            break
        
        if not found:
             print("No installed docs found. Checking 'dates' structure for reference.", flush=True)
             dates_ref = db.collection("screentime").document(uid).collection("dates")
             docs = dates_ref.limit(1).stream()
             for doc in docs:
                 print(f"Date doc sample: {doc.to_dict()}", flush=True)

except Exception as e:
    print(f"Error: {e}", flush=True)
