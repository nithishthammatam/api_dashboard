
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import time

load_dotenv()

def audit_db():
    print("Auditing DB size...")
    
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
        app = firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    users_ref = db.collection("users")
    # count using aggregation queries if possible, but stream is fine for now
    docs = users_ref.stream()
    
    user_count = 0
    sample_user_id = None
    
    for doc in docs:
        user_count += 1
        if not sample_user_id:
            sample_user_id = doc.id
            
    print(f"Total Users: {user_count}")
    
    if sample_user_id:
        print(f"Checking sample user: {sample_user_id}")
        dates_ref = db.collection("screentime").document(sample_user_id).collection("dates")
        date_docs = dates_ref.stream()
        date_count = 0
        for _ in date_docs:
            date_count += 1
        print(f"  Dates for sample user: {date_count}")

if __name__ == "__main__":
    start = time.time()
    audit_db()
    print(f"Audit took {time.time() - start:.2f} seconds")
