
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json

load_dotenv()

def inspect_direct_session_access():
    print("Attempting DIRECT ACCESS to 'sessions' collection...")
    
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
    
    user_id = "1MD8QR"
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Hypotheses regarding "userid-date"
    patterns = [
        f"{user_id}-{today_str}",      # 1MD8QR-2026-02-08
        f"{user_id}_{today_str}",      # 1MD8QR_2026-02-08
        f"{user_id}{today_str}",       # 1MD8QR2026-02-08
        f"{today_str}-{user_id}",      # 2026-02-08-1MD8QR
        user_id                        # Just userId?
    ]
    
    for doc_id in patterns:
        print(f"Checking 'sessions/{doc_id}'...")
        doc_ref = db.collection("sessions").document(doc_id)
        doc = doc_ref.get()
        if doc.exists:
            print(f"  [FOUND] Data: {json.dumps(doc.to_dict(), default=str)}")
        else:
            print("  [NOT FOUND]")

if __name__ == "__main__":
    inspect_direct_session_access()
