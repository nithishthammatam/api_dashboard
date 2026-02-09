
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import json

load_dotenv()

def inspect_exact_session_path():
    print("Inspecting Exact Path: sessions/1MD8QR/dates/2026-02-08 ...")
    
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
    
    # Path from screenshot: sessions > YQICD6 > dates > 2026-02-08
    doc_ref = db.collection("sessions").document("YQICD6").collection("dates").document("2026-02-08")
    doc = doc_ref.get()
    
    if doc.exists:
        data = doc.to_dict()
        print(f"[FOUND] Data Keys: {list(data.keys())}")
        print(f"Full Data: {json.dumps(data, default=str)}")
        
        if "totalSessions" in data:
            print(f"*** totalSessions: {data['totalSessions']} ***")
            
        if "sessions" in data: 
             sessions_list = data['sessions']
             print(f"sessions field type: {type(sessions_list)}")
             if isinstance(sessions_list, list):
                 print(f"*** Count of sessions list: {len(sessions_list)} ***")
             # print(f"sessions field: {data['sessions']}") # Too long to print
             
    else:
        print("[NOT FOUND] The document does not exist at this path.")
        
        # Debug: Check parent
        parent = db.collection("sessions").document("1MD8QR")
        print(f"Parent (sessions/1MD8QR) exists? {parent.get().exists}")

if __name__ == "__main__":
    inspect_exact_session_path()
