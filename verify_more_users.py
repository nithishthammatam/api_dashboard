
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json

load_dotenv()

def verify_session_docs():
    print("Verifying Session Counts for More Users...")
    
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
    today_str = datetime.now().strftime("%Y-%m-%d") # 2026-02-08
    print(f"Checking Date: {today_str}")

    # Iterate users collection to find valid UIDs
    users_ref = db.collection("users").limit(50) 
    docs = users_ref.stream()

    count = 0
    found_count = 0
    
    with open("verify_results.txt", "w") as f:
        f.write("-" * 60 + "\n")
        f.write(f"{'User ID':<20} | {'Session List Count':<20} | {'Status'}\n")
        f.write("-" * 60 + "\n")

        for doc in docs:
            uid = doc.id
            
            # Check sessions collection for this user
            sess_ref = db.collection("sessions").document(uid).collection("dates").document(today_str)
            sess_doc = sess_ref.get()
            
            if sess_doc.exists:
                data = sess_doc.to_dict()
                sessions = data.get("sessions", [])
                
                if isinstance(sessions, list):
                    s_count = len(sessions)
                    line = f"{uid:<20} | {s_count:<20} | {'Valid'}\n"
                    print(line.strip())
                    f.write(line)
                    found_count += 1
                else:
                    line = f"{uid:<20} | {'N/A (Not List)':<20} | {'Invalid Type'}\n"
                    print(line.strip())
                    f.write(line)
            
            count += 1
            if found_count >= 5: # Stop after finding 5 valid examples
                break

        f.write("-" * 60 + "\n")
        f.write(f"Scanned {count} users. Found {found_count} with session data today.\n")

if __name__ == "__main__":
    try:
        verify_session_docs()
    except Exception as e:
        print(f"Error: {e}")
