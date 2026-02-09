
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import json

load_dotenv()

def brute_force_collections():
    print("Brute Forcing Collection Names and IDs...")
    
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
    date_str = "2026-02-08"
    
    # Potential Collection Names
    coll_names = [
        "sessions", "Sessions", "session", "Session",
        "total_sessions", "TotalSessions",
        "user_sessions", "UserSessions",
        "daily_sessions", "DailySessions",
        "analytics", "Analytics"
    ]
    
    # Potential Doc IDs
    doc_ids = [
        f"{user_id}",
        f"{user_id}-{date_str}",
        f"{user_id}_{date_str}",
        f"{date_str}-{user_id}",
        f"{date_str}_{user_id}"
    ]
    
    for c_name in coll_names:
        print(f"Checking Collection: '{c_name}'")
        # Check if collection has ANY docs
        try:
            docs = list(db.collection(c_name).limit(1).stream())
            if len(docs) > 0:
                print(f"  [FOUND COLLECTION] '{c_name}' has documents!")
                print(f"  Sample Doc ID: {docs[0].id}")
                print(f"  Sample Keys: {list(docs[0].to_dict().keys())}")
                
                # If found, check our user
                for d_id in doc_ids:
                    doc = db.collection(c_name).document(d_id).get()
                    if doc.exists:
                        print(f"    [FOUND DOC] {c_name}/{d_id}")
                        print(f"    Data: {json.dumps(doc.to_dict(), default=str)}")
            else:
                print("  [EMPTY/NO PERMISSION]")
        except Exception as e:
            print(f"  [ERROR] {e}")

if __name__ == "__main__":
    brute_force_collections()
