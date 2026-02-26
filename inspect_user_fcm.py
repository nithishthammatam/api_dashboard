import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

load_dotenv()

def inspect_users():
    if not firebase_admin._apps:
        cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH") or {
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    print("Fetching 5 users...")
    users = db.collection("users").limit(5).stream()
    
    for user in users:
        data = user.to_dict()
        print(f"User ID: {user.id}")
        keys = list(data.keys())
        print(f"Keys: {keys}")
        
        found_token = False
        for key in keys:
            if "token" in key.lower():
                print(f"✅ Found potential token key '{key}': {str(data[key])[:20]}...")
                found_token = True
        
        if not found_token:
            print("❌ No obvious token field found.")
        
        print("-" * 20)

if __name__ == "__main__":
    inspect_users()
