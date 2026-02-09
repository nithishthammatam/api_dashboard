
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import time

load_dotenv()

def test_firebase():
    print("Testing Firebase Initialization...")
    
    private_key = os.getenv("FIREBASE_PRIVATE_KEY")
    client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
    project_id = os.getenv("FIREBASE_PROJECT_ID")
    
    if "\\n" in private_key:
        private_key = private_key.replace("\\n", "\n")
        
    try:
        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": project_id,
            "private_key": private_key,
            "client_email": client_email,
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        
        if not firebase_admin._apps:
            app = firebase_admin.initialize_app(cred)
        
        print("SUCCESS: Firebase initialized!")
        
        db = firestore.client()
        print("SUCCESS: Firestore client created")
        
        print("Attempting to fetch users collection...")
        start_time = time.time()
        users_ref = db.collection("users").limit(1)
        docs = users_ref.stream()
        
        count = 0
        for doc in docs:
            print(f"Found user: {doc.id}")
            count += 1
            
        print(f"SUCCESS: Fetched {count} users in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"FAILURE: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_firebase()
