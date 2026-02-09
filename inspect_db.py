
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def initialize_firebase():
    if firebase_admin._apps:
        return firestore.client()

    cred = None
    service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
    if service_account_path and os.path.exists(service_account_path):
        print(f"🔐 Loading credentials from secret file: {service_account_path}")
        cred = credentials.Certificate(service_account_path)
    else:
        print("🔧 Using environment variables method")
        private_key = os.getenv("FIREBASE_PRIVATE_KEY")
        client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
        project_id = os.getenv("FIREBASE_PROJECT_ID")

        if not private_key or not client_email or not project_id:
            raise ValueError("Missing Firebase environment variables")

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

db = initialize_firebase()

def inspect():
    import sys
    print("--- Getting a valid User ID ---", flush=True)
    try:
        users = db.collection("users").limit(1).stream()
        user_id = None
        for user in users:
            user_id = user.id
            print(f"Found User ID: {user_id}", flush=True)
            break
        
        if not user_id:
            print("❌ No users found in 'users' collection.", flush=True)
            return

        print(f"\n--- Checking screentime/{user_id} subcollections ---", flush=True)
        screentime_doc = db.collection("screentime").document(user_id)
        cols = screentime_doc.collections()
        found = False
        for col in cols:
            found = True
            print(f"✅ Found subcollection: {col.id}", flush=True)
            # Sample a doc
            docs = col.limit(1).stream()
            for doc in docs:
                 print(f"   Sample doc keys in {col.id}: {list(doc.to_dict().keys())}", flush=True)
                 print(f"   Sample doc data: {doc.to_dict()}", flush=True)
        if not found:
            print(f"❌ No subcollections found for screentime/{user_id}", flush=True)

        print(f"\n--- Checking installed/{user_id} ---", flush=True)
        inst_doc = db.collection("installed").document(user_id)
        if inst_doc.get().exists:
             print(f"✅ Found installed/{user_id} document", flush=True)
        else:
             print(f"❌ installed/{user_id} document does not exist", flush=True)
             
        # Check for subcollections in installed/{userId} if it's a doc-less parent?
        inst_cols = inst_doc.collections()
        found_inst = False
        for col in inst_cols:
             found_inst = True
             print(f"✅ Found subcollection in installed/{user_id}: {col.id}", flush=True)
        if not found_inst:
             print(f"❌ No subcollections in installed/{user_id}", flush=True)

    except Exception as e:
        print(f"Error inspecting DB: {e}", flush=True)

if __name__ == "__main__":
    inspect()
