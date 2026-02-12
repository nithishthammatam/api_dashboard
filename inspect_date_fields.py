import os
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()

def inspect_fields():
    # Initialize similar to main.py
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
    
    # Try to fetch a few docs from collection group "dates"
    print("Fetching 5 docs from collectionGroup('dates')...")
    docs = db.collection_group("dates").limit(5).stream()
    
    for doc in docs:
        # parent is Collection, its parent is Document (User)
        user_id = doc.reference.parent.parent.id if doc.reference.parent.parent else "Unknown"
        data = doc.to_dict()
        print(f"ID: {doc.id}, UserID: {user_id}")
        print(f"Fields: {list(data.keys())}")
        if "date" in data:
            print(f"✅ Found 'date' field: {data['date']}")
        else:
            print("❌ 'date' field NOT found.")
        print("-" * 20)

if __name__ == "__main__":
    inspect_fields()
