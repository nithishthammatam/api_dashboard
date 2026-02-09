import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

load_dotenv()

def initialize_firebase():
    if firebase_admin._apps:
        return firestore.client()
    
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

db = initialize_firebase()

print("\n=== DEEP DATABASE INSPECTION ===\n")

# Check what collections exist at root
print("Root collections:")
collections = db.collections()
for col in collections:
    print(f"  - {col.id}")

print("\n=== Checking 'screentime' collection ===\n")

# Get screentime collection
screentime_ref = db.collection("screentime")
screentime_docs = list(screentime_ref.limit(3).stream())

print(f"Number of documents in 'screentime': {len(screentime_docs)}")

if len(screentime_docs) > 0:
    for user_doc in screentime_docs:
        user_id = user_doc.id
        print(f"\n--- User: {user_id} ---")
        
        # Check if this document has data
        user_data = user_doc.to_dict()
        print(f"Document data keys: {list(user_data.keys()) if user_data else 'None'}")
        
        # Check for subcollections
        print(f"Checking subcollections for {user_id}...")
        subcols = list(user_doc.reference.collections())
        print(f"Subcollections found: {[c.id for c in subcols]}")
        
        # Check 'dates' subcollection
        dates_ref = screentime_ref.document(user_id).collection("dates")
        date_docs = list(dates_ref.limit(3).stream())
        print(f"Number of date documents: {len(date_docs)}")
        
        if len(date_docs) > 0:
            for date_doc in date_docs:
                print(f"\n  Date: {date_doc.id}")
                data = date_doc.to_dict()
                print(f"  Keys: {list(data.keys())}")
                
                apps = data.get("apps", [])
                print(f"  Number of apps: {len(apps)}")
                
                if apps and len(apps) > 0:
                    print(f"  First app: {apps[0]}")
                    
                    # Calculate totals
                    total_st = sum(app.get("totalScreenTime", 0) for app in apps if isinstance(app.get("totalScreenTime"), (int, float)))
                    total_sess = sum(app.get("sessionCount", 0) for app in apps if isinstance(app.get("sessionCount"), (int, float)))
                    
                    print(f"  Total screentime for this date: {total_st} ms")
                    print(f"  Total sessions for this date: {total_sess}")
        else:
            print("  No date documents found!")
else:
    print("No documents found in 'screentime' collection!")

print("\n=== END INSPECTION ===\n")
