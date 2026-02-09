"""
Script to check if we can access creation/update timestamps in Firestore
This is needed to implement 'Synced Today' and 'New Users Added' metrics
"""
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

print("=== CHECKING TIMESTAMPS ===\n")

# Check 1 User
users = db.collection("users").limit(1).stream()
for user in users:
    print(f"User ID: {user.id}")
    print(f"  create_time: {user.create_time}")
    print(f"  update_time: {user.update_time}")
    
    # Check 1 Date Doc
    dates = db.collection("screentime").document(user.id).collection("dates").limit(1).stream()
    for date in dates:
        print(f"  Date ID: {date.id}")
        print(f"    create_time: {date.create_time}")
        print(f"    update_time: {date.update_time}")
        
    print("-" * 20)

print("\n=== TIMESTAMPS CHECK COMPLETE ===")
