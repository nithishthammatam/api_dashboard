
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

def fetch_user_data(user_doc, db):
    user_data = user_doc.to_dict()
    user_id = user_doc.id
    user_data["id"] = user_id
    
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    # Limiting to recent dates? No, original code fetched ALL.
    # But for test, let's fetch all as in original code.
    date_docs = dates_ref.stream()
    
    count = 0
    for _ in date_docs:
        count += 1
    
    user_data["date_count"] = count
    return user_data

def test_concurrency():
    print("Testing Concurrency...")
    
    # Init Firebase
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
    
    print("Fetching all users ref...")
    users_ref = db.collection("users")
    user_docs = list(users_ref.stream()) # Convert to list to iterate
    print(f"Total Users: {len(user_docs)}")
    
    start_time = time.time()
    
    # Sequential (simulated)
    # for doc in user_docs[:5]:
    #     fetch_user_data(doc, db)
    
    # Parallel
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_user_data, doc, db) for doc in user_docs]
        
        for future in as_completed(futures):
            try:
                res = future.result()
                results.append(res)
                # print(f"User {res['id']} done")
            except Exception as e:
                print(f"Error: {e}")
                
    duration = time.time() - start_time
    print(f"Processed {len(results)} users in {duration:.2f} seconds")

if __name__ == "__main__":
    test_concurrency()
