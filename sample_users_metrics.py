import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

load_dotenv()

def sample_users(date_str="2026-02-05"):
    print(f"Sampling users for {date_str}...")
    
    if not firebase_admin._apps:
        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key": os.getenv("FIREBASE_PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    # Get 10 users who have a document for this date
    # We can use a collection group query or just iterate users
    users_ref = db.collection("users")
    user_docs = users_ref.limit(20).stream()
    
    for u_doc in user_docs:
        user_id = u_doc.id
        doc_ref = db.collection("screentime").document(user_id).collection("dates").document(date_str)
        doc = doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            ts = data.get("totalSessions", 0)
            tst = data.get("totalScreenTime", 0)
            
            # Cross-rev sessions collection
            sess_ref = db.collection("sessions").document(user_id).collection("dates").document(date_str)
            sess_doc = sess_ref.get()
            coll_sess_count = 0
            if sess_doc.exists:
                sess_data = sess_doc.to_dict()
                coll_sess_count = len(sess_data.get("sessions", []))
            
            avg = (tst / ts) if ts > 0 else 0
            print(f"User {user_id}: ST Doc Sessions={ts}, Coll Sessions={coll_sess_count}, ST={tst}")
            if ts > 0:
                print(f"   -> Avg Duration (using ST Doc): {avg/60000:.1f} min")
            if coll_sess_count > 0:
                print(f"   -> Avg Duration (using Coll): {tst/(coll_sess_count*60000):.1f} min")
        # else:
        #    print(f"User {user_id}: No data for {date_str}")

if __name__ == "__main__":
    sample_users()
