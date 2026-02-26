import asyncio
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

def initialize_db():
    if not firebase_admin._apps:
        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key": os.getenv("FIREBASE_PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        firebase_admin.initialize_app(cred)
    return firestore.client()

def check_users(date_str):
    db = initialize_db()
    print(f"--- ANALYZING DATA FOR DATE: {date_str} ---")
    
    users_ref = db.collection("users")
    user_docs = list(users_ref.stream())
    print(f"Total Users in DB: {len(user_docs)}")

    users_with_st = []
    users_st_no_sessions = []
    
    def process_user(u_doc):
        uid = u_doc.id
        # Screentime check
        st_ref = db.collection("screentime").document(uid).collection("dates").document(date_str)
        st_doc = st_ref.get()
        
        if st_doc.exists:
            st_data = st_doc.to_dict()
            st_ms = float(str(st_data.get("totalScreenTime", 0)).replace(',', ''))
            
            if st_ms > 0:
                # Session check (Priority: Sessions Collection -> Root Field -> Apps Summary)
                sess_count = 0
                
                # 1. Sessions Collection
                sess_ref = db.collection("sessions").document(uid).collection("dates").document(date_str)
                sess_doc = sess_ref.get()
                if sess_doc.exists:
                    sess_data = sess_doc.to_dict()
                    sess_list = sess_data.get("sessions", [])
                    if isinstance(sess_list, list):
                        sess_count = len(sess_list)
                
                # 2. Root Field fallback
                if sess_count == 0:
                    sess_count = float(str(st_data.get("totalSessions", 0)).replace(',', ''))
                
                # 3. Apps Summary fallback
                if sess_count == 0:
                    apps = st_data.get("apps", [])
                    if isinstance(apps, dict): apps = list(apps.values())
                    if isinstance(apps, list):
                        for app in apps:
                            try: sess_count += float(str(app.get("sessionCount", 0)).replace(',', ''))
                            except: pass
                
                return {"id": uid, "st": st_ms, "sessions": sess_count}
        return None

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(process_user, user_docs))

    for res in results:
        if res:
            users_with_st.append(res)
            if res["sessions"] == 0:
                users_st_no_sessions.append(res)

    print(f"\nUsers with ScreenTime (>0ms): {len(users_with_st)}")
    print(f"Users with ScreenTime but ZERO Sessions: {len(users_st_no_sessions)}")
    
    if users_st_no_sessions:
        print("\nList of Users with Screentime but No Sessions:")
        for u in users_st_no_sessions:
            print(f" - ID: {u['id']} | ST: {u['st']/60000:.1f} mins")
    
    return users_with_st, users_st_no_sessions

if __name__ == "__main__":
    import sys
    # Default to a date with known data for demonstration, or use today
    target = sys.argv[1] if len(sys.argv) > 1 else "2026-02-06"
    check_users(target)
