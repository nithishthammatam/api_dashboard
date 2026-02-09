
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

def process_user(doc, today_str, db):
    user_id = doc.id
    dates_ref = db.collection("screentime").document(user_id).collection("dates")
    today_doc_ref = dates_ref.document(today_str)
    try:
        today_doc = today_doc_ref.get()
        if today_doc.exists:
            data = today_doc.to_dict()
            apps_list = data.get("apps", [])
            
            if isinstance(apps_list, dict):
                 apps_list = list(apps_list.values())
                 
            session_count_sum = 0
            app_count = 0
            
            if isinstance(apps_list, list):
                for app in apps_list:
                    if isinstance(app, dict):
                        sc = app.get("sessionCount", 0)
                        try:
                            val = float(str(sc).replace(',', ''))
                            if val > 0:
                                session_count_sum += val
                                app_count += 1
                        except:
                            pass
                            
            if session_count_sum > 0 or app_count > 0:
                # print(f"DEBUG: User {user_id} -> Sum: {session_count_sum}, AppCount: {app_count}")
                return user_id, session_count_sum, app_count
    except Exception as e:
        print(f"Error for user {user_id}: {e}")
    return None

def debug_sessions():
    print("Debugging Session Counts (Multi-threaded)...")
    
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
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"Checking for Date: {today_str}")

    users_ref = db.collection("users")
    docs = list(users_ref.stream())
    
    print(f"Total Users to check: {len(docs)}")

    user_sessions = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_user, doc, today_str, db): doc for doc in docs}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                user_sessions.append(result)

    # Sort by session count sum descending
    user_sessions.sort(key=lambda x: x[1], reverse=True)
    
    total_session_sum = sum(x[1] for x in user_sessions)
    total_app_count = sum(x[2] for x in user_sessions)
    
    # Calculate sum excluding outliers (users > 200 sessions)
    filtered_sum = sum(x[1] for x in user_sessions if x[1] <= 200)
    filtered_app_count = sum(x[2] for x in user_sessions if x[1] <= 200)
    
    print(f"\n--- FILTERED TOTALS (Users <= 200 Sessions) ---")
    print(f"Filtered Session Sum: {filtered_sum}")
    print(f"Filtered App Count: {filtered_app_count}")
    
    print(f"\n--- GLOBAL TOTALS ---")
    print(f"Total Session Field Sum: {total_session_sum}")
    print(f"Total Apps Used Count: {total_app_count}")
    print(f"Total Users Checked: {len(user_sessions)}")
    
    print("\n--- Top 5 Users by Session Sum ---")
    for uid, s_sum, a_count in user_sessions[:5]:
        print(f"User {uid}: Sum={s_sum}, AppCount={a_count}")

if __name__ == "__main__":
    debug_sessions()
