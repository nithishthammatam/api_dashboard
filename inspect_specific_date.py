
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv
import json

load_dotenv()

def inspect_exact_date(user_id="1MD8QR", date_str="2026-02-06"):
    print(f"Inspecting Data for User: {user_id} on Date: {date_str}")
    
    # Init Firestore
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
    
    # Fetch Screentime Doc
    st_ref = db.collection("screentime").document(user_id).collection("dates").document(date_str)
    st_doc = st_ref.get()
    
    # Fetch Sessions Doc
    sess_ref = db.collection("sessions").document(user_id).collection("dates").document(date_str)
    sess_doc = sess_ref.get()
    
    print("\n--- SCREENTIME COLLECTION ---")
    if st_doc.exists:
        data = st_doc.to_dict()
        print(f"Document Keys: {list(data.keys())}")
        print(f"Root totalScreenTime: {data.get('totalScreenTime')}")
        print(f"Root totalSessions: {data.get('totalSessions')}")
        apps = data.get("apps", [])
        apps_list = apps if isinstance(apps, list) else list(apps.values()) if isinstance(apps, dict) else []
        
        total_st_ms = 0
        for app in apps_list:
            if isinstance(app, dict):
                try: total_st_ms += float(str(app.get("totalScreenTime", 0)).replace(',', ''))
                except: pass
        
        hours = total_st_ms / (1000*60*60)
        print(f"Document EXISTS.")
        print(f"totalScreenTime (calculated from apps): {total_st_ms} ms ({hours:.2f} hours)")
        print(f"Number of Apps: {len(apps_list)}")
        
        # Print top 5 apps for proof
        sorted_apps = sorted(apps_list, key=lambda x: float(str(x.get("totalScreenTime", 0)).replace(',', '')), reverse=True)[:5]
        for app in sorted_apps:
            name = app.get("appName", "Unknown")
            st = app.get("totalScreenTime", 0)
            sc = app.get("sessionCount", 0)
            print(f"  - {name}: ST={st}, SC={sc}")
            
    else:
        print("Document DOES NOT exist.")
        
    print("\n--- SESSIONS COLLECTION ---")
    if sess_doc.exists:
        data = sess_doc.to_dict()
        sessions = data.get("sessions", [])
        count = len(sessions) if isinstance(sessions, list) else 0
        print(f"Document EXISTS.")
        print(f"Session List Count: {count}")
        if count > 0:
            print("First 3 Sessions:")
            for s in sessions[:3]:
                print(f"  - {s}")
    else:
        print("Document DOES NOT exist.")

if __name__ == "__main__":
    inspect_exact_date()
