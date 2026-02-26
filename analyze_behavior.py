import os
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

def get_db():
    if not firebase_admin._apps:
        # Load from .env
        project_id = os.getenv("FIREBASE_PROJECT_ID")
        private_key = os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n")
        client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
        
        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": project_id,
            "private_key": private_key,
            "client_email": client_email,
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        firebase_admin.initialize_app(cred)
    return firestore.client()

def analyze(date_str):
    db = get_db()
    print(f"Analyzing users for date: {date_str}")
    
    users_ref = db.collection("users")
    all_users = list(users_ref.stream())
    print(f"Total Users in DB: {len(all_users)}")
    
    with_st = []
    st_no_sess = []

    def check_one(u_doc):
        uid = u_doc.id
        doc_ref = db.collection("screentime").document(uid).collection("dates").document(date_str)
        doc = doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            st = float(str(data.get("totalScreenTime", 0)).replace(',', ''))
            
            if st > 0:
                # Check sessions
                sess_count = 0
                
                # 1. Sessions Collection
                s_ref = db.collection("sessions").document(uid).collection("dates").document(date_str)
                s_doc = s_ref.get()
                if s_doc.exists:
                    s_data = s_doc.to_dict()
                    sess_count = len(s_data.get("sessions", []))
                
                # 2. Fallbacks
                if sess_count == 0:
                    sess_count = float(str(data.get("totalSessions", 0)).replace(',', ''))
                
                if sess_count == 0:
                    apps = data.get("apps", [])
                    if isinstance(apps, dict): apps = list(apps.values())
                    if isinstance(apps, list):
                        for a in apps:
                            try: sess_count += float(str(a.get("sessionCount", 0)).replace(',', ''))
                            except: pass
                
                return {"id": uid, "st": st, "sess": sess_count}
        return None

    with ThreadPoolExecutor(max_workers=30) as ex:
        results = list(ex.map(check_one, all_users))

    for r in results:
        if r:
            with_st.append(r)
            if r["sess"] == 0:
                st_no_sess.append(r)

    print(f"\nRESULTS FOR {date_str}")
    print(f"Users with Screentime: {len(with_st)}")
    print(f"Users with Screentime but NO Sessions: {len(st_no_sess)}")
    
    if st_no_sess:
        print("\nUSER IDs (ST but no Sessions):")
        for x in st_no_sess:
            print(f"- {x['id']} ({x['st']/60000:.1f} mins)")
            
    if with_st:
        print("\nTOP 10 USERS with Screentime:")
        for x in sorted(with_st, key=lambda i: i['st'], reverse=True)[:10]:
            print(f"- {x['id']}: {x['st']/3600000:.2f} hrs, {x['sess']} sessions")

if __name__ == "__main__":
    import sys
    d = sys.argv[1] if len(sys.argv) > 1 else "2026-02-06"
    analyze(d)
