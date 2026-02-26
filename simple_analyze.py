import os
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()

def get_db():
    if not firebase_admin._apps:
        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        firebase_admin.initialize_app(cred)
    return firestore.client()

def analyze(date_str):
    db = get_db()
    print(f"Checking date: {date_str}")
    
    users_with_st = []
    st_no_sess = []
    
    # Just iterate first 50 users for speed and avoid timeout
    users = db.collection("users").limit(100).stream()
    
    for u in users:
        uid = u.id
        st_ref = db.collection("screentime").document(uid).collection("dates").document(date_str)
        st_doc = st_ref.get()
        
        if st_doc.exists:
            data = st_doc.to_dict()
            st = float(str(data.get("totalScreenTime", 0)).replace(',', ''))
            
            if st > 0:
                # Sessions
                sess_count = 0
                s_ref = db.collection("sessions").document(uid).collection("dates").document(date_str)
                s_doc = s_ref.get()
                if s_doc.exists:
                    sess_count = len(s_doc.to_dict().get("sessions", []))
                
                if sess_count == 0:
                    sess_count = float(str(data.get("totalSessions", 0)).replace(',', ''))
                
                if sess_count == 0:
                    for a in data.get("apps", []):
                        try: sess_count += float(str(a.get("sessionCount", 0)).replace(',', ''))
                        except: pass
                
                user_info = {"id": uid, "st": st, "sess": sess_count}
                users_with_st.append(user_info)
                if sess_count == 0:
                    st_no_sess.append(user_info)

    print(f"\n--- DATA FOR {date_str} ---")
    print(f"Users found with ScreenTime: {len(users_with_st)}")
    print(f"Users with ScreenTime but 0 Sessions: {len(st_no_sess)}")
    
    if users_with_st:
        print("\nAll Users with ScreenTime:")
        for u in sorted(users_with_st, key=lambda x: x['st'], reverse=True):
            status = "[NO SESSIONS]" if u['sess'] == 0 else f"({u['sess']} sessions)"
            print(f"- {u['id']}: {u['st']/60000:.1f} mins {status}")

if __name__ == "__main__":
    import sys
    d = sys.argv[1] if len(sys.argv) > 1 else "2026-02-14"
    analyze(d)
