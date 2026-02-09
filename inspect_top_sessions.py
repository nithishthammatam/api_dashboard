
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json

load_dotenv()

def inspect_top_user():
    print("Inspecting Top Session Users...")
    
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
    
    # Limit to just the top user to ensure output isn't truncated
    top_users = ["YQICD6"] 
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Date: {today_str}")
    
    for uid in top_users:
        print(f"\n--- User: {uid} ---")
        doc_ref = db.collection("screentime").document(uid).collection("dates").document(today_str)
        doc = doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            print(f"  Doc Keys: {list(data.keys())}")
            if "totalSessions" in data:
                print(f"  *** FOUND 'totalSessions': {data['totalSessions']} ***")
                
            apps = data.get("apps", [])
            if isinstance(apps, dict):
                apps = list(apps.values())
                
            total_sess = 0
            # Print simplified table
            print(f"{'App':<20} | {'Sess'}")
            print("-" * 30)
            
            # Sort apps by session count
            apps_sorted = []
            if isinstance(apps, list):
                for app in apps:
                    if isinstance(app, dict):
                        sc = app.get("sessionCount", 0)
                        try:
                            val = float(str(sc).replace(',', ''))
                            apps_sorted.append((app.get("appName", "Unknown"), val))
                        except:
                            pass
            
            apps_sorted.sort(key=lambda x: x[1], reverse=True)
            
            # Print only top 10 apps to avoid truncation
            for name, val in apps_sorted[:10]:
                print(f"{name[:20]:<20} | {val}")
                
            print("-" * 30)
            print(f"Total Apps: {len(apps)}")
            print(f"Total Session Sum: {total_sess}")
        else:
            print("No data using server date.")

if __name__ == "__main__":
    inspect_top_user()
