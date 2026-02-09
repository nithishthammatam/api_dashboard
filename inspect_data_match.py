
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import json

load_dotenv()

def inspect_user_1MD8QR_deep():
    print("Deep Inspection of User 1MD8QR...")
    
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
    
    uid = "1MD8QR"
    today_str = datetime.now().strftime("%Y-%m-%d") # Server time 2026-02-08
    
    # Check Screentime
    doc_ref = db.collection("screentime").document(uid).collection("dates").document(today_str)
    doc = doc_ref.get()
    
    if doc.exists:
        data = doc.to_dict()
        print(f"\n[SCREENTIME] {today_str}")
        print(f"Keys: {list(data.keys())}")
        apps = data.get("apps", [])
        if isinstance(apps, dict): apps = list(apps.values())
        
        total_sc = 0
        total_sess_sum = 0
        total_items = 0
        
        print(f"{'App':<20} | {'Sess'} | {'Time'}")
        for app in apps:
            if isinstance(app, dict):
                name = app.get("appName", "Unknown")
                sc = app.get("sessionCount", 0)
                st = app.get("totalScreenTime", 0)
                print(f"{name:<20} | {sc:<4} | {st}")
                
                try: total_sess_sum += float(str(sc).replace(',',''))
                except: pass
                try: total_sc += float(str(st).replace(',',''))
                except: pass
                total_items += 1
                
        print("-" * 40)
        print(f"Total Apps Items: {total_items}")
        print(f"Total Session Sum: {total_sess_sum}")
        print(f"Total ScreenTime: {total_sc}")
        
        # Check against 37
        if total_items == 37: print("MATCH: Total Apps Items = 37")
        if total_sess_sum == 37: print("MATCH: Total Session Sum = 37")
        if total_sc == 37: print("MATCH: Total ScreenTime = 37")
        if total_sc / 60 == 37: print("MATCH: ScreenTime (hours?) = 37") 
        if total_sc / 3600 == 37: print("MATCH: ScreenTime (seconds?) = 37")

    else:
        print("[SCREENTIME] No doc for today.")

if __name__ == "__main__":
    inspect_user_1MD8QR_deep()
