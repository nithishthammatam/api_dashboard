
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def inspect_timeline(user_id="1MD8QR"):
    print(f"Inspecting Timeline for User: {user_id}")
    
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
    
    # Fetch Data
    st_ref = db.collection("screentime").document(user_id).collection("dates")
    sess_ref = db.collection("sessions").document(user_id).collection("dates")
    
    st_docs = list(st_ref.stream())
    sess_docs = list(sess_ref.stream())
    
    date_map = {}
    
    # Aggregate Screentime
    for doc in st_docs:
        date_id = doc.id
        data = doc.to_dict()
        apps = data.get("apps", [])
        apps_list = apps if isinstance(apps, list) else list(apps.values()) if isinstance(apps, dict) else []
        
        total_st = 0
        for app in apps_list:
            if isinstance(app, dict):
                try: total_st += float(str(app.get("totalScreenTime", 0)).replace(',', ''))
                except: pass
        
        if date_id not in date_map: date_map[date_id] = {"st": 0, "sess": 0}
        date_map[date_id]["st"] = total_st

    # Aggregate Sessions
    for doc in sess_docs:
        date_id = doc.id
        data = doc.to_dict()
        sessions = data.get("sessions", [])
        count = len(sessions) if isinstance(sessions, list) else 0
        
        if date_id not in date_map: date_map[date_id] = {"st": 0, "sess": 0}
        date_map[date_id]["sess"] = count

    # Sort Dates
    sorted_dates = sorted(date_map.keys())
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Calculate Sums
    sum_current_logic = 0  # Today - 7 (inclusive) = 8 days?
    count_current_logic = 0
    
    sum_7_rolling_corrected = 0 # Today - 6 (inclusive) = 7 days
    
    now = datetime.now()
    week_ago_current = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    week_ago_corrected = (now - timedelta(days=6)).strftime("%Y-%m-%d")
    
    # Write to file
    with open("timeline_stats.txt", "w") as f:
        f.write("-" * 65 + "\n")
        f.write(f"{'Date':<12} | {'Sessions':<10} | {'Screentime (ms)':<20} | {'Hours':<10}\n")
        f.write("-" * 65 + "\n")
        
        for date_str in sorted_dates:
            stats = date_map[date_str]
            sess = stats["sess"]
            st = stats["st"]
            hours = st / (1000*60*60)
            
            prefix = "   "
            if date_str == today_str: prefix = "-> "
            
            line = f"{prefix}{date_str:<12} | {sess:<10} | {st:<20} | {hours:.2f}\n"
            print(line.strip())
            f.write(line)
            
            # Logic 1: Current Dashboard (Today - 7 inclusive)
            if date_str >= week_ago_current:
                sum_current_logic += sess
                count_current_logic += 1

            # Logic 2: Correcting Rolling 7 (Today - 6 inclusive)
            if date_str >= week_ago_corrected:
                sum_7_rolling_corrected += sess
                
        f.write("-" * 65 + "\n")
        f.write(f"Current Dashboard Logic (Start: {week_ago_current}):\n")
        f.write(f"Sum Sessions: {sum_current_logic}\n")
        f.write(f"Days Included: {count_current_logic} (If 8, this is the bug)\n")
        
        f.write("-" * 65 + "\n")
        f.write(f"Corrected Rolling 7 Days (Start: {week_ago_corrected}):\n")
        f.write(f"Sum Sessions: {sum_7_rolling_corrected}\n")
        f.write("-" * 65 + "\n")

if __name__ == "__main__":
    inspect_timeline()
