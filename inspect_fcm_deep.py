import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

load_dotenv()

def inspect_users_deep():
    if not firebase_admin._apps:
        cred = credentials.Certificate(os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH") or {
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    with open("fcm_report.txt", "w", encoding="utf-8") as f:
        f.write("Fetching users for deep inspection...\n")
        users = db.collection("users").limit(50).stream() # Checked 20 before, increasing to 50
        
        found_any = False
        count = 0
        
        for user in users:
            count += 1
            data = user.to_dict()
            f.write(f"User ID: {user.id}\n")
            
            # Check explicit tokens
            candidates = ['fcmToken', 'pushToken', 'deviceToken', 'token', 'messagingToken']
            found_in_user = False
            
            for key, val in data.items():
                if any(c.lower() in key.lower() for c in candidates):
                    f.write(f"  ✅ FOUND KEY MATCH: '{key}' = {str(val)[:50]}...\n")
                    found_in_user = True
                    found_any = True
                
                # Check if nested
                if isinstance(val, dict):
                    for subkey, subval in val.items():
                         if any(c.lower() in subkey.lower() for c in candidates):
                            f.write(f"  ✅ FOUND NESTED MATCH: '{key}.{subkey}' = {str(subval)[:50]}...\n")
                            found_in_user = True
                            found_any = True

            if not found_in_user:
                # Print full dict to scan manually for values
                f.write(f"  ❌ No obvious token key. Full Data Dump:\n")
                f.write(str(data) + "\n")
            
            f.write("-" * 20 + "\n")

        if not found_any:
            f.write(f"\nSUMMARY: Scanned {count} users, NO token-like fields found.\n")
        else:
            f.write(f"\nSUMMARY: Found potential token fields as listed above.\n")

if __name__ == "__main__":
    inspect_users_deep()
