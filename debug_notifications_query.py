import sys
import os
from dotenv import load_dotenv

# Ensure we can import from current directory
sys.path.append(os.getcwd())

load_dotenv()

try:
    from database.firebase import db
    print("✅ Successfully imported db from database.firebase")
except Exception as e:
    print(f"❌ Failed to import db: {e}")
    sys.exit(1)

def debug_query():
    print("Running query from notifications.py logic...")
    with open("debug_query_results.txt", "w", encoding="utf-8") as f:
        try:
            if not db:
                f.write("❌ DB object is None/Empty\n")
                return

            users_ref = db.collection("users")
            # Exact query from notifications.py
            docs = users_ref.select(["fcmToken"]).stream()
            
            count = 0
            tokens_found = 0
            
            for doc in docs:
                count += 1
                data = doc.to_dict()
                token = data.get("fcmToken")
                
                if count <= 5:
                    f.write(f"User {doc.id}: keys={list(data.keys())}, token={token}\n")
                
                if token:
                    tokens_found += 1
            
            f.write(f"\nTotal Docs Scanned: {count}\n")
            f.write(f"Total Tokens Found: {tokens_found}\n")
            print("Query completed. Check debug_query_results.txt")
            
        except Exception as e:
            f.write(f"❌ Query failed: {e}\n")
            print(e)

if __name__ == "__main__":
    debug_query()
