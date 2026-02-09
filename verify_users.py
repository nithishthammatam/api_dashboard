
import asyncio
import os
import sys

# Ensure we can import from current directory
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

# Set dummy ALLOWED_KEYS if not present to avoid import errors in middleware
if not os.getenv("ALLOWED_KEYS"):
    os.environ["ALLOWED_KEYS"] = "test_key"

from routers.dashboard import get_all_users

async def main():
    print("🚀 Starting verification of get_all_users...", flush=True)
    try:
        response = await get_all_users()
        
        if response.get("success"):
            print(f"✅ Success! Retrieved {response.get('count')} users.", flush=True)
            users = response.get("data", [])
            if users:
                sample_user = users[0]
                print("📝 Sample User Data:", flush=True)
                print(f"   ID: {sample_user.get('id')}", flush=True)
                print(f"   Total Screentime: {sample_user.get('totalScreentime')}", flush=True)
                print(f"   Total Sessions: {sample_user.get('totalSessions')}", flush=True)
                print(f"   Avg Duration: {sample_user.get('averageSessionDuration')}", flush=True)
                print(f"   Active Days: {sample_user.get('activeDays')}", flush=True)
                print(f"   Apps Used: {sample_user.get('allAppsUsed')}", flush=True)
                
                # Check if fields are present
                required_fields = ["totalScreentime", "totalSessions", "averageSessionDuration", "activeDays", "allAppsUsed"]
                missing = [f for f in required_fields if f not in sample_user]
                if missing:
                    print(f"❌ Missing fields: {missing}", flush=True)
                else:
                     print("✅ All new fields present.", flush=True)
            else:
                print("⚠️ No users returned (empty list).", flush=True)
        else:
            print("❌ API returned partial failure.", flush=True)
            
    except Exception as e:
        print(f"❌ Error running verification: {e}", flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
