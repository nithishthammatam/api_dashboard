import asyncio
import json
import os
import sys
from dotenv import load_dotenv

# Load ENV before importing anything else that might use it
load_dotenv()

from database.firebase import db, init_error
from routers.dashboard import get_session_summary

async def verify_logic(date_str="2026-02-06"):
    print(f"Testing get_session_summary logic with date: {date_str}")
    
    if db is None:
        print(f"[FAIL] Database not initialized. Error: {init_error}")
        return

    try:
        data = await get_session_summary(target_date=date_str)
        
        print("\n--- RESULTS ---")
        print(json.dumps(data, indent=2))
        
        if "avg_sessions_per_user" in data:
            print("[PASS] Logic returned expected metrics.")
        else:
            print("[WARN] Data structure returned doesn't match expected metrics.")

    except Exception as e:
        print(f"[FAIL] Error during verification: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2026-02-06"
    asyncio.run(verify_logic(date_arg))
