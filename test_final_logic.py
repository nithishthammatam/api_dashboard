
import requests
import json
import time

def test_final_logic():
    url = "http://localhost:8000/api/dashboard/analytics"
    token = "f2bfae68d0755d9b2afbb160823cb64ebf4c8ef5459e8dc3ee386e7bb20ee7a4"
    headers = {"Authorization": f"Bearer {token}"}
    
    print(f"Testing Final Logic (Average Sessions) at {url}...")
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            payload = data.get("data", {})
            print("--- RESULTS ---")
            print(f"Today Total Sessions: {payload.get('today', {}).get('totalSessions')}")
            # Expected: Sum of (UserSum / UserApps)
            # 1MD8QR should contribute 37.
            # YQICD6 should contribute 73 (808/11).
            # Global total should be reasonable (e.g. ~400-600 range).
        else:
            print(f"Request failed: {response.text}")
            
    except Exception as e:
        print(f"Exception during test: {e}")

if __name__ == "__main__":
    test_final_logic()
