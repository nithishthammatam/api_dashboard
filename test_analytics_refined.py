
import requests
import json
import time

def test_refined_analytics():
    url = "http://localhost:8000/api/dashboard/analytics"
    token = "f2bfae68d0755d9b2afbb160823cb64ebf4c8ef5459e8dc3ee386e7bb20ee7a4"
    headers = {"Authorization": f"Bearer {token}"}
    
    print(f"Testing Corrected Session Counts at {url}...")
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            payload = data.get("data", {})
            print("--- Checking Sessions ---")
            print(f"Today Total Sessions: {payload.get('today', {}).get('totalSessions')}")
            print(f"Last Week Total Sessions: {payload.get('lastWeek', {}).get('totalSessions')}")
            
        else:
            print(f"Request failed: {response.text}")
            
    except Exception as e:
        print(f"Exception during test: {e}")

if __name__ == "__main__":
    test_refined_analytics()
