
import requests
import os
import time

def test_analytics_performance():
    url = "http://localhost:8000/api/dashboard/analytics"
    token = "f2bfae68d0755d9b2afbb160823cb64ebf4c8ef5459e8dc3ee386e7bb20ee7a4"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        print(f"Requesting {url}...")
        start_time = time.time()
        response = requests.get(url, headers=headers)
        duration = time.time() - start_time
        
        print(f"Status Code: {response.status_code}")
        print(f"Duration: {duration:.2f} seconds")
        
        if response.status_code == 200:
            data = response.json()
            print("Success! Data received.")
            print(f"Total Users in Response: {data['data']['allTime']['totalUsers']}")
        else:
            print(f"Error Response: {response.text}")
            
    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    test_analytics_performance()
