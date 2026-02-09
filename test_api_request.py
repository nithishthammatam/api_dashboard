
import requests
import os

def test_api():
    url = "http://localhost:8000/api/dashboard/users"
    token = "f2bfae68d0755d9b2afbb160823cb64ebf4c8ef5459e8dc3ee386e7bb20ee7a4"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        print(f"Requesting {url}...")
        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    test_api()
