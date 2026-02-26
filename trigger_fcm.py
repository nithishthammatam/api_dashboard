import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

def trigger_notification():
    url = "http://localhost:8001/api/notifications/send-all"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer f2bfae68d0755d9b2afbb160823cb64ebf4c8ef5459e8dc3ee386e7bb20ee7a4"
    }
    
    payload = {
        "title": "Debug Post-Restart",
        "body": "Checking log files for EOF errors.",
        "image_url": "https://example.com/debug.png",
        "data": {"debug": "true"}
    }
    
    print(f"Sending request to {url}...")
    try:
        response = requests.post(url, headers=headers, json=payload)
        print(f"Status: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    trigger_notification()
