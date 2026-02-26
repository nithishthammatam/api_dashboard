import sys
import json
from fastapi.testclient import TestClient
from main import app
from middleware.auth import verify_api_key

# Redirect stdout/stderr to file
sys.stdout = open('re_verify_notifications_log.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

# Bypass auth for testing
app.dependency_overrides[verify_api_key] = lambda: "mock_token"

client = TestClient(app)

def test_notifications():
    print("Testing POST /api/notifications/send-all with Real FCM logic...")
    payload = {
        "title": "Debug Test",
        "body": "Checking connectivity to FCM servers.",
        "image_url": "https://example.com/test.png",
        "data": {"test": "true"}
    }
    
    try:
        response = client.post("/api/notifications/send-all", json=payload)
    except Exception as e:
        print(f"Client Exception during POST: {e}")
        import traceback
        traceback.print_exc()
        return

    if response.status_code != 200:
        print(f"FAILED POST: {response.status_code}")
        print(response.text)
        return

    post_data = response.json()
    print("POST SUCCESS")
    print(json.dumps(post_data, indent=2))
    
    if post_data.get("success_count", 0) > 0:
        print(f"🎉 SUCCESS! Delivered to {post_data['success_count']} tokens.")
    else:
        print(f"⚠️ Sent attempted but 0 success. Failures: {post_data.get('failure_count', 0)}")

if __name__ == "__main__":
    test_notifications()
