import sys
import json
from fastapi.testclient import TestClient
from main import app
from middleware.auth import verify_api_key

# Redirect stdout/stderr to file
sys.stdout = open('verify_notifications_log.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

# Bypass auth for testing
app.dependency_overrides[verify_api_key] = lambda: "mock_token"

client = TestClient(app)

def test_notifications():
    print("Testing POST /api/notifications/send-all...")
    payload = {
        "title": "Welcome to New Feature!",
        "body": "Check out our latest analytics tools.",
        "image_url": "https://example.com/banner.png",
        "data": {"screen": "analytics", "id": "123"}
    }
    
    try:
        response = client.post("/api/notifications/send-all", json=payload)
    except Exception as e:
        print(f"Client Exception during POST: {e}")
        return

    if response.status_code != 200:
        print(f"FAILED POST: {response.status_code}")
        print(response.text)
        return

    post_data = response.json()
    print("POST SUCCESS")
    print(json.dumps(post_data, indent=2))
    
    assert post_data["success"] == True
    assert "recipient_count" in post_data
    assert "id" in post_data
    
    if post_data["recipient_count"] == 0:
        print("ℹ️ Verified: No tokens found (Expected behavior until tokens are added)")
    else:
        print(f"ℹ️ Attempted to send to {post_data['recipient_count']} tokens")
    
    # ---------------------------------------------------------
    
    print("\nTesting GET /api/notifications/history...")
    try:
        response = client.get("/api/notifications/history")
    except Exception as e:
        print(f"Client Exception during GET: {e}")
        return
        
    if response.status_code != 200:
        print(f"FAILED GET: {response.status_code}")
        print(response.text)
        return
        
    get_data = response.json()
    print("GET SUCCESS")
    history = get_data.get("data", [])
    print(f"History count: {len(history)}")
    
    # Verify the item we just added is in history
    found = False
    for item in history:
        if item["id"] == post_data["id"]:
            found = True
            print("Found newly created notification in history:")
            print(json.dumps(item, indent=2))
            assert item["title"] == payload["title"]
            assert item["body"] == payload["body"]
            assert item["status"] == "sent" # or sending if very fast, but code is synchronous
            break
            
    if not found:
        print("FAILED: New notification ID not found in history.")
    else:
        print("\nVerified notification flow successfully.")

if __name__ == "__main__":
    test_notifications()
