import os
import sys
import json
from fastapi.testclient import TestClient
from main import app
from middleware.auth import verify_api_key

# Redirect stdout/stderr to file to capture all logs including server errors
sys.stdout = open('verify_log.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

# Bypass auth for testing
app.dependency_overrides[verify_api_key] = lambda: "mock_token"

client = TestClient(app)

def test_dau_trend():
    print("Testing /api/dashboard/dau-trend with days=30...")
    try:
        response = client.get("/api/dashboard/dau-trend?days=30")
    except Exception as e:
        print(f"Client Exception: {e}")
        return

    if response.status_code != 200:
        print(f"FAILED: {response.status_code}")
        print(response.text)
        return

    data = response.json()
    print("SUCCESS")
    # Print summary and first/last data points
    print(json.dumps(data["summary"], indent=2))
    if data.get("data"):
        print("First day:", data["data"][0])
        print("Last day:", data["data"][-1])
    
    # Assertions
    assert "period" in data
    assert "data" in data
    assert "summary" in data
    assert len(data["data"]) == 30, f"Expected 30 days of data, got {len(data['data'])}"
    
    print("\nVerified structure and length.")

if __name__ == "__main__":
    test_dau_trend()
