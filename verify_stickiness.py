import sys
import json
from fastapi.testclient import TestClient
from main import app
from middleware.auth import verify_api_key

# Redirect stdout/stderr to file to capture all logs including server errors
sys.stdout = open('verify_stickiness_log.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

# Bypass auth for testing
app.dependency_overrides[verify_api_key] = lambda: "mock_token"

client = TestClient(app)

def test_stickiness():
    print("Testing /api/dashboard/stickiness?period=30d...")
    try:
        response = client.get("/api/dashboard/stickiness?period=30d")
    except Exception as e:
        print(f"Client Exception: {e}")
        return

    if response.status_code != 200:
        print(f"FAILED: {response.status_code}")
        print(response.text)
        return

    data = response.json()
    print("SUCCESS")
    print(json.dumps(data, indent=2))
    
    # Assertions
    assert "current_ratio" in data
    assert "status" in data
    assert "trend_7d" in data
    assert "trend_30d" in data
    assert "historical_data" in data
    assert len(data["historical_data"]) > 0, "Historical data should not be empty"
    
    # Check Math
    details = data.get("details", {})
    dau = details.get("dau", 0)
    mau = details.get("mau", 0)
    ratio = data.get("current_ratio", 0)
    
    if mau > 0:
        calc_ratio = round((dau / mau) * 100, 1)
        # Floating point tolerance
        assert abs(calc_ratio - ratio) < 0.11, f"Ratio mismatch: {calc_ratio} != {ratio}"
    else:
        assert ratio == 0, "Ratio should be 0 if MAU is 0"
    
    print("\nVerified structure and calculation logic.")

if __name__ == "__main__":
    test_stickiness()
