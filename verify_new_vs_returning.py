import sys
import json
from fastapi.testclient import TestClient
from main import app
from middleware.auth import verify_api_key

# Redirect stdout/stderr to file to capture all logs including server errors
sys.stdout = open('verify_new_returning_log.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

# Bypass auth for testing
app.dependency_overrides[verify_api_key] = lambda: "mock_token"

client = TestClient(app)

def test_new_vs_returning():
    print("Testing /api/dashboard/new-vs-returning?days=7...")
    try:
        response = client.get("/api/dashboard/new-vs-returning?days=7")
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
    assert "period" in data
    assert "data" in data
    assert "summary" in data
    
    summary = data["summary"]
    data_points = data["data"]
    
    if len(data_points) > 0:
        first_day = data_points[0]
        # Check percentage sum
        new_p = first_day.get("new_users_percent", 0)
        ret_p = first_day.get("returning_users_percent", 0)
        
        # Determine total users
        total = first_day.get("total_users", 0)
        if total > 0:
            assert abs((new_p + ret_p) - 100.0) < 0.2, f"Percentages should sum to 100: {new_p} + {ret_p} = {new_p+ret_p}"
            # Check counts
            new_c = first_day.get("new_users", 0)
            ret_c = first_day.get("returning_users", 0)
            assert new_c + ret_c == total, f"Counts should sum to total: {new_c} + {ret_c} != {total}"
            
    print("\nVerified structure and calculation logic.")

if __name__ == "__main__":
    test_new_vs_returning()
