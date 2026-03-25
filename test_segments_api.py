import requests
import json

base_url = "https://api-dashboard-8ukc.onrender.com/api/dashboard/getUserSegments"
headers = {
    "Authorization": "Bearer f2bfae68d0755d9b2afbb160823cb64ebf4c8ef5459e8dc3ee386e7bb20ee7a4"
}

def test_segments(days=30, category="all"):
    print(f"\nTesting segments: days={days}, category={category}")
    url = f"{base_url}?days={days}&category={category}"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code} - {resp.text}")
        return
    
    data = resp.json()
    trend = data.get("trend", [])
    print(f"Trend length: {len(trend)}")
    
    power_count = data.get("segments", {}).get("power", {}).get("count")
    print(f"Power user count: {power_count}")
    
    # Check if we have data for specific dates
    active_dates = [t["date"] for t in trend]
    if active_dates:
        print(f"Sample dates: {active_dates[:2]} ... {active_dates[-2:]}")

test_segments(30, "all")
test_segments(7, "all")
test_segments(30, "social")
