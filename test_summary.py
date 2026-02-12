import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

# Get API key from environment
api_key = os.getenv("ALLOWED_KEYS")

# Test the summary endpoint
url = "http://localhost:8000/api/dashboard/summary"
headers = {"Authorization": f"Bearer {api_key}"}

try:
    response = requests.get(url, headers=headers)
    print(f"Status Code: {response.status_code}")
    with open("summary_output.json", "w") as f:
        json.dump(response.json(), f, indent=2)
    print("Response saved to summary_output.json")
except Exception as e:
    print(f"Error: {e}")
