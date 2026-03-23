import os
import re

base_dir = r"c:\Users\tamat\OneDrive\Desktop\DUSTBANK\Dashkit\dashboard-api-python\routers"
files_to_merge = ["compare_periods.py", "user_profile.py", "screentime_patterns.py", "user_lifecycle.py", "alerts.py"]
dashboard_file = os.path.join(base_dir, "dashboard.py")

# Imports that might be needed by the new code, prepended to the top of dashboard.py just in case
missing_imports = """
import re
import math
from fastapi import Request, Body, Header
import firebase_admin.auth as auth
from pydantic import BaseModel
"""

with open(dashboard_file, "r", encoding="utf-8") as f:
    dash_content = f.read()

# Add missing imports if not already there
if "from fastapi import Request" not in dash_content:
    dash_content = missing_imports + "\n" + dash_content

with open(dashboard_file, "w", encoding="utf-8") as f:
    f.write(dash_content)
    f.write("\n\n" + "#" * 60 + "\n# --- APPENDED APIS ---\n" + "#" * 60 + "\n\n")

    for fname in files_to_merge:
        fpath = os.path.join(base_dir, fname)
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as in_f:
                content = in_f.read()
                
            # Remove router initialization to prevent overwriting the main dashboard router
            content = re.sub(r'router\s*=\s*APIRouter\([\s\S]*?\)', '', content)
            
            # Optionally remove redundant fastapi imports
            content = re.sub(r'^from fastapi import .*$', '', content, flags=re.MULTILINE)
            
            f.write(f"\n# --- FROM {fname} ---\n")
            f.write(content)
            
for fname in files_to_merge:
    fpath = os.path.join(base_dir, fname)
    if os.path.exists(fpath):
        os.remove(fpath)
        print(f"Removed {fname}")

