import sys
import os

def find_fieldpath():
    potentials = [
        "google.cloud.firestore",
        "google.cloud.firestore_v1",
        "google.cloud.firestore_v1.field_path",
        "firebase_admin.firestore",
    ]
    
    for p in potentials:
        try:
            mod = __import__(p, fromlist=['FieldPath'])
            if hasattr(mod, 'FieldPath'):
                print(f"✅ FOUND FieldPath in {p}")
                return
            else:
                # Try to see what it DOES have
                attrs = [a for a in dir(mod) if 'Field' in a or 'Path' in a]
                print(f"ℹ️ {p} has attributes: {attrs}")
        except Exception as e:
            print(f"❌ FAILED {p}: {e}")

if __name__ == "__main__":
    find_fieldpath()
