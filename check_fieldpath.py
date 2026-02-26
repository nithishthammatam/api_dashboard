try:
    from google.cloud import firestore
    print(f"google.cloud.firestore version: {getattr(firestore, '__version__', 'unknown')}")
    print(f"Has FieldPath: {hasattr(firestore, 'FieldPath')}")
    if hasattr(firestore, 'FieldPath'):
        print("FieldPath is in google.cloud.firestore")
    
    from google.cloud.firestore_v1 import FieldPath
    print("FieldPath imported from google.cloud.firestore_v1")
except Exception as e:
    print(f"Error: {e}")

try:
    from firebase_admin import firestore as admin_firestore
    print(f"Has FieldPath in admin_firestore: {hasattr(admin_firestore, 'FieldPath')}")
except Exception as e:
    print(f"Error admin: {e}")
