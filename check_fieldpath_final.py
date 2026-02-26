try:
    from google.cloud.firestore_v1 import FieldPath
    print("SUCCESS: Imported FieldPath from google.cloud.firestore_v1")
    print(f"document_id method exists: {hasattr(FieldPath, 'document_id')}")
except Exception as e:
    print(f"FAILED firestore_v1: {e}")

try:
    from google.cloud.firestore import FieldPath
    print("SUCCESS: Imported FieldPath from google.cloud.firestore")
except Exception as e:
    print(f"FAILED firestore: {e}")
