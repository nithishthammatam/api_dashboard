import google.cloud.firestore as firestore
print(f"Attributes in google.cloud.firestore:")
for attr in dir(firestore):
    if "FieldPath" in attr:
        print(f" - {attr}")

import google.cloud.firestore_v1 as firestore_v1
print(f"\nAttributes in google.cloud.firestore_v1:")
for attr in dir(firestore_v1):
    if "FieldPath" in attr:
        print(f" - {attr}")
        
import firebase_admin.firestore as admin_firestore
print(f"\nAttributes in firebase_admin.firestore:")
for attr in dir(admin_firestore):
    if "FieldPath" in attr:
        print(f" - {attr}")
