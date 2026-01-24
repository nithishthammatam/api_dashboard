import os
import json
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def initialize_firebase():
    """
    Initialize Firebase Admin SDK.
    Replicates the logic from Node.js:
    1. Checks for FIREBASE_SERVICE_ACCOUNT_PATH (secret file).
    2. Fallback to environment variables.
    """
    # Check if already initialized to avoid errors on reload
    if firebase_admin._apps:
        return firestore.client()

    cred = None

    # Method 1: Secret File (Production/Render/Railway)
    service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
    if service_account_path and os.path.exists(service_account_path):
        print(f"🔐 Loading credentials from secret file: {service_account_path}")
        cred = credentials.Certificate(service_account_path)
    
    # Method 2: Environment Variables (Development/Fallback)
    else:
        print("🔧 Using environment variables method")
        
        private_key = os.getenv("FIREBASE_PRIVATE_KEY")
        client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
        project_id = os.getenv("FIREBASE_PROJECT_ID")

        print(f"   Project ID: {project_id}")
        print(f"   Client Email: {client_email}")
        print(f"   Private Key Length: {len(private_key) if private_key else 0}")

        if not private_key or not client_email or not project_id:
            raise ValueError("Missing Firebase environment variables")

        token_uri = os.getenv("FIREBASE_TOKEN_URI", "https://oauth2.googleapis.com/token")

        # Handle escaped newlines (common in .env files)
        if "\\n" in private_key:
             print("   Refining private key (replacing \\n with newlines)")
             private_key = private_key.replace("\\n", "\n")
        
        # Ensure correct header/footer
        if "-----BEGIN PRIVATE KEY-----" not in private_key:
             print("⚠️ WARNING: Private key missing header")

        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": project_id,
            "private_key": private_key,
            "client_email": client_email,
            "token_uri": token_uri,
        })

    try:
        app = firebase_admin.initialize_app(cred)
        print(f"✅ Firebase Admin SDK initialized successfully: {app.name}")
    except ValueError as ve:
        print(f"❌ Firebase initialization ValueError: {ve}")
        # If the app is already initialized, we can ignore this
        if "The default Firebase app already exists" in str(ve):
             print("   (App already existed, continuing)")
        else:
             raise ve
    except Exception as e:
        print(f"❌ Firebase initialization failed: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise e

    return firestore.client()

# Global variable to store initialization error
init_error = None

# Initialize DB instance
try:
    db = initialize_firebase()
except Exception as e:
    print("="*60)
    print(f"FATAL ERROR: Could not initialize Firebase.")
    print(f"Error: {e}")
    print("="*60)
    db = None
    init_error = str(e)
