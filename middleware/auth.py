import os
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv

load_dotenv()

security_scheme = HTTPBearer()

async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security_scheme)):
    """
    Middleware-like dependency to verify API Key.
    Uses HTTPBearer to automatically extract token and enable Swagger UI 'Authorize' button.
    """
    token = credentials.credentials

    allowed_keys_env = os.getenv("ALLOWED_KEYS")
    if not allowed_keys_env:
        print("❌ CRITICAL: ALLOWED_KEYS environment variable is not set")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: Authentication not configured."
        )
    
    allowed_keys = [k.strip() for k in allowed_keys_env.split(",")]
    
    if token not in allowed_keys:
        print(f"⚠️ Failed authentication attempt from IP: {request.client.host if request.client else 'unknown'}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key. Access denied."
        )

    # If valid, just return (dependency passes)
    return token
