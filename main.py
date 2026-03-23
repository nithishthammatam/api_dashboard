from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from routers import dashboard, notifications
import os
import certifi
from dotenv import load_dotenv

load_dotenv()

# Fix for SSL EOF error on Windows
os.environ['SSL_CERT_FILE'] = certifi.where()

app = FastAPI(
    title="TimeTrace Dashboard API",
    description="Secure middleware API server for React dashboards and Firebase Firestore",
    version="1.0.0",
)

# CORS Configuration
origins_env = os.getenv("ALLOWED_ORIGINS")
if origins_env:
    origins = [o.strip() for o in origins_env.split(",")]
else:
    # Default/Warning
    print("[main] ALLOWED_ORIGINS not set - allowing all origins")
    origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compress large JSON payloads to reduce transfer time.
app.add_middleware(
    GZipMiddleware,
    minimum_size=1024,
    compresslevel=5,
)

# Include Routes
app.include_router(dashboard.router)
app.include_router(notifications.router)


@app.api_route("/", methods=["GET", "HEAD"], tags=["General"])
async def root():
    return {
        "success": True,
        "name": "TimeTrace Dashboard API",
        "version": "1.0.0",
        "description": "Secure middleware API server. Visit /docs for interactive documentation.",
        "documentation": {
            "url": "/docs",
            "type": "Swagger UI"
        }
    }


@app.get("/health", tags=["General"])
async def health_check():
    return {
        "success": True,
        "status": "healthy",
        "environment": os.getenv("RAILWAY_ENVIRONMENT_NAME", "development")
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
