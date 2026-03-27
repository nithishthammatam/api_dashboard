from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from routers import dashboard, notifications
from contextlib import asynccontextmanager
from scheduler import start_scheduler, stop_scheduler
from cache import get_cached_data, set_cached_data
from database.firebase import db
import os
import certifi
from dotenv import load_dotenv

load_dotenv()

# Fix for SSL EOF error on Windows
os.environ['SSL_CERT_FILE'] = certifi.where()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    start_scheduler()
    yield
    # Shutdown
    stop_scheduler()

app = FastAPI(
    title="TimeTrace Dashboard API",
    description="Secure middleware API server for React dashboards and Firebase Firestore",
    version="1.0.0",
    lifespan=lifespan
)

# CORS Configuration
origins_env = os.getenv("ALLOWED_ORIGINS")
if origins_env:
    origins = [o.strip() for o in origins_env.split(",")]
else:
    # Default: allow common development and production origins if NOT set
    origins = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "https://analytics-dashboard-03.vercel.app", # Example production URL
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins_env else ["*"],
    allow_credentials=True if origins_env or origins else False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Note: If origins is ["*"], allow_credentials MUST be False.
# If you need credentials (Authorization header) and allow all, 
# you should set allow_origins=["*"] and allow_credentials=False 
# OR list your origins explicitly.

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
    from cache import get_cache_stats
    return {
        "success": True,
        "status": "healthy",
        "environment": os.getenv("RAILWAY_ENVIRONMENT_NAME", "production"),
        "cache": get_cache_stats()
    }

@app.get("/api/dashboard/precomputed/summary", tags=["Precomputed (Fast)"])
async def get_precomputed_summary():
    """Returns the full dashboard summary in < 100ms. Reads from Redis → Firestore."""
    cached = get_cached_data("dashboard:summary")
    if cached:
        cached["_source"] = "redis"
        return cached
    doc = db.collection("analytics_precomputed").document("dashboard_summary").get()
    if doc.exists:
        data = doc.to_dict()
        set_cached_data("dashboard:summary", data, ttl_minutes=480)
        data["_source"] = "firestore"
        return data
    return {"error": "No precomputed data yet. Run precompute job first.", "hint": "POST /api/admin/run-precompute"}

@app.get("/api/dashboard/precomputed/top-apps", tags=["Precomputed (Fast)"])
async def get_precomputed_top_apps():
    """Returns top apps in < 100ms. Reads from Redis → Firestore."""
    cached = get_cached_data("dashboard:top_apps_30d")
    if cached:
        cached["_source"] = "redis"
        return cached
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y-%m-%d")
    doc = db.collection("analytics_precomputed").document(f"topApps_{today}").get()
    if doc.exists:
        data = doc.to_dict()
        set_cached_data("dashboard:top_apps_30d", data, ttl_minutes=480)
        data["_source"] = "firestore"
        return data
    return {"error": "No precomputed data yet."}

@app.get("/api/dashboard/precomputed/segments", tags=["Precomputed (Fast)"])
async def get_precomputed_segments():
    """Returns user segments in < 100ms. Reads from Redis → Firestore."""
    cached = get_cached_data("dashboard:user_segments")
    if cached:
        cached["_source"] = "redis"
        return cached
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y-%m-%d")
    doc = db.collection("analytics_precomputed").document(f"userSegments_{today}").get()
    if doc.exists:
        data = doc.to_dict()
        set_cached_data("dashboard:user_segments", data, ttl_minutes=480)
        data["_source"] = "firestore"
        return data
    return {"error": "No precomputed data yet."}

@app.get("/api/dashboard/precomputed/daily-metrics", tags=["Precomputed (Fast)"])
async def get_precomputed_daily_metrics():
    """Returns DAU/WAU/MAU/stickiness in < 100ms."""
    cached = get_cached_data("dashboard:daily_metrics")
    if cached:
        cached["_source"] = "redis"
        return cached
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y-%m-%d")
    doc = db.collection("analytics_precomputed").document(f"dailyMetrics_{today}").get()
    if doc.exists:
        data = doc.to_dict()
        set_cached_data("dashboard:daily_metrics", data, ttl_minutes=480)
        data["_source"] = "firestore"
        return data
    return {"error": "No precomputed data yet."}

@app.post("/api/admin/run-precompute", tags=["Admin"])
async def trigger_precompute():
    """Manually trigger the precompute job (Admin use only)"""
    from middleware.auth import verify_api_key
    import asyncio
    loop = asyncio.get_event_loop()
    from precompute_engine import run_full_precompute
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=1)
    loop.run_in_executor(executor, run_full_precompute)
    return {"status": "Precompute job triggered in background. Check logs.", "estimated_time": "60-120 seconds"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
