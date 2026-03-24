"""
APScheduler - Runs precompute_engine.run_full_precompute() every night at 1:00 AM IST (7:30 PM UTC)
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from precompute_engine import run_full_precompute
from datetime import datetime
import pytz

scheduler = BackgroundScheduler(timezone=pytz.utc)

def scheduled_precompute():
    print(f"[scheduler] Triggered at {datetime.utcnow().isoformat()} UTC")
    run_full_precompute()

# 1:00 AM IST = 7:30 PM UTC (19:30)
scheduler.add_job(
    scheduled_precompute,
    trigger=CronTrigger(hour=19, minute=30, timezone=pytz.utc),
    id="nightly_precompute",
    name="Nightly Dashboard Precompute",
    replace_existing=True,
    misfire_grace_time=3600  # Allow up to 1 hour late (in case of cold start)
)

def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        print("[scheduler] Started. Next run scheduled at 1:00 AM IST (19:30 UTC)")
        next_run = scheduler.get_job("nightly_precompute").next_run_time
        print(f"[scheduler] Next run: {next_run}")

def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        print("[scheduler] Stopped.")
