from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from app.sync import sync_category, sync_tv_category, sync_discover_movies


def start_scheduler():
    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(
        lambda: run_and_log(sync_category, "popular"),
        CronTrigger(hour=4, minute=0),
        name="Sync popular movies"
    )

    scheduler.add_job(
        lambda: run_and_log(sync_category, "top_rated"),
        CronTrigger(hour=4, minute=30),
        name="Sync top_rated movies"
    )

    scheduler.add_job(
        lambda: run_and_log(sync_tv_category, "popular"),
        CronTrigger(hour=5, minute=0),
        name="Sync popular TV"
    )

    scheduler.add_job(
        lambda: run_and_log(sync_tv_category, "top_rated"),
        CronTrigger(hour=5, minute=30),
        name="Sync top_rated TV"
    )

    scheduler.add_job(
        lambda: run_and_log(sync_discover_movies, pages=5),
        CronTrigger(hour=6, minute=0),
        name="Sync discover movies"
    )

    scheduler.start()
    logger.info("Scheduler started")


async def run_and_log(func, *args, **kwargs):
    try:
        result = await func(*args, **kwargs)
        logger.info(f"Job {func.__name__}({args}, {kwargs}) completed: {result}")
    except Exception as e:
        logger.exception(f"Scheduled job {func.__name__} failed")
