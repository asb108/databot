"""Cron service for scheduled task execution."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from pathlib import Path

from croniter import croniter
from loguru import logger

from databot.core.bus import InboundMessage, MessageBus
from databot.cron.store import CronStore


class CronService:
    """Manages and executes scheduled tasks."""

    def __init__(self, data_dir: Path, bus: MessageBus):
        self.store = CronStore(data_dir / "cron.db")
        self.bus = bus
        self._running = False

    def add_job(self, name: str, schedule: str, message: str, channel: str = "gchat") -> str:
        """Add a cron job. Returns the job ID."""
        job_id = str(uuid.uuid4())[:8]
        if not croniter.is_valid(schedule):
            raise ValueError(f"Invalid cron expression: {schedule}")
        self.store.add(job_id, name, schedule, message, channel)
        logger.info(f"Added cron job '{name}' ({job_id}): {schedule}")
        return job_id

    def remove_job(self, job_id: str) -> bool:
        """Remove a cron job."""
        removed = self.store.remove(job_id)
        if removed:
            logger.info(f"Removed cron job {job_id}")
        return removed

    def list_jobs(self) -> list[dict]:
        """List all cron jobs."""
        return self.store.list_all()

    async def run(self) -> None:
        """Run the cron scheduler loop."""
        self._running = True
        logger.info("Cron service started")

        while self._running:
            try:
                await self._check_and_execute()
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Cron error: {e}")
                await asyncio.sleep(60)

    def stop(self) -> None:
        self._running = False
        logger.info("Cron service stopping")

    async def _check_and_execute(self) -> None:
        """Check all jobs and execute any that are due."""
        now = datetime.now(timezone.utc)
        jobs = self.store.get_enabled()

        for job in jobs:
            try:
                cron = croniter(job["schedule"], now)
                prev = cron.get_prev(datetime)
                last_run = job.get("last_run")

                # If the job hasn't run since its last scheduled time, run it
                should_run = False
                if last_run is None:
                    should_run = True
                else:
                    if isinstance(last_run, str):
                        last_run_dt = datetime.fromisoformat(last_run).replace(
                            tzinfo=timezone.utc
                        )
                    else:
                        last_run_dt = last_run
                    should_run = prev > last_run_dt

                if should_run:
                    logger.info(f"Executing cron job: {job['name']}")
                    self.store.update_last_run(job["id"])

                    await self.bus.publish_inbound(
                        InboundMessage(
                            channel=job.get("channel", "gchat"),
                            sender_id="cron",
                            chat_id=f"cron:{job['name']}",
                            content=f"[Scheduled task: {job['name']}] {job['message']}",
                        )
                    )
            except Exception as e:
                logger.error(f"Error checking cron job '{job['name']}': {e}")
