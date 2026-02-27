import uuid
from datetime import datetime, timedelta, timezone

from notifier.celery_app import app
from notifier.dedup import build_dedup_key
from notifier.strategies.email_smtp import EmailSMTPStrategy
from notifier.strategies.flag_file import FlagFileStrategy
from notifier.strategies.telegram import TelegramStrategy
from shared.config import get_settings
from shared.db import get_db_session
from shared.models import AlertEvent, AlertEventChannel


def _get_active_strategies() -> list[tuple[str, object]]:
    settings = get_settings()
    strategies: list[tuple[str, object]] = []

    if settings.TELEGRAM_BOT_TOKEN and settings.TELEGRAM_CHAT_ID:
        strategies.append(
            (
                "telegram",
                TelegramStrategy(
                    bot_token=settings.TELEGRAM_BOT_TOKEN,
                    chat_id=settings.TELEGRAM_CHAT_ID,
                ),
            )
        )

    if settings.SMTP_HOST and settings.SMTP_USER and settings.SMTP_PASSWORD:
        strategies.append(
            (
                "email",
                EmailSMTPStrategy(
                    host=settings.SMTP_HOST,
                    port=settings.SMTP_PORT,
                    user=settings.SMTP_USER,
                    password=settings.SMTP_PASSWORD,
                ),
            )
        )

    strategies.append(("flag_file", FlagFileStrategy(flag_dir=settings.FLAG_FILE_DIR)))
    return strategies


@app.task(name="notifier.tasks.dispatch_notification", bind=True, queue="notification_jobs")
def dispatch_notification(self, event_type: str, severity: str, message: str, metadata: dict):
    dedup_key = build_dedup_key(event_type, metadata)

    with get_db_session() as session:
        existing = session.query(AlertEvent).filter_by(dedup_key=dedup_key).first()
        if existing is not None:
            return

        alert = AlertEvent(
            id=str(uuid.uuid4()),
            dedup_key=dedup_key,
            event_type=event_type,
            severity=severity,
            message=message,
            metadata_=metadata,
            created_at=datetime.now(timezone.utc),
        )
        session.add(alert)
        session.flush()

        channels: list[str] = []
        for channel_name, _ in _get_active_strategies():
            channel = AlertEventChannel(
                id=str(uuid.uuid4()),
                alert_id=alert.id,
                channel=channel_name,
                status="RETRYING",
                retry_count=0,
                max_retries=3,
            )
            session.add(channel)
            channels.append(channel.id)

    for channel_id in channels:
        retry_channel.apply_async(kwargs={"channel_id": channel_id}, queue="notification_jobs")


@app.task(name="notifier.tasks.retry_channel", bind=True, queue="notification_jobs")
def retry_channel(self, channel_id: str):
    with get_db_session() as session:
        channel = session.query(AlertEventChannel).filter_by(id=channel_id).first()
        if channel is None or channel.status == "SENT":
            return

        alert = session.query(AlertEvent).filter_by(id=channel.alert_id).first()
        if alert is None:
            channel.status = "FAILED"
            channel.error_message = "Alert not found"
            return

        strategies = dict(_get_active_strategies())
        strategy = strategies.get(channel.channel)

        if strategy is None:
            channel.status = "FAILED"
            channel.error_message = f"Unknown channel: {channel.channel}"
            return

        try:
            strategy.send(
                event_type=alert.event_type,
                severity=alert.severity,
                message=alert.message,
                metadata=alert.metadata_ or {},
            )
            channel.status = "SENT"
            channel.sent_at = datetime.now(timezone.utc)
            return
        except Exception as exc:
            channel.retry_count += 1
            channel.last_retry_at = datetime.now(timezone.utc)
            channel.error_message = str(exc)

            if channel.retry_count >= channel.max_retries:
                channel.status = "FAILED"
                return

            delay = 60 * (2 ** (channel.retry_count - 1))
            channel.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
            raise self.retry(exc=exc, countdown=delay)
