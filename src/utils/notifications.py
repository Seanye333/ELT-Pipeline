"""Slack and email notification helpers."""
from __future__ import annotations

import smtplib
from email.mime.text import MIMEText

import requests

from src.config.logging_config import get_logger
from src.config.settings import NotificationSettings

logger = get_logger(__name__)


class NotificationService:
    def __init__(self, config: NotificationSettings | None = None) -> None:
        self._config = config or NotificationSettings()

    def send_slack(self, message: str, color: str = "good") -> None:
        """Post a message to the configured Slack webhook."""
        if not self._config.slack_webhook_url:
            logger.warning("slack_webhook_not_configured")
            return

        payload = {
            "attachments": [
                {
                    "color": color,
                    "text": message,
                    "mrkdwn_in": ["text"],
                }
            ]
        }
        try:
            resp = requests.post(self._config.slack_webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("slack_notification_sent")
        except Exception as exc:
            logger.error("slack_notification_failed", error=str(exc))

    def send_email(self, subject: str, body: str, to: list[str]) -> None:
        """Send a plain-text email via SMTP."""
        if not self._config.smtp_host:
            logger.warning("smtp_not_configured")
            return

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = self._config.alert_email_from
        msg["To"] = ", ".join(to)

        try:
            with smtplib.SMTP(self._config.smtp_host, self._config.smtp_port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(self._config.smtp_user, self._config.smtp_password)
                smtp.sendmail(self._config.alert_email_from, to, msg.as_string())
            logger.info("email_sent", subject=subject, recipients=to)
        except Exception as exc:
            logger.error("email_failed", error=str(exc))

    def notify_pipeline_success(self, run_id: str, rows: int, files: int) -> None:
        self.send_slack(
            f":white_check_mark: *ELT Pipeline SUCCESS* | Run `{run_id}` | "
            f"{files} files | {rows:,} rows loaded",
            color="good",
        )

    def notify_pipeline_failure(self, run_id: str, error: str) -> None:
        self.send_slack(
            f":x: *ELT Pipeline FAILED* | Run `{run_id}` | Error: `{error}`",
            color="danger",
        )
