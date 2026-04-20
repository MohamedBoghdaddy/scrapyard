"""
Slack webhook notifier for Scrapyard run events.

Configuration
-------------
Set ``SLACK_WEBHOOK_URL`` in the environment (or .env file).
Enable at runtime with ``python main.py --slack``.

Message colours follow Slack's attachment colour convention:
  good    (green)  – success
  warning (yellow) – partial / degraded
  danger  (red)    – critical failure
"""
from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_HOSTNAME = socket.gethostname()


class SlackNotifier:
    """Send structured messages to a Slack Incoming Webhook."""

    def __init__(self, webhook_url: Optional[str] = None) -> None:
        self.webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL", "")
        if not self.webhook_url:
            logger.warning(
                "SlackNotifier created without a webhook URL – "
                "set SLACK_WEBHOOK_URL or pass webhook_url="
            )

    # ------------------------------------------------------------------
    # Low-level sender
    # ------------------------------------------------------------------

    async def send(
        self,
        message: str,
        title: str = "Scrapyard",
        color: str = "good",
        fields: Optional[list[Dict[str, Any]]] = None,
    ) -> bool:
        """
        POST *message* to the Slack webhook.

        Parameters
        ----------
        message : str
            Main body text (markdown supported).
        title   : str
            Bold attachment title.
        color   : str
            Sidebar colour – ``good`` | ``warning`` | ``danger`` | ``#RRGGBB``.
        fields  : list[dict]
            Optional Slack attachment fields: ``[{"title": k, "value": v, "short": bool}]``.

        Returns
        -------
        bool
            True on HTTP 200, False otherwise (never raises).
        """
        if not self.webhook_url:
            logger.debug("Slack send skipped – no webhook URL configured")
            return False

        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "fields": fields or [],
                    "footer": f"Scrapyard @ {_HOSTNAME}",
                    "ts": int(datetime.now(timezone.utc).timestamp()),
                }
            ]
        }

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    data=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.warning(
                            "Slack webhook returned HTTP %d: %s", resp.status, text
                        )
                        return False
                    return True
        except Exception as exc:
            logger.error("Failed to send Slack notification: %s", exc)
            return False

    # ------------------------------------------------------------------
    # High-level event helpers
    # ------------------------------------------------------------------

    async def notify_start(self, site: str, category_count: int = 0) -> None:
        fields = [
            {"title": "Site", "value": site, "short": True},
            {"title": "Categories", "value": str(category_count), "short": True},
            {"title": "Host", "value": _HOSTNAME, "short": True},
            {
                "title": "Started at",
                "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                "short": True,
            },
        ]
        await self.send(
            message=f":rocket: Scrape started for *{site}*",
            title="Scrapyard – Run Started",
            color="good",
            fields=fields,
        )

    async def notify_complete(self, site: str, metrics: Dict[str, Any]) -> None:
        success_rate = metrics.get("success_rate_pct", 0)
        color = "good" if success_rate >= 90 else "warning" if success_rate >= 50 else "danger"

        fields = [
            {"title": "Site", "value": site, "short": True},
            {
                "title": "Products",
                "value": str(metrics.get("total_products", 0)),
                "short": True,
            },
            {
                "title": "Success rate",
                "value": f"{success_rate:.1f}%",
                "short": True,
            },
            {
                "title": "Duration",
                "value": f"{metrics.get('elapsed_seconds', 0):.0f}s",
                "short": True,
            },
            {
                "title": "Requests",
                "value": str(metrics.get("total_requests", 0)),
                "short": True,
            },
            {
                "title": "Avg response",
                "value": f"{metrics.get('avg_response_time_s', 0):.2f}s",
                "short": True,
            },
        ]
        await self.send(
            message=f":white_check_mark: Scrape completed for *{site}*",
            title="Scrapyard – Run Completed",
            color=color,
            fields=fields,
        )

    async def notify_error(
        self, site: str, error: str, critical: bool = False
    ) -> None:
        color = "danger"
        emoji = ":rotating_light:" if critical else ":warning:"
        fields = [
            {"title": "Site", "value": site, "short": True},
            {"title": "Host", "value": _HOSTNAME, "short": True},
            {"title": "Error", "value": error[:500], "short": False},
        ]
        await self.send(
            message=f"{emoji} {'Critical error' if critical else 'Error'} scraping *{site}*",
            title="Scrapyard – Error",
            color=color,
            fields=fields,
        )

    async def notify_high_failure_rate(
        self, site: str, failure_rate_pct: float, failed: int, total: int
    ) -> None:
        """Alert when >50% of requests are failing (possible block)."""
        fields = [
            {"title": "Site", "value": site, "short": True},
            {"title": "Failure rate", "value": f"{failure_rate_pct:.1f}%", "short": True},
            {
                "title": "Failed / Total",
                "value": f"{failed} / {total}",
                "short": True,
            },
        ]
        await self.send(
            message=(
                f":rotating_light: High failure rate on *{site}* – "
                "possible IP block or site change."
            ),
            title="Scrapyard – High Failure Rate",
            color="danger",
            fields=fields,
        )
