"""Silent Discord Gateway ingestion for the breeding channel."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from .discord_ingest import IngestStore, extract_plant_ids

BREEDING_CHANNEL_ID = "1540849479371067412"

logger = logging.getLogger(__name__)

Transcriber = Callable[[str], dict[str, Any]]
# Called AFTER a message is durably stored in SQLite (the source of truth).
# Signature: processor(content: str, media_urls: list[str]) -> None.
# Failures here are logged and swallowed -- they must never affect the
# ingest cursor or the "skip" gateway response, since the raw observation
# is already safely recorded and can be reconciled/reprocessed later.
Processor = Callable[[str, list[str]], None]


def _platform_name(source: Any) -> str:
    platform = getattr(source, "platform", None)
    return str(getattr(platform, "value", platform) or "").lower()


def _message_type(event: Any) -> str:
    message_type = getattr(event, "message_type", None)
    return str(getattr(message_type, "value", message_type) or "").lower()


def _timestamp(event: Any) -> str:
    timestamp = getattr(event, "timestamp", None)
    if hasattr(timestamp, "isoformat"):
        timestamp = timestamp.isoformat()
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError("Discord Gateway event has no timestamp")
    return timestamp


def _voice_transcript(event: Any, transcriber: Transcriber | None) -> str:
    media_urls = list(getattr(event, "media_urls", None) or [])
    if transcriber is None or not media_urls:
        raise ValueError("Voice note has no configured transcription path")
    result = transcriber(str(media_urls[0]))
    transcript = result.get("transcript") if isinstance(result, dict) else None
    if not result.get("success") or not isinstance(transcript, str) or not transcript.strip():
        raise ValueError("Voice-note transcription failed")
    return transcript.strip()


def _discord_image_attachment_urls(event: Any) -> list[str]:
    """Return real Discord CDN URLs for image attachments on this event.

    ``event.media_urls`` carries locally-cached file paths written by the
    platform adapter (for vision-tool access), not the Discord CDN URL --
    ``photo_handler.process_photo()`` needs the CDN URL so it can download
    the original bytes itself before the URL expires (NICK-9). The raw
    ``discord.Message`` object (``event.raw_message``) still exposes the
    real attachment objects with their live ``.url``/``.content_type``, so
    read from there instead. Returns [] for non-image attachments, missing
    attachments, or a raw_message shape without an ``attachments`` list
    (e.g. voice-only events, or the lightweight stand-ins used in tests).
    """
    raw_message = getattr(event, "raw_message", None)
    attachments = getattr(raw_message, "attachments", None) or []
    urls: list[str] = []
    for attachment in attachments:
        content_type = str(getattr(attachment, "content_type", "") or "")
        url = getattr(attachment, "url", None)
        if url and content_type.startswith("image/"):
            urls.append(str(url))
    return urls


def _event_payload(event: Any, transcriber: Transcriber | None) -> dict[str, Any]:
    source = getattr(event, "source", None)
    message_id = str(
        getattr(event, "message_id", None)
        or getattr(source, "message_id", None)
        or ""
    )
    if not message_id:
        raise ValueError("Discord Gateway event has no message ID")

    raw_message = getattr(event, "raw_message", None)
    raw_content = getattr(raw_message, "content", None)
    content = (
        raw_content if isinstance(raw_content, str) else getattr(event, "text", "")
    )
    message: dict[str, Any] = {
        "id": message_id,
        "timestamp": _timestamp(event),
        "content": content if isinstance(content, str) else "",
        "author": {
            "id": str(getattr(source, "user_id", "") or ""),
            "name": str(getattr(source, "user_name", "") or ""),
            "isBot": bool(getattr(source, "is_bot", False)),
        },
    }
    if _message_type(event) == "voice":
        message["content"] = ""
        message["transcriptionText"] = _voice_transcript(event, transcriber)
    return {
        "channel": {"id": BREEDING_CHANNEL_ID},
        "messages": [message],
    }


def create_gateway_hook(
    *,
    store: IngestStore,
    transcriber: Transcriber | None = None,
    processor: Processor | None = None,
) -> Callable[..., dict[str, str] | None]:
    """Create a pre_gateway_dispatch hook for silent breeding ingestion."""

    def on_gateway_message(*, event: Any, **_: Any) -> dict[str, str] | None:
        source = getattr(event, "source", None)
        if source is None or _platform_name(source) != "discord":
            return None

        chat_id = str(getattr(source, "chat_id", "") or "")
        parent_id = str(getattr(source, "parent_chat_id", "") or "")
        if parent_id == BREEDING_CHANNEL_ID:
            return {"action": "skip", "reason": "breeding threads are excluded"}
        if chat_id != BREEDING_CHANNEL_ID:
            return None

        message_id = str(
            getattr(event, "message_id", None)
            or getattr(source, "message_id", None)
            or ""
        )
        try:
            if message_id:
                store.block_message(message_id, "gateway processing pending")
            payload = _event_payload(event, transcriber)
            resolving = (
                message_id if store.blocked_message_id() == message_id else None
            )
            store.ingest_export(payload, resolving_message_id=resolving)
        except Exception as exc:
            if message_id:
                try:
                    store.block_message(message_id, type(exc).__name__)
                except Exception:
                    logger.exception(
                        "Could not persist breeding ingestion block for message %s",
                        message_id,
                    )
            logger.exception(
                "Breeding Gateway ingestion failed for message %s",
                getattr(event, "message_id", None),
            )
            return {
                "action": "skip",
                "reason": "breeding ingestion failed; retry by backfill",
            }

        # The observation is now durably stored (SQLite is the source of
        # truth) regardless of what happens below. Dashboard/tracker
        # processing is best-effort on top of that: a failure here is
        # logged but never turns into an ingestion block or a changed
        # gateway response, so it can never re-trigger an agent reply.
        if processor is not None:
            content = payload["messages"][0].get("content") or payload["messages"][
                0
            ].get("transcriptionText", "")
            if extract_plant_ids(content):
                # Real Discord CDN URLs (not event.media_urls, which is
                # locally-cached file paths -- see _discord_image_attachment_urls).
                media_urls = _discord_image_attachment_urls(event)
                try:
                    processor(content, media_urls)
                except Exception:
                    logger.exception(
                        "Breeding dashboard processing failed for message %s "
                        "(observation already stored; safe to reprocess later)",
                        message_id,
                    )

        return {"action": "skip", "reason": "breeding message ingested"}

    return on_gateway_message
