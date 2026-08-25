"""Hermes plugin entry point for real-time breeding ingestion."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_CANDIDATE_ROOTS = [
    Path(os.environ["BREEDING_TRACKER_REPOSITORY_ROOT"]).expanduser()
    if os.environ.get("BREEDING_TRACKER_REPOSITORY_ROOT")
    else None,
    Path(__file__).resolve().parents[2],
    Path.cwd(),
]
_REPOSITORY_ROOT = next(
    (
        root.resolve()
        for root in _CANDIDATE_ROOTS
        if root is not None
        and (root / "breeding_tracker" / "discord_ingest.py").is_file()
    ),
    None,
)
if _REPOSITORY_ROOT is None:
    raise RuntimeError("Breeding tracker repository could not be located")
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from breeding_tracker.discord_ingest import IngestStore
from breeding_tracker.hermes_gateway import BREEDING_CHANNEL_ID, create_gateway_hook

# ~/.hermes/breeding/mule-fuel-x-nana-glue -- the dashboard/tracker
# processing pipeline (source of truth: tracker.json, dashboard, GitHub
# Pages). Separate from _REPOSITORY_ROOT, which only holds the ingestion
# plugin/store code.
_BREEDING_DIR = Path(
    os.environ.get("BREEDING_DIR")
    or (Path.home() / ".hermes" / "breeding" / "mule-fuel-x-nana-glue")
).expanduser()


def _transcribe_audio(path: str):
    from tools.transcription_tools import transcribe_audio

    return transcribe_audio(path)


def _make_dashboard_processor():
    """Build the processor that turns a stored observation into a dashboard
    update, by calling monitor_breeding_notes.process_message() in-process.

    Deterministic script call, zero LLM/provider calls per message -- this is
    the same fix pattern as the --no-agent cron conversion in NICK-11, which
    resolved a real rate-limit incident caused by spending a provider call
    per tick just to decide whether to run a deterministic script.

    Photo attachments are intentionally NOT wired here: monitor_breeding_notes
    .process_message()'s attachment path expects Discord CDN URLs (as
    DiscordChatExporter exports them), while the live Gateway event's
    media_urls carries locally-cached file paths -- a different shape that
    would need its own verified download/upload path. Until that's built,
    photo-only updates on brand-new messages are backfilled via the existing
    DiscordChatExporter replay path (README's "Verify or replay an export"),
    which already shares the same message-ID cursor and cannot duplicate
    observations.
    """
    if str(_BREEDING_DIR) not in sys.path:
        sys.path.insert(0, str(_BREEDING_DIR))
    os.environ.setdefault("BREEDING_DIR", str(_BREEDING_DIR))

    def _processor(content: str, media_urls: list[str]) -> None:
        import monitor_breeding_notes

        result = monitor_breeding_notes.process_message(content)
        if result:
            logger.info("breeding-channel-ingest: %s", result)

    return _processor


def register(ctx):
    database_path = _REPOSITORY_ROOT / "var" / "discord-ingest.sqlite3"
    database_path.parent.mkdir(parents=True, exist_ok=True)
    store = IngestStore(database_path, BREEDING_CHANNEL_ID)
    ctx.register_hook(
        "pre_gateway_dispatch",
        create_gateway_hook(
            store=store,
            transcriber=_transcribe_audio,
            processor=_make_dashboard_processor(),
        ),
    )
