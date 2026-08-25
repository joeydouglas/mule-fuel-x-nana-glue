"""Deterministic Discord breeding-channel ingestion."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_PLANT_ID = re.compile(r"(?i)(?<![A-Z0-9])MG\s*[-#]?\s*0*(\d{1,3})(?!\d)")


@dataclass(frozen=True)
class IngestResult:
    fetched: int
    processed: int
    recorded: int
    ignored_bots: int
    ignored_without_plant_id: int
    cursor: str | None


def extract_plant_ids(text: str) -> list[str]:
    """Return normalized plant IDs in first-occurrence order."""
    found: list[str] = []
    for match in _PLANT_ID.finditer(text):
        plant_id = f"MG{int(match.group(1)):02d}"
        if plant_id not in found:
            found.append(plant_id)
    return found


def extract_message_text(message: dict[str, Any]) -> tuple[str, str]:
    """Return only text supplied by Discord/exporter, never generated text."""
    content = message.get("content")
    if isinstance(content, str) and content:
        return content, "message"

    transcript_keys = ("transcriptionText", "transcription")
    for key in transcript_keys:
        transcript = message.get(key)
        if isinstance(transcript, str) and transcript:
            return transcript, "voice_transcription"
    for attachment in message.get("attachments", []):
        for key in transcript_keys:
            transcript = attachment.get(key)
            if isinstance(transcript, str) and transcript:
                return transcript, "voice_transcription"
    return "", "message"


def has_untranscribed_audio(message: dict[str, Any]) -> bool:
    """Return whether a human audio message lacks all source transcription text."""
    content, _ = extract_message_text(message)
    if content:
        return False
    for attachment in message.get("attachments", []):
        content_type = str(attachment.get("contentType") or "").lower()
        if content_type.startswith("audio/") or bool(attachment.get("isVoiceMessage")):
            return True
    return False


def build_export_command(
    *, exporter: Path, channel_id: str, output_path: Path, after: str | None
) -> list[str]:
    """Build a rate-limit-aware DCE command for only the base channel."""
    command = [
        str(exporter),
        "export",
        "--channel",
        str(channel_id),
        "--format",
        "Json",
        "--output",
        str(output_path),
        "--include-threads",
        "None",
        "--respect-rate-limits",
        "true",
        "--utc",
        "true",
    ]
    if after is not None:
        command.extend(("--after", after))
    return command


def poll_once(*, store: "IngestStore", exporter: Path, token: str) -> IngestResult:
    """Fetch one incremental channel export and commit it transactionally."""
    if not token:
        raise ValueError("Discord token is empty")
    with tempfile.TemporaryDirectory(prefix="breeding-discord-") as directory:
        output_path = Path(directory) / "export.json"
        command = build_export_command(
            exporter=exporter,
            channel_id=store.channel_id,
            output_path=output_path,
            after=store.cursor(),
        )
        environment = os.environ.copy()
        environment["DISCORD_TOKEN"] = token
        subprocess.run(command, check=True, env=environment)
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    return store.ingest_export(payload)


class IngestStore:
    """SQLite-backed cursor and exactly-once observation store."""

    def __init__(self, path: str | Path, channel_id: str):
        self.channel_id = str(channel_id)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS observations (
                message_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                author_id TEXT NOT NULL,
                author_name TEXT NOT NULL,
                content TEXT NOT NULL,
                source_type TEXT NOT NULL,
                plant_ids TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cursors (
                channel_id TEXT PRIMARY KEY,
                last_message_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ingestion_blocks (
                channel_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                reason TEXT NOT NULL
            );
            """
        )

    def close(self) -> None:
        self.connection.close()

    def cursor(self) -> str | None:
        row = self.connection.execute(
            "SELECT last_message_id FROM cursors WHERE channel_id = ?",
            (self.channel_id,),
        ).fetchone()
        return row[0] if row else None

    def blocked_message_id(self) -> str | None:
        row = self.connection.execute(
            "SELECT message_id FROM ingestion_blocks WHERE channel_id = ?",
            (self.channel_id,),
        ).fetchone()
        return row[0] if row else None

    def block_message(self, message_id: str, reason: str) -> None:
        """Durably stop the high-water cursor at the earliest failed message."""
        message_id = str(message_id)
        self.connection.execute("BEGIN IMMEDIATE")
        with self.connection:
            current = self.cursor()
            if current is not None and int(message_id) <= int(current):
                return
            blocked = self.blocked_message_id()
            if blocked is not None and int(blocked) <= int(message_id):
                return
            self.connection.execute(
                """
                INSERT INTO ingestion_blocks (channel_id, message_id, reason)
                VALUES (?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    message_id = excluded.message_id,
                    reason = excluded.reason
                """,
                (self.channel_id, message_id, reason[:512]),
            )

    def ingest_export(
        self,
        payload: dict[str, Any],
        *,
        resolving_message_id: str | None = None,
    ) -> IngestResult:
        exported_channel = str(payload.get("channel", {}).get("id", ""))
        if exported_channel != self.channel_id:
            raise ValueError(
                f"Export channel {exported_channel!r} does not match {self.channel_id!r}"
            )

        messages = sorted(payload.get("messages", []), key=lambda item: int(item["id"]))
        unresolved_audio = next(
            (
                item
                for item in messages
                if not bool(item.get("author", {}).get("isBot"))
                and has_untranscribed_audio(item)
            ),
            None,
        )
        if unresolved_audio is not None:
            unresolved_id = str(unresolved_audio["id"])
            self.block_message(unresolved_id, "untranscribed audio")
            if resolving_message_id == unresolved_id:
                resolving_message_id = None
        recorded = 0
        processed = 0
        ignored_bots = 0
        ignored_without_plant_id = 0

        # Serialize cursor reads with writes so overlapping pollers cannot
        # process from a stale cursor and later move it backwards.
        self.connection.execute("BEGIN IMMEDIATE")
        with self.connection:
            blocked = self.blocked_message_id()
            if blocked is not None:
                if resolving_message_id != blocked:
                    raise RuntimeError(
                        f"Ingestion is blocked at Discord message {blocked}"
                    )
                if not any(str(item["id"]) == blocked for item in messages):
                    raise ValueError(
                        f"Resolving payload does not contain blocked message {blocked}"
                    )
            current = self.cursor()
            pending = [
                item for item in messages if current is None or int(item["id"]) > int(current)
            ]
            for item in pending:
                processed += 1
                if bool(item.get("author", {}).get("isBot")):
                    ignored_bots += 1
                    continue
                content, source_type = extract_message_text(item)
                plant_ids = extract_plant_ids(content)
                if not plant_ids:
                    ignored_without_plant_id += 1
                    continue
                author = item.get("author", {})
                author_id = author.get("id")
                author_name = author.get("name")
                timestamp = item.get("timestamp")
                if not isinstance(author_id, str) or not author_id:
                    raise ValueError(f"Message {item['id']} has no author ID")
                if not isinstance(author_name, str) or not author_name:
                    raise ValueError(f"Message {item['id']} has no author name")
                if not isinstance(timestamp, str) or not timestamp:
                    raise ValueError(f"Message {item['id']} has no timestamp")
                cursor = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO observations (
                        message_id, channel_id, timestamp, author_id, author_name,
                        content, source_type, plant_ids
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(item["id"]),
                        self.channel_id,
                        timestamp,
                        author_id,
                        author_name,
                        content,
                        source_type,
                        json.dumps(plant_ids),
                    ),
                )
                recorded += cursor.rowcount

            if pending:
                last_message_id = str(pending[-1]["id"])
                self.connection.execute(
                    """
                    INSERT INTO cursors (channel_id, last_message_id) VALUES (?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET last_message_id = excluded.last_message_id
                    """,
                    (self.channel_id, last_message_id),
                )
            if blocked is not None:
                self.connection.execute(
                    "DELETE FROM ingestion_blocks WHERE channel_id = ?",
                    (self.channel_id,),
                )

        return IngestResult(
            fetched=len(messages),
            processed=processed,
            recorded=recorded,
            ignored_bots=ignored_bots,
            ignored_without_plant_id=ignored_without_plant_id,
            cursor=self.cursor(),
        )


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Poll and ingest the Discord breeding channel exactly once"
    )
    parser.add_argument("--channel", default="1540849479371067412")
    parser.add_argument("--database", type=Path, default=Path("var/discord-ingest.sqlite3"))
    parser.add_argument(
        "--exporter",
        type=Path,
        default=Path("~/.local/bin/discord-chat-exporter/DiscordChatExporter.Cli"),
    )
    parser.add_argument(
        "--token-file", type=Path, default=Path("~/.hermes/discord/token")
    )
    parser.add_argument(
        "--input-export",
        type=Path,
        help="Ingest an existing verified DCE JSON export instead of polling",
    )
    options = parser.parse_args(arguments)

    options.database.parent.mkdir(parents=True, exist_ok=True)
    store = IngestStore(options.database, options.channel)
    try:
        if options.input_export:
            payload = json.loads(options.input_export.read_text(encoding="utf-8"))
            blocked = store.blocked_message_id()
            resolving_message_id = None
            if blocked is not None:
                blocked_message = next(
                    (
                        item
                        for item in payload.get("messages", [])
                        if str(item.get("id")) == blocked
                    ),
                    None,
                )
                if blocked_message is not None:
                    _, source_kind = extract_message_text(blocked_message)
                    if source_kind == "voice_transcription":
                        resolving_message_id = blocked
            result = store.ingest_export(
                payload,
                resolving_message_id=resolving_message_id,
            )
        else:
            token = options.token_file.expanduser().read_text(encoding="utf-8").strip()
            result = poll_once(
                store=store,
                exporter=options.exporter.expanduser(),
                token=token,
            )
    finally:
        store.close()
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
