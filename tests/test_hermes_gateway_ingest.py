import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from breeding_tracker.discord_ingest import IngestStore
from breeding_tracker.hermes_gateway import create_gateway_hook


CHANNEL_ID = "1540849479371067412"


def make_event(
    *,
    message_id: str,
    text: str,
    channel_id: str = CHANNEL_ID,
    parent_channel_id: str | None = None,
    is_bot: bool = False,
    message_type: str = "text",
    media_urls: list[str] | None = None,
    raw_text: str | None = None,
):
    source = SimpleNamespace(
        platform=SimpleNamespace(value="discord"),
        chat_id=channel_id,
        parent_chat_id=parent_channel_id,
        thread_id=channel_id if parent_channel_id else None,
        user_id="176396743192215552",
        user_name=".mrshush",
        is_bot=is_bot,
        message_id=message_id,
    )
    return SimpleNamespace(
        text=text,
        source=source,
        message_id=message_id,
        message_type=SimpleNamespace(value=message_type),
        media_urls=media_urls or [],
        timestamp="2026-08-25T12:00:00+00:00",
        raw_message=(
            SimpleNamespace(content=raw_text) if raw_text is not None else None
        ),
    )


class HermesGatewayIngestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "events.sqlite3"
        self.store = IngestStore(self.db_path, CHANNEL_ID)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def observation_count(self, message_id: str) -> int:
        with sqlite3.connect(self.db_path) as connection:
            return connection.execute(
                "SELECT COUNT(*) FROM observations WHERE message_id = ?",
                (message_id,),
            ).fetchone()[0]

    def test_direct_channel_message_is_ingested_once_and_silently_skipped(self):
        hook = create_gateway_hook(store=self.store)
        event = make_event(message_id="1542000000000000001", text="MG18 smells like lime")

        first = hook(event=event)
        second = hook(event=event)

        self.assertEqual(
            first,
            {"action": "skip", "reason": "breeding message ingested"},
        )
        self.assertEqual(second, first)
        self.assertEqual(self.observation_count(event.message_id), 1)
        self.assertEqual(self.store.cursor(), event.message_id)

    def test_thread_message_is_silently_skipped_but_not_ingested(self):
        hook = create_gateway_hook(store=self.store)
        event = make_event(
            message_id="1542000000000000002",
            text="MG19 thread observation",
            channel_id="1542000000000000999",
            parent_channel_id=CHANNEL_ID,
        )

        result = hook(event=event)

        self.assertEqual(
            result,
            {"action": "skip", "reason": "breeding threads are excluded"},
        )
        self.assertEqual(self.observation_count(event.message_id), 0)
        self.assertIsNone(self.store.cursor())

    def test_other_channels_continue_through_normal_gateway_dispatch(self):
        hook = create_gateway_hook(store=self.store)
        event = make_event(
            message_id="1542000000000000003",
            text="MG20 elsewhere",
            channel_id="1000000000000000000",
        )

        self.assertIsNone(hook(event=event))
        self.assertEqual(self.observation_count(event.message_id), 0)

    def test_threads_under_other_channels_continue_through_gateway_dispatch(self):
        hook = create_gateway_hook(store=self.store)
        event = make_event(
            message_id="1542000000000000006",
            text="MG22 in an unrelated thread",
            channel_id="1000000000000000001",
            parent_channel_id="1000000000000000000",
        )

        self.assertIsNone(hook(event=event))
        self.assertEqual(self.observation_count(event.message_id), 0)

    def test_records_exact_raw_discord_text_not_gateway_normalization(self):
        hook = create_gateway_hook(store=self.store)
        event = make_event(
            message_id="1542000000000000009",
            text="MG25 normalized",
            raw_text="  MG25 exact Discord source  ",
        )

        hook(event=event)

        with sqlite3.connect(self.db_path) as connection:
            content = connection.execute(
                "SELECT content FROM observations WHERE message_id = ?",
                (event.message_id,),
            ).fetchone()[0]
        self.assertEqual(content, "  MG25 exact Discord source  ")

    def test_voice_note_uses_only_transcriber_output(self):
        calls = []

        def transcribe(path: str):
            calls.append(path)
            return {"success": True, "transcript": "MG21 smells like fuel"}

        hook = create_gateway_hook(store=self.store, transcriber=transcribe)
        event = make_event(
            message_id="1542000000000000004",
            text="(The user sent a message with no text content)",
            message_type="voice",
            media_urls=["/cache/voice-note.ogg"],
        )

        result = hook(event=event)

        self.assertEqual(calls, ["/cache/voice-note.ogg"])
        self.assertEqual(result["action"], "skip")
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT plant_ids, content FROM observations WHERE message_id = ?",
                (event.message_id,),
            ).fetchone()
        self.assertEqual(row, ('["MG21"]', "MG21 smells like fuel"))

    def test_failed_voice_transcription_does_not_advance_cursor(self):
        hook = create_gateway_hook(
            store=self.store,
            transcriber=lambda path: {"success": False, "error": "unavailable"},
        )
        event = make_event(
            message_id="1542000000000000005",
            text="",
            message_type="voice",
            media_urls=["/cache/voice-note.ogg"],
        )

        result = hook(event=event)

        self.assertEqual(
            result,
            {"action": "skip", "reason": "breeding ingestion failed; retry by backfill"},
        )
        self.assertIsNone(self.store.cursor())
        self.assertEqual(self.observation_count(event.message_id), 0)

        self.store.close()
        self.store = IngestStore(self.db_path, CHANNEL_ID)
        hook = create_gateway_hook(store=self.store)
        later = make_event(
            message_id="1542000000000000006",
            text="MG22 arrived after the failed voice note",
        )
        later_result = hook(event=later)
        self.assertEqual(
            later_result,
            {"action": "skip", "reason": "breeding ingestion failed; retry by backfill"},
        )
        self.assertIsNone(self.store.cursor())
        self.assertEqual(self.observation_count(later.message_id), 0)

    def test_successful_voice_retry_clears_durable_block(self):
        failed_hook = create_gateway_hook(
            store=self.store,
            transcriber=lambda path: {"success": False, "error": "unavailable"},
        )
        event = make_event(
            message_id="1542000000000000007",
            text="",
            message_type="voice",
            media_urls=["/cache/voice-note.ogg"],
        )
        failed_hook(event=event)

        retry_hook = create_gateway_hook(
            store=self.store,
            transcriber=lambda path: {
                "success": True,
                "transcript": "MG23 smells like oranges",
            },
        )
        result = retry_hook(event=event)

        self.assertEqual(
            result,
            {"action": "skip", "reason": "breeding message ingested"},
        )
        self.assertEqual(self.store.cursor(), event.message_id)
        self.assertEqual(self.observation_count(event.message_id), 1)

    def test_processor_is_called_after_successful_ingest_with_plant_id(self):
        calls = []
        hook = create_gateway_hook(
            store=self.store,
            processor=lambda content, media_urls: calls.append((content, media_urls)),
        )
        event = make_event(message_id="1542000000000000010", text="MG30 vigor 9")

        result = hook(event=event)

        self.assertEqual(result, {"action": "skip", "reason": "breeding message ingested"})
        self.assertEqual(calls, [("MG30 vigor 9", [])])

    def test_processor_is_not_called_when_no_plant_id_present(self):
        calls = []
        hook = create_gateway_hook(
            store=self.store,
            processor=lambda content, media_urls: calls.append((content, media_urls)),
        )
        event = make_event(message_id="1542000000000000011", text="no plant mentioned here")

        hook(event=event)

        self.assertEqual(calls, [])

    def test_processor_failure_does_not_change_gateway_response_or_block_cursor(self):
        def boom(content, media_urls):
            raise RuntimeError("dashboard regen exploded")

        hook = create_gateway_hook(store=self.store, processor=boom)
        event = make_event(message_id="1542000000000000012", text="MG31 processor will fail")

        result = hook(event=event)

        self.assertEqual(result, {"action": "skip", "reason": "breeding message ingested"})
        self.assertEqual(self.observation_count(event.message_id), 1)
        self.assertEqual(self.store.cursor(), event.message_id)
        self.assertIsNone(self.store.blocked_message_id())

    def test_store_failure_still_silently_skips_gateway_dispatch(self):
        class BrokenStore:
            @staticmethod
            def blocked_message_id():
                return None

            @staticmethod
            def ingest_export(payload, *, resolving_message_id=None):
                raise sqlite3.OperationalError("database unavailable")

            @staticmethod
            def block_message(message_id, reason):
                raise sqlite3.OperationalError("database unavailable")

        hook = create_gateway_hook(store=BrokenStore())
        event = make_event(
            message_id="1542000000000000008",
            text="MG24 should not trigger an agent reply",
        )

        result = hook(event=event)

        self.assertEqual(
            result,
            {"action": "skip", "reason": "breeding ingestion failed; retry by backfill"},
        )


if __name__ == "__main__":
    unittest.main()
