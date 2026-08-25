import json
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from breeding_tracker.discord_ingest import IngestStore, build_export_command, main, poll_once


CHANNEL_ID = "1540849479371067412"


def message(message_id, content, *, is_bot=False, author="Joey", attachments=None):
    return {
        "id": str(message_id),
        "timestamp": "2026-08-22T22:58:23.722+00:00",
        "content": content,
        "author": {
            "id": "176396743192215552" if not is_bot else "1538303005651111996",
            "name": author,
            "isBot": is_bot,
        },
        "attachments": attachments or [],
    }


def export(messages):
    return {
        "channel": {"id": CHANNEL_ID, "name": "breeding"},
        "messages": messages,
    }


class IngestStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "ingest.sqlite3"
        self.store = IngestStore(self.db_path, CHANNEL_ID)

    def tearDown(self):
        self.store.close()
        self.temp_dir.cleanup()

    def test_records_untagged_plant_observation_exactly_once(self):
        source = export([
            message("1540857701217865828", "MG7 - the one that got away?"),
        ])

        first = self.store.ingest_export(source)
        second = self.store.ingest_export(source)

        self.assertEqual(first.recorded, 1)
        self.assertEqual(second.recorded, 0)
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT message_id, author_id, author_name, content, plant_ids "
                "FROM observations"
            ).fetchone()
            count = connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 1)
        self.assertEqual(
            row,
            (
                "1540857701217865828",
                "176396743192215552",
                "Joey",
                "MG7 - the one that got away?",
                json.dumps(["MG07"]),
            ),
        )
        self.assertEqual(self.store.cursor(), "1540857701217865828")

    def test_ignores_bot_responses_but_advances_the_cursor(self):
        source = export([
            message(
                "1540977486538473523",
                "Cronjob Response: MG07 updated",
                is_bot=True,
                author="NickCage",
            ),
        ])

        result = self.store.ingest_export(source)

        self.assertEqual(result.recorded, 0)
        self.assertEqual(result.ignored_bots, 1)
        self.assertEqual(self.store.cursor(), "1540977486538473523")
        with sqlite3.connect(self.db_path) as connection:
            count = connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 0)

    def test_records_supplied_voice_note_transcription_without_inventing_text(self):
        voice_message = message(
            "1540985000000000000",
            "",
            attachments=[
                {
                    "fileName": "voice-message.ogg",
                    "contentType": "audio/ogg",
                    "transcriptionText": "MG15 smells like fuel today",
                }
            ],
        )

        result = self.store.ingest_export(export([voice_message]))

        self.assertEqual(result.recorded, 1)
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT content, source_type FROM observations"
            ).fetchone()
        self.assertEqual(row, ("MG15 smells like fuel today", "voice_transcription"))

    def test_concurrent_ingests_cannot_regress_the_cursor(self):
        old_started = threading.Event()
        new_started = threading.Event()

        class BlockingMessage(dict):
            def get(self, key, default=None):
                if key == "author" and not old_started.is_set():
                    old_started.set()
                    self.assert_new_ingest_started()
                return super().get(key, default)

            @staticmethod
            def assert_new_ingest_started():
                if not new_started.wait(timeout=2):
                    raise AssertionError("new ingest did not start")

        old_payload = export(
            [BlockingMessage(message("1540857701217865828", "MG7 - older"))]
        )
        new_payload = export([message("1541067549104672788", "MG15 - newer")])
        failures = []

        def ingest(payload, *, signal_start=False):
            store = IngestStore(self.db_path, CHANNEL_ID)
            try:
                if signal_start:
                    new_started.set()
                store.ingest_export(payload)
            except Exception as error:  # Preserve thread failures for the assertion.
                failures.append(error)
            finally:
                store.close()

        old_thread = threading.Thread(target=ingest, args=(old_payload,))
        new_thread = threading.Thread(
            target=ingest, args=(new_payload,), kwargs={"signal_start": True}
        )
        old_thread.start()
        self.assertTrue(old_started.wait(timeout=2))
        new_thread.start()
        old_thread.join(timeout=5)
        new_thread.join(timeout=5)

        self.assertFalse(old_thread.is_alive())
        self.assertFalse(new_thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(self.store.cursor(), "1541067549104672788")

    def test_rejects_missing_contributor_identity_without_advancing_cursor(self):
        malformed = message("1540857701217865828", "MG7 - observation")
        del malformed["author"]["id"]

        with self.assertRaisesRegex(ValueError, "author ID"):
            self.store.ingest_export(export([malformed]))

        self.assertIsNone(self.store.cursor())
        with sqlite3.connect(self.db_path) as connection:
            count = connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 0)

    def test_durable_block_prevents_polling_past_failure_until_exact_retry(self):
        blocked_id = "1540857701217865828"
        later_id = "1540857701217865829"
        self.store.block_message(blocked_id, "transcription failed")

        with self.assertRaisesRegex(RuntimeError, blocked_id):
            self.store.ingest_export(export([message(later_id, "MG08 - later")]))

        self.assertIsNone(self.store.cursor())
        self.assertEqual(self.store.blocked_message_id(), blocked_id)

        retried = message(blocked_id, "")
        retried["transcriptionText"] = "MG07 - recovered voice note"
        result = self.store.ingest_export(
            export([retried, message(later_id, "MG08 - later")]),
            resolving_message_id=blocked_id,
        )

        self.assertEqual(result.recorded, 2)
        self.assertEqual(self.store.cursor(), later_id)
        self.assertIsNone(self.store.blocked_message_id())

    def test_raw_voice_attachment_blocks_poll_cursor_until_transcribed(self):
        blocked_id = "1540857701217865830"
        raw_voice = message(
            blocked_id,
            "",
            attachments=[
                {
                    "fileName": "voice-message.ogg",
                    "contentType": "audio/ogg",
                }
            ],
        )

        with self.assertRaisesRegex(RuntimeError, blocked_id):
            self.store.ingest_export(
                export([raw_voice, message("1540857701217865831", "MG09 - later")])
            )

        self.assertIsNone(self.store.cursor())
        self.assertEqual(self.store.blocked_message_id(), blocked_id)


class ExportCommandTests(unittest.TestCase):
    def test_polls_strictly_after_exact_cursor_without_threads_and_with_rate_limits(self):
        command = build_export_command(
            exporter=Path("/opt/DiscordChatExporter.Cli"),
            channel_id=CHANNEL_ID,
            output_path=Path("/work/export.json"),
            after="1540857701217865828",
        )

        self.assertEqual(
            command,
            [
                "/opt/DiscordChatExporter.Cli",
                "export",
                "--channel",
                CHANNEL_ID,
                "--format",
                "Json",
                "--output",
                "/work/export.json",
                "--include-threads",
                "None",
                "--respect-rate-limits",
                "true",
                "--utc",
                "true",
                "--after",
                "1540857701217865828",
            ],
        )

    def test_poll_uses_exporter_output_and_keeps_token_out_of_arguments(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            database = temp / "ingest.sqlite3"
            argument_log = temp / "arguments.json"
            fake_exporter = temp / "fake-exporter"
            fake_exporter.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, pathlib, sys\n"
                f"pathlib.Path({str(argument_log)!r}).write_text(json.dumps(sys.argv[1:]))\n"
                "assert os.environ['DISCORD_TOKEN'] == 'private-token'\n"
                "output = pathlib.Path(sys.argv[sys.argv.index('--output') + 1])\n"
                f"output.write_text(json.dumps({export([message('1540857701217865828', 'MG7 - untagged')])!r}))\n"
            )
            os.chmod(fake_exporter, 0o700)
            store = IngestStore(database, CHANNEL_ID)
            try:
                result = poll_once(
                    store=store,
                    exporter=fake_exporter,
                    token="private-token",
                )
            finally:
                store.close()

            arguments = json.loads(argument_log.read_text())
            self.assertEqual(result.recorded, 1)
            self.assertNotIn("private-token", arguments)


class CommandLineTests(unittest.TestCase):
    def test_replays_a_verified_export_into_the_durable_store(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            input_path = temp / "export.json"
            database = temp / "ingest.sqlite3"
            input_path.write_text(
                json.dumps(export([message("1540857701217865828", "MG7 - untagged")]))
            )
            output = StringIO()

            with redirect_stdout(output):
                exit_code = main(
                    [
                        "--database",
                        str(database),
                        "--input-export",
                        str(input_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(output.getvalue())["recorded"], 1)

    def test_enriched_replay_resolves_exact_blocked_voice_message(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            input_path = temp / "enriched-export.json"
            database = temp / "ingest.sqlite3"
            blocked_id = "1540857701217865832"
            store = IngestStore(database, CHANNEL_ID)
            store.block_message(blocked_id, "voice transcription failed")
            store.close()
            enriched = message(blocked_id, "")
            enriched["transcriptionText"] = "MG10 - exact supplied transcript"
            input_path.write_text(json.dumps(export([enriched])))
            output = StringIO()

            with redirect_stdout(output):
                exit_code = main(
                    [
                        "--database",
                        str(database),
                        "--input-export",
                        str(input_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(output.getvalue())["recorded"], 1)
            reopened = IngestStore(database, CHANNEL_ID)
            try:
                self.assertEqual(reopened.cursor(), blocked_id)
                self.assertIsNone(reopened.blocked_message_id())
            finally:
                reopened.close()


if __name__ == "__main__":
    unittest.main()
