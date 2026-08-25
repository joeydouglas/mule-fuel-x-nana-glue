# Discord breeding-channel ingestion

`breeding_tracker.discord_ingest` polls Discord channel `1540849479371067412` with DiscordChatExporter and stores observations in SQLite.

## Guarantees

- Uses the exact Discord snowflake cursor with `--after`; no time-window overlap.
- Explicitly passes `--include-threads None` and `--respect-rate-limits true`.
- Stores the cursor and each observation in one SQLite transaction.
- Enforces one observation per Discord message ID with a primary key.
- Ignores bot-authored messages while still advancing the cursor.
- Preserves Discord author ID, author name, timestamp, and source text exactly.
- Parses untagged messages; mentions are neither required nor special.
- Uses supplied `transcriptionText` or `transcription` fields for voice notes. It never invents a transcript when none was supplied. DiscordChatExporter 2.47.3 does not emit those fields itself, so raw DCE voice attachments require an upstream transcription stage to enrich the JSON before replay.

DiscordChatExporter 2.47.3 was used to verify the command and JSON shape.

## Run once

```bash
python3 -m breeding_tracker.discord_ingest
```

Defaults:

- database: `var/discord-ingest.sqlite3`
- token file: `~/.hermes/discord/token`
- exporter: `~/.local/bin/discord-chat-exporter/DiscordChatExporter.Cli`

The token is passed to DiscordChatExporter through `DISCORD_TOKEN`, not a process argument. The command prints a JSON summary suitable for cron logs. Schedule this one-shot command with the host's existing scheduler; overlapping runs serialize on SQLite and remain idempotent.

## Real-time Gateway trigger

`integrations/hermes-breeding-ingest` is a Hermes plugin for near-real-time
ingestion. The existing Hermes Discord Gateway receives each admitted
`MESSAGE_CREATE` event. The plugin records direct `#breeding` messages in the
same SQLite store, then returns `skip` so no agent run, reply, reaction, or
thread is created. Messages in threads under `#breeding` are silently skipped
and never recorded.

The live Hermes configuration must list channel `1540849479371067412` in
`discord.free_response_channels` so untagged messages reach the hook. The plugin
still enforces the exact channel ID. Voice notes use Hermes' configured
transcription provider; a failed transcription leaves the cursor unchanged for
recovery rather than inventing text. The failed message ID is durably blocked,
so later Gateway events and exporter polls cannot skip past it; a successful
retry of that exact message clears the block transactionally. Because
DiscordChatExporter 2.47.3 does not supply native voice transcripts, resolving a
persistently failed voice note requires an enriched replay containing the real
transcription; raw exporter backfill remains blocked rather than skipping it.

DiscordChatExporter remains the outage/backfill path. Both paths reuse the same
message-ID primary key and cursor, so reconnects, duplicate Gateway delivery,
and later polls do not duplicate observations.

## Verify or replay an export

```bash
python3 -m breeding_tracker.discord_ingest \
  --database /path/to/verification.sqlite3 \
  --input-export /path/to/export.json
```

Replaying the same export records zero additional observations.

## Tests

```bash
python3 -m unittest discover -s tests -v
```
