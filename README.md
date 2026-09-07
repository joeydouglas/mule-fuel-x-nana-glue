# mule-fuel-x-nana-glue

Canonical **data repo** for the Mule Fuel x Nana Glue breeding project.

## What lives here

- `project.md` — project-level notes and cross metadata
- `plants/<ID>.md` — one markdown file per plant (`MG01`…`MG46`), the source of truth
  for every observation

This repo is read by `breeding-data-api`, which resolves exactly
`<root>/<slug>/project.md` and `<root>/<slug>/plants/<ID>.md`. Nothing else here is served.

## How it is written

The live pipeline writes here automatically. `monitor_breeding_notes.py` runs with
`CONFIG['BACKEND'] = 'markdown'`; `breeding_core.push_to_github()` stages
`project.md` and `plants/` from the local project directory
(`~/.hermes/breeding/mule-fuel-x-nana-glue/`) and pushes to this repo.

Edit the markdown, not any generated artifact.

## Legacy dashboard

The old hand-generated static HTML dashboard (`index.html`, `style.css`,
`plants/*.html`) was split out of this repo into
[`joeydouglas/mule-fuel-x-nana-glue-dashboard-legacy`](https://github.com/joeydouglas/mule-fuel-x-nana-glue-dashboard-legacy)
(NICK-701) so data and presentation no longer collide in one repo. That snapshot is
frozen and archival; GitHub Pages was never enabled on either repo.

## Discord ingestion

`breeding_tracker.discord_ingest` polls the Discord breeding channel with
DiscordChatExporter and records observations. See `breeding_tracker/` and `tests/`.

Guarantees: exact snowflake cursor via `--after` (no time-window overlap),
`--include-threads None`, `--respect-rate-limits true`, cursor + observation written in
one transaction, one observation per Discord message ID (primary key), bot messages
ignored while still advancing the cursor, author/timestamp/source text preserved verbatim.
Supplied `transcriptionText`/`transcription` fields are used for voice notes; a transcript
is never invented.

```bash
python3 -m breeding_tracker.discord_ingest
```
