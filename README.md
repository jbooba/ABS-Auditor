# MLB ABS Bot

Standalone MLB ABS challenge monitor designed for Railway deployment.

The app polls the MLB Stats API across a rolling UTC schedule window, detects completed ABS challenges, renders a challenge card, and posts it to any configured publishers.

## What It Does

- Polls all current MLB games in real time
- Sleeps when there are no live or imminent games, then wakes ahead of the next scheduled first pitch
- Detects completed ABS challenges from `reviewDetails` / `playEvents`
- Dedupes challenges across polling cycles with a local state file
- Tracks home-plate umpires and season-to-date challenge upheld rates
- Renders a standalone challenge graphic
- Deletes posted image artifacts automatically unless you opt into keeping them
- Posts challenge text + image to:
  - Discord webhook
  - BlueSky
  - X
- Exposes a tiny HTTP server for Railway health checks

## Local Run

```powershell
python -m abs_bot.main
```

Process a saved game feed instead of polling live games:

```powershell
python -m abs_bot.main --sample-json "C:\Users\jesse\Downloads\statsapiexample.json" --once
```

## Railway

Recommended start command:

```text
python railway_start.py
```

The service binds to `PORT` and exposes:

- `/health`
- `/recent`

Suggested Railway setup:

1. Deploy from GitHub.
2. Keep the start command as `python railway_start.py` if Railway does not detect it automatically.
3. Add a persistent volume and mount it at `/data` for the state file.
4. Set `ABS_STATE_FILE=/data/state/seen_challenges.json`.
5. Leave `ABS_OUTPUT_DIR` on ephemeral storage unless you explicitly want to keep artifacts.
6. Add whichever publisher secrets you want to enable.

## Environment Variables

- `PORT`: HTTP port for Railway, default `8080`
- `ABS_POLL_SECONDS`: poll cadence, default `15`
- `ABS_PREGAME_POLL_SECONDS`: lighter pregame cadence inside the activation window, default `120`
- `ABS_ACTIVATION_LEAD_MINUTES`: how early the bot wakes before the next scheduled first pitch, default `30`
- `ABS_LOOKAHEAD_DAYS`: how far ahead the idle scheduler looks for the next game, default `7`
- `ABS_OFFSEASON_SLEEP_SECONDS`: fallback sleep when no game is found in the lookahead window, default `21600`
- `ABS_OUTPUT_DIR`: image output directory, default `./output`
- `ABS_STATE_FILE`: seen challenge + umpire stats state JSON, default `./state/seen_challenges.json`
- `ABS_KEEP_ARTIFACTS`: keep generated card files after posting, default `0`
- `ABS_DISCORD_WEBHOOK_URL`: Discord webhook target
- `ABS_BLUESKY_HANDLE`: BlueSky handle
- `ABS_BLUESKY_APP_PASSWORD`: BlueSky app password
- `ABS_X_API_KEY`: X consumer API key
- `ABS_X_API_SECRET`: X consumer API secret
- `ABS_X_ACCESS_TOKEN`: X access token
- `ABS_X_ACCESS_TOKEN_SECRET`: X access token secret

## Notes

- The state file is local JSON. On Railway, move this to a persistent volume or database if you want dedupe and umpire challenge stats to survive redeploys and restarts.
- The scheduler runs in UTC and watches yesterday/today/tomorrow for active games, while looking further ahead to decide when to wake for the next slate.
- The bot prefers pitch-level `reviewDetails` when present. If MLB omits that, it falls back to the final pitch in the reviewed at-bat and records that selection reason in the output metadata.
- The displayed umpire rate is a challenge-specific upheld rate, not an all-pitches accuracy estimate.
- The renderer writes PNG when Pillow is installed. If Pillow is unavailable, it falls back to SVG.
- By default, service-mode artifacts are deleted after successful posting. Sample/manual runs still keep their rendered outputs.
- A `Procfile` and `railway_start.py` launcher are included so Railway can start the service even if the repo contents end up one folder deeper than expected during deployment.
