from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from datetime import timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict

from .challenges import extract_abs_challenges, format_post_text
from .mlb import MlbStatsApiClient
from .publishers import publishers_from_env
from .render import render_challenge_card
from .service import AbsBotService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone MLB ABS challenge bot")
    parser.add_argument("--sample-json", type=Path, help="Path to a saved MLB feed/live JSON file")
    parser.add_argument("--output-dir", type=Path, default=Path(os.getenv("ABS_OUTPUT_DIR", "output")))
    parser.add_argument("--state-file", type=Path, default=Path(os.getenv("ABS_STATE_FILE", "state/seen_challenges.json")))
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("ABS_POLL_SECONDS", "15")))
    parser.add_argument("--pregame-poll-seconds", type=int, default=int(os.getenv("ABS_PREGAME_POLL_SECONDS", "120")))
    parser.add_argument("--activation-lead-minutes", type=int, default=int(os.getenv("ABS_ACTIVATION_LEAD_MINUTES", "30")))
    parser.add_argument("--lookahead-days", type=int, default=int(os.getenv("ABS_LOOKAHEAD_DAYS", "7")))
    parser.add_argument("--offseason-sleep-seconds", type=int, default=int(os.getenv("ABS_OFFSEASON_SLEEP_SECONDS", "21600")))
    parser.add_argument("--keep-artifacts", action="store_true", default=_env_flag("ABS_KEEP_ARTIFACTS", False))
    parser.add_argument("--clip-wait-seconds", type=int, default=int(os.getenv("ABS_CLIP_WAIT_SECONDS", "900")))
    parser.add_argument("--raw-clip-wait-seconds", type=int, default=int(os.getenv("ABS_RAW_CLIP_WAIT_SECONDS", "180")))
    parser.add_argument("--final-clip-wait-seconds", type=int, default=int(os.getenv("ABS_FINAL_CLIP_WAIT_SECONDS", "2700")))
    parser.add_argument("--local-timezone", type=str, default=os.getenv("ABS_LOCAL_TIMEZONE", "America/New_York"))
    parser.add_argument("--weekly-summary-hour-local", type=int, default=int(os.getenv("ABS_WEEKLY_SUMMARY_HOUR_LOCAL", "9")))
    parser.add_argument("--regular-season-lookahead-days", type=int, default=int(os.getenv("ABS_REGULAR_SEASON_LOOKAHEAD_DAYS", "120")))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8080")))
    parser.add_argument("--once", action="store_true", help="Run a single pass and exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.sample_json:
        return run_sample(args.sample_json, args.output_dir)

    client = MlbStatsApiClient()
    publishers = publishers_from_env()
    service = AbsBotService(
        client=client,
        publishers=publishers,
        output_dir=args.output_dir,
        state_file=args.state_file,
        poll_seconds=args.poll_seconds,
        pregame_poll_seconds=args.pregame_poll_seconds,
        activation_lead=timedelta(minutes=args.activation_lead_minutes),
        lookahead_days=args.lookahead_days,
        offseason_sleep_seconds=args.offseason_sleep_seconds,
        keep_artifacts=args.keep_artifacts,
        clip_wait_seconds=args.clip_wait_seconds,
        raw_clip_wait_seconds=args.raw_clip_wait_seconds,
        final_clip_wait_seconds=args.final_clip_wait_seconds,
        local_timezone=args.local_timezone,
        weekly_summary_hour_local=args.weekly_summary_hour_local,
        regular_season_lookahead_days=args.regular_season_lookahead_days,
    )

    if args.once:
        service.poll_once()
        print(json.dumps(service.snapshot(), indent=2))
        return 0

    worker = threading.Thread(target=service.run_forever, daemon=True)
    worker.start()
    run_http_server(args.port, service)
    return 0


def run_sample(sample_path: Path, output_dir: Path) -> int:
    payload = json.loads(sample_path.read_text(encoding="utf-8"))
    challenges = extract_abs_challenges(payload)
    if not challenges:
        print("No ABS challenges found.")
        return 0

    for challenge in challenges:
        artifact_path = render_challenge_card(challenge, output_dir)
        print("=" * 72)
        print(challenge.challenge_id)
        print(artifact_path)
        print(format_post_text(challenge))
    return 0


def run_http_server(port: int, service: AbsBotService) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path in {"/", "/health"}:
                self._write_json(service.snapshot())
                return
            if self.path == "/recent":
                self._write_json({"recent_posts": service.snapshot()["recent_posts"]})
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def _write_json(self, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"ABS bot listening on 0.0.0.0:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - manual stop
        pass
    finally:
        server.server_close()


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    sys.exit(main())
