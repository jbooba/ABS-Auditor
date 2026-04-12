from __future__ import annotations

import json
import threading
from collections import deque
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Set

from .challenges import extract_abs_challenges, format_post_text
from .clips import ClipMedia, ClipLookupResult, lookup_abs_clip_options
from .mlb import MlbStatsApiClient
from .publishers import Publisher
from .render import render_challenge_card


STATE_VERSION = 8


class AbsBotService:
    def __init__(
        self,
        *,
        client: MlbStatsApiClient,
        publishers: List[Publisher],
        output_dir: Path,
        state_file: Path,
        poll_seconds: int = 15,
        pregame_poll_seconds: int = 120,
        activation_lead: timedelta = timedelta(minutes=30),
        lookahead_days: int = 7,
        offseason_sleep_seconds: int = 21600,
        keep_artifacts: bool = False,
        clip_wait_seconds: int = 900,
        raw_clip_wait_seconds: int = 180,
        final_clip_wait_seconds: int = 2700,
    ) -> None:
        self.client = client
        self.publishers = publishers
        self.output_dir = output_dir
        self.state_file = state_file
        self.poll_seconds = poll_seconds
        self.pregame_poll_seconds = pregame_poll_seconds
        self.activation_lead = activation_lead
        self.lookahead_days = lookahead_days
        self.offseason_sleep_seconds = offseason_sleep_seconds
        self.keep_artifacts = keep_artifacts
        self.clip_wait_seconds = clip_wait_seconds
        self.raw_clip_wait_seconds = raw_clip_wait_seconds
        self.final_clip_wait_seconds = final_clip_wait_seconds
        self.stop_event = threading.Event()
        self.state_lock = threading.Lock()
        self.last_error: str | None = None
        self.last_poll_started_at: str | None = None
        self.last_poll_finished_at: str | None = None
        self.last_games_checked = 0
        self.last_feeds_checked = 0
        self.last_active_games = 0
        self.last_upcoming_games = 0
        self.current_mode = "starting"
        self.next_wake_at: str | None = None
        self.next_wake_reason: str | None = None
        self.next_game_at: str | None = None
        self.total_posts = 0
        self.recent_posts: Deque[Dict[str, Any]] = deque(maxlen=20)
        self.recent_failures: Deque[Dict[str, Any]] = deque(maxlen=20)
        (
            self.seen_challenge_ids,
            self.umpire_stats,
            self.publisher_delivery,
            self.pending_clip_lookups,
            self.seen_clip_urls,
        ) = self._load_state()
        self.closed_game_pks: set[int] = set()

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            sleep_seconds = float(self.poll_seconds)
            try:
                sleep_seconds = self.poll_once()
            except Exception as exc:  # pragma: no cover - safety net
                with self.state_lock:
                    self.last_error = str(exc)
                    self.last_poll_finished_at = _utc_now_iso()
                    self.current_mode = "error"
                    self.next_wake_reason = "Retry after error"
                    self.next_wake_at = _utc_after_seconds_iso(self.poll_seconds)
                sleep_seconds = float(self.poll_seconds)
            self.stop_event.wait(max(1.0, sleep_seconds))

    def poll_once(self) -> float:
        now = datetime.now(timezone.utc)
        with self.state_lock:
            self.last_poll_started_at = now.isoformat()
            self.last_error = None

        dates = self.client.monitor_window_dates(now)
        games = [
            game
            for game in self.client.schedule_for_dates(dates)
            if self.client.should_track_game(game)
        ]
        current_game_pks = {
            int(game.get("gamePk", 0))
            for game in games
            if int(game.get("gamePk", 0))
        }
        self.closed_game_pks.intersection_update(current_game_pks)

        active_games = [game for game in games if self.client.is_active_game(game)]
        upcoming_games = sorted(
            [game for game in games if self.client.is_upcoming_game(game, now)],
            key=lambda game: self.client.game_datetime_utc(game) or datetime.max.replace(tzinfo=timezone.utc),
        )
        imminent_games = [
            game
            for game in upcoming_games
            if self.client.is_within_activation_window(game, now, self.activation_lead)
        ]

        games_to_fetch: List[Dict[str, Any]] = []
        for game in games:
            game_pk = int(game.get("gamePk", 0))
            if not game_pk:
                continue
            if self.client.is_active_game(game):
                self.closed_game_pks.discard(game_pk)
                games_to_fetch.append(game)
                continue
            if self.client.is_terminal_game(game) and game_pk not in self.closed_game_pks:
                games_to_fetch.append(game)

        new_posts = 0
        for game in games_to_fetch:
            game_pk = int(game.get("gamePk", 0))
            if not game_pk:
                continue
            feed = self.client.fetch_live_game_feed(game_pk)
            game_has_pending_delivery = False
            for challenge in extract_abs_challenges(feed):
                if challenge.challenge_id in self.seen_challenge_ids:
                    continue
                challenge = self._challenge_with_umpire_stats(challenge)
                clip_options = lookup_abs_clip_options(
                    challenge,
                    excluded_direct_urls=self._seen_clip_urls_for_other_challenges(challenge.challenge_id),
                )
                clip = self._choose_clip_for_publish(challenge, clip_options)
                if clip is None:
                    if self._should_wait_for_clip(
                        challenge,
                        game_is_terminal=self.client.is_terminal_game(game),
                    ):
                        self._mark_clip_pending(
                            challenge.challenge_id,
                            available_clip_kind=(
                                "highlight"
                                if clip_options.highlight_clip is not None
                                else None
                            ),
                            available_clip_host=(
                                clip_options.highlight_clip.host
                                if clip_options.highlight_clip is not None
                                else None
                            ),
                        )
                        game_has_pending_delivery = True
                        continue

                self._clear_clip_pending(challenge.challenge_id)
                artifact_path = None if clip else render_challenge_card(challenge, self.output_dir)
                post_text = self._post_text_for_challenge(challenge, clip)
                delivered_publishers = self.publisher_delivery.get(challenge.challenge_id, set())
                publisher_failures: list[str] = []
                for publisher in self.publishers:
                    if publisher.delivery_key in delivered_publishers:
                        continue
                    try:
                        publisher.publish(challenge, post_text, artifact_path, clip=clip)
                    except Exception as exc:
                        publisher_failures.append(f"{publisher.delivery_key}: {exc}")
                        continue
                    self._mark_publisher_delivered(challenge.challenge_id, publisher.delivery_key)
                    delivered_publishers = self.publisher_delivery.get(challenge.challenge_id, set())

                if publisher_failures:
                    self._record_failures(challenge, publisher_failures)

                artifact_retained = (self.keep_artifacts or not self.publishers) and artifact_path is not None
                completed_this_cycle = self._publishers_complete(challenge.challenge_id)
                if completed_this_cycle:
                    self._record_challenge(
                        challenge.challenge_id,
                        challenge,
                        artifact_path if artifact_retained else None,
                        post_text,
                        clip=clip,
                        artifact_retained=artifact_retained,
                    )
                if artifact_path is not None and not artifact_retained:
                    artifact_path.unlink(missing_ok=True)
                if completed_this_cycle:
                    new_posts += 1
                else:
                    game_has_pending_delivery = True
            if self.client.is_terminal_game(game):
                if game_has_pending_delivery:
                    self.closed_game_pks.discard(game_pk)
                else:
                    self.closed_game_pks.add(game_pk)

        sleep_seconds, mode, wake_reason, next_game_at = self._next_sleep_plan(
            now=now,
            active_games=active_games,
            imminent_games=imminent_games,
            upcoming_games=upcoming_games,
        )
        next_wake_at = (now + timedelta(seconds=sleep_seconds)).isoformat()

        with self.state_lock:
            self.last_games_checked = len(games)
            self.last_feeds_checked = len(games_to_fetch)
            self.last_active_games = len(active_games)
            self.last_upcoming_games = len(upcoming_games)
            self.current_mode = mode
            self.next_wake_reason = wake_reason
            self.next_wake_at = next_wake_at
            self.next_game_at = next_game_at
            self.last_poll_finished_at = _utc_now_iso()
            self.total_posts += new_posts
        return sleep_seconds

    def snapshot(self) -> Dict[str, Any]:
        with self.state_lock:
            return {
                "ok": self.last_error is None,
                "last_error": self.last_error,
                "last_poll_started_at": self.last_poll_started_at,
                "last_poll_finished_at": self.last_poll_finished_at,
                "poll_seconds": self.poll_seconds,
                "pregame_poll_seconds": self.pregame_poll_seconds,
                "clip_wait_seconds": self.clip_wait_seconds,
                "raw_clip_wait_seconds": self.raw_clip_wait_seconds,
                "final_clip_wait_seconds": self.final_clip_wait_seconds,
                "games_checked_last_cycle": self.last_games_checked,
                "feeds_checked_last_cycle": self.last_feeds_checked,
                "active_games_last_cycle": self.last_active_games,
                "upcoming_games_last_cycle": self.last_upcoming_games,
                "mode": self.current_mode,
                "next_wake_at": self.next_wake_at,
                "next_wake_reason": self.next_wake_reason,
                "next_game_at": self.next_game_at,
                "seen_challenges": len(self.seen_challenge_ids),
                "tracked_umpires": len(self.umpire_stats),
                "pending_publisher_deliveries": len(self.publisher_delivery),
                "pending_clip_lookups": len(self.pending_clip_lookups),
                "seen_clip_urls": len(self.seen_clip_urls),
                "total_posts": self.total_posts,
                "recent_posts": list(self.recent_posts),
                "recent_failures": list(self.recent_failures),
            }

    def _record_challenge(
        self,
        challenge_id: str,
        challenge: Any,
        artifact_path: Path | None,
        post_text: str,
        *,
        clip: ClipMedia | None,
        artifact_retained: bool,
    ) -> None:
        with self.state_lock:
            self.seen_challenge_ids.add(challenge_id)
            self.publisher_delivery.pop(challenge_id, None)
            self.pending_clip_lookups.pop(challenge_id, None)
            if clip is not None and clip.direct_url:
                self.seen_clip_urls[clip.direct_url] = challenge_id
            self._update_umpire_stats(challenge)
            self.recent_posts.appendleft(
                {
                    "challenge_id": challenge_id,
                    "matchup": challenge.teams.matchup_label,
                    "inning": challenge.inning_label,
                    "challenger": challenge.challenger_name,
                    "home_plate_umpire": challenge.home_plate_umpire_name,
                    "outcome": challenge.outcome_label,
                    "artifact_path": str(artifact_path) if artifact_path else None,
                    "artifact_retained": artifact_retained,
                    "clip_url": clip.direct_url if clip else None,
                    "clip_page_url": clip.page_url if clip else None,
                    "clip_host": clip.host if clip else None,
                    "media_source": "clip" if clip else "graphic",
                    "posted_at": _utc_now_iso(),
                    "text": post_text,
                }
            )
            self._save_state()

    def _mark_publisher_delivered(self, challenge_id: str, publisher_key: str) -> None:
        with self.state_lock:
            delivery = self.publisher_delivery.setdefault(challenge_id, set())
            delivery.add(publisher_key)
            self._save_state()

    def _record_failures(self, challenge: Any, failures: list[str]) -> None:
        with self.state_lock:
            message = "; ".join(failures)
            self.last_error = message
            self.recent_failures.appendleft(
                {
                    "challenge_id": challenge.challenge_id,
                    "matchup": challenge.teams.matchup_label,
                    "inning": challenge.inning_label,
                    "challenger": challenge.challenger_name,
                    "failures": failures,
                    "failed_at": _utc_now_iso(),
                }
            )

    def _post_text_for_challenge(self, challenge: Any, clip: ClipMedia | None) -> str:
        text = format_post_text(challenge)
        if clip is None:
            return text
        return f"{text}\nClip: {clip.direct_url}".strip()

    def _mark_clip_pending(
        self,
        challenge_id: str,
        *,
        available_clip_kind: str | None = None,
        available_clip_host: str | None = None,
    ) -> None:
        now_iso = _utc_now_iso()
        with self.state_lock:
            existing = self.pending_clip_lookups.get(challenge_id, {})
            self.pending_clip_lookups[challenge_id] = {
                "first_seen_at": str(existing.get("first_seen_at") or now_iso),
                "last_checked_at": now_iso,
                "attempts": int(existing.get("attempts", 0) or 0) + 1,
                "available_clip_kind": str(
                    available_clip_kind
                    or existing.get("available_clip_kind")
                    or ""
                ),
                "available_clip_host": str(
                    available_clip_host
                    or existing.get("available_clip_host")
                    or ""
                ),
            }
            self._save_state()

    def _clear_clip_pending(self, challenge_id: str) -> None:
        with self.state_lock:
            if challenge_id not in self.pending_clip_lookups:
                return
            self.pending_clip_lookups.pop(challenge_id, None)
            self._save_state()

    def _choose_clip_for_publish(
        self,
        challenge: Any,
        clip_options: ClipLookupResult,
    ) -> ClipMedia | None:
        if clip_options.raw_clip is not None:
            return clip_options.raw_clip
        if clip_options.highlight_clip is None:
            return None
        if self._should_wait_for_raw_clip(challenge):
            return None
        return clip_options.highlight_clip

    def _should_wait_for_clip(self, challenge: Any, *, game_is_terminal: bool) -> bool:
        wait_seconds = self.final_clip_wait_seconds if game_is_terminal else self.clip_wait_seconds
        if wait_seconds <= 0:
            return False
        age_seconds = self._clip_lookup_age_seconds(challenge)
        if age_seconds is None:
            return True
        return age_seconds < wait_seconds

    def _should_wait_for_raw_clip(self, challenge: Any) -> bool:
        if self.raw_clip_wait_seconds <= 0:
            return False
        age_seconds = self._clip_lookup_age_seconds(challenge)
        if age_seconds is None:
            return True
        return age_seconds < self.raw_clip_wait_seconds

    def _clip_lookup_age_seconds(self, challenge: Any) -> float | None:
        play_end_time = _parse_iso(getattr(challenge, "play_end_time", None))
        if play_end_time is not None:
            return max(0.0, (datetime.now(timezone.utc) - play_end_time).total_seconds())

        with self.state_lock:
            existing = self.pending_clip_lookups.get(challenge.challenge_id)
        if not existing:
            return None
        first_seen_at = _parse_iso(existing.get("first_seen_at"))
        if first_seen_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - first_seen_at).total_seconds())

    def _publishers_complete(self, challenge_id: str) -> bool:
        if not self.publishers:
            return True
        delivered = self.publisher_delivery.get(challenge_id, set())
        return all(publisher.delivery_key in delivered for publisher in self.publishers)

    def _load_state(
        self,
    ) -> tuple[
        set[str],
        dict[str, Dict[str, Any]],
        dict[str, Set[str]],
        dict[str, Dict[str, Any]],
        dict[str, str],
    ]:
        if not self.state_file.exists():
            return set(), {}, {}, {}, {}
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return set(), {}, {}, {}, {}
        if isinstance(payload, list):
            return {str(item) for item in payload}, {}, {}, {}, {}
        if not isinstance(payload, dict):
            return set(), {}, {}, {}, {}

        payload_version = int(payload.get("state_version", 0) or 0)
        seen_ids: set[str] = set()
        if payload_version == STATE_VERSION:
            seen_payload = payload.get("seen_challenge_ids", [])
            seen_ids = {str(item) for item in seen_payload if item is not None}
        umpire_stats: dict[str, Dict[str, Any]] = {}
        umpire_payload = payload.get("umpire_stats", {})
        if payload_version == STATE_VERSION and isinstance(umpire_payload, dict):
            for key, value in umpire_payload.items():
                if not isinstance(value, dict):
                    continue
                umpire_stats[str(key)] = {
                    "name": str(value.get("name", "")),
                    "total": int(value.get("total", 0) or 0),
                    "confirmed": int(value.get("confirmed", 0) or 0),
                    "overturned": int(value.get("overturned", 0) or 0),
                }
        publisher_delivery: dict[str, Set[str]] = {}
        delivery_payload = payload.get("publisher_delivery", {})
        if payload_version == STATE_VERSION and isinstance(delivery_payload, dict):
            for key, value in delivery_payload.items():
                if isinstance(value, list):
                    publisher_delivery[str(key)] = {
                        str(item)
                        for item in value
                        if item is not None
                    }
        pending_clip_lookups: dict[str, Dict[str, Any]] = {}
        pending_payload = payload.get("pending_clip_lookups", {})
        if payload_version == STATE_VERSION and isinstance(pending_payload, dict):
            for key, value in pending_payload.items():
                if not isinstance(value, dict):
                    continue
                pending_clip_lookups[str(key)] = {
                    "first_seen_at": str(value.get("first_seen_at", "")),
                    "last_checked_at": str(value.get("last_checked_at", "")),
                    "attempts": int(value.get("attempts", 0) or 0),
                    "available_clip_kind": str(value.get("available_clip_kind", "")),
                    "available_clip_host": str(value.get("available_clip_host", "")),
                }
        seen_clip_urls: dict[str, str] = {}
        seen_clip_payload = payload.get("seen_clip_urls", {})
        if payload_version == STATE_VERSION and isinstance(seen_clip_payload, dict):
            for clip_url, challenge_id in seen_clip_payload.items():
                if not clip_url or not challenge_id:
                    continue
                seen_clip_urls[str(clip_url)] = str(challenge_id)
        return seen_ids, umpire_stats, publisher_delivery, pending_clip_lookups, seen_clip_urls

    def _save_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps(
                {
                    "state_version": STATE_VERSION,
                    "seen_challenge_ids": sorted(self.seen_challenge_ids),
                    "umpire_stats": self.umpire_stats,
                    "publisher_delivery": {
                        key: sorted(value)
                        for key, value in self.publisher_delivery.items()
                    },
                    "pending_clip_lookups": self.pending_clip_lookups,
                    "seen_clip_urls": self.seen_clip_urls,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def _seen_clip_urls_for_other_challenges(self, challenge_id: str) -> set[str]:
        with self.state_lock:
            return {
                clip_url
                for clip_url, recorded_challenge_id in self.seen_clip_urls.items()
                if recorded_challenge_id != challenge_id
            }

    def _challenge_with_umpire_stats(self, challenge: Any) -> Any:
        key = self._umpire_state_key(challenge)
        if not key:
            return challenge

        existing = self.umpire_stats.get(
            key,
            {
                "name": challenge.home_plate_umpire_name,
                "total": 0,
                "confirmed": 0,
                "overturned": 0,
            },
        )
        confirmed = int(existing.get("confirmed", 0))
        overturned = int(existing.get("overturned", 0))
        total = int(existing.get("total", confirmed + overturned))

        total += 1
        if challenge.is_overturned:
            overturned += 1
        else:
            confirmed += 1

        return replace(
            challenge,
            umpire_challenge_total=total,
            umpire_confirmed_total=confirmed,
            umpire_overturned_total=overturned,
        )

    def _update_umpire_stats(self, challenge: Any) -> None:
        key = self._umpire_state_key(challenge)
        if not key:
            return
        self.umpire_stats[key] = {
            "name": challenge.home_plate_umpire_name,
            "total": int(challenge.umpire_challenge_total or 0),
            "confirmed": int(challenge.umpire_confirmed_total or 0),
            "overturned": int(challenge.umpire_overturned_total or 0),
        }

    @staticmethod
    def _umpire_state_key(challenge: Any) -> str | None:
        if challenge.home_plate_umpire_id is not None:
            return f"id:{challenge.home_plate_umpire_id}"
        if challenge.home_plate_umpire_name:
            return f"name:{challenge.home_plate_umpire_name.lower()}"
        return None

    def _next_sleep_plan(
        self,
        *,
        now: datetime,
        active_games: List[Dict[str, Any]],
        imminent_games: List[Dict[str, Any]],
        upcoming_games: List[Dict[str, Any]],
    ) -> tuple[float, str, str, str | None]:
        if active_games:
            return (
                float(self.poll_seconds),
                "active",
                f"{len(active_games)} active game(s) in progress",
                _game_time_iso(self.client, upcoming_games[0]) if upcoming_games else None,
            )

        next_game = upcoming_games[0] if upcoming_games else self.client.next_scheduled_game(
            now=now,
            days_ahead=self.lookahead_days,
        )
        next_game_at = _game_time_iso(self.client, next_game)
        if imminent_games:
            return (
                float(self.pregame_poll_seconds),
                "pregame",
                f"Pregame monitoring for next scheduled game at {next_game_at}",
                next_game_at,
            )

        if next_game is not None:
            game_time = self.client.game_datetime_utc(next_game)
            if game_time is None:
                return (
                    float(self.pregame_poll_seconds),
                    "pregame",
                    "Upcoming game has no scheduled UTC start time yet",
                    next_game_at,
                )
            activation_time = game_time - self.activation_lead
            if activation_time <= now:
                return (
                    float(self.pregame_poll_seconds),
                    "pregame",
                    f"Next game is inside the activation window ({next_game_at})",
                    next_game_at,
                )
            sleep_seconds = max(60.0, (activation_time - now).total_seconds())
            return (
                sleep_seconds,
                "idle",
                f"Waiting for next game activation window at {activation_time.isoformat()}",
                next_game_at,
            )

        return (
            float(self.offseason_sleep_seconds),
            "idle",
            f"No scheduled games found in the next {self.lookahead_days} day(s)",
            None,
        )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_after_seconds_iso(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _game_time_iso(client: MlbStatsApiClient, game: Dict[str, Any] | None) -> str | None:
    if not game:
        return None
    game_time = client.game_datetime_utc(game)
    return game_time.isoformat() if game_time else None
