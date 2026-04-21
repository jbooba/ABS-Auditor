from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.parse import urlencode
from urllib.request import Request, urlopen


TEAM_SUMMARY_PAGE_URL = "https://baseballsavant.mlb.com/leaderboard/abs-challenges"
TEAM_SUMMARY_SERVICE_URL = "https://baseballsavant.mlb.com/leaderboard/services/abs/{team_id}"
TEAM_SUMMARY_PAGE_SIZE = 999999999
TEAM_SUMMARY_SORT = "overturns_vs_exp_total"
TEAM_SUMMARY_SORT_DIR = "desc"
CHALLENGE_TYPE = "team-summary"
GAME_TYPE = "regular"
LEVEL = "mlb"

logger = logging.getLogger(__name__)


class SavantAbsClient:
    """Fetch and normalize season-level ABS challenge data from Baseball Savant.

    The team-summary leaderboard service exposes full-season challenge rows with
    fields such as:
    - edge distance (`edge_dist_calc`)
    - expected challenge rate (`sz_challenge_prob`)
    - run-value impact (`sz_challenge_runs`)
    - reasonable-attempt flag (`is_challengeABS_reasonable_attempt`)

    Savant serves each challenge twice in team-summary mode, once from each team
    perspective. We canonicalize to the challenging team's view (`against=False`)
    and then dedupe on `play_id`.
    """

    def __init__(self, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds

    def fetch_season_team_summary_challenges(
        self,
        *,
        year: int,
        game_type: str = GAME_TYPE,
        level: str = LEVEL,
    ) -> dict[str, dict[str, Any]]:
        """Return full-season ABS challenge rows keyed by play_id."""
        team_rows = self.fetch_team_summary_index(year=year, game_type=game_type, level=level)
        by_play_id: dict[str, dict[str, Any]] = {}
        for team_row in team_rows:
            team_id = _coerce_optional_int(team_row.get("id"))
            if team_id is None:
                continue
            detail_rows = self.fetch_team_summary_details(
                team_id=team_id,
                year=year,
                game_type=game_type,
                level=level,
            )
            for raw_row in detail_rows:
                normalized = _normalize_team_summary_row(raw_row)
                play_id = normalized.get("play_id")
                if not play_id:
                    continue
                existing = by_play_id.get(play_id)
                if existing is None or _prefer_team_summary_row(normalized, existing):
                    by_play_id[play_id] = normalized
        logger.info(
            "Savant team-summary sync produced %s unique challenge row(s) for %s",
            len(by_play_id),
            year,
        )
        return by_play_id

    def fetch_team_summary_index(
        self,
        *,
        year: int,
        game_type: str = GAME_TYPE,
        level: str = LEVEL,
    ) -> list[dict[str, Any]]:
        """Return the 30 team-summary leaderboard rows used to drive detail fetches."""
        query = urlencode(
            {
                "level": level,
                "gameType": game_type,
                "year": year,
                "challengeType": CHALLENGE_TYPE,
                "sort": TEAM_SUMMARY_SORT,
                "sortDir": TEAM_SUMMARY_SORT_DIR,
                "page": 0,
                "pageSize": TEAM_SUMMARY_PAGE_SIZE,
            }
        )
        html = self.fetch_text(f"{TEAM_SUMMARY_PAGE_URL}?{query}")
        rows = _extract_embedded_json_array(html, "absData")
        logger.info("Fetched Savant team-summary index with %s row(s)", len(rows))
        return rows

    def fetch_team_summary_details(
        self,
        *,
        team_id: int,
        year: int,
        game_type: str = GAME_TYPE,
        level: str = LEVEL,
    ) -> list[dict[str, Any]]:
        """Return expanded per-challenge rows for one team-summary team id."""
        query = urlencode(
            {
                "year": year,
                "challengeType": CHALLENGE_TYPE,
                "gameType": game_type,
                "level": level,
                "groupBy": "",
            }
        )
        payload = self.fetch_json(TEAM_SUMMARY_SERVICE_URL.format(team_id=team_id) + f"?{query}")
        rows = payload.get("data", [])
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    def fetch_text(self, url: str) -> str:
        logger.debug("Fetching Savant HTML: %s", url)
        request = Request(
            url,
            headers={
                "User-Agent": "mlb-abs-bot/0.1",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read().decode("utf-8")

    def fetch_json(self, url: str) -> dict[str, Any]:
        logger.debug("Fetching Savant JSON: %s", url)
        request = Request(
            url,
            headers={
                "User-Agent": "mlb-abs-bot/0.1",
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))


def difficulty_from_savant_row(row: dict[str, Any], *, confirmed: bool) -> float | None:
    """Return a 0-1 difficulty score for upheld challenges.

    Savant's challenge model (`sz_challenge_prob`) is the best public proxy we
    have for how tempting a challenge was before review. For umpire power
    rankings, we only apply this to confirmed calls: surviving a highly
    challengeable pitch is harder than surviving an obvious low-probability shot.
    """

    if not confirmed:
        return None
    probability = _coerce_optional_float(row.get("challenge_probability"))
    if probability is None:
        return None
    return max(0.0, min(1.0, probability))


def latest_game_date(rows: dict[str, dict[str, Any]]) -> str | None:
    """Return the latest ISO game date present in a Savant season payload."""
    latest: datetime | None = None
    latest_value: str | None = None
    for row in rows.values():
        raw_value = row.get("game_date")
        if not raw_value:
            continue
        parsed = _parse_iso(raw_value)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
            latest_value = parsed.date().isoformat()
    return latest_value


def _normalize_team_summary_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    play_id = str(raw_row.get("play_id", "")).strip()
    if not play_id:
        return {}

    raw_game_date = str(raw_row.get("game_date", "")).strip()
    game_date = _iso_date_only(raw_game_date)
    challenge_probability = _coerce_optional_float(raw_row.get("sz_challenge_prob"))
    return {
        "play_id": play_id,
        "game_pk": _coerce_optional_int(raw_row.get("game_pk")) or 0,
        "game_date": game_date,
        "game_date_raw": raw_game_date,
        "challenging_player_id": _coerce_optional_int(raw_row.get("challenging_player_id")),
        "challenger_role": _challenger_role(str(raw_row.get("team_summary_mode", "")).strip()),
        "against": bool(raw_row.get("against")),
        "team_summary_mode": str(raw_row.get("team_summary_mode", "")).strip(),
        "challenge_probability": challenge_probability,
        "challenge_probability_gain": _coerce_optional_float(raw_row.get("sz_challenge_prob_gain")),
        "challenge_probability_lost": _coerce_optional_float(raw_row.get("sz_challenge_prob_lost")),
        "reasonable_attempt": _coerce_optional_bool(raw_row.get("is_challengeABS_reasonable_attempt")),
        "challenge_runs": _coerce_optional_float(raw_row.get("sz_challenge_runs")),
        "challenge_overturned_runs": _coerce_optional_float(raw_row.get("sz_challenge_overturned_runs")),
        "challenge_lost_runs": _coerce_optional_float(raw_row.get("sz_challenge_lost_runs")),
        "edge_distance_inches": _coerce_optional_float(raw_row.get("edge_dist_calc")),
        "original_is_strike": _coerce_optional_bool(raw_row.get("original_isStrike_ump")),
        "is_overturned": _coerce_optional_bool(raw_row.get("is_challengeABS_overturned")),
        "batter_name": str(raw_row.get("batter_name_flipped") or raw_row.get("batter_name") or "").strip(),
        "pitcher_name": str(raw_row.get("pitcher_name_flipped") or raw_row.get("pitcher_name") or "").strip(),
        "catcher_name": str(raw_row.get("catcher_name_flipped") or raw_row.get("catcher_name") or "").strip(),
        "bat_team_id": _coerce_optional_int(raw_row.get("bat_team_id")),
        "fld_team_id": _coerce_optional_int(raw_row.get("fld_team_id")),
        "bat_team_abbr": str(raw_row.get("bat_team_abbr", "")).strip(),
        "fld_team_abbr": str(raw_row.get("fld_team_abbr", "")).strip(),
        "player_team_abbr": str(raw_row.get("player_team_abbr", "")).strip(),
        "opp_team_abbr": str(raw_row.get("opp_team_abbr", "")).strip(),
        "inning": _coerce_optional_int(raw_row.get("event_inning")),
        "outs": _coerce_optional_int(raw_row.get("outs")),
        "balls": _coerce_optional_int(raw_row.get("pre_ball_count")),
        "strikes": _coerce_optional_int(raw_row.get("pre_strike_count")),
        "bat_score": _coerce_optional_int(raw_row.get("bat_score")),
        "fld_score": _coerce_optional_int(raw_row.get("fld_score")),
        "plate_x": _coerce_optional_float(raw_row.get("plateX")),
        "plate_z": _coerce_optional_float(raw_row.get("plateZ")),
        "strike_zone_top": _coerce_optional_float(raw_row.get("strikeZoneTop")),
        "strike_zone_bottom": _coerce_optional_float(raw_row.get("strikeZoneBottom")),
        "zone_width_inches": _coerce_optional_float(raw_row.get("widthinches")),
    }


def _prefer_team_summary_row(candidate: dict[str, Any], existing: dict[str, Any]) -> bool:
    """Prefer the challenging-team row when deduping play_id entries."""
    candidate_is_for = not bool(candidate.get("against"))
    existing_is_for = not bool(existing.get("against"))
    if candidate_is_for != existing_is_for:
        return candidate_is_for
    candidate_role = str(candidate.get("challenger_role", ""))
    existing_role = str(existing.get("challenger_role", ""))
    if candidate_role != existing_role:
        return candidate_role in {"batter", "catcher"}
    candidate_name_score = sum(1 for key in ("batter_name", "pitcher_name", "catcher_name") if candidate.get(key))
    existing_name_score = sum(1 for key in ("batter_name", "pitcher_name", "catcher_name") if existing.get(key))
    return candidate_name_score > existing_name_score


def _challenger_role(team_summary_mode: str) -> str:
    if team_summary_mode.startswith("batter"):
        return "batter"
    if team_summary_mode.startswith("catcher"):
        return "catcher"
    return "unknown"


def _extract_embedded_json_array(html_text: str, variable_name: str) -> list[dict[str, Any]]:
    pattern = rf"const\s+{re.escape(variable_name)}\s*=\s*(\[[\s\S]*?\]);"
    match = re.search(pattern, html_text)
    if match is None:
        raise ValueError(f"Unable to find embedded array {variable_name!r} in Savant page")
    payload = json.loads(match.group(1))
    if not isinstance(payload, list):
        raise ValueError(f"Embedded payload {variable_name!r} was not a list")
    return [row for row in payload if isinstance(row, dict)]


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _iso_date_only(value: str) -> str:
    parsed = _parse_iso(value)
    if parsed is None:
        return str(value).strip()
    return parsed.date().isoformat()
