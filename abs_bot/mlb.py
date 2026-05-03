from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Set
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ACTIVE_DETAILED_STATES = {
    "In Progress",
    "Manager Challenge",
    "Warmup",
    "Delayed Start",
    "Delayed",
    "Rain Delay",
}
TERMINAL_DETAILED_STATES = {
    "Final",
    "Game Over",
    "Completed Early",
    "Cancelled",
    "Postponed",
}
SKIPPED_DETAILED_STATES = {
    "Cancelled",
    "Postponed",
}
logger = logging.getLogger(__name__)


class MlbStatsApiClient:
    def __init__(self, timeout_seconds: int = 15) -> None:
        self.timeout_seconds = timeout_seconds

    def fetch_json(self, url: str) -> Dict[str, Any]:
        logger.debug("Fetching MLB JSON: %s", url)
        request = Request(
            url,
            headers={
                "User-Agent": "mlb-abs-bot/0.1",
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        logger.debug("Fetched MLB JSON successfully: %s", url)
        return payload

    def schedule_for_dates(self, dates: Iterable[str]) -> List[Dict[str, Any]]:
        date_list = list(dates)
        games: List[Dict[str, Any]] = []
        seen: Set[int] = set()
        for date in date_list:
            query = urlencode({"sportId": 1, "date": date})
            data = self.fetch_json(f"https://statsapi.mlb.com/api/v1/schedule?{query}")
            for day in data.get("dates", []):
                for game in day.get("games", []):
                    game_pk = int(game.get("gamePk", 0))
                    if game_pk and game_pk not in seen:
                        seen.add(game_pk)
                        games.append(game)
        logger.debug("Collected %s unique game(s) for dates=%s", len(games), date_list)
        return games

    def schedule_for_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        query = urlencode({"sportId": 1, "startDate": start_date, "endDate": end_date})
        data = self.fetch_json(f"https://statsapi.mlb.com/api/v1/schedule?{query}")
        games: List[Dict[str, Any]] = []
        seen: Set[int] = set()
        for day in data.get("dates", []):
            for game in day.get("games", []):
                game_pk = int(game.get("gamePk", 0))
                if game_pk and game_pk not in seen:
                    seen.add(game_pk)
                    games.append(game)
        logger.debug(
            "Collected %s unique game(s) for range=%s..%s",
            len(games),
            start_date,
            end_date,
        )
        return games

    def fetch_live_game_feed(self, game_pk: int) -> Dict[str, Any]:
        urls = [
            f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live",
            f"https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/live",
        ]
        last_error: Exception | None = None
        for url in urls:
            try:
                logger.debug("Fetching live feed for game %s via %s", game_pk, url)
                return self.fetch_json(url)
            except Exception as exc:
                logger.warning("Failed to fetch live feed for game %s via %s: %s", game_pk, url, exc)
                last_error = exc
        if last_error is None:
            raise RuntimeError(f"Unable to fetch MLB live feed for game {game_pk}")
        raise last_error

    @staticmethod
    def default_monitor_dates(now: datetime | None = None) -> List[str]:
        return MlbStatsApiClient.monitor_window_dates(now)

    @staticmethod
    def monitor_window_dates(now: datetime | None = None) -> List[str]:
        current = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
        today = current.date()
        return [
            (today - timedelta(days=1)).isoformat(),
            today.isoformat(),
            (today + timedelta(days=1)).isoformat(),
        ]

    @staticmethod
    def lookahead_dates(now: datetime | None = None, days_ahead: int = 7) -> List[str]:
        current = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
        today = current.date()
        return [
            (today + timedelta(days=offset)).isoformat()
            for offset in range(max(0, days_ahead) + 1)
        ]

    @staticmethod
    def should_monitor_game(game: Dict[str, Any]) -> bool:
        return MlbStatsApiClient.should_track_game(game)

    @staticmethod
    def should_track_game(game: Dict[str, Any]) -> bool:
        return MlbStatsApiClient.detailed_state(game) not in SKIPPED_DETAILED_STATES

    @staticmethod
    def detailed_state(game: Dict[str, Any]) -> str:
        return game.get("status", {}).get("detailedState", "").strip()

    @staticmethod
    def matchup_label(game: Dict[str, Any]) -> str:
        teams = game.get("teams", {})
        away = teams.get("away", {}).get("team", {})
        home = teams.get("home", {}).get("team", {})
        away_label = away.get("abbreviation") or away.get("name") or "Away"
        home_label = home.get("abbreviation") or home.get("name") or "Home"
        return f"{away_label} @ {home_label}"

    @staticmethod
    def game_type(game: Dict[str, Any]) -> str:
        return str(game.get("gameType", "")).strip()

    @staticmethod
    def abstract_state(game: Dict[str, Any]) -> str:
        return game.get("status", {}).get("abstractGameState", "").strip()

    @staticmethod
    def game_datetime_utc(game: Dict[str, Any]) -> datetime | None:
        value = game.get("gameDate")
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)

    @staticmethod
    def is_terminal_game(game: Dict[str, Any]) -> bool:
        return (
            MlbStatsApiClient.abstract_state(game) == "Final"
            or MlbStatsApiClient.detailed_state(game) in TERMINAL_DETAILED_STATES
        )

    @staticmethod
    def is_active_game(game: Dict[str, Any]) -> bool:
        detailed_state = MlbStatsApiClient.detailed_state(game)
        return (
            MlbStatsApiClient.abstract_state(game) == "Live"
            or detailed_state in ACTIVE_DETAILED_STATES
        )

    @staticmethod
    def is_upcoming_game(game: Dict[str, Any], now: datetime | None = None) -> bool:
        if MlbStatsApiClient.is_terminal_game(game):
            return False
        if MlbStatsApiClient.is_active_game(game):
            return False
        game_time = MlbStatsApiClient.game_datetime_utc(game)
        if game_time is None:
            return MlbStatsApiClient.abstract_state(game) == "Preview"
        current = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
        return game_time > current

    @staticmethod
    def is_within_activation_window(
        game: Dict[str, Any],
        now: datetime,
        activation_lead: timedelta,
    ) -> bool:
        game_time = MlbStatsApiClient.game_datetime_utc(game)
        if game_time is None:
            return False
        return now >= (game_time - activation_lead)

    def next_scheduled_game(
        self,
        *,
        now: datetime | None = None,
        days_ahead: int = 7,
    ) -> Dict[str, Any] | None:
        current = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
        games = [
            game
            for game in self.schedule_for_dates(self.lookahead_dates(current, days_ahead))
            if self.should_track_game(game) and self.is_upcoming_game(game, current)
        ]
        if not games:
            return None
        return min(
            games,
            key=lambda game: self.game_datetime_utc(game) or datetime.max.replace(tzinfo=timezone.utc),
        )
