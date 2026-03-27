from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class GameTeams:
    home_id: int
    home_name: str
    home_abbrev: str
    away_id: int
    away_name: str
    away_abbrev: str

    @property
    def matchup_label(self) -> str:
        return f"{self.away_abbrev} @ {self.home_abbrev}"

    def team_name(self, team_id: Optional[int]) -> str:
        if team_id == self.home_id:
            return self.home_name
        if team_id == self.away_id:
            return self.away_name
        return "Unknown Team"

    def team_abbrev(self, team_id: Optional[int]) -> str:
        if team_id == self.home_id:
            return self.home_abbrev
        if team_id == self.away_id:
            return self.away_abbrev
        return "UNK"


@dataclass(frozen=True)
class ChallengePitch:
    event_index: Optional[int]
    play_id: Optional[str]
    pitch_number: Optional[int]
    selection_reason: str
    balls_before: Optional[int]
    strikes_before: Optional[int]
    call_code: str
    call_description: str
    pitch_type: str
    start_speed: Optional[float]
    px: Optional[float]
    pz: Optional[float]
    strike_zone_top: Optional[float]
    strike_zone_bottom: Optional[float]
    normalized_zone_top: Optional[float]
    normalized_zone_bottom: Optional[float]
    zone_number: Optional[int]
    miss_distance_inches: Optional[float]
    miss_description: Optional[str]

    @property
    def count_display(self) -> str:
        if self.balls_before is None or self.strikes_before is None:
            return "Unknown"
        return f"{self.balls_before}-{self.strikes_before}"

    @property
    def miss_display(self) -> Optional[str]:
        if self.miss_distance_inches is None:
            return None
        distance = self.miss_distance_inches
        if 0 < distance < 0.1:
            distance = 0.1
        if self.miss_description:
            return f"~{distance:.1f} in {self.miss_description}"
        return f"~{distance:.1f} in"

    @property
    def display_zone_top(self) -> Optional[float]:
        return self.normalized_zone_top or self.strike_zone_top

    @property
    def display_zone_bottom(self) -> Optional[float]:
        return self.normalized_zone_bottom or self.strike_zone_bottom


@dataclass(frozen=True)
class AbsChallenge:
    challenge_id: str
    game_pk: int
    game_status: str
    teams: GameTeams
    at_bat_index: int
    inning: int
    is_top_inning: bool
    inning_label: str
    half_inning: str
    batter_name: str
    pitcher_name: str
    challenger_name: str
    challenge_team_id: Optional[int]
    challenge_team_name: str
    challenge_team_abbrev: str
    away_score_before: int
    home_score_before: int
    outs_before: int
    runners_on_base: tuple[str, ...]
    review_type: str
    is_overturned: bool
    in_progress: bool
    result_description: str
    pitch_ended_at_bat: bool
    original_call: str
    final_call: str
    pitch: ChallengePitch
    play_end_time: Optional[str]
    batter_height_inches: Optional[float] = None
    home_plate_umpire_id: Optional[int] = None
    home_plate_umpire_name: str = ""
    umpire_challenge_total: Optional[int] = None
    umpire_confirmed_total: Optional[int] = None
    umpire_overturned_total: Optional[int] = None

    @property
    def outcome_label(self) -> str:
        return "Overturned" if self.is_overturned else "Confirmed"

    @property
    def changed_call(self) -> bool:
        return self.original_call != self.final_call

    @property
    def score_display(self) -> str:
        return (
            f"{self.teams.away_abbrev} {self.away_score_before} - "
            f"{self.teams.home_abbrev} {self.home_score_before}"
        )

    @property
    def outs_display(self) -> str:
        return f"{self.outs_before} out" if self.outs_before == 1 else f"{self.outs_before} outs"

    @property
    def runners_display(self) -> str:
        if not self.runners_on_base:
            return "Empty"

        label_map = {
            "1B": "1st",
            "2B": "2nd",
            "3B": "3rd",
        }
        labels = [label_map.get(base, base) for base in self.runners_on_base]
        if len(labels) == 3:
            return "Loaded"
        if len(labels) == 2:
            return f"{labels[0]} & {labels[1]}"
        return labels[0]

    @property
    def at_bat_result_display(self) -> str:
        if not self.result_description:
            return ""
        lowered = self.result_description.lower()
        if (
            self.pitch_ended_at_bat
            or "challenged" in lowered
            or "call on the field" in lowered
            or "was overturned" in lowered
            or "was confirmed" in lowered
        ):
            return self.result_description
        return f"Later in the at-bat: {self.result_description}"

    @property
    def home_plate_display(self) -> str:
        if not self.home_plate_umpire_name:
            return "HP: Unknown"
        return f"HP: {self.home_plate_umpire_name}"

    @property
    def umpire_upheld_rate(self) -> Optional[float]:
        if not self.umpire_challenge_total:
            return None
        if self.umpire_confirmed_total is None:
            return None
        return self.umpire_confirmed_total / self.umpire_challenge_total

    @property
    def umpire_challenge_summary(self) -> Optional[str]:
        rate = self.umpire_upheld_rate
        if rate is None or self.umpire_confirmed_total is None or self.umpire_challenge_total is None:
            return None
        return (
            f"Challenges upheld: {rate * 100:.1f}% "
            f"({self.umpire_confirmed_total}/{self.umpire_challenge_total})"
        )
