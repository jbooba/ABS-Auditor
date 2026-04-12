from __future__ import annotations

import html
import math
import re
from typing import Any, Dict, List, Optional, Tuple

from .models import AbsChallenge, ChallengePitch, GameTeams


ABS_REVIEW_TYPE = "MJ"
BASE_ORDER = ("1B", "2B", "3B")
BALL_RADIUS_INCHES = 1.4375
BALL_RADIUS_FEET = BALL_RADIUS_INCHES / 12.0
BATTER_ZONE_TOP_PCT = 0.535
BATTER_ZONE_BOTTOM_PCT = 0.270
DISPLAY_BALL_RADIUS_INCHES = BALL_RADIUS_INCHES * 0.88
DISPLAY_HORIZONTAL_RATIO_SCALE = 0.9443896052825135
DISPLAY_HORIZONTAL_RATIO_OFFSET = 0.0719654365214366
DISPLAY_VERTICAL_RATIO_SCALE = 1.0238809945341556
DISPLAY_VERTICAL_RATIO_OFFSET = 0.03892297233657865
DISPLAY_ZONE_WIDTH_INCHES = 17.0


def extract_abs_challenges(feed: Dict[str, Any]) -> List[AbsChallenge]:
    teams = _extract_teams(feed)
    player_heights = _extract_player_heights(feed)
    home_plate_umpire_id, home_plate_umpire_name = _extract_home_plate_umpire(feed)
    game_pk = int(feed.get("gamePk") or feed.get("gameData", {}).get("game", {}).get("pk") or 0)
    game_status = (
        feed.get("gameData", {})
        .get("status", {})
        .get("detailedState", "Unknown")
    )
    all_plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])

    challenges: List[AbsChallenge] = []
    current_bases: dict[str, dict[str, Any]] = {}
    current_away_score = 0
    current_home_score = 0
    current_outs = 0
    current_half_key: tuple[Any, Any] | None = None

    for play in all_plays:
        about = play.get("about", {})
        play_half_key = (about.get("inning"), about.get("halfInning"))
        if play_half_key != current_half_key:
            current_half_key = play_half_key
            current_bases = {}
            current_outs = 0

        play_start_bases = dict(current_bases)
        play_start_away_score = current_away_score
        play_start_home_score = current_home_score
        play_start_outs = current_outs

        play_review = play.get("reviewDetails")
        if _is_completed_abs_review(play_review):
            pitch_event, selection_reason = _select_review_pitch(play, play_review)
            challenges.append(
                _build_challenge(
                    game_pk=game_pk,
                    game_status=game_status,
                    teams=teams,
                    play=play,
                    review=play_review,
                    pitch_event=pitch_event,
                    selection_reason=selection_reason,
                    play_start_bases=play_start_bases,
                    play_start_away_score=play_start_away_score,
                    play_start_home_score=play_start_home_score,
                    play_start_outs=play_start_outs,
                    player_heights=player_heights,
                    home_plate_umpire_id=home_plate_umpire_id,
                    home_plate_umpire_name=home_plate_umpire_name,
                )
            )
        for event in play.get("playEvents", []):
            event_review = event.get("reviewDetails")
            if not _is_completed_abs_review(event_review):
                continue
            if play_review and _reviews_match(play_review, event_review):
                continue
            challenges.append(
                _build_challenge(
                    game_pk=game_pk,
                    game_status=game_status,
                    teams=teams,
                    play=play,
                    review=event_review,
                    pitch_event=event,
                    selection_reason="event_review",
                    play_start_bases=play_start_bases,
                    play_start_away_score=play_start_away_score,
                    play_start_home_score=play_start_home_score,
                    play_start_outs=play_start_outs,
                    player_heights=player_heights,
                    home_plate_umpire_id=home_plate_umpire_id,
                    home_plate_umpire_name=home_plate_umpire_name,
                )
            )

        current_bases = _extract_post_bases(play)
        current_away_score = int(play.get("result", {}).get("awayScore", current_away_score))
        current_home_score = int(play.get("result", {}).get("homeScore", current_home_score))
        current_outs = int(play.get("count", {}).get("outs", current_outs))

    return challenges


def format_post_text(challenge: AbsChallenge) -> str:
    speed = (
        f"{challenge.pitch.start_speed:.1f} mph"
        if challenge.pitch.start_speed is not None
        else "speed unavailable"
    )
    location = []
    if challenge.pitch.px is not None:
        location.append(f"px {challenge.pitch.px:+.2f}")
    if challenge.pitch.pz is not None:
        location.append(f"pz {challenge.pitch.pz:.2f}")
    location_text = ", ".join(location) if location else "tracking unavailable"

    lines = [
        f"ABS Challenge {challenge.outcome_label}",
        f"{_matchup_with_tags(challenge.teams)} | {challenge.inning_label}",
        f"Challenge by {challenge.challenger_name} ({challenge.challenge_team_abbrev})",
        f"Batter: {challenge.batter_name} | Pitcher: {challenge.pitcher_name}",
        challenge.home_plate_display,
        f"Situation: {challenge.pitch.count_display}, {challenge.outs_display}, {challenge.runners_display}",
        f"Score: {challenge.score_display}",
        f"Original call: {challenge.original_call}",
        f"ABS result: {challenge.final_call}",
        f"Pitch: {challenge.pitch.pitch_type} | {speed} | {location_text}",
    ]
    if challenge.umpire_challenge_summary:
        lines.insert(5, challenge.umpire_challenge_summary)
    if challenge.final_call == "Ball" and challenge.pitch.miss_display:
        lines.append(f"Miss: {challenge.pitch.miss_display}")
    if challenge.at_bat_result_display:
        lines.append(challenge.at_bat_result_display)
    return "\n".join(lines)


def format_bluesky_post_text(challenge: AbsChallenge) -> str:
    return _format_compact_social_post(challenge, limit=300)


def format_x_post_text(challenge: AbsChallenge) -> str:
    return _format_compact_social_post(challenge, limit=280)


def format_bluesky_clip_post_text(challenge: AbsChallenge, clip_url: str) -> str:
    return _format_link_social_post(challenge, clip_url=clip_url, limit=300)


def format_x_clip_post_text(challenge: AbsChallenge, clip_url: str) -> str:
    return _format_link_social_post(challenge, clip_url=clip_url, limit=280)


def _format_compact_social_post(challenge: AbsChallenge, *, limit: int) -> str:
    matchup_text = _matchup_with_tags(challenge.teams)
    if challenge.challenger_name == challenge.batter_name:
        challenge_line = (
            f"{challenge.challenger_name} challenged on a "
            f"{challenge.pitch.count_display} {challenge.pitch.pitch_type.lower()}."
        )
    else:
        challenge_line = (
            f"{challenge.challenger_name} challenged on a "
            f"{challenge.pitch.count_display} {challenge.pitch.pitch_type.lower()} "
            f"to {challenge.batter_name}."
        )

    lines = [
        f"ABS {challenge.outcome_label.lower()} the call in {matchup_text} ({challenge.inning_label}).",
        challenge_line,
        (
            f"{_call_transition_text(challenge)} "
            f"{challenge.outs_display.capitalize()}, {challenge.runners_display}."
        ),
        f"Score: {challenge.score_display} | {challenge.home_plate_display}",
    ]

    pitch_note_parts = []
    if challenge.pitch.start_speed is not None:
        pitch_note_parts.append(f"{challenge.pitch.start_speed:.1f} mph")
    if challenge.final_call == "Ball" and challenge.pitch.miss_display:
        pitch_note_parts.append(f"Miss: {challenge.pitch.miss_display}")
    if pitch_note_parts:
        lines.append(" | ".join(pitch_note_parts))

    result_line = _bluesky_result_line(challenge)
    if result_line:
        lines.append(result_line)

    return _fit_bluesky_text(lines, limit=limit)


def _format_link_social_post(challenge: AbsChallenge, *, clip_url: str, limit: int) -> str:
    reserved = min(40, len(clip_url) + 1)
    body = _format_compact_social_post(challenge, limit=max(120, limit - reserved))
    text = f"{body}\n{clip_url}"
    if len(text) <= limit:
        return text
    return f"{_truncate_line(body, max(32, limit - reserved))}\n{clip_url}"


def format_alt_text(challenge: AbsChallenge) -> str:
    miss_sentence = (
        f"The pitch missed the zone by {challenge.pitch.miss_display}. "
        if challenge.final_call == "Ball" and challenge.pitch.miss_display
        else ""
    )
    at_bat_sentence = f"{challenge.at_bat_result_display} " if challenge.at_bat_result_display else ""
    return (
        f"{challenge.teams.matchup_label} {challenge.inning_label}. "
        f"ABS challenge by {challenge.challenger_name} of {challenge.challenge_team_name}. "
        f"{challenge.home_plate_display}. "
        f"Situation was {challenge.pitch.count_display}, {challenge.outs_display}, runners {challenge.runners_display}. "
        f"Score was {challenge.score_display}. "
        f"Original call {challenge.original_call}. "
        f"Result {challenge.final_call}. "
        f"{miss_sentence}"
        f"{challenge.umpire_challenge_summary + '. ' if challenge.umpire_challenge_summary else ''}"
        f"{at_bat_sentence}"
        f"Pitch tracked at px {challenge.pitch.px}, pz {challenge.pitch.pz}."
    )


def _call_transition_text(challenge: AbsChallenge) -> str:
    if challenge.changed_call:
        return f"{challenge.original_call} -> {challenge.final_call}."
    if challenge.final_call == "Ball":
        return "Ball stays ball."
    if challenge.final_call == "Called Strike":
        return "Called strike stands."
    return f"{challenge.final_call} stands."


def _bluesky_result_line(challenge: AbsChallenge) -> str:
    text = challenge.at_bat_result_display
    if not text:
        return ""
    if "challenged" in text.lower() and ":" in text:
        text = text.split(":", 1)[1].strip()
    if text.startswith("Later in the at-bat: "):
        return f"Later: {text.removeprefix('Later in the at-bat: ')}"
    return f"Result: {text}"


def _fit_bluesky_text(lines: list[str], limit: int = 300) -> str:
    active_lines = [line for line in lines if line]
    text = "\n".join(active_lines)
    if len(text) <= limit:
        return text

    if active_lines and active_lines[-1].startswith("Result: "):
        active_lines[-1] = _truncate_line(active_lines[-1], max(32, limit - _joined_length(active_lines[:-1]) - 1))
        text = "\n".join(active_lines)
        if len(text) <= limit:
            return text

    if active_lines and active_lines[-1].startswith("Later: "):
        active_lines[-1] = _truncate_line(active_lines[-1], max(32, limit - _joined_length(active_lines[:-1]) - 1))
        text = "\n".join(active_lines)
        if len(text) <= limit:
            return text

    if len(active_lines) >= 4 and " | " in active_lines[3]:
        score_text = f"Score: {active_lines[3].split('|', 1)[0].removeprefix('Score: ').strip()}"
        active_lines[3] = score_text
        text = "\n".join(active_lines)
        if len(text) <= limit:
            return text

    if len(active_lines) >= 2:
        active_lines[1] = _truncate_line(active_lines[1], 72)
        text = "\n".join(active_lines)
        if len(text) <= limit:
            return text

    return _truncate_line(text, limit)


def _joined_length(lines: list[str]) -> int:
    if not lines:
        return 0
    return len("\n".join(lines))


def _truncate_line(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    trimmed = text[: limit - 3].rstrip()
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}..."


def _matchup_with_tags(teams: GameTeams) -> str:
    return f"{_team_hashtag(teams.away_name)} at {_team_hashtag(teams.home_name)}"


def _team_hashtag(team_name: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", team_name)
    if not words:
        return team_name

    trailing_pair = tuple(word.lower() for word in words[-2:])
    pair_map = {
        ("red", "sox"): "RedSox",
        ("white", "sox"): "WhiteSox",
        ("blue", "jays"): "BlueJays",
    }
    if trailing_pair in pair_map:
        return f"#{pair_map[trailing_pair]}"

    return f"#{words[-1].capitalize()}"


def safe_text(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _extract_teams(feed: Dict[str, Any]) -> GameTeams:
    game_teams = feed.get("gameData", {}).get("teams", {})
    home = game_teams.get("home", {})
    away = game_teams.get("away", {})
    return GameTeams(
        home_id=int(home.get("id", 0)),
        home_name=home.get("name", "Home"),
        home_abbrev=home.get("abbreviation", "HOME"),
        away_id=int(away.get("id", 0)),
        away_name=away.get("name", "Away"),
        away_abbrev=away.get("abbreviation", "AWAY"),
    )


def _extract_home_plate_umpire(feed: Dict[str, Any]) -> Tuple[Optional[int], str]:
    officials = feed.get("liveData", {}).get("boxscore", {}).get("officials", [])
    for official in officials:
        if official.get("officialType") != "Home Plate":
            continue
        official_info = official.get("official", {})
        return official_info.get("id"), official_info.get("fullName", "Unknown")
    return None, ""


def _extract_player_heights(feed: Dict[str, Any]) -> Dict[int, float]:
    players = feed.get("gameData", {}).get("players", {})
    heights: Dict[int, float] = {}
    if not isinstance(players, dict):
        return heights

    for player in players.values():
        if not isinstance(player, dict):
            continue
        player_id = player.get("id")
        height_inches = _parse_height_to_inches(player.get("height"))
        if player_id is None or height_inches is None:
            continue
        heights[int(player_id)] = height_inches
    return heights


def _is_completed_abs_review(review: Optional[Dict[str, Any]]) -> bool:
    if not review:
        return False
    return review.get("reviewType") == ABS_REVIEW_TYPE and review.get("inProgress") is False


def _select_review_pitch(
    play: Dict[str, Any],
    review: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], str]:
    events = play.get("playEvents", [])
    matching_review_events = [
        event
        for event in events
        if _reviews_match(review, event.get("reviewDetails"))
    ]
    if matching_review_events:
        return matching_review_events[-1], "matching_event_review"

    flagged_events = []
    for event in events:
        if not event.get("isPitch") or not event.get("details", {}).get("hasReview"):
            continue
        event_review = event.get("reviewDetails")
        if event_review and not _reviews_match(review, event_review):
            continue
        flagged_events.append(event)
    if flagged_events:
        return flagged_events[-1], "details_has_review"

    pitch_events = [event for event in events if event.get("isPitch")]
    if pitch_events:
        return pitch_events[-1], "fallback_last_pitch"

    return None, "no_pitch_event"


def _reviews_match(left: Optional[Dict[str, Any]], right: Optional[Dict[str, Any]]) -> bool:
    if not left or not right:
        return False
    left_player = left.get("player", {}).get("id")
    right_player = right.get("player", {}).get("id")
    return (
        left.get("reviewType") == right.get("reviewType")
        and left.get("challengeTeamId") == right.get("challengeTeamId")
        and left.get("isOverturned") == right.get("isOverturned")
        and left_player == right_player
    )


def _build_challenge(
    *,
    game_pk: int,
    game_status: str,
    teams: GameTeams,
    play: Dict[str, Any],
    review: Dict[str, Any],
    pitch_event: Optional[Dict[str, Any]],
    selection_reason: str,
    play_start_bases: dict[str, dict[str, Any]],
    play_start_away_score: int,
    play_start_home_score: int,
    play_start_outs: int,
    player_heights: Dict[int, float],
    home_plate_umpire_id: Optional[int],
    home_plate_umpire_name: str,
) -> AbsChallenge:
    about = play.get("about", {})
    matchup = play.get("matchup", {})
    batter_id = matchup.get("batter", {}).get("id")
    challenge_team_id = review.get("challengeTeamId")
    batting_team_id = teams.away_id if about.get("isTopInning") else teams.home_id
    batter_height_inches = _lookup_batter_height_inches(player_heights, batter_id)
    normalized_zone_top, normalized_zone_bottom = _height_based_zone_bounds(batter_height_inches)
    original_call, final_call = _resolve_calls(
        review,
        pitch_event,
        challenge_team_id=challenge_team_id,
        batting_team_id=batting_team_id,
    )

    pitch_details = pitch_event.get("details", {}) if pitch_event else {}
    pitch_data = pitch_event.get("pitchData", {}) if pitch_event else {}
    coordinates = pitch_data.get("coordinates", {}) if pitch_event else {}
    pitch_type = pitch_details.get("type", {}).get("description", "Unknown")
    balls_before, strikes_before = _pre_pitch_count(play, pitch_event)
    miss_distance_inches, miss_description = _pitch_miss_details(
        pitch_event,
        zone_top=normalized_zone_top or pitch_data.get("strikeZoneTop"),
        zone_bottom=normalized_zone_bottom or pitch_data.get("strikeZoneBottom"),
    )
    outs_before, away_score_before, home_score_before, runners_on_base = _situation_before_pitch(
        play,
        pitch_event,
        play_start_bases,
        play_start_outs,
        play_start_away_score,
        play_start_home_score,
    )

    pitch = ChallengePitch(
        event_index=pitch_event.get("index") if pitch_event else None,
        play_id=pitch_event.get("playId") if pitch_event else None,
        pitch_number=pitch_event.get("pitchNumber") if pitch_event else None,
        selection_reason=selection_reason,
        balls_before=balls_before,
        strikes_before=strikes_before,
        call_code=_extract_call_code(pitch_event),
        call_description=_plot_call_label(final_call),
        pitch_type=pitch_type,
        start_speed=pitch_data.get("startSpeed"),
        px=coordinates.get("pX"),
        pz=coordinates.get("pZ"),
        display_x=coordinates.get("x"),
        display_y=coordinates.get("y"),
        strike_zone_top=pitch_data.get("strikeZoneTop"),
        strike_zone_bottom=pitch_data.get("strikeZoneBottom"),
        normalized_zone_top=normalized_zone_top,
        normalized_zone_bottom=normalized_zone_bottom,
        zone_number=pitch_data.get("zone"),
        miss_distance_inches=miss_distance_inches,
        miss_description=miss_description,
    )

    inning = int(about.get("inning", 0))
    is_top_inning = bool(about.get("isTopInning"))
    inning_label = f"{'Top' if is_top_inning else 'Bot'} {inning}"
    challenger_name = (
        review.get("player", {}).get("fullName")
        or teams.team_name(challenge_team_id)
    )
    challenge_event_id = pitch.play_id or str(about.get("atBatIndex"))
    challenge_id = f"{game_pk}:{about.get('atBatIndex')}:{challenge_event_id}:{int(bool(review.get('isOverturned')))}"
    pitch_ended_at_bat = _pitch_ended_at_bat(play, pitch_event)
    result_description = _clean_result_description(play.get("result", {}).get("description", ""))
    if not pitch_ended_at_bat:
        result_description = _strip_challenge_intro(result_description)

    return AbsChallenge(
        challenge_id=challenge_id,
        game_pk=game_pk,
        game_status=game_status,
        teams=teams,
        at_bat_index=int(about.get("atBatIndex", 0)),
        inning=inning,
        is_top_inning=is_top_inning,
        inning_label=inning_label,
        half_inning=about.get("halfInning", ""),
        batter_name=matchup.get("batter", {}).get("fullName", "Unknown Batter"),
        pitcher_name=matchup.get("pitcher", {}).get("fullName", "Unknown Pitcher"),
        challenger_name=challenger_name,
        challenge_team_id=challenge_team_id,
        challenge_team_name=teams.team_name(challenge_team_id),
        challenge_team_abbrev=teams.team_abbrev(challenge_team_id),
        away_score_before=away_score_before,
        home_score_before=home_score_before,
        outs_before=outs_before,
        runners_on_base=tuple(runners_on_base),
        review_type=review.get("reviewType", ""),
        is_overturned=bool(review.get("isOverturned")),
        in_progress=bool(review.get("inProgress")),
        result_description=result_description,
        pitch_ended_at_bat=pitch_ended_at_bat,
        original_call=original_call,
        final_call=final_call,
        pitch=pitch,
        play_end_time=play.get("playEndTime") or (pitch_event.get("endTime") if pitch_event else None),
        batter_height_inches=batter_height_inches,
        home_plate_umpire_id=home_plate_umpire_id,
        home_plate_umpire_name=home_plate_umpire_name,
    )


def _extract_call_code(pitch_event: Optional[Dict[str, Any]]) -> str:
    if not pitch_event:
        return ""
    details = pitch_event.get("details", {})
    return details.get("call", {}).get("code") or details.get("code", "")


def _pre_pitch_count(
    play: Dict[str, Any],
    pitch_event: Optional[Dict[str, Any]],
) -> Tuple[Optional[int], Optional[int]]:
    if not pitch_event:
        return None, None

    balls = 0
    strikes = 0
    for event in play.get("playEvents", []):
        if event is pitch_event:
            return balls, strikes
        if event.get("isPitch"):
            count = event.get("count", {})
            balls = count.get("balls", balls)
            strikes = count.get("strikes", strikes)

    return balls, strikes


def _situation_before_pitch(
    play: Dict[str, Any],
    pitch_event: Optional[Dict[str, Any]],
    play_start_bases: dict[str, dict[str, Any]],
    play_start_outs: int,
    play_start_away_score: int,
    play_start_home_score: int,
) -> Tuple[int, int, int, list[str]]:
    target_index = pitch_event.get("index") if pitch_event else None
    if target_index is None:
        return (
            play_start_outs,
            play_start_away_score,
            play_start_home_score,
            [base for base in BASE_ORDER if base in play_start_bases],
        )

    bases = {base: dict(info) for base, info in play_start_bases.items()}
    outs = play_start_outs
    away_score = play_start_away_score
    home_score = play_start_home_score
    batting_team_is_away = bool(play.get("about", {}).get("isTopInning"))

    for runner in play.get("runners", []):
        details = runner.get("details", {})
        play_index = details.get("playIndex")
        if play_index is None or play_index >= target_index:
            continue

        runner_info = details.get("runner", {})
        runner_id = runner_info.get("id")
        movement = runner.get("movement", {})
        _remove_runner_from_bases(bases, runner_id)

        end_base = movement.get("end")
        if end_base in BASE_ORDER:
            bases[end_base] = {
                "id": runner_id,
                "name": runner_info.get("fullName", "Runner"),
            }

        if movement.get("isOut"):
            outs += 1

        if details.get("isScoringEvent"):
            if batting_team_is_away:
                away_score += 1
            else:
                home_score += 1

    return outs, away_score, home_score, [base for base in BASE_ORDER if base in bases]


def _remove_runner_from_bases(bases: dict[str, dict[str, Any]], runner_id: Any) -> None:
    if runner_id is None:
        return
    for base, info in list(bases.items()):
        if info.get("id") == runner_id:
            bases.pop(base, None)


def _extract_post_bases(play: Dict[str, Any]) -> dict[str, dict[str, Any]]:
    matchup = play.get("matchup", {})
    post_base_fields = {
        "1B": matchup.get("postOnFirst"),
        "2B": matchup.get("postOnSecond"),
        "3B": matchup.get("postOnThird"),
    }
    bases: dict[str, dict[str, Any]] = {}
    for base, runner in post_base_fields.items():
        if not runner:
            continue
        bases[base] = {
            "id": runner.get("id"),
            "name": runner.get("fullName", "Runner"),
        }
    return bases


def _resolve_calls(
    review: Dict[str, Any],
    pitch_event: Optional[Dict[str, Any]],
    *,
    challenge_team_id: Optional[int],
    batting_team_id: Optional[int],
) -> Tuple[str, str]:
    inferred = _resolve_calls_from_challenger(
        review,
        challenge_team_id=challenge_team_id,
        batting_team_id=batting_team_id,
    )
    if inferred is not None:
        return inferred

    original_call = _normalize_review_call(pitch_event)
    if not review.get("isOverturned"):
        return original_call, original_call
    if original_call == "Ball":
        return "Ball", "Called Strike"
    if original_call == "Called Strike":
        return "Called Strike", "Ball"
    return original_call, "Unknown"


def _resolve_calls_from_challenger(
    review: Dict[str, Any],
    *,
    challenge_team_id: Optional[int],
    batting_team_id: Optional[int],
) -> Optional[Tuple[str, str]]:
    if challenge_team_id is None or batting_team_id is None:
        return None

    challenger_is_offense = challenge_team_id == batting_team_id
    original_call = "Called Strike" if challenger_is_offense else "Ball"
    if not review.get("isOverturned"):
        return original_call, original_call
    return (
        original_call,
        "Ball" if challenger_is_offense else "Called Strike",
    )


def _normalize_review_call(pitch_event: Optional[Dict[str, Any]]) -> str:
    if not pitch_event:
        return "Unknown"

    details = pitch_event.get("details", {})
    call = details.get("call", {})
    description = (call.get("description") or details.get("description") or "").strip()
    description_lower = description.lower()
    code = (call.get("code") or details.get("code") or "").upper()

    if code == "B" or description_lower.startswith("ball") or details.get("isBall"):
        return "Ball"
    if code == "C" or "called strike" in description_lower:
        return "Called Strike"
    if details.get("isStrike") and "swing" not in description_lower and "foul" not in description_lower:
        return "Called Strike"
    return description or "Unknown"


def _plot_call_label(call: str) -> str:
    if call == "Called Strike":
        return "Strike"
    return call


def _pitch_miss_details(
    pitch_event: Optional[Dict[str, Any]],
    *,
    zone_top: Optional[float],
    zone_bottom: Optional[float],
) -> Tuple[Optional[float], Optional[str]]:
    if not pitch_event:
        return None, None

    pitch_data = pitch_event.get("pitchData", {})
    coordinates = pitch_data.get("coordinates", {})
    px = coordinates.get("pX")
    pz = coordinates.get("pZ")
    if None in {px, pz, zone_top, zone_bottom}:
        return None, None

    zone_half_width = 17.0 / 24.0
    left_edge = -zone_half_width - BALL_RADIUS_FEET
    right_edge = zone_half_width + BALL_RADIUS_FEET

    horizontal_miss = 0.0
    if px < left_edge:
        horizontal_miss = left_edge - px
    elif px > right_edge:
        horizontal_miss = px - right_edge

    vertical_miss = 0.0
    vertical_description: Optional[str] = None
    if pz < zone_bottom - BALL_RADIUS_FEET:
        vertical_miss = (zone_bottom - BALL_RADIUS_FEET) - pz
        vertical_description = "low"
    elif pz > zone_top + BALL_RADIUS_FEET:
        vertical_miss = pz - (zone_top + BALL_RADIUS_FEET)
        vertical_description = "high"

    if horizontal_miss == 0 and vertical_miss == 0:
        return _estimated_display_miss_details(
            px=px,
            pz=pz,
            zone_top=zone_top,
            zone_bottom=zone_bottom,
        )

    distance_inches = math.hypot(horizontal_miss, vertical_miss) * 12.0
    if horizontal_miss > 0 and vertical_miss > 0:
        description = "off corner"
    elif vertical_description:
        description = vertical_description
    else:
        description = "off edge"
    return distance_inches, description


def _estimated_display_miss_details(
    *,
    px: float,
    pz: float,
    zone_top: float,
    zone_bottom: float,
) -> Tuple[Optional[float], Optional[str]]:
    if zone_top <= zone_bottom:
        return None, None

    zone_half_width = 17.0 / 24.0
    raw_horizontal_ratio = (px + zone_half_width) / (zone_half_width * 2.0)
    raw_vertical_ratio = (zone_top - pz) / (zone_top - zone_bottom)

    display_horizontal_ratio = (raw_horizontal_ratio * DISPLAY_HORIZONTAL_RATIO_SCALE) + DISPLAY_HORIZONTAL_RATIO_OFFSET
    display_vertical_ratio = (raw_vertical_ratio * DISPLAY_VERTICAL_RATIO_SCALE) + DISPLAY_VERTICAL_RATIO_OFFSET
    ball_radius_ratio = DISPLAY_BALL_RADIUS_INCHES / DISPLAY_ZONE_WIDTH_INCHES

    horizontal_miss_inches = 0.0
    if display_horizontal_ratio < -ball_radius_ratio:
        horizontal_miss_inches = (-ball_radius_ratio - display_horizontal_ratio) * DISPLAY_ZONE_WIDTH_INCHES
    elif display_horizontal_ratio > 1.0 + ball_radius_ratio:
        horizontal_miss_inches = (display_horizontal_ratio - (1.0 + ball_radius_ratio)) * DISPLAY_ZONE_WIDTH_INCHES

    vertical_miss_inches = 0.0
    vertical_description: Optional[str] = None
    if display_vertical_ratio < -ball_radius_ratio:
        vertical_miss_inches = (-ball_radius_ratio - display_vertical_ratio) * DISPLAY_ZONE_WIDTH_INCHES
        vertical_description = "high"
    elif display_vertical_ratio > 1.0 + ball_radius_ratio:
        vertical_miss_inches = (display_vertical_ratio - (1.0 + ball_radius_ratio)) * DISPLAY_ZONE_WIDTH_INCHES
        vertical_description = "low"

    if horizontal_miss_inches == 0 and vertical_miss_inches == 0:
        return None, None

    distance_inches = math.hypot(horizontal_miss_inches, vertical_miss_inches)
    if horizontal_miss_inches > 0 and vertical_miss_inches > 0:
        description = "off corner"
    elif vertical_description:
        description = vertical_description
    else:
        description = "off edge"
    return distance_inches, description


def _pitch_ended_at_bat(
    play: Dict[str, Any],
    pitch_event: Optional[Dict[str, Any]],
) -> bool:
    if not pitch_event:
        return False
    pitch_events = [event for event in play.get("playEvents", []) if event.get("isPitch")]
    if not pitch_events:
        return False
    return pitch_event is pitch_events[-1]


def _clean_result_description(text: str) -> str:
    cleaned = re.sub(r"\s*\([^)]*\)", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"\s+([,.:;!?])", r"\1", cleaned)
    return cleaned


def _strip_challenge_intro(text: str) -> str:
    lowered = text.lower()
    if "challenged" in lowered and ":" in text:
        return text.split(":", 1)[1].strip()
    return text


def _lookup_batter_height_inches(player_heights: Dict[int, float], batter_id: Any) -> Optional[float]:
    if batter_id is None:
        return None
    try:
        return player_heights.get(int(batter_id))
    except (TypeError, ValueError):
        return None


def _height_based_zone_bounds(height_inches: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if height_inches is None or height_inches <= 0:
        return None, None
    return (
        (height_inches * BATTER_ZONE_TOP_PCT) / 12.0,
        (height_inches * BATTER_ZONE_BOTTOM_PCT) / 12.0,
    )


def _parse_height_to_inches(height_text: Any) -> Optional[float]:
    if not height_text:
        return None
    match = re.search(r"(\d+)\s*'\s*(\d+)", str(height_text))
    if not match:
        return None
    feet = int(match.group(1))
    inches = int(match.group(2))
    return float((feet * 12) + inches)
