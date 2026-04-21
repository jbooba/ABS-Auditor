from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from math import ceil, sqrt
from pathlib import Path
from typing import Any, Iterable

from .render import CARD_BG, CARD_MUTED, CARD_OUTLINE, _load_font

try:  # pragma: no cover - optional dependency
    from PIL import Image, ImageDraw

    PIL_AVAILABLE = True
except ImportError:  # pragma: no cover - fallback path
    PIL_AVAILABLE = False


# ABS power score tuning constants.
POWER_PRIOR_WEIGHT = 12.0
POWER_WILSON_Z = 1.96
POWER_SAMPLE_CAP = 30
POWER_RECENT_DAYS = 14
POWER_RECENT_SAMPLE_CAP = 8.0
POWER_MOMENTUM_MAX_ABS = 0.08
POWER_DIFFICULTY_SCALE = 0.06
POWER_DIFFICULTY_MAX_ABS = 0.03
POWER_WEIGHT_BAYES = 0.55
POWER_WEIGHT_WILSON = 0.35
POWER_WEIGHT_RAW = 0.10
POWER_WEIGHT_SAMPLE = 0.08

LEADERBOARD_WIDTH = 2000
LEADERBOARD_MIN_HEIGHT = 1450
OUTER_MARGIN = 42
HEADER_BAND_HEIGHT = 150
SUMMARY_TOP = 218
SUMMARY_CARD_HEIGHT = 164
SUMMARY_CARD_GAP = 18
TABLE_TOP = 428
TABLE_HEADER_HEIGHT = 50
ROW_HEIGHT = 38
TABLE_BOTTOM_PADDING = 72
TABLE_LEFT = OUTER_MARGIN + 10
TABLE_RIGHT = LEADERBOARD_WIDTH - OUTER_MARGIN - 10
COLUMN_GAP = 34
TABLE_BG = "#f5f1e8"
TABLE_ALT = "#fff8eb"
TABLE_TOP_FIVE = "#f6efe0"
TABLE_MOST_CHALLENGED = "#fff1dd"
SUMMARY_CARD_BG = "#f8f1e3"
SUMMARY_CARD_ALT = "#fdf7eb"
SUMMARY_CARD_HOT = "#fce8d5"


@dataclass(frozen=True)
class UmpireStanding:
    key: str
    name: str
    total: int
    confirmed: int
    overturned: int
    upheld_rate: float
    bayes_rate: float
    wilson: float
    sample_factor: float
    recent_confirmed: int
    recent_overturned: int
    momentum: float
    difficulty_avg: float | None
    difficulty_bonus: float
    abs_power_score: float

    @property
    def upheld_percent(self) -> float:
        return self.upheld_rate * 100.0

    @property
    def bayes_percent(self) -> float:
        return self.bayes_rate * 100.0

    @property
    def wilson_percent(self) -> float:
        return self.wilson * 100.0

    @property
    def summary(self) -> str:
        return f"{self.upheld_percent:.1f}% ({self.confirmed}/{self.total})"

    @property
    def record_display(self) -> str:
        return f"{self.confirmed}-{self.overturned}"

    @property
    def recent_total(self) -> int:
        return self.recent_confirmed + self.recent_overturned

    @property
    def recent_record_display(self) -> str:
        return f"{self.recent_confirmed}-{self.recent_overturned}"

    @property
    def momentum_display(self) -> str:
        points = self.momentum * 100.0
        sign = "+" if points > 0 else ""
        return f"{sign}{points:.1f}"


@dataclass(frozen=True)
class UmpireLeaderboard:
    leaderboard_id: str
    kind: str
    title: str
    subtitle: str
    standings: tuple[UmpireStanding, ...]
    league_average: float


def wilson_lower_bound(wins: int, total: int, z: float = POWER_WILSON_Z) -> float:
    """Return a conservative lower-bound estimate for a binomial success rate."""
    if total <= 0:
        return 0.0
    proportion = wins / total
    z2 = z * z
    denominator = 1.0 + (z2 / total)
    center = proportion + (z2 / (2.0 * total))
    margin = z * sqrt(
        (proportion * (1.0 - proportion) / total)
        + (z2 / (4.0 * total * total))
    )
    return max(0.0, (center - margin) / denominator)


def bayesian_rate(
    wins: int,
    total: int,
    league_avg: float,
    prior_weight: float = POWER_PRIOR_WEIGHT,
) -> float:
    """Shrink each umpire toward the league average to calm tiny samples."""
    if total <= 0:
        return league_avg
    return (wins + (prior_weight * league_avg)) / (total + prior_weight)


def sample_factor(total: int, sample_cap: int = POWER_SAMPLE_CAP) -> float:
    """Return a saturating sample bonus from 0 to 1."""
    if total <= 0:
        return 0.0
    return min(1.0, sqrt(total / sample_cap))


def momentum_adjustment(
    recent_wins: int | None,
    recent_total: int | None,
    league_avg: float,
    max_abs: float = POWER_MOMENTUM_MAX_ABS,
) -> float:
    """Apply a small recent-form nudge when enough 14-day data exists."""
    if recent_wins is None or recent_total is None or recent_total < 3:
        return 0.0
    recent_rate = recent_wins / recent_total
    strength = min(1.0, recent_total / POWER_RECENT_SAMPLE_CAP)
    value = (recent_rate - league_avg) * strength
    return max(-max_abs, min(max_abs, value))


def difficulty_adjustment(
    difficulty_avg: float | None,
    max_abs: float = POWER_DIFFICULTY_MAX_ABS,
) -> float:
    """Apply an optional bonus/penalty for handling tougher upheld calls."""
    if difficulty_avg is None:
        return 0.0
    value = (difficulty_avg - 0.5) * POWER_DIFFICULTY_SCALE
    return max(-max_abs, min(max_abs, value))


def abs_power_score(
    wins: int,
    losses: int,
    league_avg: float,
    *,
    recent_wins: int | None = None,
    recent_losses: int | None = None,
    difficulty_avg: float | None = None,
    prior_weight: float = POWER_PRIOR_WEIGHT,
    sample_cap: int = POWER_SAMPLE_CAP,
) -> dict[str, float | int | None]:
    """Compute the weekly ABS ranking components for one umpire."""
    total = wins + losses
    raw_rate = (wins / total) if total > 0 else 0.0
    bayes = bayesian_rate(wins, total, league_avg, prior_weight)
    wilson = wilson_lower_bound(wins, total)
    sample = sample_factor(total, sample_cap)

    recent_total = None
    if recent_wins is not None and recent_losses is not None:
        recent_total = recent_wins + recent_losses

    momentum = momentum_adjustment(recent_wins, recent_total, league_avg)
    difficulty = difficulty_adjustment(difficulty_avg)

    core = (
        (POWER_WEIGHT_BAYES * bayes)
        + (POWER_WEIGHT_WILSON * wilson)
        + (POWER_WEIGHT_RAW * raw_rate)
    )
    adjusted = core + (POWER_WEIGHT_SAMPLE * sample) + momentum + difficulty
    score = max(0.0, min(100.0, adjusted * 100.0))

    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "raw_rate": raw_rate,
        "bayes_rate": bayes,
        "wilson": wilson,
        "sample_factor": sample,
        "momentum": momentum,
        "difficulty_bonus": difficulty,
        "difficulty_avg": difficulty_avg,
        "recent_wins": recent_wins,
        "recent_losses": recent_losses,
        "abs_power_score": score,
    }


def build_umpire_standings(
    umpire_stats: dict[str, dict],
    *,
    challenge_history: Iterable[dict[str, Any]] = (),
    as_of: date | None = None,
    include_momentum: bool = True,
) -> tuple[UmpireStanding, ...]:
    """Build season-to-date standings sorted by ABS power score."""
    as_of = as_of or date.today()
    league_average = _league_average(umpire_stats)
    recent_stats = _recent_stats_by_umpire(challenge_history, as_of=as_of)
    difficulty_averages = _difficulty_average_by_umpire(challenge_history)

    standings: list[UmpireStanding] = []
    for key, value in umpire_stats.items():
        if not isinstance(value, dict):
            continue
        total = int(value.get("total", 0) or 0)
        confirmed = int(value.get("confirmed", 0) or 0)
        overturned = int(value.get("overturned", 0) or 0)
        if total <= 0:
            continue

        name = str(value.get("name", "")).strip() or str(key)
        recent = recent_stats.get(str(key), {"confirmed": 0, "overturned": 0})
        recent_confirmed = int(recent.get("confirmed", 0) or 0)
        recent_overturned = int(recent.get("overturned", 0) or 0)
        difficulty_avg = _coerce_optional_float(value.get("difficulty_avg"))
        if difficulty_avg is None:
            difficulty_avg = difficulty_averages.get(str(key))

        components = abs_power_score(
            confirmed,
            overturned,
            league_average,
            recent_wins=recent_confirmed if include_momentum else None,
            recent_losses=recent_overturned if include_momentum else None,
            difficulty_avg=difficulty_avg,
        )

        standings.append(
            UmpireStanding(
                key=str(key),
                name=name,
                total=total,
                confirmed=confirmed,
                overturned=overturned,
                upheld_rate=float(components["raw_rate"]),
                bayes_rate=float(components["bayes_rate"]),
                wilson=float(components["wilson"]),
                sample_factor=float(components["sample_factor"]),
                recent_confirmed=recent_confirmed if include_momentum else 0,
                recent_overturned=recent_overturned if include_momentum else 0,
                momentum=float(components["momentum"]) if include_momentum else 0.0,
                difficulty_avg=difficulty_avg,
                difficulty_bonus=float(components["difficulty_bonus"]),
                abs_power_score=float(components["abs_power_score"]),
            )
        )

    standings.sort(
        key=lambda standing: (
            -standing.abs_power_score,
            -standing.bayes_rate,
            -standing.wilson,
            -standing.total,
            standing.name.lower(),
        )
    )
    return tuple(standings)


def build_weekly_leaderboard(
    *,
    umpire_stats: dict[str, dict],
    challenge_history: Iterable[dict[str, Any]] = (),
    week_start: date,
    week_end: date,
) -> UmpireLeaderboard:
    standings = build_umpire_standings(
        umpire_stats,
        challenge_history=challenge_history,
        as_of=week_end,
        include_momentum=True,
    )
    title = f"Weekly ABS Umpire Leaderboard (Thru {week_end.strftime('%b %d')})"
    subtitle = (
        f"Season-to-date HP umpire power ranking through {week_end.strftime('%b %d, %Y')} | "
        "Sorted by ABS Power Score"
    )
    leaderboard_id = f"leaderboard:weekly:{week_end.isoformat()}"
    return UmpireLeaderboard(
        leaderboard_id=leaderboard_id,
        kind="weekly",
        title=title,
        subtitle=subtitle,
        standings=standings,
        league_average=_league_average(umpire_stats),
    )


def build_season_champion_leaderboard(
    *,
    umpire_stats: dict[str, dict],
    challenge_history: Iterable[dict[str, Any]] = (),
    season_year: int,
    as_of: date | None = None,
) -> UmpireLeaderboard:
    standings = build_umpire_standings(
        umpire_stats,
        challenge_history=challenge_history,
        as_of=as_of,
        include_momentum=False,
    )
    title = f"ABS Regular Season Champion ({season_year})"
    subtitle = "Final season-to-date power ranking | Sorted by ABS Power Score"
    leaderboard_id = f"leaderboard:season:{season_year}"
    return UmpireLeaderboard(
        leaderboard_id=leaderboard_id,
        kind="season",
        title=title,
        subtitle=subtitle,
        standings=standings,
        league_average=_league_average(umpire_stats),
    )


def format_leaderboard_post_text(leaderboard: UmpireLeaderboard, *, limit: int = 280) -> str:
    top_five = leaderboard.standings[:5]
    title_block = [leaderboard.title]
    if leaderboard.kind == "season" and top_five:
        leader = top_five[0]
        title_block.append(
            f"Champion: {leader.name} - "
            f"{leader.upheld_percent:.1f}% ({leader.record_display}, {leader.total})"
        )
    standings_block = []
    for index, standing in enumerate(top_five, start=1):
        standings_block.append(
            f"{index}. {_truncate_name(standing.name, 18)} - "
            f"{standing.upheld_percent:.1f}% ({standing.record_display}, {standing.total})"
        )

    blocks = [title_block, standings_block]
    most_challenged = _most_challenged_standing(leaderboard.standings)
    if most_challenged is not None:
        blocks.append(
            [
                f"Most challenged: {_truncate_name(most_challenged.name, 18)} - "
                f"{most_challenged.upheld_percent:.1f}% "
                f"({most_challenged.record_display}, {most_challenged.total})"
            ]
        )
    return _truncate_blocks(blocks, limit)


def format_leaderboard_alt_text(leaderboard: UmpireLeaderboard) -> str:
    top_three = ", ".join(
        f"{index}. {standing.name} ({standing.upheld_percent:.1f}%)"
        for index, standing in enumerate(leaderboard.standings[:3], start=1)
    )
    most_challenged = _most_challenged_standing(leaderboard.standings)
    most_challenged_text = ""
    if most_challenged is not None:
        most_challenged_text = (
            f" Most challenged umpire: {most_challenged.name} "
            f"at {most_challenged.upheld_percent:.1f}% on {most_challenged.total} challenges."
        )
    if top_three:
        return (
            f"{leaderboard.title}. {leaderboard.subtitle}. "
            f"Top three: {top_three}.{most_challenged_text}"
        )
    return f"{leaderboard.title}. {leaderboard.subtitle}.{most_challenged_text}"


def render_umpire_leaderboard(leaderboard: UmpireLeaderboard, output_dir: Path) -> Path:
    if not PIL_AVAILABLE:
        raise RuntimeError("Weekly leaderboard rendering requires Pillow.")

    output_dir.mkdir(parents=True, exist_ok=True)
    rows_per_column = max(1, ceil(len(leaderboard.standings) / 2))
    image_height = max(
        LEADERBOARD_MIN_HEIGHT,
        TABLE_TOP + TABLE_HEADER_HEIGHT + (rows_per_column * ROW_HEIGHT) + TABLE_BOTTOM_PADDING,
    )
    output_path = output_dir / f"{leaderboard.leaderboard_id.replace(':', '_')}.png"

    image = Image.new("RGB", (LEADERBOARD_WIDTH, image_height), TABLE_BG)
    draw = ImageDraw.Draw(image)

    title_font = _load_font(44, bold=True)
    subtitle_font = _load_font(24)
    card_label_font = _load_font(18, bold=True)
    card_body_font = _load_font(24, bold=True)
    card_small_font = _load_font(19, bold=True)
    header_font = _load_font(24, bold=True)
    body_font = _load_font(23, bold=True)

    draw.rounded_rectangle(
        (OUTER_MARGIN, 24, LEADERBOARD_WIDTH - OUTER_MARGIN, image_height - 24),
        radius=28,
        fill=CARD_BG,
        outline=CARD_OUTLINE,
        width=4,
    )
    draw.rounded_rectangle(
        (OUTER_MARGIN + 26, 42, LEADERBOARD_WIDTH - OUTER_MARGIN - 26, 42 + HEADER_BAND_HEIGHT),
        radius=22,
        fill=CARD_OUTLINE,
    )
    draw.text((OUTER_MARGIN + 54, 72), leaderboard.title, fill="#ffffff", font=title_font)
    draw.text((OUTER_MARGIN + 54, 122), leaderboard.subtitle, fill=CARD_MUTED, font=subtitle_font)

    _draw_summary_cards(
        draw,
        standings=leaderboard.standings,
        label_font=card_label_font,
        body_font=card_body_font,
        small_font=card_small_font,
    )

    left_rows, right_rows = _split_standings_columns(leaderboard.standings)
    column_width = (TABLE_RIGHT - TABLE_LEFT - COLUMN_GAP) / 2.0
    left_column_x = TABLE_LEFT
    right_column_x = TABLE_LEFT + column_width + COLUMN_GAP
    most_challenged = _most_challenged_standing(leaderboard.standings)

    _draw_table_column(
        draw,
        header_font=header_font,
        body_font=body_font,
        column_left=left_column_x,
        column_top=TABLE_TOP,
        column_width=column_width,
        standings=left_rows,
        rank_offset=0,
        most_challenged_key=most_challenged.key if most_challenged else None,
    )
    _draw_table_column(
        draw,
        header_font=header_font,
        body_font=body_font,
        column_left=right_column_x,
        column_top=TABLE_TOP,
        column_width=column_width,
        standings=right_rows,
        rank_offset=len(left_rows),
        most_challenged_key=most_challenged.key if most_challenged else None,
    )

    image.save(output_path, format="PNG")
    return output_path


def _draw_summary_cards(
    draw: "ImageDraw.ImageDraw",
    *,
    standings: tuple[UmpireStanding, ...],
    label_font,
    body_font,
    small_font,
) -> None:
    cards = [
        _best_all_around_card(standings),
        _safest_leader_card(standings),
        _hottest_ump_card(standings),
        _workhorse_card(standings),
    ]
    card_width = (
        LEADERBOARD_WIDTH - (OUTER_MARGIN * 2) - 52 - (SUMMARY_CARD_GAP * (len(cards) - 1))
    ) / float(len(cards))
    for index, (label, lines, fill) in enumerate(cards):
        left = OUTER_MARGIN + 26 + (index * (card_width + SUMMARY_CARD_GAP))
        right = left + card_width
        draw.rounded_rectangle(
            (left, SUMMARY_TOP, right, SUMMARY_TOP + SUMMARY_CARD_HEIGHT),
            radius=22,
            fill=fill,
            outline=CARD_OUTLINE,
            width=2,
        )
        draw.text((left + 22, SUMMARY_TOP + 16), label, fill=CARD_OUTLINE, font=label_font)
        draw.text((left + 22, SUMMARY_TOP + 46), lines[0], fill=CARD_OUTLINE, font=body_font)
        draw.text((left + 22, SUMMARY_TOP + 86), lines[1], fill=CARD_OUTLINE, font=small_font)
        draw.text((left + 22, SUMMARY_TOP + 114), lines[2], fill=CARD_OUTLINE, font=small_font)


def _best_all_around_card(standings: tuple[UmpireStanding, ...]) -> tuple[str, tuple[str, str, str], str]:
    leader = standings[0] if standings else None
    if leader is None:
        return ("BEST ALL-AROUND", ("No data yet", "", ""), SUMMARY_CARD_BG)
    return (
        "BEST ALL-AROUND",
        (
            leader.name,
            f"Power {leader.abs_power_score:.1f} | Upheld {leader.upheld_percent:.1f}%",
            f"Record {leader.record_display} on {leader.total} challenges",
        ),
        SUMMARY_CARD_BG,
    )


def _safest_leader_card(standings: tuple[UmpireStanding, ...]) -> tuple[str, tuple[str, str, str], str]:
    safest = _safest_leader_standing(standings)
    if safest is None:
        return ("SAFEST LEADER", ("No data yet", "", ""), SUMMARY_CARD_ALT)
    return (
        "SAFEST LEADER",
        (
            safest.name,
            f"Wilson floor {safest.wilson_percent:.1f}% | Bayes {safest.bayes_percent:.1f}%",
            f"Record {safest.record_display} on {safest.total} challenges",
        ),
        SUMMARY_CARD_ALT,
    )


def _hottest_ump_card(standings: tuple[UmpireStanding, ...]) -> tuple[str, tuple[str, str, str], str]:
    hottest = _hottest_ump_standing(standings)
    if hottest is None:
        return (
            "HOTTEST UMP",
            (
                "Awaiting recent sample",
                "Need 3 reviewed challenges in the last 14 days",
                "Momentum stays neutral until then",
            ),
            SUMMARY_CARD_HOT,
        )
    return (
        "HOTTEST UMP",
        (
            hottest.name,
            f"Momentum {hottest.momentum_display} | Last 14d {hottest.recent_record_display}",
            f"Season record {hottest.record_display} on {hottest.total} challenges",
        ),
        SUMMARY_CARD_HOT,
    )


def _workhorse_card(standings: tuple[UmpireStanding, ...]) -> tuple[str, tuple[str, str, str], str]:
    workhorse = _most_challenged_standing(standings)
    if workhorse is None:
        return ("WORKHORSE", ("No data yet", "", ""), TABLE_MOST_CHALLENGED)
    return (
        "WORKHORSE",
        (
            workhorse.name,
            f"Most challenged: {workhorse.total} | Record {workhorse.record_display}",
            f"Upheld {workhorse.upheld_percent:.1f}% | Power {workhorse.abs_power_score:.1f}",
        ),
        TABLE_MOST_CHALLENGED,
    )


def _draw_table_column(
    draw: "ImageDraw.ImageDraw",
    *,
    header_font,
    body_font,
    column_left: float,
    column_top: int,
    column_width: float,
    standings: tuple[UmpireStanding, ...],
    rank_offset: int,
    most_challenged_key: str | None,
) -> None:
    column_right = column_left + column_width
    draw.rounded_rectangle(
        (column_left, column_top, column_right, column_top + TABLE_HEADER_HEIGHT),
        radius=14,
        fill=CARD_OUTLINE,
    )
    _draw_table_headers(
        draw,
        header_font,
        column_left=column_left,
        column_top=column_top,
        column_width=column_width,
    )

    for index, standing in enumerate(standings, start=1):
        rank = rank_offset + index
        row_top = column_top + TABLE_HEADER_HEIGHT + ((index - 1) * ROW_HEIGHT)
        row_bottom = row_top + ROW_HEIGHT
        if standing.key == most_challenged_key:
            fill = TABLE_MOST_CHALLENGED
        elif rank <= 5:
            fill = TABLE_TOP_FIVE
        else:
            fill = TABLE_ALT if rank % 2 == 0 else CARD_BG
        draw.rectangle((column_left, row_top, column_right, row_bottom), fill=fill)
        draw.line((column_left, row_bottom, column_right, row_bottom), fill="#ddd6c7", width=1)
        _draw_standing_row(
            draw,
            body_font,
            standing,
            rank,
            row_top,
            column_left=column_left,
            column_width=column_width,
        )


def _draw_table_headers(draw: "ImageDraw.ImageDraw", font, *, column_left: float, column_top: int, column_width: float) -> None:
    columns = _column_positions(column_left, column_width)
    headers = [
        ("Rank", columns["rank"]),
        ("HP Umpire", columns["name"]),
        ("Upheld%", columns["rate"]),
        ("Record", columns["record"]),
        ("Total", columns["total"]),
    ]
    for label, x in headers:
        draw.text((x, column_top + 12), label, fill="#ffffff", font=font)


def _draw_standing_row(
    draw: "ImageDraw.ImageDraw",
    font,
    standing: UmpireStanding,
    rank: int,
    row_top: int,
    *,
    column_left: float,
    column_width: float,
) -> None:
    columns = _column_positions(column_left, column_width)
    draw.text((columns["rank"] + 8, row_top + 7), str(rank), fill=CARD_OUTLINE, font=font)
    draw.text((columns["name"], row_top + 7), standing.name, fill=CARD_OUTLINE, font=font)
    draw.text((columns["rate"], row_top + 7), f"{standing.upheld_percent:.1f}%", fill=CARD_OUTLINE, font=font)
    draw.text((columns["record"], row_top + 7), standing.record_display, fill=CARD_OUTLINE, font=font)
    draw.text((columns["total"], row_top + 7), str(standing.total), fill=CARD_OUTLINE, font=font)


def _column_positions(column_left: float, column_width: float) -> dict[str, float]:
    return {
        "rank": column_left + 18,
        "name": column_left + 88,
        "rate": column_left + (column_width * 0.61),
        "record": column_left + (column_width * 0.775),
        "total": column_left + (column_width * 0.905),
    }


def _split_standings_columns(standings: tuple[UmpireStanding, ...]) -> tuple[tuple[UmpireStanding, ...], tuple[UmpireStanding, ...]]:
    rows_per_column = max(1, ceil(len(standings) / 2))
    return standings[:rows_per_column], standings[rows_per_column:]


def _most_challenged_standing(standings: Iterable[UmpireStanding]) -> UmpireStanding | None:
    standings_list = list(standings)
    if not standings_list:
        return None
    return max(
        standings_list,
        key=lambda standing: (standing.total, standing.confirmed, standing.name.lower()),
    )


def _safest_leader_standing(standings: Iterable[UmpireStanding]) -> UmpireStanding | None:
    standings_list = list(standings)
    if not standings_list:
        return None
    return max(
        standings_list,
        key=lambda standing: (standing.wilson, standing.bayes_rate, standing.total, standing.name.lower()),
    )


def _hottest_ump_standing(standings: Iterable[UmpireStanding]) -> UmpireStanding | None:
    eligible = [standing for standing in standings if standing.recent_total >= 3]
    if not eligible:
        return None
    hottest = max(
        eligible,
        key=lambda standing: (standing.momentum, standing.recent_total, standing.abs_power_score, standing.name.lower()),
    )
    if hottest.momentum <= 0:
        return None
    return hottest


def _league_average(umpire_stats: dict[str, dict]) -> float:
    total_confirmed = 0
    total_challenges = 0
    for value in umpire_stats.values():
        if not isinstance(value, dict):
            continue
        total_confirmed += int(value.get("confirmed", 0) or 0)
        total_challenges += int(value.get("total", 0) or 0)
    if total_challenges <= 0:
        return 0.0
    return total_confirmed / total_challenges


def _recent_stats_by_umpire(
    challenge_history: Iterable[dict[str, Any]],
    *,
    as_of: date,
) -> dict[str, dict[str, int]]:
    window_start = as_of - timedelta(days=POWER_RECENT_DAYS - 1)
    recent_stats: dict[str, dict[str, int]] = {}
    for entry in challenge_history:
        if not isinstance(entry, dict):
            continue
        umpire_key = str(entry.get("umpire_key", "")).strip()
        if not umpire_key:
            continue
        occurred_on = _history_entry_date(entry.get("occurred_at"))
        if occurred_on is None or occurred_on < window_start or occurred_on > as_of:
            continue
        bucket = recent_stats.setdefault(umpire_key, {"confirmed": 0, "overturned": 0})
        bucket["confirmed"] += int(entry.get("confirmed", 0) or 0)
        bucket["overturned"] += int(entry.get("overturned", 0) or 0)
    return recent_stats


def _difficulty_average_by_umpire(
    challenge_history: Iterable[dict[str, Any]],
) -> dict[str, float]:
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    for entry in challenge_history:
        if not isinstance(entry, dict):
            continue
        umpire_key = str(entry.get("umpire_key", "")).strip()
        if not umpire_key:
            continue
        difficulty = _coerce_optional_float(entry.get("difficulty"))
        if difficulty is None:
            continue
        totals[umpire_key] = totals.get(umpire_key, 0.0) + difficulty
        counts[umpire_key] = counts.get(umpire_key, 0) + 1
    return {
        key: (totals[key] / counts[key])
        for key in totals.keys()
        if counts.get(key, 0) > 0
    }


def _history_entry_date(raw_value: Any) -> date | None:
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _coerce_optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truncate_name(name: str, limit: int) -> str:
    if len(name) <= limit:
        return name
    if limit <= 3:
        return name[:limit]
    trimmed = name[: limit - 3].rstrip()
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}..."


def _truncate_blocks(blocks: Iterable[Iterable[str]], limit: int) -> str:
    active_blocks = [
        [line for line in block if line]
        for block in blocks
    ]
    active_blocks = [block for block in active_blocks if block]
    text = "\n\n".join("\n".join(block) for block in active_blocks)
    if len(text) <= limit:
        return text

    collapsed = [line for block in active_blocks for line in block]
    text = "\n".join(collapsed)
    if len(text) <= limit:
        return text

    if active_blocks and len(active_blocks) >= 3:
        condensed_blocks = active_blocks[:2]
        text = "\n\n".join("\n".join(block) for block in condensed_blocks)
        if len(text) <= limit:
            return text

    active_lines = list(collapsed)
    while len("\n".join(active_lines)) > limit and len(active_lines) > 3:
        active_lines.pop()

    text = "\n".join(active_lines)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."
