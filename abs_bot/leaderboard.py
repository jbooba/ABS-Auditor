from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

from .render import CARD_ACCENT, CARD_BG, CARD_MUTED, CARD_OUTLINE, _load_font

try:  # pragma: no cover - optional dependency
    from PIL import Image, ImageDraw

    PIL_AVAILABLE = True
except ImportError:  # pragma: no cover - fallback path
    PIL_AVAILABLE = False


LEADERBOARD_WIDTH = 1600
LEADERBOARD_MIN_HEIGHT = 1100
ROW_HEIGHT = 34
HEADER_HEIGHT = 250
TABLE_TOP = 250
TABLE_LEFT = 68
TABLE_RIGHT = LEADERBOARD_WIDTH - 68
TABLE_HEADER_HEIGHT = 44
BOTTOM_PADDING = 70
TABLE_BG = "#f5f1e8"
TABLE_ALT = "#fff8eb"
TABLE_TOP_FIVE = "#f6efe0"


@dataclass(frozen=True)
class UmpireStanding:
    key: str
    name: str
    total: int
    confirmed: int
    overturned: int
    upheld_rate: float

    @property
    def upheld_percent(self) -> float:
        return self.upheld_rate * 100.0

    @property
    def summary(self) -> str:
        return f"{self.upheld_percent:.1f}% ({self.confirmed}/{self.total})"


@dataclass(frozen=True)
class UmpireLeaderboard:
    leaderboard_id: str
    kind: str
    title: str
    subtitle: str
    standings: tuple[UmpireStanding, ...]


def build_umpire_standings(umpire_stats: dict[str, dict]) -> tuple[UmpireStanding, ...]:
    standings: list[UmpireStanding] = []
    for key, value in umpire_stats.items():
        if not isinstance(value, dict):
            continue
        total = int(value.get("total", 0) or 0)
        confirmed = int(value.get("confirmed", 0) or 0)
        overturned = int(value.get("overturned", 0) or 0)
        if total <= 0:
            continue
        name = str(value.get("name", "")).strip() or key
        standings.append(
            UmpireStanding(
                key=str(key),
                name=name,
                total=total,
                confirmed=confirmed,
                overturned=overturned,
                upheld_rate=(confirmed / total) if total else 0.0,
            )
        )

    standings.sort(
        key=lambda standing: (
            -standing.upheld_rate,
            -standing.total,
            standing.name.lower(),
        )
    )
    return tuple(standings)


def build_weekly_leaderboard(
    *,
    umpire_stats: dict[str, dict],
    week_start: date,
    week_end: date,
) -> UmpireLeaderboard:
    standings = build_umpire_standings(umpire_stats)
    title = f"ABS Umpire Leaderboard ({week_start.strftime('%b %d')} - {week_end.strftime('%b %d')})"
    subtitle = f"Week ending {week_end.strftime('%b %d, %Y')} | Season-to-date upheld rate"
    leaderboard_id = f"leaderboard:weekly:{week_end.isoformat()}"
    return UmpireLeaderboard(
        leaderboard_id=leaderboard_id,
        kind="weekly",
        title=title,
        subtitle=subtitle,
        standings=standings,
    )


def build_season_champion_leaderboard(
    *,
    umpire_stats: dict[str, dict],
    season_year: int,
) -> UmpireLeaderboard:
    standings = build_umpire_standings(umpire_stats)
    title = f"ABS Regular Season Champion ({season_year})"
    subtitle = "Final upheld-rate standings"
    leaderboard_id = f"leaderboard:season:{season_year}"
    return UmpireLeaderboard(
        leaderboard_id=leaderboard_id,
        kind="season",
        title=title,
        subtitle=subtitle,
        standings=standings,
    )


def format_leaderboard_post_text(leaderboard: UmpireLeaderboard, *, limit: int = 280) -> str:
    top_five = leaderboard.standings[:5]
    lines = [leaderboard.title]
    if leaderboard.kind == "season" and top_five:
        leader = top_five[0]
        lines.append(f"Champion: {leader.name} — {leader.upheld_percent:.1f}%")
    for index, standing in enumerate(top_five, start=1):
        lines.append(f"{index}. {_truncate_name(standing.name, 18)} — {standing.upheld_percent:.1f}%")
    lines.append("Full table attached.")
    return _truncate_lines(lines, limit)


def format_leaderboard_alt_text(leaderboard: UmpireLeaderboard) -> str:
    top_three = ", ".join(
        f"{index}. {standing.name} ({standing.upheld_percent:.1f}%)"
        for index, standing in enumerate(leaderboard.standings[:3], start=1)
    )
    if top_three:
        return f"{leaderboard.title}. {leaderboard.subtitle}. Top three: {top_three}."
    return f"{leaderboard.title}. {leaderboard.subtitle}."


def render_umpire_leaderboard(leaderboard: UmpireLeaderboard, output_dir: Path) -> Path:
    if not PIL_AVAILABLE:
        raise RuntimeError("Weekly leaderboard rendering requires Pillow.")

    output_dir.mkdir(parents=True, exist_ok=True)
    image_height = max(
        LEADERBOARD_MIN_HEIGHT,
        HEADER_HEIGHT + TABLE_HEADER_HEIGHT + (len(leaderboard.standings) * ROW_HEIGHT) + BOTTOM_PADDING,
    )
    output_path = output_dir / f"{leaderboard.leaderboard_id.replace(':', '_')}.png"

    image = Image.new("RGB", (LEADERBOARD_WIDTH, image_height), TABLE_BG)
    draw = ImageDraw.Draw(image)

    title_font = _load_font(36, bold=True)
    subtitle_font = _load_font(22)
    header_font = _load_font(21, bold=True)
    body_font = _load_font(20)
    small_font = _load_font(18)

    draw.rounded_rectangle(
        (28, 28, LEADERBOARD_WIDTH - 28, image_height - 28),
        radius=28,
        fill=CARD_BG,
        outline=CARD_OUTLINE,
        width=4,
    )
    draw.rounded_rectangle(
        (56, 56, LEADERBOARD_WIDTH - 56, 180),
        radius=22,
        fill=CARD_OUTLINE,
    )
    draw.text((84, 82), leaderboard.title, fill="#ffffff", font=title_font)
    draw.text((84, 128), leaderboard.subtitle, fill=CARD_MUTED, font=subtitle_font)

    if leaderboard.standings:
        leader = leaderboard.standings[0]
        draw.text(
            (84, 190),
            f"Current leader: {leader.name} | Upheld {leader.upheld_percent:.1f}% ({leader.confirmed}/{leader.total})",
            fill=CARD_OUTLINE,
            font=subtitle_font,
        )

    table_top = TABLE_TOP
    draw.rounded_rectangle(
        (TABLE_LEFT, table_top, TABLE_RIGHT, table_top + TABLE_HEADER_HEIGHT),
        radius=14,
        fill=CARD_OUTLINE,
    )
    _draw_table_headers(draw, header_font, table_top)

    for index, standing in enumerate(leaderboard.standings, start=1):
        row_top = table_top + TABLE_HEADER_HEIGHT + ((index - 1) * ROW_HEIGHT)
        row_bottom = row_top + ROW_HEIGHT
        fill = TABLE_TOP_FIVE if index <= 5 else (TABLE_ALT if index % 2 == 0 else CARD_BG)
        draw.rectangle((TABLE_LEFT, row_top, TABLE_RIGHT, row_bottom), fill=fill)
        draw.line((TABLE_LEFT, row_bottom, TABLE_RIGHT, row_bottom), fill="#ddd6c7", width=1)
        _draw_standing_row(draw, body_font, small_font, standing, index, row_top)

    image.save(output_path, format="PNG")
    return output_path


def _draw_table_headers(draw: "ImageDraw.ImageDraw", font, table_top: int) -> None:
    headers = [
        ("Rank", 96),
        ("HP Umpire", 170),
        ("Upheld%", 920),
        ("Confirmed", 1080),
        ("Overturned", 1230),
        ("Total", 1400),
    ]
    for label, x in headers:
        draw.text((x, table_top + 11), label, fill="#ffffff", font=font)


def _draw_standing_row(draw: "ImageDraw.ImageDraw", body_font, small_font, standing: UmpireStanding, rank: int, row_top: int) -> None:
    draw.text((104, row_top + 7), str(rank), fill=CARD_OUTLINE, font=body_font)
    draw.text((170, row_top + 7), standing.name, fill=CARD_OUTLINE, font=body_font)
    draw.text((934, row_top + 7), f"{standing.upheld_percent:.1f}%", fill=CARD_OUTLINE, font=body_font)
    draw.text((1108, row_top + 7), str(standing.confirmed), fill=CARD_OUTLINE, font=body_font)
    draw.text((1262, row_top + 7), str(standing.overturned), fill=CARD_OUTLINE, font=body_font)
    draw.text((1412, row_top + 7), str(standing.total), fill=CARD_OUTLINE, font=body_font)


def _truncate_name(name: str, limit: int) -> str:
    if len(name) <= limit:
        return name
    if limit <= 3:
        return name[:limit]
    trimmed = name[: limit - 3].rstrip()
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}..."


def _truncate_lines(lines: Iterable[str], limit: int) -> str:
    active_lines = [line for line in lines if line]
    text = "\n".join(active_lines)
    if len(text) <= limit:
        return text

    if active_lines and active_lines[-1] == "Full table attached.":
        active_lines.pop()
        text = "\n".join(active_lines)
        if len(text) <= limit:
            return text

    while len("\n".join(active_lines)) > limit and len(active_lines) > 3:
        active_lines.pop()

    text = "\n".join(active_lines)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."
