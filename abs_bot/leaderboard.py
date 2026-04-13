from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import ceil
from pathlib import Path
from typing import Iterable

from .render import CARD_BG, CARD_MUTED, CARD_OUTLINE, _load_font

try:  # pragma: no cover - optional dependency
    from PIL import Image, ImageDraw

    PIL_AVAILABLE = True
except ImportError:  # pragma: no cover - fallback path
    PIL_AVAILABLE = False


LEADERBOARD_WIDTH = 2000
LEADERBOARD_MIN_HEIGHT = 1450
OUTER_MARGIN = 42
HEADER_BAND_HEIGHT = 150
SUMMARY_TOP = 218
SUMMARY_CARD_HEIGHT = 150
SUMMARY_CARD_GAP = 26
TABLE_TOP = 404
TABLE_HEADER_HEIGHT = 46
ROW_HEIGHT = 34
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

    @property
    def record_display(self) -> str:
        return f"{self.confirmed}-{self.overturned}"


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
        lines.append(
            f"Champion: {leader.name} - "
            f"{leader.upheld_percent:.1f}% ({leader.record_display}, {leader.total})"
        )
    for index, standing in enumerate(top_five, start=1):
        lines.append(
            f"{index}. {_truncate_name(standing.name, 18)} - "
            f"{standing.upheld_percent:.1f}% ({standing.record_display}, {standing.total})"
        )

    most_challenged = _most_challenged_standing(leaderboard.standings)
    if most_challenged is not None:
        lines.append(
            f"Most challenged: {_truncate_name(most_challenged.name, 18)} - "
            f"{most_challenged.upheld_percent:.1f}% "
            f"({most_challenged.record_display}, {most_challenged.total})"
        )
    lines.append("Totals show sample size. Full table attached.")
    return _truncate_lines(lines, limit)


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
        return f"{leaderboard.title}. {leaderboard.subtitle}. Top three: {top_three}.{most_challenged_text}"
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
    card_body_font = _load_font(28, bold=True)
    card_small_font = _load_font(20)
    header_font = _load_font(22, bold=True)
    body_font = _load_font(21)

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

    leader = leaderboard.standings[0] if leaderboard.standings else None
    most_challenged = _most_challenged_standing(leaderboard.standings)
    _draw_summary_cards(
        draw,
        leader=leader,
        most_challenged=most_challenged,
        label_font=card_label_font,
        body_font=card_body_font,
        small_font=card_small_font,
    )

    left_rows = leaderboard.standings[:rows_per_column]
    right_rows = leaderboard.standings[rows_per_column:]
    column_width = (TABLE_RIGHT - TABLE_LEFT - COLUMN_GAP) / 2.0
    left_column_x = TABLE_LEFT
    right_column_x = TABLE_LEFT + column_width + COLUMN_GAP

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
        rank_offset=rows_per_column,
        most_challenged_key=most_challenged.key if most_challenged else None,
    )

    image.save(output_path, format="PNG")
    return output_path


def _draw_summary_cards(
    draw: "ImageDraw.ImageDraw",
    *,
    leader: UmpireStanding | None,
    most_challenged: UmpireStanding | None,
    label_font,
    body_font,
    small_font,
) -> None:
    card_width = (LEADERBOARD_WIDTH - (OUTER_MARGIN * 2) - 52 - (SUMMARY_CARD_GAP * 2)) / 3.0
    card_lefts = [
        OUTER_MARGIN + 26,
        OUTER_MARGIN + 26 + card_width + SUMMARY_CARD_GAP,
        OUTER_MARGIN + 26 + ((card_width + SUMMARY_CARD_GAP) * 2),
    ]
    cards = [
        (
            "CURRENT LEADER",
            (
                leader.name if leader else "No data yet",
                f"{leader.upheld_percent:.1f}% upheld" if leader else "",
                f"Record {leader.record_display} on {leader.total} challenges" if leader else "",
            ),
            SUMMARY_CARD_BG,
        ),
        (
            "LARGEST SAMPLE",
            (
                most_challenged.name if most_challenged else "No data yet",
                f"{most_challenged.upheld_percent:.1f}% upheld" if most_challenged else "",
                (
                    f"Record {most_challenged.record_display} on {most_challenged.total} challenges"
                    if most_challenged
                    else ""
                ),
            ),
            TABLE_MOST_CHALLENGED,
        ),
        (
            "HOW TO READ THIS",
            (
                "Rate ranks first",
                "Total challenges = sample size",
                "Early-season rates can swing fast",
            ),
            SUMMARY_CARD_ALT,
        ),
    ]
    for index, (label, lines, fill) in enumerate(cards):
        left = card_lefts[index]
        right = left + card_width
        draw.rounded_rectangle(
            (left, SUMMARY_TOP, right, SUMMARY_TOP + SUMMARY_CARD_HEIGHT),
            radius=22,
            fill=fill,
            outline=CARD_OUTLINE,
            width=2,
        )
        draw.text((left + 24, SUMMARY_TOP + 18), label, fill=CARD_OUTLINE, font=label_font)
        draw.text((left + 24, SUMMARY_TOP + 48), lines[0], fill=CARD_OUTLINE, font=body_font)
        draw.text((left + 24, SUMMARY_TOP + 88), lines[1], fill=CARD_OUTLINE, font=small_font)
        draw.text((left + 24, SUMMARY_TOP + 114), lines[2], fill=CARD_OUTLINE, font=small_font)


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
    _draw_table_headers(draw, header_font, column_left=column_left, column_top=column_top, column_width=column_width)

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
        draw.text((x, column_top + 11), label, fill="#ffffff", font=font)


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
        "rate": column_left + (column_width * 0.63),
        "record": column_left + (column_width * 0.79),
        "total": column_left + (column_width * 0.91),
    }


def _most_challenged_standing(standings: Iterable[UmpireStanding]) -> UmpireStanding | None:
    standings_list = list(standings)
    if not standings_list:
        return None
    return max(
        standings_list,
        key=lambda standing: (standing.total, standing.confirmed, standing.name.lower()),
    )


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

    if active_lines and active_lines[-1] == "Totals show sample size. Full table attached.":
        active_lines[-1] = "Totals show sample size."
        text = "\n".join(active_lines)
        if len(text) <= limit:
            return text

    while len("\n".join(active_lines)) > limit and len(active_lines) > 3:
        active_lines.pop()

    text = "\n".join(active_lines)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."
