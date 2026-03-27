from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Tuple

from .models import AbsChallenge

try:  # pragma: no cover - optional dependency
    from PIL import Image, ImageDraw, ImageFont

    PIL_AVAILABLE = True
except ImportError:  # pragma: no cover - fallback path
    PIL_AVAILABLE = False


CARD_WIDTH = 1200
CARD_HEIGHT = 800
CARD_OUTLINE = "#13293d"
CARD_BG = "#fffaf2"
CARD_ACCENT = "#fca311"
CARD_MUTED = "#d9e2ec"
FOOTER_CENTER_X = 838
FOOTER_TOP_Y = 658
FOOTER_MAX_CHARS = 54
FOOTER_MAX_LINES = 4
FOOTER_LINE_HEIGHT = 24
PLOT_BG = "#f0ead6"
BALL_RADIUS_PX = 14


def render_challenge_card(challenge: AbsChallenge, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _artifact_stem(challenge)
    if PIL_AVAILABLE:
        png_path = output_dir / f"{stem}.png"
        _render_png(challenge, png_path)
        return png_path

    svg_path = output_dir / f"{stem}.svg"
    svg_path.write_text(_build_svg(challenge), encoding="utf-8")
    return svg_path


def _artifact_stem(challenge: AbsChallenge) -> str:
    return f"{challenge.game_pk}_{challenge.at_bat_index}_{challenge.pitch.pitch_number or 0}_{challenge.challenge_team_abbrev.lower()}_abs"


def _render_png(challenge: AbsChallenge, output_path: Path) -> None:  # pragma: no cover - visual output
    image = Image.new("RGB", (CARD_WIDTH, CARD_HEIGHT), "#f5f1e8")
    draw = ImageDraw.Draw(image)

    title_font = _load_font(28, bold=True)
    body_font = _load_font(22)
    small_font = _load_font(18)

    draw.rounded_rectangle((32, 32, CARD_WIDTH - 32, CARD_HEIGHT - 32), radius=28, fill=CARD_BG, outline=CARD_OUTLINE, width=4)
    draw.rounded_rectangle((58, 58, 500, 742), radius=20, fill=CARD_OUTLINE)
    draw.rounded_rectangle((535, 58, 1142, 742), radius=20, fill=PLOT_BG)

    draw.text((86, 90), "ABS Challenge", fill="#fefefe", font=title_font)
    draw.text((86, 125), challenge.outcome_label.upper(), fill=CARD_ACCENT if challenge.is_overturned else "#9ad1d4", font=title_font)
    draw.text((86, 180), challenge.teams.matchup_label, fill="#fefefe", font=body_font)
    draw.text((86, 210), challenge.inning_label, fill=CARD_MUTED, font=body_font)
    draw.text(
        (86, 270),
        f"Challenge by: {challenge.challenger_name} ({challenge.challenge_team_abbrev})",
        fill="#fefefe",
        font=body_font,
    )
    draw.text((86, 325), f"Batter: {challenge.batter_name}", fill="#fefefe", font=body_font)
    draw.text((86, 355), f"Pitcher: {challenge.pitcher_name}", fill="#fefefe", font=body_font)
    draw.text((86, 390), challenge.home_plate_display, fill="#fefefe", font=body_font)
    if challenge.umpire_challenge_summary:
        draw.text((86, 420), challenge.umpire_challenge_summary, fill=CARD_MUTED, font=small_font)
    draw.text((86, 460), f"Score: {challenge.score_display}", fill=CARD_MUTED, font=body_font)
    draw.text((86, 490), f"Count: {challenge.pitch.count_display} | Outs: {challenge.outs_before}", fill=CARD_MUTED, font=body_font)
    _draw_bases_diamond_png(draw, challenge, center_x=398, center_y=500, size=28, base_size=9)
    draw.text((86, 575), f"Original: {challenge.original_call}", fill="#fefefe", font=body_font)
    draw.text((86, 605), f"ABS: {challenge.final_call}", fill=CARD_ACCENT if challenge.changed_call else "#9ad1d4", font=body_font)

    speed = (
        f"{challenge.pitch.start_speed:.1f} mph"
        if challenge.pitch.start_speed is not None
        else "Unknown velocity"
    )
    pitch_line = f"{challenge.pitch.pitch_type} | {speed}"
    pitch_line_y = 667
    if challenge.final_call == "Ball" and challenge.pitch.miss_display:
        draw.text((86, 637), f"Miss: {challenge.pitch.miss_display}", fill=CARD_MUTED, font=body_font)
    else:
        pitch_line_y = 637
    draw.text((86, pitch_line_y), pitch_line, fill="#fefefe", font=body_font)

    zone_left, zone_top = 700, 110
    zone_right, zone_bottom = 1010, 620
    draw.rectangle((zone_left, zone_top, zone_right, zone_bottom), outline=CARD_OUTLINE, width=2)
    _draw_strike_zone_png(draw, challenge, zone_left, zone_top, zone_right, zone_bottom, body_font, small_font)
    _draw_centered_footer_png(
        draw,
        challenge.at_bat_result_display,
        center_x=FOOTER_CENTER_X,
        top_y=FOOTER_TOP_Y,
        font=small_font,
    )

    image.save(output_path, format="PNG")


def _draw_strike_zone_png(
    draw: "ImageDraw.ImageDraw",
    challenge: AbsChallenge,
    left: int,
    top: int,
    right: int,
    bottom: int,
    body_font: "ImageFont.ImageFont",
    small_font: "ImageFont.ImageFont",
) -> None:  # pragma: no cover - visual output
    zone_half_width = 17.0 / 24.0
    zone_top_ft = challenge.pitch.strike_zone_top or 3.5
    zone_bottom_ft = challenge.pitch.strike_zone_bottom or 1.5
    if zone_top_ft <= zone_bottom_ft:
        zone_top_ft = 3.5
        zone_bottom_ft = 1.5

    def to_px(x_value: float, y_value: float) -> Tuple[float, float]:
        x_span = zone_half_width * 2
        y_span = zone_top_ft - zone_bottom_ft
        x = left + (((x_value + zone_half_width) / x_span) * (right - left))
        y = top + (((zone_top_ft - y_value) / y_span) * (bottom - top))
        return x, y

    width = right - left
    height = bottom - top
    for fraction in (1 / 3, 2 / 3):
        grid_x = left + (width * fraction)
        draw.line((grid_x, top, grid_x, bottom), fill="#d6d0ba", width=1)

    for fraction in (1 / 3, 2 / 3):
        grid_y = top + (height * fraction)
        draw.line((left, grid_y, right, grid_y), fill="#d6d0ba", width=1)

    if challenge.pitch.px is not None and challenge.pitch.pz is not None:
        px, py = to_px(challenge.pitch.px, challenge.pitch.pz)
        fill = "#d62828" if challenge.final_call == "Ball" else "#2a9d8f"
        draw.ellipse(
            (px - BALL_RADIUS_PX, py - BALL_RADIUS_PX, px + BALL_RADIUS_PX, py + BALL_RADIUS_PX),
            fill=fill,
            outline=CARD_OUTLINE,
            width=2,
        )
        draw.text((px + BALL_RADIUS_PX + 8, py - 10), challenge.pitch.call_description, fill=CARD_OUTLINE, font=small_font)


def _draw_bases_diamond_png(
    draw: "ImageDraw.ImageDraw",
    challenge: AbsChallenge,
    *,
    center_x: int,
    center_y: int,
    size: int,
    base_size: int,
) -> None:  # pragma: no cover - visual output
    occupied = set(challenge.runners_on_base)
    home = (center_x, center_y + size)
    first = (center_x + size, center_y)
    second = (center_x, center_y - size)
    third = (center_x - size, center_y)

    draw.line((home, first, second, third, home), fill=CARD_MUTED, width=2)
    for base_name, center in (("1B", first), ("2B", second), ("3B", third)):
        _draw_base_png(draw, center, size=base_size, occupied=base_name in occupied)
    _draw_home_plate_png(draw, home, size=base_size)


def _build_svg(challenge: AbsChallenge) -> str:
    zone_half_width = 17.0 / 24.0
    zone_top_ft = challenge.pitch.strike_zone_top or 3.5
    zone_bottom_ft = challenge.pitch.strike_zone_bottom or 1.5
    if zone_top_ft <= zone_bottom_ft:
        zone_top_ft = 3.5
        zone_bottom_ft = 1.5
    plot_left, plot_top, plot_width, plot_height = 650, 110, 380, 510

    def to_px(x_value: float, y_value: float) -> Tuple[float, float]:
        x = plot_left + (((x_value + zone_half_width) / (zone_half_width * 2)) * plot_width)
        y = plot_top + (((zone_top_ft - y_value) / (zone_top_ft - zone_bottom_ft)) * plot_height)
        return x, y

    pitch_circle = ""
    pitch_label = ""
    if challenge.pitch.px is not None and challenge.pitch.pz is not None:
        pitch_x, pitch_y = to_px(challenge.pitch.px, challenge.pitch.pz)
        pitch_fill = "#d62828" if challenge.final_call == "Ball" else "#2a9d8f"
        pitch_circle = (
            f'<circle cx="{pitch_x:.1f}" cy="{pitch_y:.1f}" r="{BALL_RADIUS_PX}" fill="{pitch_fill}" stroke="{CARD_OUTLINE}" stroke-width="2" />'
        )
        pitch_label = (
            f'<text x="{pitch_x + BALL_RADIUS_PX + 10:.1f}" y="{pitch_y + 6:.1f}" font-size="20" fill="{CARD_OUTLINE}">{escape(challenge.pitch.call_description)}</text>'
        )

    speed = (
        f"{challenge.pitch.start_speed:.1f} mph"
        if challenge.pitch.start_speed is not None
        else "Unknown velocity"
    )
    outcome_fill = "#fca311" if challenge.is_overturned else "#9ad1d4"
    footer_lines = _wrap_text(
        challenge.at_bat_result_display,
        max_chars=FOOTER_MAX_CHARS,
        max_lines=FOOTER_MAX_LINES,
    )
    footer_tspans = "".join(
        f'<tspan x="{FOOTER_CENTER_X}" dy="{0 if idx == 0 else FOOTER_LINE_HEIGHT}">{escape(line)}</tspan>'
        for idx, line in enumerate(footer_lines)
    )
    bases_svg = _build_bases_diamond_svg(challenge, center_x=398, center_y=500, size=28, base_size=9)
    grid_svg = "".join(
        f'<line x1="{plot_left + (plot_width * fraction):.1f}" y1="{plot_top}" x2="{plot_left + (plot_width * fraction):.1f}" y2="{plot_top + plot_height}" stroke="#d6d0ba" stroke-width="1" />'
        for fraction in (1 / 3, 2 / 3)
    )
    grid_svg += "".join(
        f'<line x1="{plot_left}" y1="{plot_top + (plot_height * fraction):.1f}" x2="{plot_left + plot_width}" y2="{plot_top + (plot_height * fraction):.1f}" stroke="#d6d0ba" stroke-width="1" />'
        for fraction in (1 / 3, 2 / 3)
    )
    miss_svg = ""
    pitch_svg_y = 678
    if challenge.final_call == "Ball" and challenge.pitch.miss_display:
        miss_svg = (
            f'<text x="86" y="646" font-size="22" fill="{CARD_MUTED}">Miss: {escape(challenge.pitch.miss_display)}</text>'
        )
    else:
        pitch_svg_y = 646

    umpire_summary_svg = ""
    if challenge.umpire_challenge_summary:
        umpire_summary_svg = (
            f'<text x="86" y="432" font-size="18" fill="{CARD_MUTED}">{escape(challenge.umpire_challenge_summary)}</text>'
        )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{CARD_WIDTH}" height="{CARD_HEIGHT}" viewBox="0 0 {CARD_WIDTH} {CARD_HEIGHT}">
  <rect width="{CARD_WIDTH}" height="{CARD_HEIGHT}" fill="#f5f1e8" />
  <rect x="32" y="32" width="1136" height="736" rx="28" fill="{CARD_BG}" stroke="{CARD_OUTLINE}" stroke-width="4" />
  <rect x="58" y="58" width="442" height="684" rx="20" fill="{CARD_OUTLINE}" />
  <rect x="535" y="58" width="607" height="684" rx="20" fill="{PLOT_BG}" />
  <text x="86" y="100" font-size="28" font-weight="700" fill="#ffffff">ABS Challenge</text>
  <text x="86" y="138" font-size="28" font-weight="700" fill="{outcome_fill}">{escape(challenge.outcome_label.upper())}</text>
  <text x="86" y="190" font-size="26" fill="#ffffff">{escape(challenge.teams.matchup_label)}</text>
  <text x="86" y="222" font-size="22" fill="{CARD_MUTED}">{escape(challenge.inning_label)}</text>
  <text x="86" y="282" font-size="24" fill="#ffffff">Challenge by: {escape(challenge.challenger_name)} ({escape(challenge.challenge_team_abbrev)})</text>
  <text x="86" y="336" font-size="22" fill="#ffffff">Batter: {escape(challenge.batter_name)}</text>
  <text x="86" y="368" font-size="22" fill="#ffffff">Pitcher: {escape(challenge.pitcher_name)}</text>
  <text x="86" y="400" font-size="22" fill="#ffffff">{escape(challenge.home_plate_display)}</text>
  {umpire_summary_svg}
  <text x="86" y="470" font-size="22" fill="{CARD_MUTED}">Score: {escape(challenge.score_display)}</text>
  <text x="86" y="502" font-size="22" fill="{CARD_MUTED}">Count: {escape(challenge.pitch.count_display)} | Outs: {challenge.outs_before}</text>
  {bases_svg}
  <text x="86" y="586" font-size="22" fill="#ffffff">Original: {escape(challenge.original_call)}</text>
  <text x="86" y="618" font-size="22" fill="{outcome_fill if challenge.changed_call else '#9ad1d4'}">ABS: {escape(challenge.final_call)}</text>
  {miss_svg}
  <text x="86" y="{pitch_svg_y}" font-size="22" fill="#ffffff">{escape(challenge.pitch.pitch_type)} | {escape(speed)}</text>
  <rect x="{plot_left}" y="{plot_top}" width="{plot_width}" height="{plot_height}" fill="none" stroke="{CARD_OUTLINE}" stroke-width="2" />
  {grid_svg}
  {pitch_circle}
  {pitch_label}
  <text x="{FOOTER_CENTER_X}" y="{FOOTER_TOP_Y}" font-size="18" fill="{CARD_OUTLINE}" text-anchor="middle">{footer_tspans}</text>
</svg>
"""


def _draw_centered_footer_png(
    draw: "ImageDraw.ImageDraw",
    text: str,
    *,
    center_x: int,
    top_y: int,
    font: "ImageFont.ImageFont",
) -> None:  # pragma: no cover - visual output
    lines = _wrap_text(text, max_chars=FOOTER_MAX_CHARS, max_lines=FOOTER_MAX_LINES)
    y = top_y
    for line in lines:
        if not line:
            continue
        bbox = draw.textbbox((0, 0), line, font=font)
        width = bbox[2] - bbox[0]
        draw.text((center_x - (width / 2), y), line, fill=CARD_OUTLINE, font=font)
        y += FOOTER_LINE_HEIGHT


def _load_font(size: int, *, bold: bool = False) -> "ImageFont.ImageFont":
    candidates = []
    if bold:
        candidates.extend(
            [
                "DejaVuSerif-Bold.ttf",
                "LiberationSerif-Bold.ttf",
                "DejaVuSans-Bold.ttf",
                "C:/Windows/Fonts/georgiab.ttf",
                "C:/Windows/Fonts/timesbd.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
                "/usr/share/fonts/truetype/liberation2/LiberationSerif-Bold.ttf",
            ]
        )
    else:
        candidates.extend(
            [
                "DejaVuSerif.ttf",
                "LiberationSerif-Regular.ttf",
                "DejaVuSans.ttf",
                "C:/Windows/Fonts/georgia.ttf",
                "C:/Windows/Fonts/times.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                "/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf",
            ]
        )

    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _wrap_text(text: str, *, max_chars: int, max_lines: int) -> list[str]:
    words = text.split()
    if not words:
        return [""]

    lines: list[str] = []
    current_words: list[str] = []

    for word in words:
        candidate = " ".join(current_words + [word]).strip()
        if current_words and len(candidate) > max_chars and len(lines) < max_lines - 1:
            lines.append(" ".join(current_words))
            current_words = [word]
            continue
        current_words.append(word)

    current = " ".join(current_words).strip()
    if len(current) > max_chars:
        current = current[: max_chars - 1].rstrip()
        if " " in current:
            current = current.rsplit(" ", 1)[0]
        current = f"{current}..."
    lines.append(current)
    return lines[:max_lines]


def _draw_base_png(
    draw: "ImageDraw.ImageDraw",
    center: tuple[int, int],
    *,
    size: int,
    occupied: bool,
) -> None:  # pragma: no cover - visual output
    cx, cy = center
    points = [
        (cx, cy - size),
        (cx + size, cy),
        (cx, cy + size),
        (cx - size, cy),
    ]
    draw.polygon(points, fill=CARD_ACCENT if occupied else CARD_BG, outline=CARD_OUTLINE)


def _draw_home_plate_png(
    draw: "ImageDraw.ImageDraw",
    center: tuple[int, int],
    *,
    size: int,
) -> None:  # pragma: no cover - visual output
    cx, cy = center
    points = [
        (cx - size, cy - size + 1),
        (cx + size, cy - size + 1),
        (cx + size, cy + 1),
        (cx, cy + size),
        (cx - size, cy + 1),
    ]
    draw.polygon(points, fill=CARD_BG, outline=CARD_OUTLINE)


def _build_bases_diamond_svg(
    challenge: AbsChallenge,
    *,
    center_x: int,
    center_y: int,
    size: int,
    base_size: int,
) -> str:
    occupied = set(challenge.runners_on_base)
    home = (center_x, center_y + size)
    first = (center_x + size, center_y)
    second = (center_x, center_y - size)
    third = (center_x - size, center_y)
    path = " ".join(f"{x},{y}" for x, y in (home, first, second, third, home))

    return f"""
  <polyline points="{path}" fill="none" stroke="{CARD_MUTED}" stroke-width="2" />
  {_build_base_svg(first, size=base_size, occupied='1B' in occupied)}
  {_build_base_svg(second, size=base_size, occupied='2B' in occupied)}
  {_build_base_svg(third, size=base_size, occupied='3B' in occupied)}
  {_build_home_plate_svg(home, size=base_size)}
"""


def _build_base_svg(center: tuple[int, int], *, size: int, occupied: bool) -> str:
    cx, cy = center
    points = " ".join(
        f"{x},{y}"
        for x, y in (
            (cx, cy - size),
            (cx + size, cy),
            (cx, cy + size),
            (cx - size, cy),
        )
    )
    fill = CARD_ACCENT if occupied else CARD_BG
    return f'<polygon points="{points}" fill="{fill}" stroke="{CARD_OUTLINE}" stroke-width="2" />'


def _build_home_plate_svg(center: tuple[int, int], *, size: int) -> str:
    cx, cy = center
    points = " ".join(
        f"{x},{y}"
        for x, y in (
            (cx - size, cy - size + 1),
            (cx + size, cy - size + 1),
            (cx + size, cy + 1),
            (cx, cy + size),
            (cx - size, cy + 1),
        )
    )
    return f'<polygon points="{points}" fill="{CARD_BG}" stroke="{CARD_OUTLINE}" stroke-width="2" />'
