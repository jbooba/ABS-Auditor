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


OUTPUT_SCALE = 2
CANVAS_WIDTH = 1200
CANVAS_HEIGHT = 800
CARD_WIDTH = CANVAS_WIDTH * OUTPUT_SCALE
CARD_HEIGHT = CANVAS_HEIGHT * OUTPUT_SCALE
CARD_OUTLINE = "#13293d"
CARD_BG = "#fffaf2"
CARD_ACCENT = "#fca311"
CARD_MUTED = "#d9e2ec"
SVG_FONT_FAMILY = "'Noto Serif', 'DejaVu Serif', Georgia, serif"
FONT_DIR = Path(__file__).resolve().parent / "assets" / "fonts"
PLOT_LEFT = 700
PLOT_TOP = 110
PLOT_WIDTH = 310
PLOT_HEIGHT = 510
PLOT_RIGHT = PLOT_LEFT + PLOT_WIDTH
PLOT_BOTTOM = PLOT_TOP + PLOT_HEIGHT
FOOTER_CENTER_X = PLOT_LEFT + (PLOT_WIDTH // 2)
FOOTER_TOP_Y = 658
FOOTER_MAX_CHARS = 54
FOOTER_MAX_LINES = 4
FOOTER_LINE_HEIGHT = 24
PLOT_BG = "#f0ead6"
PLATE_WIDTH_INCHES = 17.0
BASEBALL_DIAMETER_INCHES = 2.89
BALL_RADIUS_INCHES = BASEBALL_DIAMETER_INCHES / 2.0
BALL_RADIUS_FEET = BALL_RADIUS_INCHES / 12.0
ZONE_WIDTH_TARGET_RATIO = 0.74
ZONE_HEIGHT_TARGET_RATIO = 0.68
ZONE_CENTER_Y_RATIO = 0.42
PERSPECTIVE_BAND_HEIGHT = 38


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
    image = Image.new("RGB", (CANVAS_WIDTH, CANVAS_HEIGHT), "#f5f1e8")
    draw = ImageDraw.Draw(image)

    title_font = _load_font(28, bold=True)
    body_font = _load_font(22)
    small_font = _load_font(18)

    draw.rounded_rectangle((32, 32, CANVAS_WIDTH - 32, CANVAS_HEIGHT - 32), radius=28, fill=CARD_BG, outline=CARD_OUTLINE, width=4)
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

    _draw_strike_zone_png(draw, challenge, PLOT_LEFT, PLOT_TOP, PLOT_RIGHT, PLOT_BOTTOM, small_font)
    _draw_centered_footer_png(
        draw,
        challenge.at_bat_result_display,
        center_x=FOOTER_CENTER_X,
        top_y=FOOTER_TOP_Y,
        font=small_font,
    )

    if OUTPUT_SCALE != 1:
        resampling = getattr(Image, "Resampling", Image)
        image = image.resize((CARD_WIDTH, CARD_HEIGHT), resample=resampling.LANCZOS)

    image.save(output_path, format="PNG")


def _draw_strike_zone_png(
    draw: "ImageDraw.ImageDraw",
    challenge: AbsChallenge,
    left: int,
    top: int,
    right: int,
    bottom: int,
    small_font: "ImageFont.ImageFont",
) -> None:  # pragma: no cover - visual output
    sx1, sy1, sx2, sy2, px_per_inch, zone_top_ft, zone_bottom_ft = _zone_layout(challenge)
    draw.rounded_rectangle((sx1, sy1, sx2, sy2), radius=10, outline=CARD_OUTLINE, width=2)
    _draw_plot_home_plate_png(draw, sx1, sx2)

    if challenge.pitch.px is not None and challenge.pitch.pz is not None:
        zone_center_x = (sx1 + sx2) / 2.0
        zone_bottom_y = sy2
        px = zone_center_x + (challenge.pitch.px * 12.0 * px_per_inch)
        py = zone_bottom_y - ((challenge.pitch.pz - zone_bottom_ft) * 12.0 * px_per_inch)
        fill = "#d62828" if challenge.final_call == "Ball" else "#2a9d8f"
        ball_radius_px = (BASEBALL_DIAMETER_INCHES * px_per_inch) / 2.0
        draw.ellipse(
            (px - ball_radius_px, py - ball_radius_px, px + ball_radius_px, py + ball_radius_px),
            fill=fill,
            outline=CARD_OUTLINE,
            width=2,
        )
        label_text = challenge.pitch.call_description
        label_bbox = draw.textbbox((0, 0), label_text, font=small_font)
        label_width = label_bbox[2] - label_bbox[0]
        if px > (PLOT_LEFT + (PLOT_WIDTH * 0.62)):
            label_x = px - ball_radius_px - 10 - label_width
        else:
            label_x = px + ball_radius_px + 8
        draw.text((label_x, py - 10), label_text, fill=CARD_OUTLINE, font=small_font)


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
    sx1, sy1, sx2, sy2, px_per_inch, _, zone_bottom_ft = _zone_layout(challenge)
    home_plate_svg = _build_plot_home_plate_svg(sx1, sx2)

    pitch_circle = ""
    pitch_label = ""
    if challenge.pitch.px is not None and challenge.pitch.pz is not None:
        zone_center_x = (sx1 + sx2) / 2.0
        zone_bottom_y = sy2
        pitch_x = zone_center_x + (challenge.pitch.px * 12.0 * px_per_inch)
        pitch_y = zone_bottom_y - ((challenge.pitch.pz - zone_bottom_ft) * 12.0 * px_per_inch)
        pitch_fill = "#d62828" if challenge.final_call == "Ball" else "#2a9d8f"
        ball_radius_px = (BASEBALL_DIAMETER_INCHES * px_per_inch) / 2.0
        label_anchor = "start"
        label_x = pitch_x + ball_radius_px + 10
        if pitch_x > (PLOT_LEFT + (PLOT_WIDTH * 0.62)):
            label_anchor = "end"
            label_x = pitch_x - ball_radius_px - 10
        pitch_circle = (
            f'<circle cx="{pitch_x:.1f}" cy="{pitch_y:.1f}" r="{ball_radius_px:.1f}" fill="{pitch_fill}" stroke="{CARD_OUTLINE}" stroke-width="2" />'
        )
        pitch_label = (
            f'<text x="{label_x:.1f}" y="{pitch_y + 6:.1f}" font-size="20" fill="{CARD_OUTLINE}" text-anchor="{label_anchor}">{escape(challenge.pitch.call_description)}</text>'
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

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{CARD_WIDTH}" height="{CARD_HEIGHT}" viewBox="0 0 {CANVAS_WIDTH} {CANVAS_HEIGHT}" style="font-family: {SVG_FONT_FAMILY};">
  <rect width="{CANVAS_WIDTH}" height="{CANVAS_HEIGHT}" fill="#f5f1e8" />
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
  <rect x="{sx1:.1f}" y="{sy1:.1f}" width="{sx2 - sx1:.1f}" height="{sy2 - sy1:.1f}" rx="10" fill="none" stroke="{CARD_OUTLINE}" stroke-width="2" />
  {home_plate_svg}
  {pitch_circle}
  {pitch_label}
  <text x="{FOOTER_CENTER_X}" y="{FOOTER_TOP_Y}" font-size="18" fill="{CARD_OUTLINE}" text-anchor="middle">{footer_tspans}</text>
</svg>
"""


def _strike_zone_geometry(challenge: AbsChallenge) -> tuple[float, float, float]:
    zone_half_width = PLATE_WIDTH_INCHES / 24.0
    zone_top_ft = challenge.pitch.display_zone_top or 3.5
    zone_bottom_ft = challenge.pitch.display_zone_bottom or 1.5
    if zone_top_ft <= zone_bottom_ft:
        zone_top_ft = 3.5
        zone_bottom_ft = 1.5
    return zone_half_width, zone_top_ft, zone_bottom_ft


def _zone_layout(challenge: AbsChallenge) -> tuple[float, float, float, float, float, float, float]:
    zone_half_width, zone_top_ft, zone_bottom_ft = _strike_zone_geometry(challenge)
    zone_width_inches = PLATE_WIDTH_INCHES
    zone_height_inches = (zone_top_ft - zone_bottom_ft) * 12.0
    px_per_inch = min(
        (PLOT_WIDTH * ZONE_WIDTH_TARGET_RATIO) / zone_width_inches,
        (PLOT_HEIGHT * ZONE_HEIGHT_TARGET_RATIO) / zone_height_inches,
    )

    zone_width_px = zone_width_inches * px_per_inch
    zone_height_px = zone_height_inches * px_per_inch
    zone_center_x = PLOT_LEFT + (PLOT_WIDTH / 2.0)
    zone_center_y = PLOT_TOP + (PLOT_HEIGHT * ZONE_CENTER_Y_RATIO)
    sx1 = zone_center_x - (zone_width_px / 2.0)
    sy1 = zone_center_y - (zone_height_px / 2.0)
    sx2 = zone_center_x + (zone_width_px / 2.0)
    sy2 = zone_center_y + (zone_height_px / 2.0)
    return sx1, sy1, sx2, sy2, px_per_inch, zone_top_ft, zone_bottom_ft


def _draw_plot_home_plate_png(
    draw: "ImageDraw.ImageDraw",
    zone_left: float,
    zone_right: float,
) -> None:  # pragma: no cover - visual output
    band, plate = _plot_home_plate_geometry(zone_left, zone_right)
    draw.rounded_rectangle(band, radius=6, fill="#dad9d7")
    draw.polygon(plate, fill="#faf7f0", outline=CARD_OUTLINE)


def _build_plot_home_plate_svg(zone_left: float, zone_right: float) -> str:
    band, plate = _plot_home_plate_geometry(zone_left, zone_right)
    band_left, band_top, band_right, band_bottom = band
    plate_points = " ".join(f"{x:.1f},{y:.1f}" for x, y in plate)
    return (
        f'<rect x="{band_left:.1f}" y="{band_top:.1f}" width="{band_right - band_left:.1f}" '
        f'height="{band_bottom - band_top:.1f}" rx="6" fill="#dad9d7" />'
        f'<polygon points="{plate_points}" fill="#faf7f0" stroke="{CARD_OUTLINE}" stroke-width="1.5" />'
    )


def _plot_home_plate_geometry(
    zone_left: float,
    zone_right: float,
) -> tuple[tuple[float, float, float, float], tuple[tuple[float, float], ...]]:
    zone_width = zone_right - zone_left
    center_x = (zone_left + zone_right) / 2.0
    band_left = PLOT_LEFT + 16
    band_right = PLOT_RIGHT - 16
    band_top = min(PLOT_BOTTOM + 6, FOOTER_TOP_Y - 42)
    band_bottom = band_top + PERSPECTIVE_BAND_HEIGHT
    band = (
        band_left,
        band_top,
        band_right,
        band_bottom,
    )

    plate_top_width = zone_width
    plate_shoulder_width = plate_top_width * 0.95
    plate_top_y = band_top + 3
    plate_shoulder_y = band_top + (PERSPECTIVE_BAND_HEIGHT * 0.46)
    plate_bottom_y = band_bottom - 2
    plate = (
        (center_x - (plate_top_width / 2.0), plate_top_y),
        (center_x + (plate_top_width / 2.0), plate_top_y),
        (center_x + (plate_shoulder_width / 2.0), plate_shoulder_y),
        (center_x, plate_bottom_y),
        (center_x - (plate_shoulder_width / 2.0), plate_shoulder_y),
    )
    return band, plate


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
                str(FONT_DIR / "NotoSerif-Bold.ttf"),
                str(FONT_DIR / "DejaVuSerif-Bold.ttf"),
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
                str(FONT_DIR / "NotoSerif-Regular.ttf"),
                str(FONT_DIR / "DejaVuSerif.ttf"),
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
