from __future__ import annotations

import json
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote_plus, urlencode, urlparse
from urllib.request import Request, urlopen

from .models import AbsChallenge


USER_AGENT = "mlb-abs-bot/0.1"
ALLOWED_CLIP_HOSTS = {
    "bdata-producedclips.mlb.com",
    "mlb-cuts-diamond.mlb.com",
    "milb-cuts-diamond.mlb.com",
}
RAW_CLIP_HOSTS = {"bdata-producedclips.mlb.com"}
GAME_CONTENT_CACHE_TTL_SECONDS = 30.0
_GAME_CONTENT_CACHE: dict[int, tuple[float, tuple[dict[str, Any], ...]]] = {}


@dataclass(frozen=True)
class ClipMedia:
    direct_url: str
    host: str
    clip_kind: str
    playback_name: str
    page_url: str
    social_url: str
    title: str
    description: str
    thumbnail_url: str
    lookup_method: str
    score: int


@dataclass(frozen=True)
class ClipLookupResult:
    raw_clip: ClipMedia | None
    highlight_clip: ClipMedia | None

    @property
    def best_available(self) -> ClipMedia | None:
        return self.raw_clip or self.highlight_clip


def lookup_best_abs_clip(challenge: AbsChallenge) -> Optional[ClipMedia]:
    return lookup_abs_clip_options(challenge).best_available


def lookup_abs_clip_options(
    challenge: AbsChallenge,
    *,
    excluded_direct_urls: Iterable[str] | None = None,
) -> ClipLookupResult:
    if not challenge.pitch.play_id:
        return ClipLookupResult(raw_clip=None, highlight_clip=None)

    excluded_urls = {
        str(url)
        for url in (excluded_direct_urls or ())
        if url
    }
    exact_media = _fetch_fastball_play_media(challenge.pitch.play_id)
    exact_media_text = _normalize_text(
        " ".join(
            part
            for part in [
                exact_media.get("title", ""),
                exact_media.get("description", ""),
            ]
            if part
        )
    )

    candidates: list[ClipMedia] = []
    for item in _fetch_game_content_items(challenge.game_pk):
        score = _score_content_item(item, challenge, exact_media_text=exact_media_text)
        if score < 180:
            continue

        title = str(item.get("headline") or item.get("title") or "")
        description = str(item.get("blurb") or item.get("description") or "")
        page_url = _item_page_url(item, challenge.pitch.play_id)
        social_url = _preferred_social_url(page_url)
        thumbnail_url = _item_thumbnail_url(item)
        for playback in item.get("playbacks") or []:
            clip_url = str(playback.get("url") or "")
            host = _clip_host_from_url(clip_url)
            if host not in ALLOWED_CLIP_HOSTS:
                continue
            if clip_url in excluded_urls:
                continue
            playback_name = str(playback.get("name") or "")
            candidates.append(
                ClipMedia(
                    direct_url=clip_url,
                    host=host,
                    clip_kind="raw" if host in RAW_CLIP_HOSTS else "highlight",
                    playback_name=playback_name,
                    page_url=page_url,
                    social_url=social_url,
                    title=title,
                    description=description,
                    thumbnail_url=thumbnail_url,
                    lookup_method="game_content",
                    score=score + _playback_rank(host, playback_name),
                )
            )

    if not candidates:
        return ClipLookupResult(raw_clip=None, highlight_clip=None)

    return ClipLookupResult(
        raw_clip=_choose_best_clip(candidates, clip_kind="raw"),
        highlight_clip=_choose_best_clip(candidates, clip_kind="highlight"),
    )


def _fetch_game_content_items(game_pk: int) -> tuple[dict[str, Any], ...]:
    cached = _GAME_CONTENT_CACHE.get(game_pk)
    now = time.monotonic()
    if cached is not None:
        fetched_at, items = cached
        if (now - fetched_at) < GAME_CONTENT_CACHE_TTL_SECONDS:
            return items

    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/content?language=en"
    data = _fetch_json(url)
    items = ((data.get("highlights") or {}).get("highlights") or {}).get("items") or []
    normalized_items = tuple(item for item in items if isinstance(item, dict))
    _GAME_CONTENT_CACHE[game_pk] = (now, normalized_items)
    return normalized_items


@lru_cache(maxsize=2048)
def _fetch_fastball_play_media(play_id: str) -> dict[str, Any]:
    query = "\n".join(
        [
            "query Search($queryType: QueryType!, $query: String!, $page: Int, $limit: Int, $feedPreference: FeedPreference, $languagePreference: LanguagePreference, $contentPreference: ContentPreference) {",
            "  search(queryType: $queryType, languagePreference: $languagePreference, contentPreference: $contentPreference, feedPreference: $feedPreference, limit: $limit, page: $page, query: $query) {",
            "    plays {",
            "      gamePk",
            "      mediaPlayback {",
            "        id",
            "        title",
            "        description",
            "        date",
            "        feeds {",
            "          playbacks {",
            "            name",
            "            url",
            "          }",
            "        }",
            "      }",
            "    }",
            "  }",
            "}",
        ]
    )
    variables = json.dumps(
        {
            "queryType": "STRUCTURED",
            "query": f'PlayId = ["{play_id}"]',
            "limit": 10,
            "page": 0,
            "languagePreference": "EN",
            "contentPreference": "MIXED",
        }
    )
    url = "https://fastball-gateway.mlb.com/graphql?" + urlencode(
        {
            "query": query,
            "operationName": "Search",
            "variables": variables,
        }
    )
    data = _fetch_json(url)
    plays = (((data.get("data") or {}).get("search") or {}).get("plays") or [])
    for play in plays:
        for media in play.get("mediaPlayback") or []:
            if str(media.get("id") or "") != play_id:
                continue
            playbacks: list[dict[str, Any]] = []
            for feed in media.get("feeds") or []:
                playbacks.extend(feed.get("playbacks") or [])
            return {
                "title": str(media.get("title") or ""),
                "description": str(media.get("description") or ""),
                "playbacks": playbacks,
            }
    return {"title": "", "description": "", "playbacks": []}


def _score_content_item(
    item: Dict[str, Any],
    challenge: AbsChallenge,
    *,
    exact_media_text: str,
) -> int:
    title = str(item.get("headline") or item.get("title") or "")
    description = str(item.get("blurb") or item.get("description") or "")
    combined_text = _normalize_text(" ".join(part for part in [title, description] if part))
    slug_text = _normalize_text(str(item.get("slug") or item.get("id") or ""))
    keywords = item.get("keywordsAll") or []
    keyword_values = {
        _normalize_text(" ".join(str(keyword.get(key) or "") for key in ("value", "displayName")))
        for keyword in keywords
        if isinstance(keyword, dict)
    }
    keyword_text = " ".join(part for part in keyword_values if part)
    search_text = " ".join(part for part in [combined_text, slug_text, keyword_text] if part)
    review_marker = _has_review_marker(search_text)

    player_signals = 0
    score = 0
    if "abs" in search_text:
        score += 120
    if "challenge" in search_text or "challenged" in search_text:
        score += 70
    if "capture review" in search_text:
        score += 90
    if "review" in search_text:
        score += 20
    if challenge.is_overturned and "overturned" in search_text:
        score += 60
    if (not challenge.is_overturned) and ("confirmed" in search_text or "stands" in search_text):
        score += 60

    for name, weight in (
        (challenge.batter_name, 110),
        (challenge.challenger_name, 90),
        (challenge.pitcher_name, 45),
    ):
        normalized_name = _normalize_text(name)
        if normalized_name and normalized_name in search_text:
            score += weight
            player_signals += 1

    inning_half = _normalize_text(challenge.half_inning)
    inning_num = str(challenge.inning)
    inning_phrases = {
        f"{inning_half} {inning_num}",
        f"{inning_half} of the {inning_num}",
        f"{inning_half} of the {inning_num}th",
    }
    if any(phrase.strip() and phrase in search_text for phrase in inning_phrases):
        score += 30

    if challenge.final_call == "Called Strike":
        if any(term in search_text for term in ("strikeout", "called out on strikes", "called strike")):
            score += 45
    elif challenge.final_call == "Ball":
        if any(term in search_text for term in ("walk", "ball", "called ball")):
            score += 45

    if exact_media_text:
        overlap = len(_token_set(search_text) & _token_set(exact_media_text))
        score += min(160, overlap * 20)
        if overlap >= 2:
            player_signals += 1

    game_pk_text = str(challenge.game_pk)
    if any(game_pk_text in text for text in keyword_values):
        score += 50

    item_date = _parse_mlb_datetime(item.get("date"))
    play_end_time = _parse_mlb_datetime(challenge.play_end_time)
    if item_date is not None and play_end_time is not None:
        published_after_play_seconds = (item_date - play_end_time).total_seconds()
        if published_after_play_seconds < -120:
            return 0
        if published_after_play_seconds <= 120:
            score += 120
        elif published_after_play_seconds <= 300:
            score += 90
        elif published_after_play_seconds <= 900:
            score += 70
        elif published_after_play_seconds <= 2700:
            score += 40
        elif published_after_play_seconds <= 10800:
            score += 10
        else:
            score -= 20

    clip_hosts = {
        _clip_host_from_url(str(playback.get("url") or ""))
        for playback in item.get("playbacks") or []
    }
    if any(host in ALLOWED_CLIP_HOSTS for host in clip_hosts):
        score += 25

    if not review_marker:
        return 0

    has_generic_review_title = review_marker and player_signals == 0
    if has_generic_review_title:
        return 0

    return score


def _playback_rank(host: str, playback_name: str) -> int:
    playback_name = playback_name.lower()
    if host in RAW_CLIP_HOSTS:
        if playback_name == "mp4avc":
            return 500
        if "mp4" in playback_name:
            return 450
        return 400
    if host in {"mlb-cuts-diamond.mlb.com", "milb-cuts-diamond.mlb.com"}:
        if playback_name == "mp4avc":
            return 320
        if "highbit" in playback_name:
            return 300
        if "hls" in playback_name or "cloud" in playback_name:
            return 260
    return 0


def _choose_best_clip(candidates: list[ClipMedia], *, clip_kind: str) -> ClipMedia | None:
    ranked = [
        clip
        for clip in candidates
        if clip.clip_kind == clip_kind
    ]
    if not ranked:
        return None

    ranked.sort(
        key=lambda clip: (
            -clip.score,
            clip.title.lower(),
            clip.direct_url,
        )
    )
    seen_urls: set[str] = set()
    for clip in ranked:
        if clip.direct_url in seen_urls:
            continue
        seen_urls.add(clip.direct_url)
        return clip
    return None


def _clip_host_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except ValueError:
        return ""


def _item_page_url(item: Dict[str, Any], play_id: str) -> str:
    slug = str(item.get("slug") or item.get("id") or "").strip("/")
    if slug:
        return f"https://www.mlb.com/video/{slug}"
    return f"https://www.mlb.com/video/?q={quote_plus(f'PlayId = \"{play_id}\"')}"


def _item_thumbnail_url(item: Dict[str, Any]) -> str:
    cuts = (((item.get("image") or {}).get("cuts")) or [])
    best_src = ""
    best_width = -1
    for cut in cuts:
        if not isinstance(cut, dict):
            continue
        src = str(cut.get("src") or "")
        width = int(cut.get("width") or 0)
        if src and width > best_width:
            best_src = src
            best_width = width
    return best_src


@lru_cache(maxsize=2048)
def _preferred_social_url(page_url: str) -> str:
    if not page_url:
        return ""
    try:
        request = Request(
            page_url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urlopen(request, timeout=20) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return page_url

    for pattern in (
        r'<meta\s+name="twitter:player"\s+content="([^"]+)"',
        r'"embedUrl":"(https://streamable\.com/m/[^"]+)"',
    ):
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return match.group(1).replace("&amp;", "&")
    return page_url


def _token_set(text: str) -> set[str]:
    return {
        token
        for token in _normalize_text(text).split()
        if len(token) >= 3
    }


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-z0-9]+", " ", ascii_text.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _has_review_marker(text: str) -> bool:
    markers = (
        "abs",
        "challenge",
        "challenged",
        "capture review",
        "review",
    )
    return any(marker in text for marker in markers)


def _parse_mlb_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        return None


def _fetch_json(url: str) -> Dict[str, Any]:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))
