from __future__ import annotations

import json
import mimetypes
import os
import uuid
from pathlib import Path
from typing import List
from urllib.request import Request, urlopen

from .challenges import (
    format_alt_text,
    format_bluesky_clip_embed_text,
    format_bluesky_post_text,
    format_x_clip_post_text,
    format_x_post_text,
)
from .clips import ClipMedia
from .models import AbsChallenge


class Publisher:
    @property
    def delivery_key(self) -> str:
        raise NotImplementedError

    def publish(
        self,
        challenge: AbsChallenge,
        text: str,
        image_path: Path | None,
        *,
        clip: ClipMedia | None = None,
    ) -> None:
        raise NotImplementedError


class DiscordWebhookPublisher(Publisher):
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    @property
    def delivery_key(self) -> str:
        return "discord"

    def publish(
        self,
        challenge: AbsChallenge,
        text: str,
        image_path: Path | None,
        *,
        clip: ClipMedia | None = None,
    ) -> None:
        if clip is not None:
            payload = {"content": f"{text}\n{clip.direct_url}".strip()}
            request = Request(
                self.webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "User-Agent": "mlb-abs-bot/0.1",
                },
                method="POST",
            )
            with urlopen(request, timeout=20) as response:
                response.read()
            return

        if image_path is None:
            raise RuntimeError("Discord image publishing requires an image path.")
        payload_json = json.dumps({"content": text})
        body, content_type = _multipart_encode(
            fields={"payload_json": payload_json},
            file_field="files[0]",
            file_path=image_path,
        )
        request = Request(
            self.webhook_url,
            data=body,
            headers={"Content-Type": content_type, "User-Agent": "mlb-abs-bot/0.1"},
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            response.read()


class BlueSkyPublisher(Publisher):
    def __init__(self, handle: str, app_password: str) -> None:
        self.handle = handle
        self.app_password = app_password

    @property
    def delivery_key(self) -> str:
        return f"bluesky:{self.handle.lower()}"

    def publish(
        self,
        challenge: AbsChallenge,
        text: str,
        image_path: Path | None,
        *,
        clip: ClipMedia | None = None,
    ) -> None:  # pragma: no cover - network integration
        try:
            from atproto import Client, models
        except ImportError as exc:
            raise RuntimeError("BlueSky publishing requires the 'atproto' package.") from exc

        client = Client()
        client.login(self.handle, self.app_password)
        if clip is not None:
            thumb_blob = None
            try:
                thumb_blob = _upload_bluesky_thumb(client, clip.thumbnail_url)
            except Exception:
                thumb_blob = None
            external_kwargs = {
                "uri": clip.social_url or clip.page_url or clip.direct_url,
                "title": _truncate_embed_text(clip.title, 100),
                "description": _truncate_embed_text(clip.description or clip.title, 280),
            }
            if thumb_blob is not None:
                external_kwargs["thumb"] = thumb_blob

            client.send_post(
                text=format_bluesky_clip_embed_text(challenge),
                embed=models.AppBskyEmbedExternal.Main(
                    external=models.AppBskyEmbedExternal.External(**external_kwargs)
                ),
            )
            return

        if image_path is None:
            raise RuntimeError("BlueSky image publishing requires an image path.")
        with image_path.open("rb") as infile:
            blob = client.upload_blob(infile.read())
        client.send_post(
            text=format_bluesky_post_text(challenge),
            embed=models.AppBskyEmbedImages.Main(
                images=[
                    models.AppBskyEmbedImages.Image(
                        image=blob.blob,
                        alt=format_alt_text(challenge),
                    )
                ]
            ),
        )


class XPublisher(Publisher):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        access_token: str,
        access_token_secret: str,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret

    @property
    def delivery_key(self) -> str:
        return "x"

    def publish(
        self,
        challenge: AbsChallenge,
        text: str,
        image_path: Path | None,
        *,
        clip: ClipMedia | None = None,
    ) -> None:  # pragma: no cover - network integration
        try:
            import tweepy
        except ImportError as exc:
            raise RuntimeError("X publishing requires the 'tweepy' package.") from exc

        auth = tweepy.OAuth1UserHandler(
            self.api_key,
            self.api_secret,
            self.access_token,
            self.access_token_secret,
        )
        api_v1 = tweepy.API(auth)
        client = tweepy.Client(
            consumer_key=self.api_key,
            consumer_secret=self.api_secret,
            access_token=self.access_token,
            access_token_secret=self.access_token_secret,
        )
        if clip is not None:
            client.create_tweet(
                text=format_x_clip_post_text(
                    challenge,
                    clip.page_url or clip.social_url or clip.direct_url,
                )
            )
            return

        if image_path is None:
            raise RuntimeError("X image publishing requires an image path.")
        media = api_v1.media_upload(filename=str(image_path))
        client.create_tweet(text=format_x_post_text(challenge), media_ids=[media.media_id_string])


def publishers_from_env() -> List[Publisher]:
    publishers: List[Publisher] = []

    discord_webhook = os.getenv("ABS_DISCORD_WEBHOOK_URL", "").strip()
    if discord_webhook:
        publishers.append(DiscordWebhookPublisher(discord_webhook))

    bluesky_handle = os.getenv("ABS_BLUESKY_HANDLE", "").strip()
    bluesky_password = os.getenv("ABS_BLUESKY_APP_PASSWORD", "").strip()
    if bluesky_handle and bluesky_password:
        publishers.append(BlueSkyPublisher(bluesky_handle, bluesky_password))

    x_api_key = os.getenv("ABS_X_API_KEY", "").strip()
    x_api_secret = os.getenv("ABS_X_API_SECRET", "").strip()
    x_access_token = os.getenv("ABS_X_ACCESS_TOKEN", "").strip()
    x_access_token_secret = os.getenv("ABS_X_ACCESS_TOKEN_SECRET", "").strip()
    if all([x_api_key, x_api_secret, x_access_token, x_access_token_secret]):
        publishers.append(
            XPublisher(
                api_key=x_api_key,
                api_secret=x_api_secret,
                access_token=x_access_token,
                access_token_secret=x_access_token_secret,
            )
        )

    return publishers


def _multipart_encode(
    *,
    fields: dict[str, str],
    file_field: str,
    file_path: Path,
) -> tuple[bytes, str]:
    boundary = f"----absbot{uuid.uuid4().hex}"
    chunks: List[bytes] = []

    for name, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"),
                value.encode("utf-8"),
                b"\r\n",
            ]
        )

    content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
    filename = file_path.name
    chunks.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"\r\n'
                f"Content-Type: {content_type}\r\n\r\n"
            ).encode("utf-8"),
            file_path.read_bytes(),
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )

    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


def _upload_bluesky_thumb(client: object, thumbnail_url: str) -> object | None:
    if not thumbnail_url:
        return None
    request = Request(
        thumbnail_url,
        headers={"User-Agent": "mlb-abs-bot/0.1"},
    )
    with urlopen(request, timeout=20) as response:
        content_type = response.headers.get_content_type()
        if not content_type.startswith("image/"):
            return None
        data = response.read()
    if not data:
        return None
    upload = client.upload_blob(data)
    return upload.blob


def _truncate_embed_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    trimmed = text[: limit - 3].rstrip()
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}..."
