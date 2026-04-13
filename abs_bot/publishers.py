from __future__ import annotations

import json
import io
import mimetypes
import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from typing import List
from urllib.request import Request, urlopen

from .challenges import (
    format_alt_text,
    format_clip_alt_text,
    format_bluesky_clip_embed_text,
    format_bluesky_post_text,
    format_x_clip_native_post_text,
    format_x_clip_post_text,
    format_x_post_text,
)
from .clips import ClipMedia
from .models import AbsChallenge


class Publisher:
    @property
    def delivery_key(self) -> str:
        raise NotImplementedError

    def publish_media_post(
        self,
        text: str,
        image_path: Path | None = None,
        *,
        alt_text: str = "",
    ) -> None:
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

    def publish_media_post(
        self,
        text: str,
        image_path: Path | None = None,
        *,
        alt_text: str = "",
    ) -> None:
        if image_path is None:
            payload = {"content": text}
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
        if clip is not None:
            if not _is_mp4_url(clip.direct_url):
                try:
                    from atproto import Client
                except ImportError as exc:
                    raise RuntimeError("BlueSky publishing requires the 'atproto' package.") from exc
                client = Client()
                client.login(self.handle, self.app_password)
                _publish_bluesky_external_clip(client, challenge, clip)
                return
            clip_path = _download_clip_to_temp(clip.direct_url)
            try:
                session = _create_bluesky_session(self.handle, self.app_password)
                try:
                    video_blob = _upload_bluesky_native_video(session, clip_path)
                except Exception as native_exc:
                    try:
                        video_blob = _upload_bluesky_video_blob(session, clip_path)
                    except Exception as blob_exc:
                        raise RuntimeError(
                            "BlueSky native video upload failed via both the video service "
                            f"and direct uploadBlob fallback. Service error: {native_exc}; "
                            f"uploadBlob error: {blob_exc}"
                        ) from blob_exc
                _create_bluesky_video_post(
                    session=session,
                    text=format_bluesky_clip_embed_text(challenge),
                    video_blob=video_blob,
                    alt_text=format_clip_alt_text(challenge),
                    aspect_ratio=_clip_aspect_ratio(clip),
                )
            finally:
                clip_path.unlink(missing_ok=True)
            return

        try:
            from atproto import Client, models
        except ImportError as exc:
            raise RuntimeError("BlueSky publishing requires the 'atproto' package.") from exc

        client = Client()
        client.login(self.handle, self.app_password)
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

    def publish_media_post(
        self,
        text: str,
        image_path: Path | None = None,
        *,
        alt_text: str = "",
    ) -> None:  # pragma: no cover - network integration
        try:
            from atproto import Client, models
        except ImportError as exc:
            raise RuntimeError("BlueSky publishing requires the 'atproto' package.") from exc

        client = Client()
        client.login(self.handle, self.app_password)
        if image_path is None:
            client.send_post(text=text)
            return

        with image_path.open("rb") as infile:
            blob = client.upload_blob(infile.read())
        client.send_post(
            text=text,
            embed=models.AppBskyEmbedImages.Main(
                images=[
                    models.AppBskyEmbedImages.Image(
                        image=blob.blob,
                        alt=alt_text or "",
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
            if not _is_mp4_url(clip.direct_url):
                client.create_tweet(
                    text=format_x_clip_post_text(
                        challenge,
                        clip.page_url or clip.social_url or clip.direct_url,
                    )
                )
                return
            clip_path = _download_clip_to_temp(clip.direct_url)
            try:
                media = api_v1.chunked_upload(
                    filename=str(clip_path),
                    file_type="video/mp4",
                    wait_for_async_finalize=True,
                    media_category="tweet_video",
                )
            finally:
                clip_path.unlink(missing_ok=True)
            client.create_tweet(
                text=format_x_clip_native_post_text(challenge),
                media_ids=[media.media_id_string],
            )
            return

        if image_path is None:
            raise RuntimeError("X image publishing requires an image path.")
        media = api_v1.media_upload(filename=str(image_path))
        client.create_tweet(text=format_x_post_text(challenge), media_ids=[media.media_id_string])

    def publish_media_post(
        self,
        text: str,
        image_path: Path | None = None,
        *,
        alt_text: str = "",
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
        if image_path is None:
            client.create_tweet(text=text)
            return
        media = api_v1.media_upload(filename=str(image_path))
        client.create_tweet(text=text, media_ids=[media.media_id_string])


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


def _download_clip_to_temp(clip_url: str) -> Path:
    suffix = Path(urlparse(clip_url).path).suffix or ".mp4"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        request = Request(
            clip_url,
            headers={"User-Agent": "mlb-abs-bot/0.1"},
        )
        with urlopen(request, timeout=60) as response:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)
        return Path(tmp.name)


def _is_mp4_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".mp4")


def _clip_aspect_ratio(clip: ClipMedia) -> dict[str, int] | None:
    if not clip.thumbnail_url:
        return None
    try:
        from PIL import Image
    except ImportError:
        return None
    request = Request(
        clip.thumbnail_url,
        headers={"User-Agent": "mlb-abs-bot/0.1"},
    )
    try:
        with urlopen(request, timeout=20) as response:
            data = response.read()
    except Exception:
        return None
    if not data:
        return None
    try:
        with Image.open(io.BytesIO(data)) as image:
            width, height = image.size
    except Exception:
        return None
    if width <= 0 or height <= 0:
        return None
    return {"width": int(width), "height": int(height)}


def _create_bluesky_session(handle: str, app_password: str) -> dict:
    request = Request(
        "https://bsky.social/xrpc/com.atproto.server.createSession",
        data=json.dumps(
            {
                "identifier": handle,
                "password": app_password,
            }
        ).encode("utf-8"),
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "mlb-abs-bot/0.1",
        },
        method="POST",
    )
    return _read_json_response(request)


def _upload_bluesky_native_video(session: dict, clip_path: Path) -> dict:
    service_token = _get_bluesky_service_auth(
        access_jwt=str(session["accessJwt"]),
        audience=f"did:web:{urlparse('https://bsky.social').hostname}",
        lxm="com.atproto.repo.uploadBlob",
    )
    upload_url = (
        "https://video.bsky.app/xrpc/app.bsky.video.uploadVideo?"
        + urlencode(
            {
                "did": str(session["did"]),
                "name": clip_path.name,
            }
        )
    )
    request = Request(
        upload_url,
        data=clip_path.read_bytes(),
        headers={
            "Authorization": f"Bearer {service_token}",
            "Content-Type": "video/mp4",
            "Content-Length": str(clip_path.stat().st_size),
            "Connection": "close",
            "User-Agent": "mlb-abs-bot/0.1",
        },
        method="POST",
    )
    payload = _read_json_response(request, allow_error_json=True)
    job_status = _extract_bluesky_job_status(payload)
    blob = _extract_bluesky_blob(job_status)
    if blob is not None:
        return blob
    job_id = str(job_status.get("jobId") or "")
    if not job_id:
        raise RuntimeError("BlueSky video upload did not return a job ID.")

    deadline = time.monotonic() + 300.0
    while time.monotonic() < deadline:
        status_request = Request(
            "https://video.bsky.app/xrpc/app.bsky.video.getJobStatus?" + urlencode({"jobId": job_id}),
            headers={"User-Agent": "mlb-abs-bot/0.1"},
            method="GET",
        )
        status_payload = _read_json_response(status_request, allow_error_json=True)
        status = _extract_bluesky_job_status(status_payload)
        blob = _extract_bluesky_blob(status)
        if blob is not None:
            return blob

        state = str(status.get("state") or "").lower()
        if state == "failed":
            message = str(status.get("message") or "BlueSky video processing failed.")
            raise RuntimeError(message)

        time.sleep(1.0)

    raise RuntimeError("Timed out waiting for BlueSky video processing.")


def _upload_bluesky_video_blob(session: dict, clip_path: Path) -> dict:
    request = Request(
        "https://bsky.social/xrpc/com.atproto.repo.uploadBlob",
        data=clip_path.read_bytes(),
        headers={
            "Authorization": f"Bearer {session['accessJwt']}",
            "Content-Type": "video/mp4",
            "Content-Length": str(clip_path.stat().st_size),
            "Connection": "close",
            "User-Agent": "mlb-abs-bot/0.1",
        },
        method="POST",
    )
    payload = _read_json_response(request)
    blob = payload.get("blob")
    if not isinstance(blob, dict):
        raise RuntimeError("BlueSky uploadBlob response did not include a blob.")
    return blob


def _get_bluesky_service_auth(*, access_jwt: str, audience: str, lxm: str) -> str:
    expires_at = int((datetime.now(timezone.utc) + timedelta(minutes=30)).timestamp())
    request = Request(
        "https://bsky.social/xrpc/com.atproto.server.getServiceAuth?"
        + urlencode({"aud": audience, "lxm": lxm, "exp": expires_at}),
        headers={
            "Authorization": f"Bearer {access_jwt}",
            "User-Agent": "mlb-abs-bot/0.1",
        },
        method="GET",
    )
    payload = _read_json_response(request)
    token = str(payload.get("token") or "")
    if not token:
        raise RuntimeError("BlueSky did not return a service auth token.")
    return token


def _create_bluesky_video_post(
    *,
    session: dict,
    text: str,
    video_blob: dict,
    alt_text: str,
    aspect_ratio: dict[str, int] | None,
) -> None:
    embed = {
        "$type": "app.bsky.embed.video",
        "video": video_blob,
        "alt": alt_text,
    }
    if aspect_ratio is not None:
        embed["aspectRatio"] = aspect_ratio

    payload = {
        "repo": str(session["did"]),
        "collection": "app.bsky.feed.post",
        "record": {
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "langs": ["en"],
            "embed": embed,
        },
    }
    request = Request(
        "https://bsky.social/xrpc/com.atproto.repo.createRecord",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {session['accessJwt']}",
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "mlb-abs-bot/0.1",
        },
        method="POST",
    )
    _read_json_response(request)


def _publish_bluesky_external_clip(client: object, challenge: AbsChallenge, clip: ClipMedia) -> None:
    try:
        from atproto import models
    except ImportError as exc:
        raise RuntimeError("BlueSky publishing requires the 'atproto' package.") from exc

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


def _extract_bluesky_job_status(payload: dict) -> dict:
    job_status = payload.get("jobStatus")
    if isinstance(job_status, dict):
        return job_status
    return payload


def _extract_bluesky_blob(payload: dict) -> dict | None:
    blob = payload.get("blob")
    if isinstance(blob, dict):
        return blob
    error = payload.get("error")
    if error == "already_exists":
        inner_blob = payload.get("blob")
        if isinstance(inner_blob, dict):
            return inner_blob
    return None


def _read_json_response(request: Request, *, allow_error_json: bool = False) -> dict:
    try:
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if allow_error_json:
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                pass
        raise RuntimeError(f"HTTP {exc.code}: {body}") from exc


def _truncate_embed_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    trimmed = text[: limit - 3].rstrip()
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}..."
