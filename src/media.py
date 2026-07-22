from __future__ import annotations

import asyncio
import ipaddress
import math
import socket
from collections.abc import Awaitable, Callable, Iterable
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import httpx

from .models import Attachment, DownloadedMedia


ALLOWED_MEDIA_HOSTS = {
    "cdn.discordapp.com",
    "images-ext-1.discordapp.net",
    "images-ext-2.discordapp.net",
    "media.discordapp.net",
    "pbs.twimg.com",
}
DISCORD_PROXY_HOSTS = {
    "cdn.discordapp.com",
    "images-ext-1.discordapp.net",
    "images-ext-2.discordapp.net",
    "media.discordapp.net",
}


def _mapping_values(raw: Any) -> Iterable[Any]:
    return raw.values() if isinstance(raw, dict) else raw if isinstance(raw, list) else []


def _valid_discord_proxy(url: Any) -> bool:
    if not isinstance(url, str) or not url:
        return False
    try:
        parsed = urlsplit(url)
        return (
            parsed.scheme == "https"
            and parsed.hostname in DISCORD_PROXY_HOSTS
            and parsed.port in (None, 443)
            and parsed.username is None
            and parsed.password is None
        )
    except ValueError:
        return False


def _declared_size(item: dict[str, Any]) -> int | None:
    size = item.get("size")
    if not isinstance(size, (int, float)) or isinstance(size, bool) or not math.isfinite(size) or size < 0:
        return None
    return int(size)


def _content_type(item: dict[str, Any]) -> str | None:
    value = item.get("content_type") or item.get("contentType")
    return str(value) if value else None


def _embed_filename(item: dict[str, Any], url: str, embed_index: int, kind: str) -> str:
    explicit = item.get("filename")
    if explicit:
        return str(explicit)
    try:
        name = PurePosixPath(urlsplit(url).path).name
    except ValueError:
        name = ""
    return name or f"embed-{embed_index + 1}-{kind}"


def extract_attachments(event: dict[str, Any]) -> list[Attachment]:
    message = event.get("message", {})
    message = message if isinstance(message, dict) else {}
    raw = message.get("attachments", event.get("attachments", []))
    values = _mapping_values(raw)
    result: list[Attachment] = []
    seen_urls: set[str] = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        url = item.get("url") or item.get("proxy_url") or item.get("proxyUrl")
        if not isinstance(url, str) or not url or url in seen_urls:
            continue
        seen_urls.add(url)
        result.append(Attachment(url, str(item.get("filename") or "attachment"), _content_type(item), _declared_size(item)))

    raw_embeds = message.get("embeds")
    if isinstance(raw_embeds, list):
        embeds = raw_embeds
    elif isinstance(raw_embeds, dict):
        embed_keys = {"author", "title", "description", "fields", "footer", "url", "image", "images", "thumbnail", "video", "provider", "type"}
        embeds = [raw_embeds] if embed_keys.intersection(raw_embeds) else list(raw_embeds.values())
    else:
        embeds = []
    for embed_index, raw_embed in enumerate(embeds[:10]):
        if not isinstance(raw_embed, dict):
            continue
        raw_images = raw_embed.get("images")
        images = raw_images if isinstance(raw_images, list) else list(raw_images.values()) if isinstance(raw_images, dict) else []
        media_items = [("image", raw_embed.get("image"))]
        media_items.extend((f"image-{index + 1}", item) for index, item in enumerate(images))
        media_items.append(("thumbnail", raw_embed.get("thumbnail")))
        for kind, item in media_items:
            if not isinstance(item, dict):
                continue
            proxy_url = item.get("proxy_url") or item.get("proxyUrl")
            source_url = item.get("url")
            url = proxy_url if _valid_discord_proxy(proxy_url) else source_url
            if not isinstance(url, str) or not url or url in seen_urls:
                continue
            seen_urls.add(url)
            result.append(Attachment(url, _embed_filename(item, url, embed_index, kind), _content_type(item), _declared_size(item)))
    return result


def media_kind(content_type: str) -> str:
    if content_type.startswith("image/"):
        return "photo"
    if content_type.startswith("video/"):
        return "video"
    return "document"


async def system_resolver(host: str) -> list[str]:
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    return list({str(info[4][0]) for info in infos})


class MediaHandler:
    def __init__(
        self,
        client: httpx.AsyncClient,
        max_bytes: int,
        timeout_s: float,
        max_attachments: int = 20,
        max_total_bytes: int = 40 * 1024 * 1024,
        allowed_hosts: set[str] | None = None,
        resolver: Callable[[str], Awaitable[list[str]]] = system_resolver,
    ) -> None:
        self.client = client
        self.max_bytes = max_bytes
        self.timeout_s = timeout_s
        self.max_attachments = max_attachments
        self.max_total_bytes = max_total_bytes
        self.allowed_hosts = allowed_hosts or ALLOWED_MEDIA_HOSTS
        self.resolver = resolver

    async def _safe_url(self, url: str) -> None:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname not in self.allowed_hosts or parsed.port not in (None, 443) or parsed.username is not None:
            raise ValueError("unsafe media URL")
        addresses = await self.resolver(parsed.hostname)
        if not addresses:
            raise ValueError("media host did not resolve")
        for address in addresses:
            ip = ipaddress.ip_address(address)
            if not ip.is_global or ip.is_multicast or ip.is_reserved or ip.is_unspecified or ip.is_loopback or ip.is_private or ip.is_link_local:
                raise ValueError("media host resolved to non-global address")

    async def download_all(self, event: dict[str, Any], attachments: list[Attachment] | None = None) -> tuple[list[DownloadedMedia], list[str]]:
        attachments = extract_attachments(event) if attachments is None else attachments
        allowed = attachments[: self.max_attachments]
        failed = [item.url for item in attachments[self.max_attachments :]]
        media: list[DownloadedMedia] = []
        total = 0
        for attachment in allowed:
            try:
                remaining = self.max_total_bytes - total
                if remaining <= 0:
                    raise ValueError("aggregate media too large")
                item = await self.download(attachment, remaining)
                media.append(item)
                total += len(item.data)
            except (httpx.HTTPError, ValueError, TypeError, OSError):
                failed.append(attachment.url)
        return media, failed

    async def download(self, attachment: Attachment, byte_budget: int | None = None) -> DownloadedMedia:
        await self._safe_url(attachment.url)
        limit = self.max_bytes if byte_budget is None else min(self.max_bytes, byte_budget)
        if limit <= 0:
            raise ValueError("media byte budget exhausted")
        if attachment.declared_size is not None and attachment.declared_size > limit:
            raise ValueError("declared media too large")
        chunks: list[bytes] = []
        total = 0
        async with self.client.stream("GET", attachment.url, timeout=self.timeout_s) as response:
            if 300 <= response.status_code < 400:
                raise ValueError("media redirect rejected")
            response.raise_for_status()
            length = response.headers.get("content-length")
            if length is not None:
                try:
                    content_length = int(length)
                except (TypeError, ValueError) as exc:
                    raise ValueError("invalid content length") from exc
                if content_length < 0:
                    raise ValueError("invalid content length")
                if content_length > limit:
                    raise ValueError("header media too large")
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > limit:
                    raise ValueError("streamed media too large")
                chunks.append(chunk)
            response_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
            content_type = response_type or attachment.content_type or "application/octet-stream"
        return DownloadedMedia(attachment, b"".join(chunks), content_type, media_kind(content_type))
