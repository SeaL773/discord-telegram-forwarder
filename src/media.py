from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable
from typing import Any
from urllib.parse import urlsplit

import httpx

from .models import Attachment, DownloadedMedia


ALLOWED_MEDIA_HOSTS = {"cdn.discordapp.com", "media.discordapp.net"}


def extract_attachments(event: dict[str, Any]) -> list[Attachment]:
    message = event.get("message", {})
    message = message if isinstance(message, dict) else {}
    raw = message.get("attachments", event.get("attachments", []))
    values: Iterable[Any] = raw.values() if isinstance(raw, dict) else raw if isinstance(raw, list) else []
    result: list[Attachment] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        url = item.get("url") or item.get("proxy_url") or item.get("proxyUrl")
        if not isinstance(url, str) or not url:
            continue
        size = item.get("size")
        result.append(Attachment(url, str(item.get("filename") or "attachment"), str(item.get("content_type") or item.get("contentType") or "") or None, int(size) if isinstance(size, (int, float)) else None))
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

    async def download_all(self, event: dict[str, Any]) -> tuple[list[DownloadedMedia], list[str]]:
        attachments = extract_attachments(event)
        allowed = attachments[: self.max_attachments]
        failed = [item.url for item in attachments[self.max_attachments :]]
        media: list[DownloadedMedia] = []
        total = 0
        for attachment in allowed:
            try:
                item = await self.download(attachment)
                if total + len(item.data) > self.max_total_bytes:
                    raise ValueError("aggregate media too large")
                media.append(item)
                total += len(item.data)
            except (httpx.HTTPError, ValueError, TypeError, OSError):
                failed.append(attachment.url)
        return media, failed

    async def download(self, attachment: Attachment) -> DownloadedMedia:
        await self._safe_url(attachment.url)
        if attachment.declared_size is not None and attachment.declared_size > self.max_bytes:
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
                    if int(length) > self.max_bytes:
                        raise ValueError("header media too large")
                except (TypeError, ValueError) as exc:
                    raise ValueError("invalid content length") from exc
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > self.max_bytes:
                    raise ValueError("streamed media too large")
                chunks.append(chunk)
            response_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
            content_type = response_type or attachment.content_type or "application/octet-stream"
        return DownloadedMedia(attachment, b"".join(chunks), content_type, media_kind(content_type))
