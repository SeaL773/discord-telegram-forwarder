from __future__ import annotations

import html
import re
from typing import Any

from .models import FormattedMessage


ICONS = {"CREATED": "🆕", "EDITED": "✏️", "DELETED": "🗑️", "GHOST_PINGED": "👻"}


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first(data: dict[str, Any], *names: str, default: str = "") -> str:
    for name in names:
        value = data.get(name)
        if value is not None:
            return str(value)
    return default


def _content(event: dict[str, Any]) -> str:
    message = _dict(event.get("message"))
    return _first(message, "content", default=_first(event, "content"))


def _reply(message: dict[str, Any]) -> str:
    ref: Any = message.get("referenced_message") or message.get("referencedMessage")
    reference = _dict(message.get("reference"))
    ref = ref or reference.get("referenced_message") or reference.get("referencedMessage")
    message_reference = _dict(message.get("message_reference") or message.get("messageReference"))
    ref = ref or message_reference.get("resolved")
    ref = _dict(ref)
    for _ in range(5):
        nested = next((ref.get(key) for key in ("resolved", "message", "referenced_message", "referencedMessage") if isinstance(ref.get(key), dict)), None)
        if not isinstance(nested, dict):
            break
        ref = nested
    text = _first(ref, "content")
    if not text:
        return ""
    excerpt = text[:240] + ("…" if len(text) > 240 else "")
    return f"\n<blockquote>{html.escape(excerpt)}</blockquote>"


def _edited(event: dict[str, Any], current: str) -> str:
    history = event.get("editHistory") or event.get("edit_history")
    if not isinstance(history, list) or not history:
        history = _dict(event.get("message")).get("editHistory")
    if isinstance(history, list) and history:
        previous = history[-1]
        if isinstance(previous, dict):
            before = _first(previous, "content", "old_content", "oldContent")
        else:
            before = str(previous)
        if before and before != current:
            return f"<b>Before:</b>\n{html.escape(before)}\n\n<b>After:</b>\n{html.escape(current)}"
    return html.escape(current)


_TOKEN = re.compile(r"</?(?:b|blockquote)>|<a href=\"[^\"]*\">|</a>|&(?:amp|lt|gt|quot|#x27);|.", re.DOTALL)


def _utf16_units(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _sanitize_unicode(value: str) -> str:
    return value.encode("utf-16-le", errors="surrogatepass").decode("utf-16-le", errors="replace")


def _visible_units(token: str) -> int:
    if token.startswith("<"):
        return 0
    return _utf16_units(html.unescape(token))


def _stack_after(stack: list[str], token: str) -> list[str]:
    updated = stack.copy()
    if token in ("<b>", "<blockquote>"):
        updated.append(token[1:-1])
    elif token.startswith("<a href="):
        updated.append("a")
    elif token.startswith("</"):
        tag = token[2:-1]
        if updated and updated[-1] == tag:
            updated.pop()
    return updated


def truncate_html(text: str, limit: int) -> str:
    text = _sanitize_unicode(text)
    tokens = _TOKEN.findall(text)
    if sum(_visible_units(token) for token in tokens) <= limit:
        return text
    output: list[str] = []
    stack: list[str] = []
    visible_size = 0
    truncated = False
    for token in tokens:
        updated_stack = _stack_after(stack, token)
        token_units = _visible_units(token)
        if visible_size + token_units + 1 > limit:
            truncated = True
            break
        output.append(token)
        visible_size += token_units
        stack = updated_stack
    if not truncated:
        return "".join(output)
    closing = "".join(f"</{tag}>" for tag in reversed(stack))
    return "".join(output) + "…" + closing


def _sticker_lines(message: dict[str, Any]) -> str:
    raw = message.get("sticker_items") or message.get("stickerItems")
    if isinstance(raw, dict):
        stickers = list(raw.values())
    elif isinstance(raw, list):
        stickers = raw
    else:
        return ""
    lines: list[str] = []
    extensions = {1: "png", 2: "png", 3: "json", 4: "gif", "PNG": "png", "APNG": "png", "LOTTIE": "json", "GIF": "gif"}
    for value in stickers:
        sticker = _dict(value)
        sticker_id = _first(sticker, "id", "sticker_id", "stickerId")
        name = _first(sticker, "name", default=sticker_id or "Sticker")
        raw_format = sticker.get("format_type", sticker.get("formatType"))
        if isinstance(raw_format, str) and raw_format.isdigit():
            raw_format = int(raw_format)
        extension = extensions.get(raw_format, "png")
        escaped_name = html.escape(name)
        if sticker_id:
            host = "media.discordapp.net" if extension == "gif" else "cdn.discordapp.com"
            url = f"https://{host}/stickers/{sticker_id}.{extension}"
            lines.append(f'🏷️ <a href="{html.escape(url, quote=True)}">{escaped_name}</a>')
        else:
            lines.append(f"🏷️ {escaped_name}")
    return "\n" + "\n".join(lines) if lines else ""


def format_event(event: dict[str, Any]) -> FormattedMessage:
    message = _dict(event.get("message"))
    event_type = _first(event, "event_type", default="CREATED")
    channel = _first(message, "channel_name", "channelName", "channel_id", "channelId", default="unknown-channel")
    guild = _first(message, "guild_name", "guildName", "guildId", "guild_id", default="DM")
    author_data = _dict(message.get("author"))
    author = _first(message, "author_name", "authorName", default=_first(author_data, "display_name", "displayName", "username", default="Unknown"))
    content = _content(event)
    body = _edited(event, content) if event_type == "EDITED" else html.escape(content)
    text = (
        f"{ICONS.get(event_type, 'ℹ️')} <b>#{html.escape(channel)}</b> @ {html.escape(guild)}\n"
        f"👤 {html.escape(author)}\n━━━━━━━━━━\n{body}{_reply(message)}{_sticker_lines(message)}"
    )
    return FormattedMessage(truncate_html(text, 4096), truncate_html(text, 1024))


def add_fallbacks(formatted: FormattedMessage, urls: list[str]) -> FormattedMessage:
    unique_urls = list(dict.fromkeys(urls))
    if not unique_urls:
        return formatted
    lines = "\n".join(f'<a href="{html.escape(url, quote=True)}">Attachment</a>' for url in unique_urls)
    return FormattedMessage(truncate_html(f"{formatted.text}\n{lines}", 4096), truncate_html(f"{formatted.caption}\n{lines}", 1024))
