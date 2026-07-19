from __future__ import annotations

import html
import re
import unicodedata
from typing import Any
from urllib.parse import urlsplit

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


def _embeds(message: dict[str, Any]) -> list[dict[str, Any]]:
    raw = message.get("embeds")
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, dict):
        embed_keys = {"author", "title", "description", "fields", "footer", "url", "image", "images", "thumbnail", "video", "provider", "type"}
        values = [raw] if embed_keys.intersection(raw) else list(raw.values())
    else:
        values = []
    return [value for value in values[:10] if isinstance(value, dict)]


def _safe_link(value: Any) -> str:
    if not isinstance(value, str) or not value:
        return ""
    if any(
        not character.isascii()
        or character.isspace()
        or unicodedata.category(character).startswith("C")
        for character in value
    ):
        return ""
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username is not None or parsed.password is not None or "\\" in parsed.netloc:
            return ""
        if re.search(r"%(?![0-9A-Fa-f]{2})", value):
            return ""
        _ = parsed.port
    except ValueError:
        return ""
    return value


def _discord_markdown(value: str) -> str:
    escaped = html.escape(value)
    escaped = re.sub(r"\*\*\*([^*\n]+?)\*\*\*", r"<b><i>\1</i></b>", escaped)
    escaped = re.sub(r"\*\*([^*\n]+?)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*\n]+?)\*(?!\*)", r"<i>\1</i>", escaped)
    lines: list[str] = []
    for line in escaped.split("\n"):
        heading = re.fullmatch(r"#{1,6}[ \t]+(.+)", line)
        lines.append(f"<b>{heading.group(1)}</b>" if heading else line)
    return "\n".join(lines)


def _embed_lines(message: dict[str, Any], content: str) -> str:
    seen = {content} if content else set()
    sections: list[str] = []

    def unique_text(value: Any) -> str:
        if value is None:
            return ""
        text = str(value)
        if not text or text in seen:
            return ""
        seen.add(text)
        return text

    for embed in _embeds(message):
        lines: list[str] = []
        author = unique_text(_dict(embed.get("author")).get("name"))
        if author:
            lines.append(html.escape(author))

        source_url = _safe_link(embed.get("url"))
        title = unique_text(embed.get("title"))
        title_used_url = bool(title and source_url)
        if title:
            escaped_title = html.escape(title)
            if source_url:
                lines.append(f'<a href="{html.escape(source_url, quote=True)}">{escaped_title}</a>')
            else:
                lines.append(f"<b>{escaped_title}</b>")

        description = unique_text(embed.get("description"))
        if description:
            lines.append(_discord_markdown(description))

        raw_fields = embed.get("fields")
        fields = raw_fields if isinstance(raw_fields, list) else list(raw_fields.values()) if isinstance(raw_fields, dict) else []
        for raw_field in fields[:25]:
            field = _dict(raw_field)
            name = unique_text(field.get("name"))
            value = unique_text(field.get("value"))
            if name and value:
                lines.append(f"<b>{html.escape(name)}</b>\n{_discord_markdown(value)}")
            elif name:
                lines.append(f"<b>{html.escape(name)}</b>")
            elif value:
                lines.append(_discord_markdown(value))

        footer = unique_text(_dict(embed.get("footer")).get("text"))
        if footer:
            lines.append(html.escape(footer))
        if source_url and not title_used_url:
            lines.append(f'<a href="{html.escape(source_url, quote=True)}">Source</a>')
        if lines:
            sections.append("\n".join(lines))
    return "\n\n" + "\n\n".join(sections) if sections else ""


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
    return f"\n<blockquote>{_discord_markdown(excerpt)}</blockquote>"


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
            return f"<b>Before:</b>\n{_discord_markdown(before)}\n\n<b>After:</b>\n{_discord_markdown(current)}"
    return _discord_markdown(current)


_TOKEN = re.compile(r"</?(?:b|i|blockquote)>|<a href=\"[^\"]*\">|</a>|&(?:amp|lt|gt|quot|#x27);|.", re.DOTALL)


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
    if token in ("<b>", "<i>", "<blockquote>"):
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
        extension = extensions.get(raw_format, "png") if isinstance(raw_format, (int, str)) else "png"
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
    channel = _first(message, "channel_name", "channelName", default=_first(event, "channel_name", "channelName", default=_first(message, "channel_id", "channelId", default="unknown-channel")))
    guild = _first(message, "guild_name", "guildName", default=_first(event, "guild_name", "guildName", default=_first(message, "guildId", "guild_id", default="DM")))
    author_data = _dict(message.get("author"))
    author = _first(message, "author_name", "authorName", default=_first(author_data, "display_name", "displayName", "username", default="Unknown"))
    content = _content(event)
    body = _edited(event, content) if event_type == "EDITED" else _discord_markdown(content)
    text = (
        f"{ICONS.get(event_type, 'ℹ️')} <b>#{html.escape(channel)}</b> @ {html.escape(guild)}\n"
        f"👤 {html.escape(author)}\n━━━━━━━━━━\n{body}{_reply(message)}{_sticker_lines(message)}{_embed_lines(message, content)}"
    )
    return FormattedMessage(truncate_html(text, 4096), truncate_html(text, 1024))


def add_fallbacks(formatted: FormattedMessage, urls: list[str]) -> FormattedMessage:
    unique_urls = list(dict.fromkeys(urls))
    if not unique_urls:
        return formatted

    def append_links(body: str, limit: int) -> str:
        safe_urls = [_safe_link(url) for url in unique_urls]
        lines = [f'<a href="{html.escape(url, quote=True)}">Attachment</a>' if url else "Attachment unavailable" for url in safe_urls]
        labels = ["Attachment" if url else "Attachment unavailable" for url in safe_urls]
        for count in range(len(lines), 0, -1):
            suffix = "\n" + "\n".join(lines[:count])
            suffix_units = _utf16_units("\n" + "\n".join(labels[:count]))
            if suffix_units > limit:
                continue
            prefix_budget = limit - suffix_units
            prefix = "" if prefix_budget == 0 else truncate_html(body, prefix_budget)
            return prefix + suffix
        return truncate_html(body, limit)

    return FormattedMessage(append_links(formatted.text, 4096), append_links(formatted.caption, 1024))
