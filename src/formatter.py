from __future__ import annotations

import html
import re
import unicodedata
from typing import Any
from urllib.parse import urlsplit

from .models import FormattedMessage


RICH_HTML_LIMIT = 32768


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


def _discord_markdown(value: str, *, repair_embed: bool = False) -> str:
    escaped = html.escape(value)
    escaped = re.sub(r"\*\*\*([^*\n]+?)\*\*\*", r"<b><i>\1</i></b>", escaped)
    escaped = re.sub(r"\*\*([^*\n]+?)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*\n]+?)\*(?!\*)", r"<i>\1</i>", escaped)
    if repair_embed:
        escaped = re.sub(r"(?m)^\*\*(?!\*)", "", escaped)
        escaped = re.sub(r"(?m)(?<!\*)\*\*$", "", escaped)
    lines: list[str] = []
    for line in escaped.split("\n"):
        heading = re.fullmatch(r"#{1,6}[ \t]+(.+)", line)
        lines.append(f"<b>{heading.group(1)}</b>" if heading else line)
    return "\n".join(lines)


_BOLD_DELIMITER = re.compile(r"(?<!\*)\*\*(?!\*)")
_BOLD_OPENING = re.compile(r"^[ \t]*\*\*(?!\*)")
_BOLD_CLOSING = re.compile(r"(?<!\*)\*\*[ \t]*$")


def _pair_embed_paragraph_bold(value: str) -> str:
    parts = re.split(r"(\n+)", value)
    for opening_index in range(0, len(parts), 2):
        opening_part = parts[opening_index]
        if len(_BOLD_DELIMITER.findall(opening_part)) != 1 or _BOLD_OPENING.search(opening_part) is None:
            continue
        closing_index: int | None = None
        for candidate_index in range(opening_index + 2, len(parts), 2):
            candidate = parts[candidate_index]
            delimiter_count = len(_BOLD_DELIMITER.findall(candidate))
            if delimiter_count == 0:
                continue
            if delimiter_count == 1 and _BOLD_CLOSING.search(candidate) is not None:
                closing_index = candidate_index
            break
        if closing_index is None:
            continue
        parts[opening_index] = _BOLD_OPENING.sub("", parts[opening_index], count=1)
        parts[closing_index] = _BOLD_CLOSING.sub("", parts[closing_index], count=1)
        for paragraph_index in range(opening_index, closing_index + 1, 2):
            if parts[paragraph_index]:
                parts[paragraph_index] = f"**{parts[paragraph_index]}**"
    return "".join(parts)


def _classic_embed_markdown(value: str) -> str:
    def plain_markdown(plain: str) -> str:
        repaired = _pair_embed_paragraph_bold(plain)
        return "".join(
            part if index % 2 else _discord_markdown(part, repair_embed=True)
            for index, part in enumerate(re.split(r"(\n{2,})", repaired))
        )

    output: list[str] = []
    cursor = 0
    for match in re.finditer(r"```[^\n`]*\n?.*?```", value, re.DOTALL):
        output.append(plain_markdown(value[cursor:match.start()]))
        output.append(html.escape(match.group(0)))
        cursor = match.end()
    output.append(plain_markdown(value[cursor:]))
    return "".join(output)


def _telegram_hashtag(value: str) -> str:
    output: list[str] = []
    separator = False
    for character in unicodedata.normalize("NFKC", value).strip():
        category = unicodedata.category(character)
        if character == "_":
            separator = True
        elif category[0] in {"L", "M", "N"}:
            if separator and output and output[-1] != "_":
                output.append("_")
            output.append(character)
            separator = False
        else:
            separator = True
    slug = "".join(output).strip("_")
    return slug if slug and not slug.isdecimal() else "channel"


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
            lines.append(_classic_embed_markdown(description))

        raw_fields = embed.get("fields")
        fields = raw_fields if isinstance(raw_fields, list) else list(raw_fields.values()) if isinstance(raw_fields, dict) else []
        for raw_field in fields[:25]:
            field = _dict(raw_field)
            name = unique_text(field.get("name"))
            value = unique_text(field.get("value"))
            if name and value:
                lines.append(f"<b>{html.escape(name)}</b>\n{_classic_embed_markdown(value)}")
            elif name:
                lines.append(f"<b>{html.escape(name)}</b>")
            elif value:
                lines.append(_classic_embed_markdown(value))

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


def _reply_text(message: dict[str, Any]) -> str:
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
    return text[:240] + ("…" if len(text) > 240 else "") if text else ""


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
            lines.append(f'<a href="{html.escape(url, quote=True)}">{escaped_name}</a>')
        else:
            lines.append(escaped_name)
    return "\n" + "\n".join(lines) if lines else ""


def _edit_before(event: dict[str, Any], current: str) -> str:
    history = event.get("editHistory") or event.get("edit_history")
    if not isinstance(history, list) or not history:
        history = _dict(event.get("message")).get("editHistory")
    if isinstance(history, list) and history:
        previous = history[-1]
        before = _first(previous, "content", "old_content", "oldContent") if isinstance(previous, dict) else str(previous)
        if before and before != current:
            return before
    return ""


def _paragraphs(value: str, *, repair_embed: bool = False) -> str:
    if not value:
        return ""
    output: list[str] = []
    cursor = 0
    for match in re.finditer(r"```([^\n`]*)\n?(.*?)```", value, re.DOTALL):
        plain = value[cursor:match.start()]
        if repair_embed:
            plain = _pair_embed_paragraph_bold(plain)
        output.extend(f"<p>{_discord_markdown(part, repair_embed=repair_embed)}</p>" for part in re.split(r"\n{2,}", plain) if part)
        language = match.group(1).strip()
        language_attribute = f' class="language-{html.escape(language, quote=True)}"' if re.fullmatch(r"[A-Za-z0-9_+.-]{1,32}", language) else ""
        output.append(f"<pre><code{language_attribute}>{html.escape(match.group(2))}</code></pre>")
        cursor = match.end()
    plain = value[cursor:]
    if repair_embed:
        plain = _pair_embed_paragraph_bold(plain)
    output.extend(f"<p>{_discord_markdown(part, repair_embed=repair_embed)}</p>" for part in re.split(r"\n{2,}", plain) if part)
    return "".join(output)


def _rich_embeds(message: dict[str, Any], content: str) -> str:
    seen = {content} if content else set()
    sections: list[str] = []

    def unique(value: Any) -> str:
        text = "" if value is None else str(value)
        if not text or text in seen:
            return ""
        seen.add(text)
        return text

    for embed in _embeds(message):
        parts: list[str] = []
        author = unique(_dict(embed.get("author")).get("name"))
        title = unique(embed.get("title"))
        source = _safe_link(embed.get("url"))
        heading = title or author
        if heading:
            label = html.escape(heading)
            parts.append(f'<h4><a href="{html.escape(source, quote=True)}">{label}</a></h4>' if source and title else f"<h4>{label}</h4>")
        if author and title:
            parts.append(f"<p><i>{html.escape(author)}</i></p>")
        description = unique(embed.get("description"))
        if description:
            parts.append(_paragraphs(description, repair_embed=True))
        raw_fields = embed.get("fields")
        fields = raw_fields if isinstance(raw_fields, list) else list(raw_fields.values()) if isinstance(raw_fields, dict) else []
        for raw_field in fields[:25]:
            field = _dict(raw_field)
            name = unique(field.get("name"))
            value = unique(field.get("value"))
            if name:
                parts.append(f"<p><b>{html.escape(name)}</b></p>")
            if value:
                parts.append(_paragraphs(value, repair_embed=True))
        footer = unique(_dict(embed.get("footer")).get("text"))
        if footer:
            parts.append(f"<p><i>{html.escape(footer)}</i></p>")
        if source and not title:
            parts.append(f'<p><a href="{html.escape(source, quote=True)}">Source</a></p>')
        if parts:
            sections.append("<hr/>" + "".join(parts))
    return "".join(sections)


_RICH_TOKEN = re.compile(r"<[^>]+>|&(?:amp|lt|gt|quot|#x27);|.", re.DOTALL)
_RICH_OPEN = re.compile(r"<([a-z][a-z0-9-]*)(?:\s[^>]*)?\s*/?>", re.IGNORECASE)
_RICH_BLOCK_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "pre", "footer", "hr", "blockquote", "img", "video", "tg-collage"}


def truncate_rich_html(value: str, limit: int = RICH_HTML_LIMIT, max_blocks: int = 500) -> str:
    if limit < 0 or max_blocks < 0:
        raise ValueError("rich message limits must be nonnegative")
    value = _sanitize_unicode(value)
    output: list[str] = []
    stack: list[str] = []
    raw_size = 0
    utf16_size = 0
    block_count = 0
    for token in _RICH_TOKEN.findall(value):
        closing_match = re.fullmatch(r"</([a-z][a-z0-9-]*)>", token, re.IGNORECASE)
        opening_match = _RICH_OPEN.fullmatch(token)
        next_stack = stack.copy()
        if closing_match:
            tag = closing_match.group(1).lower()
            if next_stack and next_stack[-1] == tag:
                next_stack.pop()
        elif opening_match:
            tag = opening_match.group(1).lower()
            if tag in _RICH_BLOCK_TAGS:
                if block_count >= max_blocks:
                    return "".join(output) + "".join(f"</{open_tag}>" for open_tag in reversed(stack))
                block_count += 1
            if not token.endswith("/>"):
                next_stack.append(tag)
        closing = "".join(f"</{tag}>" for tag in reversed(next_stack))
        suffix = "…" + closing
        token_raw_size = len(token)
        token_utf16_size = _utf16_units(token)
        if raw_size + token_raw_size + len(suffix) > limit or utf16_size + token_utf16_size + _utf16_units(suffix) > limit:
            return "".join(output) + "…" + "".join(f"</{tag}>" for tag in reversed(stack))
        output.append(token)
        raw_size += token_raw_size
        utf16_size += token_utf16_size
        stack = next_stack
    return "".join(output)


def _event_metadata(guild: str, event_type: str) -> str:
    labels = {"EDITED": "Edited", "DELETED": "Deleted", "GHOST_PINGED": "Ghost ping"}
    label = labels.get(event_type)
    return html.escape(guild) + (f" · {label}" if label else "")


def format_event(event: dict[str, Any], extracted_media_count: int = 0) -> FormattedMessage:
    _ = extracted_media_count
    message = _dict(event.get("message"))
    event_type = _first(event, "event_type", default="CREATED")
    channel = _first(message, "channel_name", "channelName", default=_first(event, "channel_name", "channelName", default=_first(message, "channel_id", "channelId", default="unknown-channel")))
    guild = _first(message, "guild_name", "guildName", default=_first(event, "guild_name", "guildName", default=_first(message, "guildId", "guild_id", default="DM")))
    author_data = _dict(message.get("author"))
    author = _first(message, "author_name", "authorName", default=_first(author_data, "display_name", "displayName", "username", default="Unknown"))
    content = _content(event)
    channel_hashtag = _telegram_hashtag(channel)
    body = _edited(event, content) if event_type == "EDITED" else _discord_markdown(content)
    metadata = _event_metadata(guild, event_type)
    text = (
        f"<b>{html.escape(author)}</b> in <b>#{channel_hashtag}</b>\n"
        f"{body}{_reply(message)}{_sticker_lines(message)}{_embed_lines(message, content)}"
        f"\n<i>{metadata}</i>"
    )
    classic = FormattedMessage(truncate_html(text, 4096), truncate_html(text, 1024))
    before = _edit_before(event, content)
    rich_parts = [f"<h3>#{channel_hashtag}</h3>"]
    reply = _reply_text(message)
    if reply:
        rich_parts.append(f"<blockquote>{_discord_markdown(reply)}</blockquote>")
    if before:
        rich_parts.append("<h4>Before</h4>" + _paragraphs(before) + "<h4>After</h4>")
    rich_parts.append(_paragraphs(content))
    stickers = _sticker_lines(message).strip()
    if stickers:
        rich_parts.append(f"<p>{stickers}</p>")
    rich_parts.append(_rich_embeds(message, content))
    rich_parts.append(f"<footer><i>{html.escape(author)} · {metadata}</i></footer>")
    return FormattedMessage(classic.text, classic.caption, "editorial", truncate_rich_html("".join(rich_parts)))


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

    return FormattedMessage(append_links(formatted.text, 4096), append_links(formatted.caption, 1024), formatted.style, formatted.rich_html)


def rich_fallback_html(rich_html: str, urls: list[str]) -> str:
    unique_urls = list(dict.fromkeys(urls))
    if not unique_urls:
        return rich_html
    links = "".join(
        f'<p><a href="{html.escape(safe, quote=True)}">Attachment</a></p>' if (safe := _safe_link(url)) else "<p>Attachment unavailable</p>"
        for url in unique_urls
    )
    return truncate_rich_html(rich_html + "<hr/>" + links)
