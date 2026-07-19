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


def truncate_html(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    output: list[str] = []
    stack: list[str] = []
    size = 0
    truncated = False
    for token in _TOKEN.findall(text):
        closing = "".join(f"</{tag}>" for tag in reversed(stack))
        if size + len(token) + 1 + len(closing) > limit:
            truncated = True
            break
        output.append(token)
        size += len(token)
        if token in ("<b>", "<blockquote>"):
            stack.append(token[1:-1])
        elif token.startswith("<a href="):
            stack.append("a")
        elif token.startswith("</"):
            tag = token[2:-1]
            if stack and stack[-1] == tag:
                stack.pop()
    if not truncated:
        return "".join(output)
    closing = "".join(f"</{tag}>" for tag in reversed(stack))
    return "".join(output) + "…" + closing


def format_event(event: dict[str, Any]) -> FormattedMessage:
    message = _dict(event.get("message"))
    event_type = _first(event, "event_type", default="CREATED")
    channel = _first(message, "channel_name", "channelName", default="unknown-channel")
    guild = _first(message, "guild_name", "guildName", default="DM")
    author_data = _dict(message.get("author"))
    author = _first(message, "author_name", "authorName", default=_first(author_data, "display_name", "displayName", "username", default="Unknown"))
    content = _content(event)
    body = _edited(event, content) if event_type == "EDITED" else html.escape(content)
    text = (
        f"{ICONS.get(event_type, 'ℹ️')} <b>#{html.escape(channel)}</b> @ {html.escape(guild)}\n"
        f"👤 {html.escape(author)}\n━━━━━━━━━━\n{body}{_reply(message)}"
    )
    return FormattedMessage(truncate_html(text, 4096), truncate_html(text, 1024))


def add_fallbacks(formatted: FormattedMessage, urls: list[str]) -> FormattedMessage:
    if not urls:
        return formatted
    lines = "\n".join(f'<a href="{html.escape(url, quote=True)}">Attachment</a>' for url in urls)
    return FormattedMessage(truncate_html(f"{formatted.text}\n{lines}", 4096), truncate_html(f"{formatted.caption}\n{lines}", 1024))
