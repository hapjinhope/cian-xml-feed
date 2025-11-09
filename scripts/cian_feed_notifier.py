"""Notify Telegram about the current status of the CIAN XML feed import."""

from __future__ import annotations

import os
from datetime import datetime

import httpx
from dotenv import load_dotenv


load_dotenv()

CIAN_API_BASE_URL = os.getenv("CIAN_API_BASE_URL", "https://public-api.cian.ru")
CIAN_API_TOKEN = os.getenv("CIAN_API_TOKEN")
CIAN_IMAGES_PAGE_SIZE = int(os.getenv("CIAN_IMAGES_PAGE_SIZE", "100"))

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
TG_THREAD_ID = os.getenv("TG_THREAD_ID")
TG_ERROR_USER_ID = os.getenv("TG_ERROR_USER_ID")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_OBJECTS_TABLE", "objects")
SUPABASE_STATUS_FIELD = os.getenv("SUPABASE_STATUS_FIELD", "status")
SUPABASE_URL_FIELD = os.getenv("SUPABASE_URL_FIELD", "cian_url")

CIAN_STATUS_MAP = {
    "Published": "published",
    "Moderate": "moderate",
    "Draft": "draft",
    "Deactivated": "deactivated",
    "RemovedByModerator": "removed",
    "Refused": "refused",
    "Blocked": "blocked",
    "Deleted": "deleted",
    "Sold": "sold",
}

CIAN_TIMEOUT = float(os.getenv("CIAN_API_TIMEOUT", "15"))


def _ensure_env(variable: str) -> str:
    value = os.getenv(variable)
    if not value:
        raise RuntimeError(f"Environment variable {variable} is not set")
    return value


def _client() -> httpx.Client:
    headers = {}
    token = os.getenv("CIAN_API_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(base_url=CIAN_API_BASE_URL.rstrip("/"), timeout=CIAN_TIMEOUT, headers=headers)


def _fetch_json(client: httpx.Client, path: str, *, params: dict | None = None) -> dict:
    response = client.get(path, params=params)
    response.raise_for_status()
    return response.json()


def _map_status(status: str | None) -> str | None:
    if not status:
        return None
    return CIAN_STATUS_MAP.get(status, status.lower())


def _sync_cian_updates(offers: list[dict]) -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return

    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    with httpx.Client(timeout=CIAN_TIMEOUT, headers=headers) as client:
        for offer in offers:
            ext_id = offer.get("externalId")
            cian_url = offer.get("url")
            cian_status = _map_status(offer.get("status"))
            payload: dict[str, str] = {}
            if not ext_id or not cian_url:
                # even если нет url, но есть статус — продолжаем
                if not cian_status:
                    continue
            if cian_url:
                payload[SUPABASE_URL_FIELD] = cian_url
            if cian_status:
                payload[SUPABASE_STATUS_FIELD] = cian_status
            if not payload:
                continue

            try:
                response = client.patch(
                    endpoint,
                    params={"external_id": f"eq.{ext_id}"},
                    json=payload,
                )
                response.raise_for_status()
            except httpx.HTTPError:
                # логгирование можно добавить позже
                continue


def _format_dt(value: str | None) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return value


def build_report() -> tuple[str, bool]:
    with _client() as client:
        last_info = _fetch_json(client, "/v1/get-last-order-info")
        order_report = _fetch_json(client, "/v1/get-order")
        images_report = _fetch_json(
            client,
            "/v1/get-images-report",
            params={"page": 1, "pageSize": CIAN_IMAGES_PAGE_SIZE},
        )

    info = last_info.get("result", {})
    offers_problem = bool(info.get("hasOffersProblems"))
    images_problem = bool(info.get("hasImagesProblems"))
    problems = []
    if offers_problem:
        problems.append("объявления")
    if images_problem:
        problems.append("фотографии")

    offers_details = order_report.get("result", {}).get("offers", [])
    images_details = images_report.get("result", {}).get("items", [])

    _sync_cian_updates(offers_details)

    lines = ["*CIAN · Отчёт по выгрузке*", ""]
    lines.append(f"• Фид: `{info.get('activeFeedUrls', ['—'])[0] if info.get('activeFeedUrls') else '—'}`")
    lines.append(f"• Заказ: `{info.get('orderId', '—')}`")
    lines.append(f"• Проверка: {_format_dt(info.get('lastFeedCheckDate'))}")
    lines.append(f"• Обработка: {_format_dt(info.get('lastProcessDate'))}")
    lines.append(f"• Проблемных объявлений: `{len(offers_details)}`")
    lines.append(f"• Проблемных фото: `{len(images_details)}`")

    ACTION_HINT = "Исправь данные объявления в Supabase и дождись следующего импорта."

    if offers_details:
        lines.append("")
        lines.append("*Проблемные объявления*:")
        for offer in offers_details[:10]:
            ext_id = offer.get("externalId", "—")
            url = offer.get("url")
            errors = offer.get("errors") or offer.get("warnings") or []
            reason = "; ".join(errors) if errors else "Причина не указана"
            status = offer.get("status", "—")
            link = f"[ссылка]({url})" if url else "ссылка недоступна"
            lines.append(
                f"• ID `{ext_id}` · статус `{status}` · {link}\n  Причина: {reason}\n  Действие: {ACTION_HINT}"
            )

    if images_details:
        lines.append("")
        lines.append("*Проблемные фото*:")
        for item in images_details[:10]:
            lines.append(
                f"• ID `{item.get('externalId', '—')}` · {item.get('url','')}\n  Ошибка: {item.get('errorText','—')}"
            )

    mention_needed = bool(offers_details or images_details)
    if mention_needed and TG_ERROR_USER_ID:
        lines.append("")
        lines.append(f"⚠️ [Модератор](tg://user?id={TG_ERROR_USER_ID}) проверь выгрузку!")
    elif not problems:
        lines.append("")
        lines.append("✅ Ошибок не обнаружено")

    return "\n".join(lines), mention_needed


def send_to_telegram(text: str) -> None:
    token = _ensure_env("TG_BOT_TOKEN")
    chat_id = _ensure_env("TG_CHAT_ID")
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if TG_THREAD_ID:
        payload["message_thread_id"] = int(TG_THREAD_ID)

    response = httpx.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=payload,
        timeout=CIAN_TIMEOUT,
    )
    response.raise_for_status()


def main() -> None:
    text, _ = build_report()
    send_to_telegram(text)


if __name__ == "__main__":
    main()
