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

    lines = ["*CIAN XML импорт*", ""]
    lines.append(f"• feed: `{info.get('activeFeedUrls', ['—'])[0] if info.get('activeFeedUrls') else '—'}`")
    lines.append(f"• order id: `{info.get('orderId', '—')}`")
    lines.append(f"• last check: {_format_dt(info.get('lastFeedCheckDate'))}")
    lines.append(f"• last process: {_format_dt(info.get('lastProcessDate'))}")
    lines.append(f"• offers with issues: `{len(offers_details)}`")
    lines.append(f"• images with issues: `{len(images_details)}`")

    if offers_details:
        preview = ", ".join(str(item.get("externalId")) for item in offers_details[:5])
        lines.append(f"  ↳ problem offers: `{preview}`")
    if images_details:
        preview_img = ", ".join(str(item.get("externalId")) for item in images_details[:5])
        lines.append(f"  ↳ problem photos: `{preview_img}`")

    mention_needed = bool(problems)
    if mention_needed and TG_ERROR_USER_ID:
        lines.append("")
        lines.append(f"⚠️ [Ответственный](tg://user?id={TG_ERROR_USER_ID}) проверь выгрузку!")
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
