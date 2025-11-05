# main.py
import logging
import os
import re
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from supabase import Client, create_client
import xml.etree.ElementTree as ET
from xml.dom import minidom

# --------------------------------------
# Логирование
# --------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------
# Инициализация FastAPI и Supabase
# --------------------------------------
app = FastAPI(title="CIAN XML Feed Generator")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL или SUPABASE_KEY не заданы в переменных окружения")
    # Не падаем при импорте модуля, но запросы вернут 500
    supabase: Client | None = None
else:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# --------------------------------------
# Вспомогательные функции
# --------------------------------------

def escape_xml(text: str | None) -> str:
    """Экранирование XML-символов."""
    if not text:
        return ""
    text = str(text)
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    text = text.replace('"', "&quot;")
    text = text.replace("'", "&apos;")
    return text


def parse_price(price_str: str | None) -> str:
    """Извлечение цены из строки. Возвращает только цифры без пробелов."""
    if not price_str:
        return "0"
    match = re.search(r"(\d+[\s\d]*)", str(price_str))
    if match:
        return match.group(1).replace(" ", "")
    return "0"


def _parse_float_from_details(pattern: str, details: str | None) -> str:
    """Вспомогательный парсер площадей из строки с деталями квартиры."""
    if not details:
        return "0"
    match = re.search(pattern, details)
    if match:
        return match.group(1).replace(",", ".")
    return "0"


# --------------------------------------
# Работа с Supabase
# --------------------------------------

def get_apartments_from_supabase() -> list[dict]:
    """Получаем все квартиры из таблицы `objects` в Supabase."""
    if supabase is None:
        raise RuntimeError("Supabase клиент не инициализирован")

    try:
        response = supabase.table("objects").select("*").execute()
        data = response.data or []
        logger.info("Из Supabase получено объектов: %s", len(data))
        return data
    except Exception as e:  # noqa: BLE001
        logger.error("Ошибка при получении данных из Supabase: %s", e)
        raise


# --------------------------------------
# Построение XML фида
# --------------------------------------

def build_xml_feed(apartments: list[dict]) -> str:
    """Строим XML-фид для ЦИАН из списка квартир."""

    feed = ET.Element("Feed")
    feed_version = ET.SubElement(feed, "Feed_Version")
    feed_version.text = "2"

    for idx, apt in enumerate(apartments, start=1):
        obj = ET.SubElement(feed, "Object")

        # --- Основные данные ---
        external_id = ET.SubElement(obj, "ExternalId")
        external_id.text = str(apt.get("external_id") or f"apt_{idx}")

        status = ET.SubElement(obj, "Status")
        status.text = apt.get("status") or "published"

        category = ET.SubElement(obj, "Category")
        category.text = "flatRent"  # мы работаем с долгосрочной арендой квартир

        type_elem = ET.SubElement(obj, "Type")
        type_elem.text = "apartment"

        # --- Адрес ---
        address = ET.SubElement(obj, "Address")
        address_line = ET.SubElement(address, "AddressLine")
        address_line.text = escape_xml(apt.get("address", ""))

        # --- Описание ---
        description = ET.SubElement(obj, "Description")
        description.text = escape_xml(apt.get("description", ""))

        # --- Характеристики квартиры ---
        rooms = ET.SubElement(obj, "RoomCount")
        rooms.text = str(apt.get("rooms") or 1)

        floor = ET.SubElement(obj, "Floor")
        floor.text = str(apt.get("floor") or 1)

        floors_total = ET.SubElement(obj, "FloorsTotal")
        floors_total.text = str(apt.get("total_floors") or 1)

        # --- Площади (из поля apartment_details) ---
        apart_details: str | None = apt.get("apartment_details")

        square = ET.SubElement(obj, "Square")
        square.text = _parse_float_from_details(r"Площадь: ([\d,\.]+)", apart_details)

        living_space = ET.SubElement(obj, "LivingSpace")
        living_space.text = _parse_float_from_details(r"Жилая: ([\d,\.]+)", apart_details)

        kitchen_space = ET.SubElement(obj, "KitchenSpace")
        kitchen_space.text = _parse_float_from_details(r"Кухня: ([\d,\.]+)", apart_details)

        # --- Цена и условия (из rental_conditions) ---
        rental: str | None = apt.get("rental_conditions")

        price = ET.SubElement(obj, "Price")
        price_match = re.search(r"Цена: ([\d\s]+)", rental or "")
        price.text = parse_price(price_match.group(1) if price_match else None)

        deposit = ET.SubElement(obj, "DepositSum")
        deposit_match = re.search(r"Залог: ([\d\s]+)", rental or "")
        deposit.text = parse_price(deposit_match.group(1) if deposit_match else None)

        prepay = ET.SubElement(obj, "PrepayMonths")
        prepay_match = re.search(r"Предоплата: (\d+)", rental or "")
        prepay.text = prepay_match.group(1) if prepay_match else "1"

        lease_type = ET.SubElement(obj, "LeaseTermType")
        lease_type.text = "longTerm"

        lease = ET.SubElement(obj, "Lease")
        lease.text = escape_xml(rental or "")

        # --- Фотографии ---
        photos = ET.SubElement(obj, "Photos")

        main_photo = apt.get("main_photo_url")
        if main_photo:
            photo = ET.SubElement(photos, "Photo")
            photo.text = escape_xml(main_photo)

        photos_json = apt.get("photos_json")
        # Поддерживаем и dict, и list
        if isinstance(photos_json, dict):
            iterable = photos_json.values()
        elif isinstance(photos_json, list):
            iterable = photos_json
        else:
            iterable = []

        for url in iterable:
            if url and url != main_photo:
                photo = ET.SubElement(photos, "Photo")
                photo.text = escape_xml(url)

        # --- Промо ---
        promo = ET.SubElement(obj, "PromotionType")
        promo.text = apt.get("promotion_type") or "noPromotion"

    # Красивое форматирование XML
    xml_bytes = ET.tostring(feed, encoding="utf-8")
    xml_str = minidom.parseString(xml_bytes).toprettyxml(indent="  ", encoding="utf-8")

    # minidom возвращает bytes, декодируем в строку
    return xml_str.decode("utf-8")


# --------------------------------------
# Маршруты FastAPI
# --------------------------------------

@app.get("/feed.xml")
async def get_feed() -> Response:
    """Endpoint для ЦИАН — возвращает XML-фид."""
    try:
        apartments = get_apartments_from_supabase()

        if not apartments:
            logger.warning("В Supabase нет квартир, возвращаем пустой фид")
            empty_feed = (
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<Feed><Feed_Version>2</Feed_Version></Feed>"
            )
            return Response(content=empty_feed, media_type="application/xml")

        xml_feed = build_xml_feed(apartments)
        logger.info("Сгенерирован фид с %s квартирами", len(apartments))

        return Response(content=xml_feed, media_type="application/xml")

    except RuntimeError as e:
        logger.error("Критическая ошибка конфигурации: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        logger.error("Неожиданная ошибка при генерации фида: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.get("/health")
async def health() -> dict:
    """Health check для Railway."""
    status = "ok" if supabase is not None else "supabase_not_configured"
    return {"status": status}


@app.get("/api/count")
async def count_apartments() -> dict:
    """Подсчёт количества квартир в таблице objects."""
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase не сконфигурирован")

    try:
        response = supabase.table("objects").select("id", count="exact").execute()
        return {"count": response.count or 0}
    except Exception as e:  # noqa: BLE001
        logger.error("Ошибка при подсчёте квартир: %s", e)
        raise HTTPException(status_code=500, detail="Ошибка при обращении к Supabase") from e

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
