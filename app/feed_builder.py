"""Utilities for building CIAN XML feed."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Iterable
from xml.dom import minidom

AMENITY_MAP = {
    "Холодильник": "Fridge",
    "Посудомоечная машина": "Dishwasher",
    "Стиральная машина": "Washer",
    "Кондиционер": "Conditioner",
    "Телевизор": "TV",
    "Интернет": "Internet",
    "Ванна": "Bath",
    "Душевая кабина": "Shower",
    "Мебель на кухне": "KitchenFurniture",
    "Мебель в комнатах": "RoomFurniture",
}


def escape_xml(text: str | None) -> str:
    """Escape XML-sensitive characters."""
    if not text:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def parse_price(value: str | None) -> str:
    """Extract only digits from a price string."""
    if not value:
        return "0"
    match = re.search(r"(\d[\d\s\u00a0]*)", value)
    if not match:
        return "0"
    digits = re.sub(r"\D", "", match.group(1))
    return digits or "0"


def _extract_float(pattern: str, source: str | None) -> str:
    if not source:
        return "0"
    match = re.search(pattern, source)
    return match.group(1).replace(",", ".") if match else "0"


def _normalize_phone(phone: str | None) -> str | None:
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if not digits:
        return None
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return f"+{digits}"
    if digits.startswith("+"):
        return digits
    return f"+{digits}" if digits else None


def _collect_phones(*sources: Any) -> list[str]:
    phones: list[str] = []
    for source in sources:
        if not source:
            continue
        values: list[Any]
        if isinstance(source, (list, tuple, set)):
            values = list(source)
        elif isinstance(source, dict):
            values = list(source.values())
        else:
            values = re.split(r"[;,/]+", str(source))

        for value in values:
            phone = _normalize_phone(str(value).strip())
            if phone and phone not in phones:
                phones.append(phone)
    return phones


def _split_items(text: str | None) -> list[str]:
    if not text:
        return []
    items = re.split(r"[,\n;]+", text)
    return [item.strip() for item in items if item.strip()]


def _parse_amenities(text: str | None) -> list[str]:
    amenities: list[str] = []
    for item in _split_items(text):
        amenities.append(AMENITY_MAP.get(item, item))
    return amenities


def build_feed(apartments: list[dict[str, Any]]) -> str:
    """Generate XML feed document."""

    feed = ET.Element("Feed")
    version = ET.SubElement(feed, "Feed_Version")
    version.text = "2"

    for idx, apt in enumerate(apartments, start=1):
        obj = ET.SubElement(feed, "Object")

        ET.SubElement(obj, "ExternalId").text = str(
            apt.get("external_id") or f"apt_{idx}"
        )
        ET.SubElement(obj, "Status").text = apt.get("status") or "published"
        ET.SubElement(obj, "Category").text = "flatRent"
        ET.SubElement(obj, "Type").text = "apartment"

        address = ET.SubElement(obj, "Address")
        ET.SubElement(address, "AddressLine").text = escape_xml(apt.get("address"))

        ET.SubElement(obj, "Description").text = escape_xml(apt.get("description"))

        rooms_value = int(apt.get("rooms") or 0)
        if rooms_value <= 0:
            flat_rooms = "1"
        elif rooms_value > 5:
            flat_rooms = "6"
        else:
            flat_rooms = str(rooms_value)
        ET.SubElement(obj, "FlatRoomsCount").text = flat_rooms

        ET.SubElement(obj, "FloorNumber").text = str(apt.get("floor") or 1)
        ET.SubElement(obj, "FloorsTotal").text = str(apt.get("total_floors") or 1)

        details = apt.get("apartment_details")
        total_area = _extract_float(r"Площадь:\s*([\d,\.]+)", details)
        living_space = _extract_float(r"Жилая:\s*([\d,\.]+)", details)
        kitchen_space = _extract_float(r"Кухня:\s*([\d,\.]+)", details)

        ET.SubElement(obj, "TotalArea").text = total_area
        ET.SubElement(obj, "LivingSpace").text = living_space
        ET.SubElement(obj, "KitchenSpace").text = kitchen_space

        rental = apt.get("rental_conditions") or ""
        price_match = re.search(r"Цена:\s*([\d\s\u00a0]+)", rental)
        deposit_match = re.search(r"Залог:\s*([\d\s\u00a0]+)", rental)
        prepay_match = re.search(r"Предоплата:\s*(\d+)", rental)

        bargain = ET.SubElement(obj, "BargainTerms")
        ET.SubElement(bargain, "Price").text = parse_price(
            price_match.group(1) if price_match else None
        )
        ET.SubElement(bargain, "Currency").text = "RUB"
        ET.SubElement(bargain, "PaymentPeriod").text = "month"
        ET.SubElement(bargain, "Deposit").text = parse_price(
            deposit_match.group(1) if deposit_match else None
        )
        ET.SubElement(bargain, "Prepay").text = (
            prepay_match.group(1) if prepay_match else "1"
        )
        ET.SubElement(bargain, "ClientFee").text = "false"
        ET.SubElement(bargain, "LandlordFee").text = "true"
        ET.SubElement(bargain, "BargainStatus").text = "approved"

        photos = ET.SubElement(obj, "Photos")
        photo_urls: list[str] = []
        main_photo = apt.get("main_photo_url")
        if main_photo:
            photo_urls.append(str(main_photo))

        photos_json = apt.get("photos_json")
        if isinstance(photos_json, dict):
            iterable: Iterable[Any] = photos_json.values()
        elif isinstance(photos_json, list):
            iterable = photos_json
        else:
            iterable = []

        for url in iterable:
            if url:
                url_str = str(url)
                if url_str not in photo_urls:
                    photo_urls.append(url_str)

        def add_photo(url: str, is_main: bool) -> None:
            photo_el = ET.SubElement(photos, "Photo")
            ET.SubElement(photo_el, "FullUrl").text = escape_xml(url)
            ET.SubElement(photo_el, "IsDefault").text = "true" if is_main else "false"

        if photo_urls:
            add_photo(photo_urls[0], True)
            for extra in photo_urls[1:]:
                add_photo(extra, False)

        ET.SubElement(obj, "PromotionType").text = (
            apt.get("promotion_type") or "noPromotion"
        )

        agent_info = apt.get("agent") or {}
        contact_name = (
            agent_info.get("name")
            or apt.get("agent_name")
            or apt.get("agent_full_name")
        )
        contact_email = agent_info.get("email") or apt.get("agent_email")
        phones = _collect_phones(
            agent_info.get("phones"),
            agent_info.get("phone"),
            apt.get("agent_phone"),
        )

        if phones:
            contacts = ET.SubElement(obj, "Contacts")
            contact = ET.SubElement(contacts, "Contact")
            if contact_name:
                ET.SubElement(contact, "Name").text = escape_xml(contact_name)
            phones_el = ET.SubElement(contact, "Phones")
            for phone in phones:
                phone_el = ET.SubElement(phones_el, "Phone")
                ET.SubElement(phone_el, "Number").text = phone
            if contact_email:
                ET.SubElement(contact, "Email").text = escape_xml(contact_email)

        building = ET.SubElement(obj, "Building")
        if apt.get("complex_name"):
            ET.SubElement(building, "Name").text = escape_xml(apt["complex_name"])
        total_floors = apt.get("total_floors")
        if total_floors:
            ET.SubElement(building, "FloorsNumber").text = str(total_floors)
        house_details = apt.get("house_details")
        if house_details:
            year_match = re.search(r"Год:\s*(\d{4})", house_details)
            if year_match:
                ET.SubElement(building, "BuildYear").text = year_match.group(1)

        amenities = _parse_amenities(apt.get("apartment_amenities"))
        if amenities:
            amenities_el = ET.SubElement(obj, "FlatAmenities")
            for amenity in amenities:
                ET.SubElement(amenities_el, "Amenity").text = amenity

    xml_bytes = ET.tostring(feed, encoding="utf-8")
    xml_str = minidom.parseString(xml_bytes).toprettyxml(
        indent="  ", encoding="utf-8"
    )
    return xml_str.decode("utf-8")
