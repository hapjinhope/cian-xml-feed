"""Utilities for building CIAN XML feed."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any
from xml.dom import minidom


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
    match = re.search(r"(\d[\d\s]*)", value)
    return match.group(1).replace(" ", "") if match else "0"


def _extract_float(pattern: str, source: str | None) -> str:
    if not source:
        return "0"
    match = re.search(pattern, source)
    return match.group(1).replace(",", ".") if match else "0"


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

        ET.SubElement(obj, "RoomCount").text = str(apt.get("rooms") or 1)
        ET.SubElement(obj, "Floor").text = str(apt.get("floor") or 1)
        ET.SubElement(obj, "FloorsTotal").text = str(apt.get("total_floors") or 1)

        details = apt.get("apartment_details")
        ET.SubElement(obj, "Square").text = _extract_float(
            r"Площадь:\s*([\d,\.]+)", details
        )
        ET.SubElement(obj, "LivingSpace").text = _extract_float(
            r"Жилая:\s*([\d,\.]+)", details
        )
        ET.SubElement(obj, "KitchenSpace").text = _extract_float(
            r"Кухня:\s*([\d,\.]+)", details
        )

        rental = apt.get("rental_conditions") or ""
        price_match = re.search(r"Цена:\s*([\d\s]+)", rental)
        deposit_match = re.search(r"Залог:\s*([\d\s]+)", rental)
        prepay_match = re.search(r"Предоплата:\s*(\d+)", rental)

        ET.SubElement(obj, "Price").text = parse_price(
            price_match.group(1) if price_match else None
        )
        ET.SubElement(obj, "DepositSum").text = parse_price(
            deposit_match.group(1) if deposit_match else None
        )
        ET.SubElement(obj, "PrepayMonths").text = (
            prepay_match.group(1) if prepay_match else "1"
        )
        ET.SubElement(obj, "LeaseTermType").text = "longTerm"
        ET.SubElement(obj, "Lease").text = escape_xml(rental)

        photos = ET.SubElement(obj, "Photos")
        main_photo = apt.get("main_photo_url")
        if main_photo:
            ET.SubElement(photos, "Photo").text = escape_xml(main_photo)

        photos_json = apt.get("photos_json")
        if isinstance(photos_json, dict):
            iterable = photos_json.values()
        elif isinstance(photos_json, list):
            iterable = photos_json
        else:
            iterable = []

        for url in iterable:
            if url and url != main_photo:
                ET.SubElement(photos, "Photo").text = escape_xml(url)

        ET.SubElement(obj, "PromotionType").text = (
            apt.get("promotion_type") or "noPromotion"
        )

    xml_bytes = ET.tostring(feed, encoding="utf-8")
    xml_str = minidom.parseString(xml_bytes).toprettyxml(
        indent="  ", encoding="utf-8"
    )
    return xml_str.decode("utf-8")
