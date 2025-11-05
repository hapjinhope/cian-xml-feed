"""Utilities for building CIAN XML feed."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Iterable, Tuple
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


def _split_phone(phone: str) -> tuple[str, str] | None:
    digits = re.sub(r"\D", "", phone)
    if not digits:
        return None
    if len(digits) <= 10:
        number = digits.zfill(10)
        return "+7", number
    country_digits = digits[:-10] or "7"
    number_digits = digits[-10:]
    return f"+{country_digits}", number_digits


def _split_items(text: str | None) -> list[str]:
    if not text:
        return []
    items = re.split(r"[,\n;]+", text)
    return [item.strip() for item in items if item.strip()]


def _parse_amenities(text: str | None) -> Tuple[list[str], set[str]]:
    amenities: list[str] = []
    original: set[str] = set()
    for item in _split_items(text):
        original.add(item.lower())
        amenities.append(AMENITY_MAP.get(item, item))
    return amenities, original


def _get_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "да"}:
            return True
        if normalized in {"false", "0", "no", "n", "нет"}:
            return False
    return None


def _set_bool(parent: ET.Element, tag: str, value: bool | None) -> None:
    if value is None:
        return
    ET.SubElement(parent, tag).text = "true" if value else "false"


def _parse_bathroom_counts(details: str | None) -> tuple[str | None, str | None]:
    if not details:
        return None, None
    match = re.search(r"Санузел:\s*([\w\s\-]+)", details, re.IGNORECASE)
    if not match:
        return None, None
    info = match.group(1).lower()
    digits = re.findall(r"\d+", info)
    count = digits[0] if digits else "1"
    separate = combined = None
    if "раздель" in info:
        separate = count
    if "совмещ" in info or "совм" in info:
        combined = count
    return separate, combined


def _parse_count(details: str | None, label: str) -> str | None:
    if not details:
        return None
    match = re.search(fr"{label}:\s*(\d+)", details)
    return match.group(1) if match else None


def _parse_windows(details: str | None, apt: dict[str, Any]) -> str | None:
    direct = apt.get("windows_view_type")
    if direct:
        return str(direct)
    if not details:
        return None
    match = re.search(r"Окна:\s*([^.]+)", details)
    if not match:
        return None
    text = match.group(1).lower()
    has_street = "улиц" in text or "улицу" in text
    has_yard = "двор" in text
    if has_street and has_yard:
        return "streetAndYard"
    if has_street:
        return "street"
    if has_yard:
        return "yard"
    return None


def _parse_room_type(details: str | None, apt: dict[str, Any]) -> str | None:
    value = apt.get("room_type")
    if value:
        return str(value)
    if not details:
        return None
    match = re.search(r"Планировка:\s*([^.]+)", details)
    if not match:
        return None
    text = match.group(1).lower()
    if "совмещ" in text and "изолир" in text:
        return "both"
    if "свобод" in text or "своб" in text:
        return "free"
    if "изол" in text:
        return "separated"
    if "смеж" in text:
        return "adjacent"
    return None


def build_feed(apartments: list[dict[str, Any]]) -> str:
    """Generate XML feed document."""

    feed = ET.Element("Feed")
    ET.SubElement(feed, "Feed_Version").text = "2"

    for idx, apt in enumerate(apartments, start=1):
        obj = ET.SubElement(feed, "Object")

        ET.SubElement(obj, "ExternalId").text = str(
            apt.get("external_id") or f"apt_{idx}"
        )

        auction_bet = apt.get("auction_bet")
        if auction_bet:
            auction = ET.SubElement(obj, "Auction")
            ET.SubElement(auction, "Bet").text = str(auction_bet)

        ET.SubElement(obj, "Status").text = apt.get("status") or "published"
        ET.SubElement(obj, "Category").text = "flatRent"

        room_type = _parse_room_type(apt.get("apartment_details"), apt)
        if room_type:
            ET.SubElement(obj, "RoomType").text = room_type

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

        beds_count = apt.get("beds_count") or apt.get("beds") or apt.get("bedsNumber")
        if beds_count:
            ET.SubElement(obj, "BedsCount").text = str(beds_count)

        ET.SubElement(obj, "FloorNumber").text = str(apt.get("floor") or 1)
        ET.SubElement(obj, "FloorsTotal").text = str(apt.get("total_floors") or 1)

        details = apt.get("apartment_details")
        total_area = _extract_float(r"Площадь:\s*([\d,\.]+)", details)
        living_space = _extract_float(r"Жилая:\s*([\d,\.]+)", details)
        kitchen_space = _extract_float(r"Кухня:\s*([\d,\.]+)", details)

        ET.SubElement(obj, "TotalArea").text = total_area
        ET.SubElement(obj, "LivingSpace").text = living_space
        ET.SubElement(obj, "KitchenSpace").text = kitchen_space

        separate_wc, combined_wc = _parse_bathroom_counts(details)
        separate_wc = separate_wc or apt.get("separate_wc_count")
        combined_wc = combined_wc or apt.get("combined_wc_count")
        if separate_wc:
            ET.SubElement(obj, "SeparateWcsCount").text = str(separate_wc)
        if combined_wc:
            ET.SubElement(obj, "CombinedWcsCount").text = str(combined_wc)

        loggias_count = apt.get("loggias_count") or _parse_count(details, "Лоджий")
        balconies_count = apt.get("balconies_count") or _parse_count(details, "Балконов")
        if loggias_count:
            ET.SubElement(obj, "LoggiasCount").text = str(loggias_count)
        if balconies_count:
            ET.SubElement(obj, "BalconiesCount").text = str(balconies_count)

        windows_view = _parse_windows(details, apt)
        if windows_view:
            ET.SubElement(obj, "WindowsViewType").text = windows_view

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

        _set_bool(
            obj,
            "IsApartments",
            _get_bool(
                apt.get("is_apartments")
                or apt.get("is_apartment")
                or (apt.get("property_type") == "apartments")
            ),
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
                phone_parts = _split_phone(phone)
                if not phone_parts:
                    continue
                country_code, local_number = phone_parts
                phone_el = ET.SubElement(phones_el, "PhoneSchema")
                ET.SubElement(phone_el, "CountryCode").text = country_code
                ET.SubElement(phone_el, "Number").text = local_number
            if contact_email:
                ET.SubElement(contact, "Email").text = escape_xml(contact_email)

        if apt.get("subagent_first_name") or apt.get("subagent_last_name"):
            sub_agent = ET.SubElement(obj, "SubAgent")
            if apt.get("subagent_email"):
                ET.SubElement(sub_agent, "Email").text = escape_xml(
                    apt["subagent_email"]
                )
            if apt.get("subagent_phone"):
                ET.SubElement(sub_agent, "Phone").text = escape_xml(
                    str(apt["subagent_phone"])
                )
            if apt.get("subagent_first_name"):
                ET.SubElement(sub_agent, "FirstName").text = escape_xml(
                    apt["subagent_first_name"]
                )
            if apt.get("subagent_last_name"):
                ET.SubElement(sub_agent, "LastName").text = escape_xml(
                    apt["subagent_last_name"]
                )
            if apt.get("subagent_avatar_url"):
                ET.SubElement(sub_agent, "AvatarUrl").text = escape_xml(
                    apt["subagent_avatar_url"]
                )

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

        jk_id = apt.get("jk_id") or apt.get("complex_id")
        if jk_id or apt.get("complex_name"):
            jk = ET.SubElement(obj, "JKSchema")
            if jk_id:
                ET.SubElement(jk, "Id").text = str(jk_id)
            if apt.get("complex_name"):
                ET.SubElement(jk, "Name").text = escape_xml(apt["complex_name"])
            if (
                apt.get("house_id")
                or apt.get("building_name")
                or apt.get("apartment_number")
                or apt.get("section_number")
            ):
                house = ET.SubElement(jk, "House")
                if apt.get("house_id"):
                    ET.SubElement(house, "Id").text = str(apt["house_id"])
                if apt.get("building_name"):
                    ET.SubElement(house, "Name").text = escape_xml(
                        apt["building_name"]
                    )
                if apt.get("apartment_number") or apt.get("section_number"):
                    flat = ET.SubElement(house, "Flat")
                    if apt.get("apartment_number"):
                        ET.SubElement(flat, "FlatNumber").text = str(
                            apt["apartment_number"]
                        )
                    if apt.get("section_number"):
                        ET.SubElement(flat, "SectionNumber").text = str(
                            apt["section_number"]
                        )

        amenities, amenity_original = _parse_amenities(apt.get("apartment_amenities"))
        if amenities:
            amenities_el = ET.SubElement(obj, "FlatAmenities")
            for amenity in amenities:
                ET.SubElement(amenities_el, "Amenity").text = amenity

        amenities_lower = {item.lower() for item in amenity_original}
        rent_lower = rental.lower()

        def has_keyword(keyword: str) -> bool:
            return keyword.lower() in amenities_lower

        bool_specs = [
            ("HasInternet", "has_internet", "интернет"),
            ("HasFurniture", "has_furniture", None),
            ("HasPhone", "has_phone", "телефон"),
            ("HasKitchenFurniture", "has_kitchen_furniture", "мебель на кухне"),
            ("HasTv", "has_tv", "телевизор"),
            ("HasWasher", "has_washer", "стиральная машина"),
            ("HasConditioner", "has_conditioner", "кондиционер"),
            ("HasRamp", "has_ramp", None),
            ("HasBathtub", "has_bathtub", "ванна"),
            ("HasShower", "has_shower", "душевая кабина"),
            ("HasDishwasher", "has_dishwasher", "посудомоечная машина"),
            ("HasFridge", "has_fridge", "холодильник"),
            ("PetsAllowed", "pets_allowed", None),
            ("ChildrenAllowed", "children_allowed", None),
        ]

        for tag, field, keyword in bool_specs:
            value = _get_bool(apt.get(field))
            if value is None and keyword:
                value = True if has_keyword(keyword) else None
            if tag == "HasFurniture" and (value is None or value is False):
                value = has_keyword("мебель в комнатах") or has_keyword(
                    "мебель на кухне"
                )
                if not value:
                    value = _get_bool(apt.get("has_room_furniture"))
                if not value:
                    value = _get_bool(apt.get("has_kitchen_furniture"))
            if tag == "PetsAllowed" and value is None:
                if "можно с животными" in rent_lower or "с животными" in rent_lower:
                    value = True
                elif "без животных" in rent_lower:
                    value = False
            if tag == "ChildrenAllowed" and value is None:
                if "можно с детьми" in rent_lower or "с детьми" in rent_lower:
                    value = True
                elif "без детей" in rent_lower:
                    value = False
            _set_bool(obj, tag, value)

    xml_bytes = ET.tostring(feed, encoding="utf-8")
    xml_str = minidom.parseString(xml_bytes).toprettyxml(
        indent="  ", encoding="utf-8"
    )
    return xml_str.decode("utf-8")
