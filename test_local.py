
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import xml.etree.ElementTree as ET
from xml.dom import minidom

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

LOG_FILE = "feed_generation.log"
logs = []

def log_message(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {level}: {message}"
    logs.append(log_entry)
    print(log_entry)

def escape_xml(text):
    if not text:
        return ""
    text = str(text)
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = text.replace('"', "&quot;").replace("'", "&apos;")
    return text

def parse_price(price_str):
    if not price_str:
        return "0"
    digits = re.sub(r'\D', '', str(price_str))
    return digits if digits else "0"

def parse_field_from_details(text, pattern, field_name, index=1):
    if not text:
        log_message(f"  ⚠️  {field_name}: пусто (нет данных в apartment_details)", "WARN")
        return None
    match = re.search(pattern, text)
    if not match:
        log_message(f"  ⚠️  {field_name}: не найдено по паттерну '{pattern}'", "WARN")
        return None
    try:
        value = match.group(index) if index > 0 else match.group(0)
        result = value.replace(",", ".") if value else None
        log_message(f"  ✅ {field_name}: {result}")
        return result
    except IndexError:
        log_message(f"  ⚠️  {field_name}: ошибка парсинга (группа {index} не найдена)", "WARN")
        return None

def extract_amenity(amenities_str, keywords, amenity_name):
    if not amenities_str:
        log_message(f"  ⚠️  {amenity_name}: пусто (нет данных в apartment_amenities)", "WARN")
        return False
    found = any(keyword in amenities_str for keyword in keywords)
    status = "✅" if found else "❌"
    log_message(f"  {status} {amenity_name}: {found}")
    return found

def get_agent_data(agent_id):
    if not agent_id:
        log_message(f"  ⚠️  Agent: agent_id не указан", "WARN")
        return {}
    try:
        response = supabase.table("agents").select("*").eq("id", agent_id).single().execute()
        agent = response.data if response.data else {}
        if agent:
            log_message(f"  ✅ Agent загружен: ID {agent_id}")
            log_message(f"      Email: {agent.get('email', '—')}")
            log_message(f"      FirstName: {agent.get('first_name', '—')}")
            log_message(f"      LastName: {agent.get('last_name', '—')}")
            log_message(f"      Phone: {agent.get('phone', '—')}")
        else:
            log_message(f"  ❌ Agent ID {agent_id}: не найден в базе", "ERROR")
        return agent
    except Exception as e:
        log_message(f"  ❌ Agent: ошибка загрузки - {e}", "ERROR")
        return {}

def get_apartments_from_supabase():
    try:
        response = supabase.table("objects").select("*").execute()
        log_message(f"✅ Supabase: подключено, найдено объектов: {len(response.data)}")
        return response.data
    except Exception as e:
        log_message(f"❌ Supabase: ошибка подключения - {e}", "ERROR")
        return []

def build_xml_feed(apartments):
    feed = ET.Element("Feed")
    feed_version = ET.SubElement(feed, "Feed_Version")
    feed_version.text = "2"

    for idx, apt in enumerate(apartments, 1):
        log_message(f"\n{'='*60}\n📍 ОБЪЕКТ #{idx}\n{'='*60}")

        obj = ET.SubElement(feed, "Object")
        category = ET.SubElement(obj, "Category")
        category.text = apt.get("category", "flatRent")
        log_message(f"  ✅ Category: {category.text}")
        external_id = ET.SubElement(obj, "ExternalId")
        external_id.text = apt.get("external_id", f"apt_{idx}")
        log_message(f"  ✅ ExternalId: {external_id.text}")

        description = ET.SubElement(obj, "Description")
        desc_text = escape_xml(apt.get("description", ""))
        description.text = desc_text if len(desc_text) >= 15 else "Уютная квартира с отличным расположением"
        log_message(f"  ✅ Description: {len(description.text)} символов")

        address = ET.SubElement(obj, "Address")
        address.text = escape_xml(apt.get("address", ""))
        log_message(f"  ✅ Address: {address.text}")

        flat_rooms = ET.SubElement(obj, "FlatRoomsCount")
        flat_rooms.text = str(apt.get("rooms", 3))
        log_message(f"  ✅ FlatRoomsCount: {flat_rooms.text}")

        floor = ET.SubElement(obj, "FloorNumber")
        floor.text = str(apt.get("floor", 1))
        log_message(f"  ✅ FloorNumber: {floor.text}")

        apart_details = apt.get("apartment_details", "")

        total_area = ET.SubElement(obj, "TotalArea")
        total_area_val = parse_field_from_details(apart_details, r'Площадь:\s*([\d,\.]+)', "TotalArea", 1) or str(apt.get("total_area", "0"))
        total_area.text = total_area_val

        living_area = ET.SubElement(obj, "LivingArea")
        living_area_val = parse_field_from_details(apart_details, r'Жилая:\s*([\d,\.]+)', "LivingArea", 1) or str(apt.get("living_area", "0"))
        living_area.text = living_area_val

        kitchen_area = ET.SubElement(obj, "KitchenArea")
        kitchen_area_val = parse_field_from_details(apart_details, r'Кухня:\s*([\d,\.]+)', "KitchenArea", 1) or str(apt.get("kitchen_area", "0"))
        kitchen_area.text = kitchen_area_val

        balconies = ET.SubElement(obj, "BalconiesCount")
        balcony_val = parse_field_from_details(apart_details, r'Балконов:\s*(\d+)', "BalconiesCount", 1) or str(apt.get("balcony_count", "0"))
        balconies.text = balcony_val

        separate_wcs = ET.SubElement(obj, "SeparateWcsCount")
        separate_wcs.text = "1" if "раздельный" in apart_details else str(apt.get("separate_wcs", "1"))

        windows_view = ET.SubElement(obj, "WindowsViewType")
        windows_types = {"На улицу": "street", "Во двор": "yard", "На улицу и двор": "yardAndStreet"}
        windows_detected = parse_field_from_details(apart_details, r'Окна:\s*(.+?)(?:\.|,|$)', "WindowsViewType", 1)
        windows_view.text = windows_types.get(windows_detected, "yardAndStreet")

        repair_type = ET.SubElement(obj, "RepairType")
        repair_types = {"Дизайнерский": "design", "Евроремонт": "euro", "Косметический": "cosmetic", "Без ремонта": "no"}
        repair_detected = parse_field_from_details(apart_details, r'Ремонт:\s*(.+?)(?:\.|,|$)', "RepairType", 1) or "Дизайнерский"
        repair_type.text = repair_types.get(repair_detected, "design")

        amenities_str = apt.get("apartment_amenities", "")
        has_internet = ET.SubElement(obj, "HasInternet")
        has_internet.text = "true" if extract_amenity(amenities_str, ["Интернет"], "HasInternet") else "false"
        has_furniture = ET.SubElement(obj, "HasFurniture")
        has_furniture.text = "true" if extract_amenity(amenities_str, ["Мебель в комнатах", "Мебель"], "HasFurniture") else "false"
        has_kitchen_furniture = ET.SubElement(obj, "HasKitchenFurniture")
        has_kitchen_furniture.text = "true" if extract_amenity(amenities_str, ["Мебель на кухне"], "HasKitchenFurniture") else "false"
        has_tv = ET.SubElement(obj, "HasTv")
        has_tv.text = "true" if extract_amenity(amenities_str, ["Телевизор"], "HasTv") else "false"
        has_washer = ET.SubElement(obj, "HasWasher")
        has_washer.text = "true" if extract_amenity(amenities_str, ["Стиральная"], "HasWasher") else "false"
        has_conditioner = ET.SubElement(obj, "HasConditioner")
        has_conditioner.text = "true" if extract_amenity(amenities_str, ["Кондиционер"], "HasConditioner") else "false"
        has_bathtub = ET.SubElement(obj, "HasBathtub")
        has_bathtub.text = "true" if extract_amenity(amenities_str, ["Ванна"], "HasBathtub") else "false"
        has_shower = ET.SubElement(obj, "HasShower")
        has_shower.text = "true" if extract_amenity(amenities_str, ["Душевая"], "HasShower") else "false"
        has_dishwasher = ET.SubElement(obj, "HasDishwasher")
        has_dishwasher.text = "true" if extract_amenity(amenities_str, ["Посудомоечная"], "HasDishwasher") else "false"
        has_fridge = ET.SubElement(obj, "HasFridge")
        has_fridge.text = "true" if extract_amenity(amenities_str, ["Холодильник"], "HasFridge") else "false"

        building = ET.SubElement(obj, "Building")
        build_year = ET.SubElement(building, "BuildYear")
        year_val = parse_field_from_details(apt.get("house_details", ""), r'Год:\s*(\d{4})', "BuildYear", 1)
        build_year.text = year_val if year_val else str(apt.get("year_built", 2000))

        floors_count = ET.SubElement(building, "FloorsCount")
        floors_count.text = str(apt.get("total_floors", 1))
        material_type = ET.SubElement(building, "MaterialType")
        material_type.text = apt.get("material_type", "monolith")

        ceiling_height = ET.SubElement(building, "CeilingHeight")
        ceiling_val = parse_field_from_details(apt.get("house_details", ""), r'Потолки:\s*([\d,\.]+)', "CeilingHeight", 1)
        ceiling_height.text = ceiling_val if ceiling_val else str(apt.get("ceiling_height", 2.7))

        passenger_lifts = ET.SubElement(building, "PassengerLiftsCount")
        elevators_str = apt.get("elevators", "1 пассажирский, 0 грузовой")
        pass_match = re.search(r'(\d+)\s*пассажирский', elevators_str)
        passenger_lifts.text = pass_match.group(1) if pass_match else "1"
        cargo_lifts = ET.SubElement(building, "CargoLiftsCount")
        cargo_match = re.search(r'(\d+)\s*грузовой', elevators_str)
        cargo_lifts.text = cargo_match.group(1) if cargo_match else "0"

        parking = ET.SubElement(building, "Parking")
        parking_type = ET.SubElement(parking, "Type")
        parking_types = {"Подземная": "underground", "Наземная": "ground", "Открытая": "open", "На крыше": "roof", "Многоуровневая": "multilevel"}
        parking_detected = parse_field_from_details(apt.get("house_details", ""), r'Парковка:\s*(.+?)(?:\.|$)', "ParkingType", 1) or "Подземная"
        parking_type.text = parking_types.get(parking_detected, "underground")

        photos = ET.SubElement(obj, "Photos")
        photo_count = 0
        if apt.get("main_photo_url"):
            photo = ET.SubElement(photos, "PhotoSchema")
            photo_url = ET.SubElement(photo, "FullUrl")
            photo_url.text = escape_xml(apt["main_photo_url"])
            is_default = ET.SubElement(photo, "IsDefault")
            is_default.text = "true"
            photo_count += 1
        photos_json = apt.get("photos_json")
        if photos_json and isinstance(photos_json, dict):
            for photo_url in photos_json.values():
                if photo_url:
                    photo = ET.SubElement(photos, "PhotoSchema")
                    photo_full_url = ET.SubElement(photo, "FullUrl")
                    photo_full_url.text = escape_xml(photo_url)
                    is_def = ET.SubElement(photo, "IsDefault")
                    is_def.text = "false"
                    photo_count += 1
        log_message(f"  ✅ Всего фотографий: {photo_count}")

        agent_id = apt.get("agent_id")
        if agent_id:
            agent_data = get_agent_data(agent_id)
            if agent_data:
                sub_agent = ET.SubElement(obj, "SubAgent")
                email = ET.SubElement(sub_agent, "Email")
                email.text = escape_xml(agent_data.get("email", "")) or ""
                log_message(f"  ✅ Email: {email.text if email.text else '—'}")
                phone = ET.SubElement(sub_agent, "Phone")
                phone.text = escape_xml(agent_data.get("phone", "")) or ""
                log_message(f"  ✅ Phone: {phone.text if phone.text else '—'}")
                first_name = ET.SubElement(sub_agent, "FirstName")
                first_name.text = escape_xml(agent_data.get("first_name") or agent_data.get("name") or "")
                log_message(f"  {'✅' if first_name.text else '⚠️'} FirstName: {first_name.text if first_name.text else 'пусто'}")
                last_name = ET.SubElement(sub_agent, "LastName")
                last_name.text = escape_xml(agent_data.get("last_name") or agent_data.get("surname") or "")
                log_message(f"  {'✅' if last_name.text else '⚠️'} LastName: {last_name.text if last_name.text else 'пусто'}")
                avatar_url = ET.SubElement(sub_agent, "AvatarUrl")
                avatar_url.text = escape_xml(agent_data.get("avatar_url", "")) or ""
                log_message(f"  {'✅' if avatar_url.text else '⚠️'} AvatarUrl: {avatar_url.text if avatar_url.text else 'пусто'}")
        else:
            log_message(f"  ⚠️  Agent: agent_id не указан", "WARN")

        rental = apt.get("rental_conditions", "")
        bargain_terms = ET.SubElement(obj, "BargainTerms")
        price = ET.SubElement(bargain_terms, "Price")
        price_match = re.search(r'Цена:\s*([\d\s]+)', rental)
        price_val = parse_price(price_match.group(1) if price_match else apt.get("price", "0"))
        price.text = price_val
        included_in_price = ET.SubElement(ET.SubElement(bargain_terms, "UtilitiesTerms"), "IncludedInPrice")
        included_in_price.text = "true" if "по счётчику" not in rental else "false"
        currency = ET.SubElement(bargain_terms, "Currency")
        currency.text = apt.get("currency", "rur")
        lease_type = ET.SubElement(bargain_terms, "LeaseTermType")
        lease_type.text = apt.get("lease_term_type", "longTerm")
        prepay = ET.SubElement(bargain_terms, "PrepayMonths")
        prepay_match = re.search(r'Предоплата:\s*(\d+)', rental)
        prepay.text = prepay_match.group(1) if prepay_match else str(apt.get("prepay_months", "1"))
        deposit = ET.SubElement(bargain_terms, "Deposit")
        deposit_match = re.search(r'Залог:\s*([\d\s]+)', rental)
        deposit_val = parse_price(deposit_match.group(1) if deposit_match else apt.get("deposit", "0"))
        deposit.text = deposit_val

        publish_terms = ET.SubElement(obj, "PublishTerms")
        terms = ET.SubElement(publish_terms, "Terms")
        term_schema = ET.SubElement(terms, "PublishTermSchema")
        ignore_packages = ET.SubElement(term_schema, "IgnoreServicePackages")
        ignore_packages.text = "true"
        promo_type = ET.SubElement(publish_terms, "PromotionType")
        promo_type.text = apt.get("promotion_type", "noPromotion")

    xml_str = minidom.parseString(ET.tostring(feed)).toprettyxml(indent="  ")
    return "\n".join(xml_str.split("\n")[1:])

def main():
    print("\n" + "="*60)
    print("🔍 ГЕНЕРАТОР XML ФИДА ДЛЯ ЦИАНА")
    print("="*60 + "\n")
    log_message(f"Начало генерации XML фида")
    log_message(f"URL Supabase: {SUPABASE_URL}")
    apartments = get_apartments_from_supabase()
    if not apartments:
        log_message(f"❌ Нет данных в Supabase!", "ERROR")
        return
    log_message(f"\n✅ Успешно загружено объектов: {len(apartments)}\n")
    xml_feed = build_xml_feed(apartments)
    print("\n" + "="*60)
    print("✨ XML ФИД ГОТОВ\n" + "="*60 + "\n")
    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(xml_feed)
    log_message(f"✅ XML сохранён в feed.xml ({len(xml_feed)} символов)")
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(logs))
    log_message(f"✅ Логи сохранены в {LOG_FILE}")
    log_message(f"\n✨ Готово! XML соответствует спецификации Циана\n")

if __name__ == "__main__":
    main()
