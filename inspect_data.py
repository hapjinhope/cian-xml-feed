# inspect_data.py - ИНСПЕКЦИЯ ДАННЫХ ИЗ SUPABASE
import os
import json
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def print_section(title):
    """Печать заголовка секции"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def inspect_objects():
    """Инспекция таблицы objects"""
    print_section("📦 ТАБЛИЦА: objects")
    
    try:
        response = supabase.table("objects").select("*").execute()
        objects = response.data
        
        print(f"\n✅ Найдено объектов: {len(objects)}\n")
        
        for idx, obj in enumerate(objects, 1):
            print(f"\n{'─'*80}")
            print(f"🏠 ОБЪЕКТ #{idx}")
            print(f"{'─'*80}\n")
            
            # Основные поля
            print("📋 ОСНОВНЫЕ ПОЛЯ:")
            print(f"  • ID: {obj.get('id', '—')}")
            print(f"  • External ID: {obj.get('external_id', '—')}")
            print(f"  • Status: {obj.get('status', '—')}")
            print(f"  • Category: {obj.get('category', '—')}")
            print(f"  • Agent ID: {obj.get('agent_id', '—')}")
            
            # Адрес и локация
            print("\n📍 АДРЕС:")
            print(f"  • Address: {obj.get('address', '—')}")
            print(f"  • Complex Name: {obj.get('complex_name', '—')}")
            
            # Характеристики квартиры
            print("\n🏡 ХАРАКТЕРИСТИКИ:")
            print(f"  • Rooms: {obj.get('rooms', '—')}")
            print(f"  • Floor: {obj.get('floor', '—')}")
            print(f"  • Total Floors: {obj.get('total_floors', '—')}")
            
            # Детальные данные (строки)
            print("\n📝 ДЕТАЛЬНЫЕ ОПИСАНИЯ:")
            print(f"  • Description: {len(obj.get('description', ''))} символов")
            print(f"      Preview: {obj.get('description', '—')[:100]}...")
            
            print(f"\n  • Apartment Details:")
            apt_details = obj.get('apartment_details', '—')
            print(f"      {apt_details}")
            
            print(f"\n  • House Details:")
            print(f"      {obj.get('house_details', '—')}")
            
            print(f"\n  • Apartment Amenities:")
            print(f"      {obj.get('apartment_amenities', '—')}")
            
            print(f"\n  • Rental Conditions:")
            print(f"      {obj.get('rental_conditions', '—')}")
            
            # Фото
            print("\n📸 ФОТОГРАФИИ:")
            print(f"  • Main Photo URL: {obj.get('main_photo_url', '—')[:60]}...")
            photos_json = obj.get('photos_json', {})
            if isinstance(photos_json, dict):
                print(f"  • Photos JSON: {len(photos_json)} фото")
            else:
                print(f"  • Photos JSON: {photos_json}")
            
            # Продвижение
            print("\n🚀 ПРОДВИЖЕНИЕ:")
            print(f"  • Promotion Type: {obj.get('promotion_type', '—')}")
            print(f"  • Promotion Bet: {obj.get('promotion_bet', '—')}")
            
            # Даты
            print("\n📅 ДАТЫ:")
            print(f"  • Created At: {obj.get('created_at', '—')}")
            print(f"  • Updated At: {obj.get('updated_at', '—')}")
            
            # JSON дамп для детального просмотра
            print("\n🔍 ПОЛНЫЙ JSON:")
            print(json.dumps(obj, indent=2, ensure_ascii=False))
            
    except Exception as e:
        print(f"❌ Ошибка при получении objects: {e}")

def inspect_agents():
    """Инспекция таблицы agents"""
    print_section("👤 ТАБЛИЦА: agents")
    
    try:
        response = supabase.table("agents").select("*").execute()
        agents = response.data
        
        print(f"\n✅ Найдено агентов: {len(agents)}\n")
        
        for idx, agent in enumerate(agents, 1):
            print(f"\n{'─'*80}")
            print(f"👨‍💼 АГЕНТ #{idx}")
            print(f"{'─'*80}\n")
            
            print(f"  • ID: {agent.get('id', '—')}")
            print(f"  • Email: {agent.get('email', '—')}")
            print(f"  • Phone: {agent.get('phone', '—')}")
            print(f"  • First Name: {agent.get('first_name', '—')}")
            print(f"  • Last Name: {agent.get('last_name', '—')}")
            print(f"  • Avatar URL: {agent.get('avatar_url', '—')}")
            
            print("\n🔍 ПОЛНЫЙ JSON:")
            print(json.dumps(agent, indent=2, ensure_ascii=False))
            
    except Exception as e:
        print(f"❌ Ошибка при получении agents: {e}")

def main():
    print("\n" + "="*80)
    print(" "*25 + "🔍 ИНСПЕКЦИЯ ДАННЫХ SUPABASE")
    print("="*80)
    
    print(f"\n📍 URL: {SUPABASE_URL}\n")
    
    # Инспектируем обе таблицы
    inspect_objects()
    inspect_agents()
    
    print("\n" + "="*80)
    print(" "*30 + "✨ ГОТОВО!")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
