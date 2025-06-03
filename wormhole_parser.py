import requests
import json
from datetime import datetime, timedelta
import time
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import pathlib

# Загружаем переменные окружения
load_dotenv()

# Конфигурация базы данных
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432')
}

# Создаем директорию для данных, если её нет
DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(exist_ok=True)

PROGRESS_FILE = DATA_DIR / "last_processed_date.txt"

def get_last_processed_date():
    """Получает дату последней обработанной транзакции"""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            date_str = f.read().strip()
            return datetime.fromisoformat(date_str)
    return datetime(2022, 3, 1)  # Начальная дата

def save_last_processed_date(date):
    """Сохраняет дату последней обработанной транзакции"""
    with open(PROGRESS_FILE, 'w') as f:
        f.write(date.isoformat())

def get_db_connection():
    """Создает подключение к базе данных"""
    return psycopg2.connect(**DB_CONFIG)

def insert_transaction(transaction_data, has_data_block):
    """Вставляет транзакцию в соответствующую таблицу базы данных"""
    table_name = "txs" if has_data_block else "txs_transport"
    insert_query = f"""
    INSERT INTO {table_name} (
        from_address, from_chain, from_hash, from_token, from_amount, from_timestamp,
        to_address, to_chain, to_hash, to_timestamp, to_token, to_amount,
        status, bridge_id, bridge_from, bridge_to
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s
    );
    """
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_query, transaction_data)
        conn.commit()

CHAIN_ID_TO_NAME = {
    0: "Unset",
    1: "Solana",
    2: "Ethereum",
    3: "Terra",
    4: "BSC",
    5: "Polygon",
    6: "Avalanche",
    7: "Oasis",
    8: "Algorand",
    9: "Aurora",
    10: "Fantom",
    11: "Karura",
    12: "Acala",
    13: "Klaytn",
    14: "Celo",
    15: "Near",
    16: "Moonbeam",
    18: "Terra2",
    19: "Injective",
    20: "Osmosis",
    21: "Sui",
    22: "Aptos",
    23: "Arbitrum",
    24: "Optimism",
    25: "Gnosis",
    26: "PythNet",
    28: "Xpla",
    29: "Btc",
    30: "Base",
    31: "FileCoin",
    32: "Sei",
    33: "Rootstock",
    34: "Scroll",
    35: "Mantle",
    36: "Blast",
    37: "XLayer",
    38: "Linea",
    39: "Berachain",
    40: "SeiEVM",
    41: "Eclipse",
    42: "BOB",
    43: "Snaxchain",
    44: "Unichain",
    45: "Worldchain",
    46: "Ink",
    47: "HyperEVM",
    48: "Monad",
    49: "Movement",
    3104: "Wormchain",
    4000: "Cosmoshub",
    4001: "Evmos",
    4002: "Kujira",
    4003: "Neutron",
    4004: "Celestia",
    4005: "Stargaze",
    4006: "Seda",
    4007: "Dymension",
    4008: "Provenance",
    4009: "Noble",
    10002: "Sepolia",
    10003: "ArbitrumSepolia",
    10004: "BaseSepolia",
    10005: "OptimismSepolia",
    10006: "Holesky",
    10007: "PolygonSepolia",
}

def get_chain_name(chain_id):
    return CHAIN_ID_TO_NAME.get(chain_id, f"Unknown Chain ({chain_id})").lower()

def generate_date_ranges():
    """Генерирует диапазоны дат для обработки"""
    last_date = get_last_processed_date()
    current_date = last_date + timedelta(days=1)
    end_date = datetime.now() - timedelta(days=1)  # Вчерашний день
    
    while current_date <= end_date:
        yield (
            current_date.strftime("%Y-%m-%dT00:00:00.000Z"),
            current_date.strftime("%Y-%m-%dT23:59:59.999Z")
        )
        current_date = current_date + timedelta(days=1)

def get_bridge_id():
    """Получает ID моста Wormhole из базы данных"""
    query = """
        SELECT id FROM public.bridges 
        WHERE name = 'https://wormholescan.io/' 
        LIMIT 1
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            if result:
                return result[0]
            raise Exception("Bridge ID не найден в базе данных")

def parse_wormhole_data_for_date_range(from_date, to_date):
    """Парсит данные для указанного диапазона дат"""
    url = "https://api.wormholescan.io/api/v1/operations"
    page = 0
    total_processed = 0
    total_transport = 0
    
    # Получаем bridge_id один раз в начале
    bridge_id = get_bridge_id()
    
    while True:
        params = {
            "page": page,
            "pageSize": 50,
            "sortOrder": "DESC",
            "from": from_date,
            "to": to_date
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            operations = data.get("operations", [])
            if not operations:
                break
                
            for operation in operations:
                # Получаем время и конвертируем в читаемый формат
                timestamp = operation.get("sourceChain", {}).get("timestamp")
                if timestamp:
                    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    unix_timestamp = int(dt.timestamp())
                
                # Получаем данные о сетях и адресах
                from_chain_id = operation.get("sourceChain", {}).get("chainId")
                from_address = operation.get("sourceChain", {}).get("from")
                from_tx = operation.get("sourceChain", {}).get("transaction", {}).get("txHash")
                
                to_chain_id = operation.get("content", {}).get("standarizedProperties", {}).get("toChain")
                to_address = operation.get("content", {}).get("standarizedProperties", {}).get("toAddress")
                to_tx = operation.get("content", {}).get("standarizedProperties", {}).get("toTransactionHash")
                
                # Пропускаем транзакции с пустым адресом получателя
                if not to_address:
                    continue
                
                # Проверяем наличие блока data с нужными полями
                data_block = operation.get("data", {})
                has_data_block = bool(data_block and "tokenAmount" in data_block and "symbol" in data_block)
                
                # Получаем имена сетей
                from_chain = get_chain_name(from_chain_id)
                to_chain = get_chain_name(to_chain_id)
                
                # Подготавливаем данные для вставки
                transaction_data = (
                    from_address,
                    from_chain,  # используем имя сети вместо ID
                    from_tx,
                    data_block.get('symbol') if has_data_block else None,
                    float(data_block.get('tokenAmount', 0)) if has_data_block else None,
                    unix_timestamp,
                    to_address,
                    to_chain,  # используем имя сети вместо ID
                    to_tx,
                    None,  # to_timestamp будет заполнен позже
                    data_block.get('symbol') if has_data_block else None,
                    float(data_block.get('tokenAmount', 0)) if has_data_block else None,
                    operation.get("sourceChain", {}).get("status", "unknown"),
                    bridge_id,
                    from_chain,  # используем то же имя сети
                    to_chain  # используем то же имя сети
                )
                
                # Вставляем данные в соответствующую таблицу
                insert_transaction(transaction_data, has_data_block)
                
                # Выводим данные
                print("-" * 100)
                print(f"Время: {time_str}")
                print(f"Откуда: {from_chain} ({from_address})")
                print(f"Транзакция откуда: {from_tx}")
                print(f"Куда: {to_chain} ({to_address})")
                if to_tx:
                    print(f"Транзакция куда: {to_tx}")
                if has_data_block:
                    print(f"Сумма: {data_block['tokenAmount']} {data_block['symbol']}")
                    total_processed += 1
                else:
                    print("Транспортная транзакция")
                    total_transport += 1
                print("-" * 100)
            
            page += 1
            # Добавляем небольшую задержку между запросами
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при выполнении запроса: {e}")
            break
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            break
    
    return total_processed, total_transport

def parse_wormhole_data():
    """Основная функция для парсинга данных"""
    total_transactions = 0
    total_transport = 0
    last_processed_date = None
    
    try:
        for from_date, to_date in generate_date_ranges():
            print(f"\nОбработка периода: {from_date} - {to_date}")
            processed, transport = parse_wormhole_data_for_date_range(from_date, to_date)
            total_transactions += processed
            total_transport += transport
            print(f"Обработано транзакций за период: {processed}")
            print(f"Обработано транспортных транзакций за период: {transport}")
            
            # Сохраняем прогресс после каждого дня
            current_date = datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            save_last_processed_date(current_date)
            last_processed_date = current_date
            
            # Добавляем небольшую задержку между днями
            time.sleep(1)
        
        print(f"\nВсего обработано транзакций: {total_transactions}")
        print(f"Всего обработано транспортных транзакций: {total_transport}")
        if last_processed_date:
            print(f"Последняя обработанная дата: {last_processed_date.strftime('%Y-%m-%d')}")
            
    except KeyboardInterrupt:
        print("\nОбработка прервана пользователем")
        if last_processed_date:
            print(f"Последняя обработанная дата: {last_processed_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        print(f"\nПроизошла ошибка: {e}")
        if last_processed_date:
            print(f"Последняя обработанная дата: {last_processed_date.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    parse_wormhole_data() 