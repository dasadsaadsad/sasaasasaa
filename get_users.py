import logging
from telethon import TelegramClient, events
import sqlite3
from config import config
import datetime
import asyncio
from concurrent.futures import ProcessPoolExecutor
import pytz
import functools

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Подключение к базе данных SQLite
def db_connect():
    try:
        conn = sqlite3.connect('databases/users/users.db')
        logger.info("Успешное подключение к базе данных")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        raise

def create_table():
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS messaged_users (user_id INTEGER PRIMARY KEY)''')
        conn.commit()
        conn.close()
        logger.info("Таблица успешно создана или уже существует")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при работе с базой данных: {e}")

# Функция для добавления user_id в базу данных
def add_user_to_db(user_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM messaged_users WHERE user_id = ?', (user_id,))
        if cursor.fetchone() is None:
            cursor.execute('INSERT INTO messaged_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logger.info(f'Новый пользователь {user_id} добавлен в базу данных')
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при добавлении пользователя в базу данных: {e}")

# Создание клиента Telegram
client = TelegramClient('anon', config.api_id, config.api_hash)


async def get_messages(client, channel_entity):
    three_months_ago = datetime.datetime.now(tz=pytz.utc) - datetime.timedelta(days=30)
    all_messages = []

    try:
        async for message in client.iter_messages(channel_entity, reverse=True, offset_date=three_months_ago):
            message_date = message.date.astimezone(pytz.utc)
            logger.info(f"MESSAGE DATE IS: {str(message_date)}")

            if message_date < three_months_ago:
                logger.info(f"Достигнуто сообщение старше 3 месяцев: {str(message_date)}")
                break

            all_messages.append(message)

        logger.info(f"Всего сообщений получено за последние 3 месяца: {len(all_messages)}")

    except Exception as e:
        logger.error(f"Ошибка при получении сообщений: {e}")

    return all_messages


async def main():
    try:
        await client.start()
        logger.info("Клиент Telegram успешно запущен")
        create_table()

        channel_entity = await client.get_entity(config.channel_usernames[4])
        logger.info(channel_entity)
        all_messages = await get_messages(client, channel_entity)

        # Добавление user_id в базу данных с использованием параллелизма
        with ProcessPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor, add_user_to_db, message.sender_id
                )
                for message in all_messages
            ]
            await asyncio.gather(*tasks)

        await client.disconnect()
        logger.info("Клиент Telegram успешно отключен")
    except Exception as e:
        logger.error(f"Ошибка в главной функции: {e}")

if __name__ == '__main__':
    asyncio.run(main())
