from telethon import TelegramClient, events, sync
import logging
import config
import sqlite3

logging.basicConfig(level=logging.DEBUG)

try:
    # Подключение к базе данных SQLite
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS messaged_users (user_id INTEGER PRIMARY KEY)''')
    conn.commit()
    logging.info('База данных SQLite успешно инициализирована')
except Exception as e:
    logging.error(f'Ошибка при инициализации базы данных SQLite: {e}')

try:
    # Используйте API ID и HASH из config
    client = TelegramClient('dsadassdadad', config.api_id, config.api_hash)
    logging.info('Клиент Telegram успешно создан')
except Exception as e:
    logging.error(f'Ошибка при создании клиента Telegram: {e}')

@client.on(events.NewMessage(chats=config.channel_usernames))
async def handler(event):
    try:
        logging.info("NEW MESSAGE")
        user_id = event.sender_id
        # Получаем информацию о чате
        chat = await event.get_chat()
        chat_title = getattr(chat, 'title', 'некий чат')  # Получаем название чата или используем 'некий чат' по умолчанию
        message_text = event.message.text if event.message else 'Сообщение без текста'  # Получаем текст сообщения или стандартное сообщение

        cursor.execute('SELECT user_id FROM messaged_users WHERE user_id = ?', (user_id,))
        if cursor.fetchone() is None:
            cursor.execute('INSERT INTO messaged_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logging.info(f'Новый пользователь {user_id} добавлен в базу данных')
            try:
                # Формируем сообщение со ссылкой на профиль пользователя
                user_link = f"[Пользователь с ID {user_id}](tg://user?id={user_id})"
                message_content = f'{user_link} впервые написал в чат "{chat_title}": {message_text}'
                
                # Отправляем сообщение с информацией о новом пользователе, названии чата и тексте сообщения
                await client.send_message(-1002353242489, message_content, parse_mode='md')
                logging.info(f'Сообщение о новом пользователе {user_id} отправлено')
            except Exception as e:
                logging.error(f'Ошибка при отправке сообщения о новом пользователе {user_id}: {e}')
    except Exception as e:
        logging.error(f'Ошибка при обработке нового сообщения: {e}')

async def main():
    await client.start()
    await client.catch_up()  # Подписаться на обновления в реальном времени
    logging.info("Client started. Listening for new messages...")
    await client.run_until_disconnected()

with client:
    client.loop.run_until_complete(main())
