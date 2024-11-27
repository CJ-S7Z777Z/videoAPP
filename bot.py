
import sqlite3
import yt_dlp
import requests
import os
import telegram
import asyncio
import ssl
import vk_api
import urllib.parse
import boto3
import botocore.exceptions
import logging
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler
)
import aiohttp

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Главные администраторы
ADMIN_CHAT_IDS = [1276928573, 332786197, 1786980999, 228845914]  # Замените на реальные ID главных админов

# VK App ID
VK_CLIENT_ID = '52616258'  # Укажите здесь ваш ID приложения VK

# Состояния для ConversationHandler
AUTHORIZATION = 0
WAITING_GROUP_TOKEN = 1
WAITING_GROUP_ID = 2
WAITING_ADMIN_ID = 3
WAITING_REMOVE_ADMIN_ID = 4
WAITING_GROUP_REMOVE_ID = 5
WAITING_FOR_USER_ID = 6
WAITING_FOR_TARIFF_SELECTION = 7

# Данные для подключения к Yandex Cloud
AWS_ACCESS_KEY_ID = 'YCAJE4t3j8XcCLHEl79Vg0cFz' # Замените на ваш Access Key ID
AWS_SECRET_ACCESS_KEY = 'YCOTRa_l6J4ANGdAbMSOtgy8lwEkYhKBqlHxPjs7' # Замените на ваш Secret Access Key

# Имя бакета и ключ для файла базы данных
BUCKET_NAME = 'class-18'  # Замените на имя вашего бакета
DB_FILE_KEY = 'vk_groups.db'  # Имя файла базы данных в бакете

# Настройка клиента S3 для Yandex Cloud
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url='https://storage.yandexcloud.net'
)

# Функция для загрузки базы данных из бакета Yandex Cloud
def download_db():
    try:
        s3_client.download_file(BUCKET_NAME, DB_FILE_KEY, 'vk_groups.db')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("Файл базы данных не найден в бакете, будет создан новый.")
        else:
            print(f"Ошибка при загрузке базы данных: {e}")
            raise

# Функция для загрузки базы данных в бакет Yandex Cloud
def upload_db():
    try:
        s3_client.upload_file('vk_groups.db', BUCKET_NAME, DB_FILE_KEY)
    except Exception as e:
        print(f"Ошибка при загрузке базы данных: {e}")

# Менеджер контекста для операций с базой данных
class DatabaseManager:
    def __enter__(self):
        download_db()
        self.conn = sqlite3.connect('vk_groups.db')
        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.commit()
        self.conn.close()
        upload_db()

# Создание базы данных
def setup_database():
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('''
        CREATE TABLE IF NOT EXISTS admins
        (chat_id INTEGER PRIMARY KEY, user_token TEXT)
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS groups
        (group_id TEXT, token TEXT, name TEXT, admin_chat_id INTEGER)
        ''')
        conn.commit()

# Остальные функции работы с базой данных
def get_admin_token(chat_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('SELECT user_token FROM admins WHERE chat_id=?', (chat_id,))
        result = c.fetchone()
    if result:
        return result[0]
    else:
        return None

def update_admin_token(chat_id, user_token):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO admins (chat_id, user_token) VALUES (?, ?)', (chat_id, user_token))
        conn.commit()

def is_admin(chat_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM admins WHERE chat_id=?', (chat_id,))
        result = c.fetchone()
    return result is not None

def add_admin_to_db(chat_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO admins (chat_id) VALUES (?)', (chat_id,))
        conn.commit()

def remove_admin_from_db(chat_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM admins WHERE chat_id=?', (chat_id,))
        conn.commit()

def get_admins():
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id FROM admins')
        admins = c.fetchall()
    return admins

def add_group_to_db(group_id, token, name, admin_chat_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('INSERT INTO groups VALUES (?, ?, ?, ?)', (group_id, token, name, admin_chat_id))
        conn.commit()

def get_groups(admin_chat_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM groups WHERE admin_chat_id=?', (admin_chat_id,))
        groups = c.fetchall()
    return groups

def remove_group_from_db(group_id, admin_chat_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM groups WHERE group_id=? AND admin_chat_id=?', (group_id, admin_chat_id))
        conn.commit()

# Функция для запросов к мини-приложению
def get_user_from_mini_app(telegram_id):
    try:
        response = requests.get(f'https://ВАШ_ДОМЕН/api/users/{telegram_id}')
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except:
        return None

def decrement_user_balance(telegram_id):
    try:
        response = requests.post(f'https://ВАШ_ДОМЕН/api/users/{telegram_id}/decrement_balance')
        return response.status_code == 200
    except:
        return False

def add_user_history(telegram_id, action_type, details):
    try:
        data = {
            'action_type': action_type,
            'details': details
        }
        requests.post(f'https://ВАШ_ДОМЕН/api/users/{telegram_id}/add_history', json=data)
    except:
        pass

# Генерация ссылки для авторизации VK
def get_vk_auth_url():
    SCOPE = 'video,wall,groups,offline'
    return f'https://oauth.vk.com/authorize?client_id={VK_CLIENT_ID}&display=page&redirect_uri=https://oauth.vk.com/blank.html&scope={SCOPE}&response_type=token&v=5.131'

# Извлечение access_token из ссылки
def extract_access_token_from_url(url):
    parsed = urllib.parse.urlparse(url)
    fragment_params = urllib.parse.parse_qs(parsed.fragment)
    return fragment_params.get('access_token', [None])[0]

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    # Проверяем, является ли пользователь администратором
    if chat_id in ADMIN_CHAT_IDS or is_admin(chat_id):
        # Проверяем, есть ли у администратора user_token
        user_token = get_admin_token(chat_id)
        if not user_token:
            # Отправляем ссылку для авторизации
            auth_url = get_vk_auth_url()
            await update.message.reply_text(
                f'Вы не авторизованы.\nПожалуйста, перейдите по ссылке для авторизации:\n{auth_url}\n'
                'После авторизации скопируйте всю ссылку из адресной строки и отправьте мне.'
            )
            return AUTHORIZATION
        else:
            # Отправляем главное меню
            keyboard = [
                [KeyboardButton('Добавить группу'), KeyboardButton('Мои группы')],
                [KeyboardButton('Удалить группу')],
                [KeyboardButton('Открыть доступ пользователю')]
            ]
            if chat_id in ADMIN_CHAT_IDS:
                keyboard.append([KeyboardButton('Добавить администратора'), KeyboardButton('Удалить администратора')])
                keyboard.append([KeyboardButton('Администраторы')])
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                'Привет! Я помогу скачивать видео из TikTok, YouTube, VK клипы и публиковать видео в группы ВКонтакте.',
                reply_markup=reply_markup
            )
            return ConversationHandler.END
    else:
        # Проверяем, есть ли у пользователя активный тариф
        user_data = get_user_from_mini_app(chat_id)
        if user_data and user_data['subscription_status'] == 'active':
            # Отправляем главное меню для пользователей
            await update.message.reply_text(
                'Привет! Отправьте ссылку на видео из TikTok, YouTube, VK или Instagram.'
            )
            return ConversationHandler.END
        else:
            # Отправляем сообщение о неактивном тарифе
            await update.message.reply_text(
                'Ваш тариф не активен. Пожалуйста, активируйте тариф в мини-приложении.',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('Главная', url='https://t.me/ВАШ_БОТ?start=app')]
                ])
            )
            return ConversationHandler.END

# Обработка команды /app
async def app_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Откройте мини-приложение:',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('Главная', url='https://t.me/ВАШ_БОТ?start=app')]
        ])
    )

# Обработка ссылки авторизации
async def handle_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    url = update.message.text.strip()
    # Извлекаем access_token из ссылки
    access_token = extract_access_token_from_url(url)
    if access_token:
        # Добавляем администратора в базу данных, если его нет
        if not is_admin(chat_id):
            add_admin_to_db(chat_id)
        # Сохраняем access_token в базе данных
        update_admin_token(chat_id, access_token)
        await update.message.reply_text('✅ Вы успешно авторизовались!')
        # Отправляем главное меню
        await start(update, context)
        return ConversationHandler.END
    else:
        await update.message.reply_text(
            'Не удалось извлечь access_token. Убедитесь, что вы отправили полную ссылку из адресной строки после авторизации.'
        )
        return AUTHORIZATION

# Остальные функции остаются без изменений, кроме добавления проверок тарифа для обычных пользователей

# Команда 'Добавить группу'
async def add_group_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id in ADMIN_CHAT_IDS or is_admin(chat_id):
        reply_markup = ReplyKeyboardMarkup(
            [[KeyboardButton('Отмена')]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await update.message.reply_text(
            'Пожалуйста, отправьте токен группы ВКонтакте:',
            reply_markup=reply_markup
        )
        return WAITING_GROUP_TOKEN
    else:
        await update.message.reply_text(
            'Этот бот доступен только для администраторов.'
        )
        return ConversationHandler.END

async def group_token_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    context.user_data['group_token'] = text
    reply_markup = ReplyKeyboardMarkup(
        [[KeyboardButton('Отмена')]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await update.message.reply_text(
        'Теперь отправьте ID группы (без минуса):',
        reply_markup=reply_markup
    )
    return WAITING_GROUP_ID

async def group_id_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    chat_id = update.message.chat_id
    group_id = f"-{text}"
    group_token = context.user_data['group_token']

    try:
        vk_session = vk_api.VkApi(token=group_token)
        vk = vk_session.get_api()
        group_info = vk.groups.getById(group_id=group_id[1:])[0]

        add_group_to_db(group_id, group_token, group_info['name'], chat_id)

        await update.message.reply_text(
            f'✅ Группа "{group_info["name"]}" успешно добавлена!'
        )
    except Exception as e:
        await update.message.reply_text(
            f'❌ Ошибка при добавлении группы: {str(e)}'
        )

    await start(update, context)
    return ConversationHandler.END

# Команда 'Мои группы'
async def show_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id in ADMIN_CHAT_IDS or is_admin(chat_id):
        groups = get_groups(chat_id)
        if not groups:
            await update.message.reply_text('Нет добавленных групп.')
            return

        message = "Добавленные группы:\n\n"
        for i, group in enumerate(groups, 1):
            message += f"{i}. {group[2]} (ID: {group[0]})\n"

        await update.message.reply_text(message)
    else:
        await update.message.reply_text(
            'Этот бот доступен только для администраторов.'
        )

# Команда 'Добавить администратора'
async def add_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id in ADMIN_CHAT_IDS:
        reply_markup = ReplyKeyboardMarkup(
            [[KeyboardButton('Отмена')]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await update.message.reply_text(
            'Пожалуйста, отправьте ID пользователя, которого вы хотите добавить в качестве администратора:',
            reply_markup=reply_markup
        )
        return WAITING_ADMIN_ID
    else:
        await update.message.reply_text(
            'Команда доступна только для главных администраторов.'
        )
        return ConversationHandler.END

async def add_admin_id_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        new_admin_chat_id = int(text)
        if new_admin_chat_id not in ADMIN_CHAT_IDS and not is_admin(new_admin_chat_id):
            add_admin_to_db(new_admin_chat_id)
            await update.message.reply_text(
                f'✅ Пользователь с ID {new_admin_chat_id} добавлен в качестве администратора!'
            )
        else:
            await update.message.reply_text(
                f'❌ Пользователь с ID {new_admin_chat_id} уже является администратором.'
            )
    except ValueError:
        await update.message.reply_text(
            'Неверный формат ID пользователя. Введите число.'
        )
        return WAITING_ADMIN_ID
    await start(update, context)
    return ConversationHandler.END

# Команда 'Удалить администратора'
async def remove_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id in ADMIN_CHAT_IDS:
        admins = get_admins()
        if not admins:
            await update.message.reply_text('Нет добавленных администраторов.')
            return ConversationHandler.END

        message = "Добавленные администраторы:\n\n"
        for i, admin in enumerate(admins, 1):
            message += f"{i}. ID: {admin[0]}\n"

        reply_markup = ReplyKeyboardMarkup(
            [[KeyboardButton('Отмена')]],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        await update.message.reply_text(
            f"{message}\nПожалуйста, отправьте ID администратора, которого вы хотите удалить:",
            reply_markup=reply_markup
        )
        return WAITING_REMOVE_ADMIN_ID
    else:
        await update.message.reply_text(
            'Команда доступна только для главных администраторов.'
        )
        return ConversationHandler.END

async def remove_admin_id_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        admin_chat_id = int(text)
        if is_admin(admin_chat_id):
            remove_admin_from_db(admin_chat_id)
            await update.message.reply_text(
                f'✅ Пользователь с ID {admin_chat_id} удален из списка администраторов!'
            )
        else:
            await update.message.reply_text(
                f'❌ Пользователь с ID {admin_chat_id} не является администратором.'
            )
    except ValueError:
        await update.message.reply_text(
            'Неверный формат ID пользователя. Введите число.'
        )
        return WAITING_REMOVE_ADMIN_ID
    await start(update, context)
    return ConversationHandler.END

# Команда 'Удалить группу'
async def remove_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id in ADMIN_CHAT_IDS or is_admin(chat_id):
        groups = get_groups(chat_id)
        if not groups:
            await update.message.reply_text('Нет добавленных групп.')
            return ConversationHandler.END

        message = "Добавленные группы:\n\n"
        for i, group in enumerate(groups, 1):
            message += f"{i}. {group[2]} (ID: {group[0]})\n"

        reply_markup = ReplyKeyboardMarkup(
            [[KeyboardButton(str(i)) for i in range(1, len(groups)+1)],
             [KeyboardButton('Отмена')]],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        await update.message.reply_text(
            f"{message}\nПожалуйста, выберите номер группы для удаления или нажмите 'Отмена':",
            reply_markup=reply_markup
        )
        context.user_data['groups'] = groups
        return WAITING_GROUP_REMOVE_ID
    else:
        await update.message.reply_text(
            'Этот бот доступен только для администраторов.'
        )
        return ConversationHandler.END

async def group_remove_id_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        group_index = int(text) - 1
        groups = context.user_data.get('groups', [])
        if 0 <= group_index < len(groups):
            group_id = groups[group_index][0]
            remove_group_from_db(group_id, update.message.chat_id)
            await update.message.reply_text(
                f'✅ Группа {groups[group_index][2]} (ID: {group_id}) удалена!'
            )
        else:
            await update.message.reply_text(
                'Неверный номер группы.'
            )
            return WAITING_GROUP_REMOVE_ID
    except ValueError:
        await update.message.reply_text(
            'Неверный формат номера группы. Введите число.'
        )
        return WAITING_GROUP_REMOVE_ID
    await start(update, context)
    return ConversationHandler.END

# Команда 'Администраторы'
async def show_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id in ADMIN_CHAT_IDS:
        admins = get_admins()
        if not admins:
            await update.message.reply_text('Нет добавленных администраторов.')
            return

        message = "Добавленные администраторы:\n\n"
        for i, admin in enumerate(admins, 1):
            message += f"{i}. ID: {admin[0]}\n"

        await update.message.reply_text(message)
    else:
        await update.message.reply_text(
            'Команда доступна только для главных администраторов.'
        )

# Обработка сообщений с ссылками на видео
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    user_data = get_user_from_mini_app(chat_id)
    if chat_id in ADMIN_CHAT_IDS or is_admin(chat_id):
        # Администраторская логика
        user_token = get_admin_token(chat_id)
        if not user_token:
            await update.message.reply_text(
                'Вы не авторизованы. Используйте команду /start, чтобы авторизоваться.'
            )
            return

        groups = get_groups(chat_id)
        if not groups:
            await update.message.reply_text(
                'Нет добавленных групп. Используйте кнопку "Добавить группу", чтобы добавить группу.'
            )
            return

        # Проверяем, содержит ли сообщение ссылку на видео
        url = update.message.text.strip()
        if any(domain in url for domain in ['tiktok.com', 'youtube.com', 'youtu.be', 'vk.com', 'instagram.com']):
            # Отправляем сообщение "Идет загрузка..."
            loading_message = await update.message.reply_text('Идет загрузка...')
            try:
                # Создаем директорию для администратора
                admin_video_dir = f'videos/{chat_id}'
                if not os.path.exists(admin_video_dir):
                    os.makedirs(admin_video_dir)

                # Загрузка видео
                ydl_options = {
                    'format': 'best',
                    'outtmpl': f'{admin_video_dir}/downloaded_video.%(ext)s',
                    'quiet': True,
                    'socket_timeout': 600,
                    'geo_bypass': True,
                    'geo_bypass_country': 'DE',
                }
                with yt_dlp.YoutubeDL(ydl_options) as ydl:
                    result = ydl.extract_info(url, download=True)
                    video_file = ydl.prepare_filename(result)

                await asyncio.sleep(1)
                await loading_message.delete()

                # Публикация видео в группы
                keyboard = []
                row = []
                for i, group in enumerate(groups, 1):
                    row.append(InlineKeyboardButton(group[2], callback_data=f"post_{i-1}"))
                    if len(row) == 2:
                        keyboard.append(row)
                        row = []
                if row:
                    keyboard.append(row)

                reply_markup = InlineKeyboardMarkup(keyboard)

                # Отправляем сообщение с клавиатурой
                await update.message.reply_text(
                    'Выберите группу для публикации:',
                    reply_markup=reply_markup
                )

                # Сохраняем видео и информацию о группах для обработки в button_callback
                context.user_data['video_path'] = video_file
                context.user_data['groups'] = groups
                context.user_data['user_token'] = user_token

                # Устанавливаем таймер для удаления видео
                DELETE_TIMEOUT = 90  # Время в секундах

                async def delete_video_after_timeout(chat_id, video_path, timeout):
                    try:
                        await asyncio.sleep(timeout)
                        if os.path.exists(video_path):
                            os.remove(video_path)
                            print(f"Видео файл для chat_id {chat_id} удален после тайм-аута.")
                            # Удаляем папку администратора, если она пуста
                            admin_video_dir = os.path.dirname(video_path)
                            try:
                                os.rmdir(admin_video_dir)
                            except OSError:
                                pass  # Папка не пуста
                    except asyncio.CancelledError:
                        # Задача была отменена, ничего не делаем
                        pass

                delete_task = asyncio.create_task(delete_video_after_timeout(chat_id, video_file, DELETE_TIMEOUT))
                context.user_data['delete_task'] = delete_task

            except Exception as e:
                await update.message.reply_text(
                    f'Ошибка при скачивании видео: {str(e)}'
                )
        else:
            await update.message.reply_text(
                'Пожалуйста, отправьте ссылку на видео из TikTok, YouTube, VK или Instagram.'
            )
    elif user_data and user_data['subscription_status'] == 'active':
        # Логика для пользователей с активным тарифом
        # Проверяем, есть ли у пользователя оставшиеся видео
        if user_data['video_balance'] <= 0:
            await update.message.reply_text(
                'Ваш лимит скачивания видео исчерпан. Пожалуйста, активируйте новый тариф.',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('Главная', url='https://t.me/ВАШ_БОТ?start=app')]
                ])
            )
            return

        # Обработка ссылки на видео
        url = update.message.text.strip()
        if any(domain in url for domain in ['tiktok.com', 'youtube.com', 'youtu.be', 'vk.com', 'instagram.com']):
            # Отправляем сообщение "Идет загрузка..."
            loading_message = await update.message.reply_text('Идет загрузка...')
            try:
                # Скачиваем видео
                ydl_options = {
                    'format': 'best',
                    'outtmpl': f'videos/{chat_id}/downloaded_video.%(ext)s',
                    'quiet': True,
                    'socket_timeout': 600,
                    'geo_bypass': True,
                    'geo_bypass_country': 'DE',
                }
                with yt_dlp.YoutubeDL(ydl_options) as ydl:
                    result = ydl.extract_info(url, download=True)
                    video_file = ydl.prepare_filename(result)

                await asyncio.sleep(1)
                await loading_message.delete()

                # Отправляем видео пользователю
                await update.message.reply_video(video=open(video_file, 'rb'))

                # Удаляем видео
                if os.path.exists(video_file):
                    os.remove(video_file)

                # Вычитаем из баланса
                decrement_user_balance(chat_id)

                # Добавляем запись в историю
                add_user_history(chat_id, 'download', {
                    'title': result.get('title'),
                    'source': result.get('extractor_key'),
                    'link': url
                })

                # Информируем о оставшемся балансе
                user_data = get_user_from_mini_app(chat_id)
                await update.message.reply_text(
                    f"Остаток по тарифу: {user_data['video_balance']} видео"
                )

            except Exception as e:
                await update.message.reply_text(f'Ошибка при скачивании видео: {str(e)}')

        else:
            await update.message.reply_text(
                'Пожалуйста, отправьте ссылку на видео из TikTok, YouTube, VK или Instagram.'
            )

    else:
        await update.message.reply_text(
            'Ваш тариф не активен. Пожалуйста, активируйте тариф в мини-приложении.',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('Главная', url='https://t.me/ВАШ_БОТ?start=app')]
            ])
        )

# Функции для работы с VK API остаются без изменений

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)  # Вызов функции start
    return ConversationHandler.END  # Завершение текущего разговора

# Обработка текста
async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    text = update.message.text.strip()
    if text == 'Добавить группу':
        return await add_group_start(update, context)
    elif text == 'Мои группы':
        await show_groups(update, context)
    elif text == 'Удалить группу':
        return await remove_group(update, context)
    elif text =='Добавить администратора':
        return await add_admin_start(update, context)
    elif text == 'Удалить администратора':
        return await remove_admin_start(update, context)
    elif text == 'Администраторы':
        await show_admins(update, context)
    elif text == 'Открыть доступ пользователю':
        return await grant_access_start(update, context)
    elif text == 'Отмена':
        await cancel(update, context)
    else:
        await handle_message(update, context)  # Обрабатываем как возможную ссылку на видео

# Функции для администраторов по открытию доступа пользователям
async def grant_access_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id in ADMIN_CHAT_IDS or is_admin(chat_id):
        await update.message.reply_text('Пожалуйста, отправьте ID пользователя, которому хотите открыть доступ:')
        return WAITING_FOR_USER_ID
    else:
        await update.message.reply_text('Вы не уполномочены использовать эту команду.')
        return ConversationHandler.END

async def receive_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.text.strip()
    context.user_data['target_user_id'] = user_id

    # Предлагаем тарифы
    tariffs = [
        {'name': 'Пробный', 'quantity': 100},
        {'name': 'Новичок', 'quantity': 500},
        {'name': 'Любитель', 'quantity': 1000},
        {'name': 'Профи', 'quantity': 2000},
        {'name': 'Бизнес', 'quantity': 3000},
    ]

    buttons = [
        [InlineKeyboardButton(f"{tariff['name']} ({tariff['quantity']} видео)", callback_data=f"tariff_{tariff['name']}_{tariff['quantity']}")] for tariff in tariffs
    ]

    reply_markup = InlineKeyboardMarkup(buttons)
    await update.message.reply_text('Выберите тариф для пользователя:', reply_markup=reply_markup)
    return WAITING_FOR_TARIFF_SELECTION

async def handle_tariff_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split('_')
    tariff_name = data[1]
    video_balance = int(data[2])
    target_user_id = context.user_data['target_user_id']

    # Активируем тариф для пользователя через API мини-приложения
    response = requests.post(f'https://ВАШ_ДОМЕН/api/users/{target_user_id}/activate_tariff', json={
        'tariff_name': tariff_name,
        'video_balance': video_balance,
        'price': 'Предоставлено администратором'
    })

    if response.status_code == 200:
        await query.edit_message_text(f'Тариф {tariff_name} активирован для пользователя {target_user_id}.')
        # Отправляем уведомление пользователю
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f'Администратор активировал для вас тариф {tariff_name}.'
            )
        except:
            pass
    else:
        await query.edit_message_text('Не удалось активировать тариф.')
    return ConversationHandler.END

def main():
    setup_database()

    application = ApplicationBuilder().token('7868968139:AAH6_cihU6bAME_WehUmGUclTw5sFMfLTac').build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CommandHandler('app', app_command),
            MessageHandler(filters.Regex('^(Добавить группу)$'), add_group_start),
            MessageHandler(filters.Regex('^(Добавить администратора)$'), add_admin_start),
            MessageHandler(filters.Regex('^(Удалить администратора)$'), remove_admin_start),
            MessageHandler(filters.Regex('^(Удалить группу)$'), remove_group),
            MessageHandler(filters.Regex('^(Открыть доступ пользователю)$'), grant_access_start),
        ],
        states={
            AUTHORIZATION: [
                MessageHandler(filters.Regex('^Отмена$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_authorization)
            ],
            WAITING_GROUP_TOKEN: [
                MessageHandler(filters.Regex('^Отмена$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, group_token_received)
            ],
            WAITING_GROUP_ID: [
                MessageHandler(filters.Regex('^Отмена$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, group_id_received)
            ],
            WAITING_ADMIN_ID: [
                MessageHandler(filters.Regex('^Отмена$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_admin_id_received)
            ],
            WAITING_REMOVE_ADMIN_ID: [
                MessageHandler(filters.Regex('^Отмена$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, remove_admin_id_received)
            ],
            WAITING_GROUP_REMOVE_ID: [
                MessageHandler(filters.Regex('^Отмена$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, group_remove_id_received)
            ],
            WAITING_FOR_USER_ID: [
                MessageHandler(filters.Regex('^Отмена$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_user_id)
            ],
            WAITING_FOR_TARIFF_SELECTION: [
                CallbackQueryHandler(handle_tariff_selection)
            ],
        },
        fallbacks=[
            CommandHandler('start', cancel),
            MessageHandler(filters.Regex('^Отмена$'), cancel)
        ],
    )

    application.add_handler(conv_handler)
    application.add_handler(MessageHandler(filters.Regex('^(Мои группы)$'), show_groups))
    application.add_handler(MessageHandler(filters.Regex('^(Администраторы)$'), show_admins))
    application.add_handler(CallbackQueryHandler(handle_tariff_selection))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    application.run_polling()

if __name__ == '__main__':
    main()
