from flask import Flask, request, jsonify
import sqlite3
import datetime
import boto3
import botocore.exceptions

app = Flask(__name__)

# Данные для подключения к Yandex Cloud
AWS_ACCESS_KEY_ID = 'YCAJE4t3j8XcCLHEl79Vg0cFz' # Замените на ваш Access Key ID
AWS_SECRET_ACCESS_KEY = 'YCOTRa_l6J4ANGdAbMSOtgy8lwEkYhKBqlHxPjs7' # Замените на ваш Secret Access Key
BUCKET_NAME = 'class-18'  # Замените на имя вашего бакета
DB_FILE_KEY = 'users.db'

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
        s3_client.download_file(BUCKET_NAME, DB_FILE_KEY, 'users.db')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("Файл базы данных не найден в бакете, будет создан новый.")
        else:
            print(f"Ошибка при загрузке базы данных: {e}")
            raise

# Функция для загрузки базы данных в бакет Yandex Cloud
def upload_db():
    try:
        s3_client.upload_file('users.db', BUCKET_NAME, DB_FILE_KEY)
    except Exception as e:
        print(f"Ошибка при загрузке базы данных: {e}")

# Менеджер контекста для операций с базой данных
class DatabaseManager:
    def __enter__(self):
        download_db()
        self.conn = sqlite3.connect('users.db')
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
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            photo_url TEXT,
            subscription_status TEXT,
            tariff_name TEXT,
            video_balance INTEGER,
            total_downloaded_videos INTEGER
        )
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS activity_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            action_type TEXT,
            details TEXT,
            timestamp TEXT,
            FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
        )
        ''')
        conn.commit()

# Функции для работы с базой данных
def get_user(telegram_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM users WHERE telegram_id=?', (telegram_id,))
        result = c.fetchone()
        if result:
            user = {
                'telegram_id': result[0],
                'first_name': result[1],
                'last_name': result[2],
                'username': result[3],
                'photo_url': result[4],
                'subscription_status': result[5],
                'tariff_name': result[6],
                'video_balance': result[7],
                'total_downloaded_videos': result[8]
            }
            # Получаем историю действий
            c.execute('SELECT action_type, details, timestamp FROM activity_history WHERE telegram_id=?', (telegram_id,))
            history = []
            for row in c.fetchall():
                history.append({
                    'action_type': row[0],
                    'details': eval(row[1]),
                    'timestamp': row[2]
                })
            user['history'] = history
            return user
        else:
            return None

def create_user(user_data):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('''
        INSERT OR IGNORE INTO users (telegram_id, first_name, last_name, username, photo_url, subscription_status, video_balance, total_downloaded_videos)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_data['telegram_id'],
            user_data['first_name'],
            user_data['last_name'],
            user_data['username'],
            user_data['photo_url'],
            'inactive',
            0,
            0
        ))
        conn.commit()

def activate_tariff(telegram_id, tariff_name, video_balance, price):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('''
        UPDATE users SET subscription_status=?, tariff_name=?, video_balance=? WHERE telegram_id=?
        ''', ('active', tariff_name, video_balance, telegram_id))
        # Добавляем запись в историю
        details = {
            'tariff_name': tariff_name,
            'quantity': video_balance,
            'price': price
        }
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute('''
        INSERT INTO activity_history (telegram_id, action_type, details, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (telegram_id, 'subscription', str(details), timestamp))
        conn.commit()

def decrement_video_balance(telegram_id):
    with DatabaseManager() as conn:
        c = conn.cursor()
        c.execute('SELECT video_balance FROM users WHERE telegram_id=?', (telegram_id,))
        result = c.fetchone()
        if result and result[0] > 0:
            new_balance = result[0] - 1
            c.execute('UPDATE users SET video_balance=? WHERE telegram_id=?', (new_balance, telegram_id))
            conn.commit()
            return True
        else:
            return False

def add_activity_history(telegram_id, action_type, details):
    with DatabaseManager() as conn:
        c = conn.cursor()
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute('''
        INSERT INTO activity_history (telegram_id, action_type, details, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (telegram_id, action_type, str(details), timestamp))
        conn.commit()

# Маршруты API
@app.route('/api/users', methods=['POST'])
def api_create_user():
    data = request.json
    create_user(data)
    return jsonify({'message': 'User created'}), 200

@app.route('/api/users/<int:telegram_id>', methods=['GET'])
def api_get_user(telegram_id):
    user = get_user(telegram_id)
    if user:
        return jsonify(user), 200
    else:
        return jsonify({'error': 'User not found'}), 404

@app.route('/api/users/<int:telegram_id>/activate_tariff', methods=['POST'])
def api_activate_tariff(telegram_id):
    data = request.json
    activate_tariff(telegram_id, data['tariff_name'], data['video_balance'], data['price'])
    return jsonify({'message': 'Tariff activated'}), 200

@app.route('/api/users/<int:telegram_id>/decrement_balance', methods=['POST'])
def api_decrement_balance(telegram_id):
    success = decrement_video_balance(telegram_id)
    if success:
        return jsonify({'message': 'Balance updated'}), 200
    else:
        return jsonify({'error': 'Unable to update balance'}), 400

@app.route('/api/users/<int:telegram_id>/add_history', methods=['POST'])
def api_add_history(telegram_id):
    data = request.json
    add_activity_history(telegram_id, data['action_type'], data['details'])
    return jsonify({'message': 'History added'}), 200

if __name__ == '__main__':
    setup_database()
    app.run(host='0.0.0.0', port=5000)
