from datetime import datetime, UTC, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from prometheus_client import Counter, Histogram
from prometheus_flask_exporter import PrometheusMetrics
from apscheduler.schedulers.background import BackgroundScheduler
import sqlite3
import json
import uuid
import logging
import signal
import sys
import time
from functools import lru_cache
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Метрики Prometheus
request_count = Counter('http_requests_total', 'Total HTTP requests')
request_duration = Histogram('http_request_duration_seconds', 'HTTP request duration')

# Конфигурация приложения
class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-here'
    DATABASE = 'chat.db'
    DEBUG = False
    STATS_CACHE_TTL = 5  # время жизни кэша статистики в секундах

class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False

# Инициализация Flask и расширений
app = Flask(__name__)
app.config.from_object(DevelopmentConfig if os.environ.get('FLASK_ENV') == 'development' else ProductionConfig)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000", "http://your-frontend-domain.com"]}})
socketio = SocketIO(app, cors_allowed_origins=["http://localhost:3000", "https://0cf7-84-54-86-80.ngrok-free.app"], async_mode='gevent')


# Настройка лимитера запросов
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

class ChatUser:
    """Класс для представления пользователя чата"""
    def __init__(self, id, gender, age_group, preferred_gender, preferred_age_group):
        self.id = id
        self.gender = gender
        self.age_group = age_group
        self.preferred_gender = preferred_gender
        self.preferred_age_group = preferred_age_group
        self.partner_id = None
        self.socket_id = None
        self.last_active = datetime.now(UTC)

# Глобальные словари и списки
users = {}
waiting_users = []
stats_cache = {'timestamp': None, 'data': None}

def get_db_connection():
    """Создает соединение с базой данных с поддержкой timestamp"""
    conn = sqlite3.connect(
        app.config['DATABASE'],
        detect_types=sqlite3.PARSE_DECLTYPES
    )
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Инициализация базы данных"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    gender TEXT,
                    age_group TEXT,
                    preferred_gender TEXT,
                    preferred_age_group TEXT,
                    status TEXT,
                    last_active TIMESTAMP,
                    socket_id TEXT
                )
            ''')
            conn.commit()
            logger.info("База данных успешно инициализирована")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")

def cleanup():
    """Очистка ресурсов при завершении"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM users')
            conn.commit()
        logger.info("Очистка ресурсов выполнена успешно")
    except Exception as e:
        logger.error(f"Ошибка при очистке ресурсов: {e}")

def signal_handler(sig, frame):
    """Обработчик сигналов завершения"""
    logger.info("Получен сигнал завершения, закрываем соединения...")
    cleanup()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@lru_cache(maxsize=1)
def get_cached_stats():
    """Получение кэшированной статистики"""
    now = datetime.now(UTC)
    if (stats_cache['timestamp'] is None or 
        (now - stats_cache['timestamp']).seconds > app.config['STATS_CACHE_TTL']):
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM users WHERE status = "waiting"')
            stats_cache['data'] = c.fetchone()[0]
            stats_cache['timestamp'] = now
    return stats_cache['data']

def is_match(user1, user2):
    """Проверка совместимости пользователей"""
    gender_match = (
        user1.preferred_gender == '' or 
        user2.gender == user1.preferred_gender
    ) and (
        user2.preferred_gender == '' or 
        user1.gender == user2.preferred_gender
    )
    
    age_match = (
        user1.preferred_age_group == '' or 
        user2.age_group == user1.preferred_age_group
    ) and (
        user2.preferred_age_group == '' or 
        user1.age_group == user2.preferred_age_group
    )
    
    return gender_match and age_match

@app.before_request
def before_request():
    """Действия перед обработкой запроса"""
    request.start_time = time.time()

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = 'http://localhost:5173'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


@app.route('/api/stats', methods=['POST'])
@limiter.limit("1 per 5 seconds")
def get_stats():
    """Получение статистики пользователей"""
    try:
        data = request.json
        total = get_cached_stats()
        
        matching = 0
        if data.get('gender') and data.get('ageGroup'):
            with get_db_connection() as conn:
                c = conn.cursor()
                conditions = []
                params = []
                
                if data.get('preferredGender'):
                    conditions.append('gender = ?')
                    params.append(data['preferredGender'])
                    
                if data.get('preferredAgeGroup'):
                    conditions.append('age_group = ?')
                    params.append(data['preferredAgeGroup'])
                    
                if conditions:
                    query = f'''
                        SELECT COUNT(*) FROM users 
                        WHERE status = "waiting" 
                        AND {' AND '.join(conditions)}
                    '''
                    c.execute(query, params)
                    matching = c.fetchone()[0]
                else:
                    matching = total
                    
        return jsonify({'total': total, 'matching': matching})
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500

@app.route('/api/start-chat', methods=['POST'])
@limiter.limit("5 per minute")
def start_chat():
    """Начало нового чата"""
    try:
        data = request.json
        user_id = str(uuid.uuid4())
        
        new_user = ChatUser(
            id=user_id,
            gender=data['gender'],
            age_group=data['ageGroup'],
            preferred_gender=data['preferredGender'],
            preferred_age_group=data['preferredAgeGroup']
        )
        
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO users (
                    id, gender, age_group, preferred_gender, 
                    preferred_age_group, status, last_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id,
                data['gender'],
                data['ageGroup'],
                data['preferredGender'],
                data['preferredAgeGroup'],
                'waiting',
                datetime.now(UTC)
            ))
            conn.commit()
        
        users[user_id] = new_user
        waiting_users.append(user_id)
        
        # Инвалидируем кэш статистики
        get_cached_stats.cache_clear()
        
        return jsonify({'userId': user_id})
    except Exception as e:
        logger.error(f"Ошибка при создании чата: {e}")
        return jsonify({'error': 'Ошибка при создании чата'}), 500

@socketio.on('connect')
def handle_connect():
    """Обработка подключения клиента"""
    logger.info(f"Клиент подключился: {request.sid}")

@socketio.on('register')
def handle_register(data):
    """Регистрация пользователя в системе"""
    try:
        user_id = data['userId']
        if user_id in users:
            users[user_id].socket_id = request.sid
            join_room(request.sid)
            try_match_users(user_id)
    except Exception as e:
        logger.error(f"Ошибка при регистрации: {e}")

@socketio.on('message')
def handle_message(data):
    """Обработка сообщений"""
    try:
        sender_sid = request.sid
        sender = None
        for user in users.values():
            if user.socket_id == sender_sid:
                sender = user
                break

        if sender and sender.partner_id:
            partner = users.get(sender.partner_id)
            if partner and partner.socket_id:
                emit('message', {
                    'content': data['content'],
                    'sender': 'partner'
                }, room=partner.socket_id)
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    """Обработка отключения клиента"""
    try:
        for user in users.values():
            if user.socket_id == request.sid:
                handle_user_disconnect(user.id)
                break
    except Exception as e:
        logger.error(f"Ошибка при отключении: {e}")

def handle_user_disconnect(user_id):
    """Обработка отключения пользователя"""
    try:
        if user_id in users:
            user = users[user_id]
            if user.partner_id and user.partner_id in users:
                partner = users[user.partner_id]
                if partner.socket_id:
                    emit('partner_disconnected', room=partner.socket_id)
            
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute('DELETE FROM users WHERE id = ?', (user_id,))
                conn.commit()
            
            if user_id in waiting_users:
                waiting_users.remove(user_id)
            del users[user_id]
            
            # Инвалидируем кэш статистики
            get_cached_stats.cache_clear()
    except Exception as e:
        logger.error(f"Ошибка при обработке отключения пользователя: {e}")

def try_match_users(user_id):
    """Попытка найти подходящего собеседника"""
    try:
        if user_id not in users or user_id not in waiting_users:
            logger.debug(f"Пользователь {user_id} не найден в списке ожидающих")
            return False
        
        current_user = users[user_id]
        
        for waiting_id in waiting_users[:]:
            if waiting_id == user_id:
                continue
                
            waiting_user = users[waiting_id]
            if is_match(current_user, waiting_user):
                try:
                    waiting_users.remove(waiting_id)
                    waiting_users.remove(user_id)
                    
                    current_user.partner_id = waiting_id
                    waiting_user.partner_id = user_id
                    
                    with get_db_connection() as conn:
                        c = conn.cursor()
                        c.execute('''
                            UPDATE users 
                            SET status = 'chatting', last_active = ? 
                            WHERE id IN (?, ?)
                        ''', (datetime.now(UTC), user_id, waiting_id))
                        conn.commit()
                    
                    # Очищаем кэш статистики после изменения статусов
                    get_cached_stats.cache_clear()
                    
                    # Отправляем уведомления обоим пользователям
                    current_user_data = {
                        'partner': {
                            'gender': waiting_user.gender,
                            'ageGroup': waiting_user.age_group
                        }
                    }
                    
                    waiting_user_data = {
                        'partner': {
                            'gender': current_user.gender,
                            'ageGroup': current_user.age_group
                        }
                    }
                    
                    emit('partner_found', current_user_data, room=current_user.socket_id)
                    emit('partner_found', waiting_user_data, room=waiting_user.socket_id)
                    
                    logger.info(f"Успешно создан матч между пользователями {user_id} и {waiting_id}")
                    return True
                except Exception as e:
                    logger.error(f"Ошибка при создании матча: {e}")
                    # Откатываем изменения в случае ошибки
                    if waiting_id not in waiting_users:
                        waiting_users.append(waiting_id)
                    if user_id not in waiting_users:
                        waiting_users.append(user_id)
                    current_user.partner_id = None
                    waiting_user.partner_id = None
                    return False
        
        logger.debug(f"Подходящий партнер для пользователя {user_id} не найден")
        return False
    except Exception as e:
        logger.error(f"Ошибка при поиске собеседника: {e}")
        return False

def create_app():
    """Функция создания приложения"""
    app.config.from_object(DevelopmentConfig)
    
    # Инициализация лимитера
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=["200 per day", "50 per hour"],
        storage_uri="memory://"
    )
    
    # Добавляем лимиты для конкретных эндпоинтов
    limiter.limit("1 per 5 seconds")(app.route('/api/stats', methods=['POST'])(get_stats))
    limiter.limit("5 per minute")(app.route('/api/start-chat', methods=['POST'])(start_chat))
    
    # Инициализация базы данных
    init_db()
    
    # Настройка обработчиков сигналов
    def signal_handler(sig, frame):
        logger.info("Получен сигнал завершения, закрываем соединения...")
        # Очистка ресурсов перед выходом
        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute('DELETE FROM users')  # Очищаем таблицу пользователей
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при очистке БД: {e}")
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Добавляем метрики Prometheus
    metrics = PrometheusMetrics(app)
    
    # Добавляем базовые метрики
    metrics.info('app_info', 'Application info', version='1.0.0')
    
    # Настраиваем мониторинг запросов
    @app.before_request
    def before_request():
        request.start_time = time.time()
        
    @app.after_request
    def after_request(response):
        if hasattr(request, 'start_time'):
            request_duration.observe(time.time() - request.start_time)
        return response
    
    return app

if __name__ == '__main__':
    try:
        logger.info("Запуск сервера...")
        app = create_app()
        
        # Настройка периодической очистки неактивных пользователей
        def cleanup_inactive_users():
            try:
                timeout = timedelta(minutes=30)  # Тайм-аут неактивности
                with get_db_connection() as conn:
                    c = conn.cursor()
                    c.execute('''
                        DELETE FROM users 
                        WHERE last_active < ?
                    ''', (datetime.now(UTC) - timeout,))
                    if c.rowcount > 0:
                        logger.info(f"Удалено {c.rowcount} неактивных пользователей")
                    conn.commit()
                    get_cached_stats.cache_clear()  # Очищаем кэш после удаления
            except Exception as e:
                logger.error(f"Ошибка при очистке неактивных пользователей: {e}")

        # Запускаем периодическую очистку каждые 15 минут
        scheduler = BackgroundScheduler()
        scheduler.add_job(cleanup_inactive_users, 'interval', minutes=15)
        scheduler.start()
        
        # Запуск сервера
        socketio.run(
            app,
            debug=app.config['DEBUG'],
            port=5000,
            host='0.0.0.0',
            use_reloader=False
        )
    except Exception as e:
        logger.error(f"Ошибка при запуске сервера: {e}")
    finally:
        scheduler.shutdown()