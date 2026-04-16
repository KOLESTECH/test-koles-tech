import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any

import asyncpg
import pytz

logger = logging.getLogger(__name__)

# ========== DATABASE CONNECTION POOL ==========
class DatabasePool:
    _pool = None
    
    @classmethod
    async def get_pool(cls, database_url=None):
        if cls._pool is None:
            if not database_url:
                logger.error("DATABASE_URL не указан")
                return None
                
            if database_url.startswith("postgres://"):
                conn_string = database_url.replace("postgres://", "postgresql://", 1)
            else:
                conn_string = database_url
            
            if "sslmode" not in conn_string:
                if "?" in conn_string:
                    conn_string += "&sslmode=require"
                else:
                    conn_string += "?sslmode=require"
            
            cls._pool = await asyncpg.create_pool(
                conn_string,
                min_size=10,
                max_size=30,
                command_timeout=60
            )
        return cls._pool
    
    @classmethod
    async def close_pool(cls):
        if cls._pool:
            await cls._pool.close()
            cls._pool = None

async def execute_query(query: str, *args, database_url=None) -> Any:
    """Выполняет SQL запрос с использованием пула соединений"""
    pool = await DatabasePool.get_pool(database_url)
    if not pool:
        logger.error("Не удалось получить пул соединений")
        return None
        
    async with pool.acquire() as conn:
        try:
            if query.strip().upper().startswith("SELECT"):
                result = await conn.fetch(query, *args)
                return [dict(row) for row in result] if result else []
            else:
                # Для INSERT/UPDATE/DELETE
                result = await conn.fetch(query, *args)
                if result:
                    # Если есть RETURNING, возвращаем список словарей
                    return [dict(row) for row in result]
                # Если нет RETURNING, возвращаем строку статуса
                return "OK"
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}\nЗапрос: {query}")
            raise

# ========== TARIFFS ==========
TARIFFS = {
    'mini': {
        "name": "Mini",
        "price": 2,
        "currency": "USD",
        "channels_limit": 1,
        "daily_posts_limit": 1,
        "ai_requests_limit": 2,
        "ai_copies_limit": 1,
        "description": "Базовый тариф",
        "emoji": "🌱",
        "trial_days": 14
    },
    'standard': {
        "name": "Standard",
        "price": 4,
        "currency": "USD",
        "channels_limit": 3,
        "daily_posts_limit": 5,
        "ai_requests_limit": 15,
        "ai_copies_limit": 5,
        "description": "Для активных пользователей",
        "emoji": "⭐",
        "trial_days": 0
    },
    'pro': {
        "name": "PRO",
        "price": 6,
        "currency": "USD",
        "channels_limit": 5,
        "daily_posts_limit": 10,
        "ai_requests_limit": 30,
        "ai_copies_limit": 10,
        "description": "Максимальные возможности",
        "emoji": "👑",
        "trial_days": 0
    },
    'admin': {
        "name": "Admin",
        "price": 0,
        "currency": "USD",
        "channels_limit": 999,
        "daily_posts_limit": 999,
        "ai_requests_limit": 999,
        "ai_copies_limit": 999,
        "description": "Безлимитный доступ",
        "emoji": "⚡",
        "trial_days": 0
    }
}

# ========== DATABASE INITIALIZATION ==========
async def init_database(database_url=None):
    """Инициализация базы данных с оптимизированными индексами"""
    queries = [
        # Таблица пользователей
        '''
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            tariff TEXT DEFAULT 'mini',
            posts_today INTEGER DEFAULT 0,
            posts_reset_date DATE DEFAULT CURRENT_DATE,
            ai_requests_used INTEGER DEFAULT 0,
            ai_reset_date DATE DEFAULT CURRENT_DATE,
            is_active BOOLEAN DEFAULT TRUE,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            last_seen TIMESTAMPTZ DEFAULT NOW(),
            tariff_expires DATE DEFAULT NULL,
            subscription_days INTEGER DEFAULT 0
        )
        ''',
        
        # Индексы для таблицы users
        '''
        CREATE INDEX IF NOT EXISTS idx_users_tariff ON users(tariff)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_users_created ON users(created_at)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_users_tariff_expires ON users(tariff_expires)
        ''',
        
        # Таблица каналов
        '''
        CREATE TABLE IF NOT EXISTS channels (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            channel_name TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, channel_id)
        )
        ''',
        
        # Индексы для таблицы channels
        '''
        CREATE INDEX IF NOT EXISTS idx_channels_user ON channels(user_id)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_channels_active ON channels(is_active)
        ''',
        
        # Таблица запланированных постов
        '''
        CREATE TABLE IF NOT EXISTS scheduled_posts (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            message_type TEXT NOT NULL,
            message_text TEXT,
            media_file_id TEXT,
            media_caption TEXT,
            scheduled_time TIMESTAMPTZ NOT NULL,
            is_sent BOOLEAN DEFAULT FALSE,
            sent_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            error_message TEXT,
            retry_count INTEGER DEFAULT 0
        )
        ''',
        
        # Индексы для таблицы scheduled_posts
        '''
        CREATE INDEX IF NOT EXISTS idx_scheduled_time ON scheduled_posts(scheduled_time)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_user_scheduled ON scheduled_posts(user_id, scheduled_time)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_sent_status ON scheduled_posts(is_sent)
        ''',
        
        # Таблица заказов тарифов
        '''
        CREATE TABLE IF NOT EXISTS tariff_orders (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            tariff TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            order_date TIMESTAMPTZ DEFAULT NOW(),
            processed_date TIMESTAMPTZ,
            admin_notes TEXT
        )
        ''',
        
        # Индексы для таблицы tariff_orders
        '''
        CREATE INDEX IF NOT EXISTS idx_orders_status ON tariff_orders(status)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_orders_user ON tariff_orders(user_id)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_orders_date ON tariff_orders(order_date)
        ''',
        
        # Таблица логов AI запросов
        '''
        CREATE TABLE IF NOT EXISTS ai_request_logs (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            service_type TEXT NOT NULL,
            prompt_length INTEGER,
            response_length INTEGER,
            success BOOLEAN DEFAULT FALSE,
            error_message TEXT,
            api_key_index INTEGER,
            model_name TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        ''',
        
        # Индексы для таблицы ai_request_logs
        '''
        CREATE INDEX IF NOT EXISTS idx_logs_user_date ON ai_request_logs(user_id, created_at)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_logs_success ON ai_request_logs(success)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_logs_service ON ai_request_logs(service_type)
        ''',
        
        # Таблица для хранения языковых предпочтений пользователей
        '''
        CREATE TABLE IF NOT EXISTS user_languages (
            user_id BIGINT PRIMARY KEY,
            language_code TEXT DEFAULT 'ru',
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        ''',
        
        # Индексы для таблицы user_languages
        '''
        CREATE INDEX IF NOT EXISTS idx_languages_user ON user_languages(user_id)
        '''
    ]
    
    try:
        for query in queries:
            await execute_query(query, database_url=database_url)
        logger.info("✅ База данных инициализирована с оптимизированными индексами")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def migrate_database(database_url=None):
    """Миграция базы данных для существующих таблиц"""
    try:
        # Добавляем колонку ai_requests_used если ее нет
        try:
            await execute_query('''
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS ai_requests_used INTEGER DEFAULT 0
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка ai_requests_used")
        except Exception as e:
            logger.warning(f"Ошибка добавления ai_requests_used: {e}")
        
        # Добавляем колонку ai_reset_date если ее нет
        try:
            await execute_query('''
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS ai_reset_date DATE DEFAULT CURRENT_DATE
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка ai_reset_date")
        except Exception as e:
            logger.warning(f"Ошибка добавления ai_reset_date: {e}")
        
        # Добавляем колонку subscription_days если ее нет
        try:
            await execute_query('''
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS subscription_days INTEGER DEFAULT 0
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка subscription_days")
        except Exception as e:
            logger.warning(f"Ошибка добавления subscription_days: {e}")
        
        # Добавляем колонку tariff_expires если ее нет
        try:
            await execute_query('''
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS tariff_expires DATE DEFAULT NULL
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка tariff_expires")
        except Exception as e:
            logger.warning(f"Ошибка добавления tariff_expires: {e}")
        
        # Добавляем колонки в scheduled_posts если их нет
        try:
            await execute_query('''
                ALTER TABLE scheduled_posts 
                ADD COLUMN IF NOT EXISTS error_message TEXT
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка error_message")
        except Exception as e:
            logger.warning(f"Ошибка добавления error_message: {e}")
        
        try:
            await execute_query('''
                ALTER TABLE scheduled_posts 
                ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка retry_count")
        except Exception as e:
            logger.warning(f"Ошибка добавления retry_count: {e}")
        
        # Создаем таблицу user_languages если ее нет
        try:
            await execute_query('''
                CREATE TABLE IF NOT EXISTS user_languages (
                    user_id BIGINT PRIMARY KEY,
                    language_code TEXT DEFAULT 'ru',
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''', database_url=database_url)
            logger.info("✅ Создана таблица user_languages")
        except Exception as e:
            logger.warning(f"Ошибка создания user_languages: {e}")
        
        logger.info("✅ Миграции завершены")
    except Exception as e:
        logger.error(f"❌ Ошибка миграции БД: {e}")

# ========== USER FUNCTIONS ==========
async def update_user_activity(user_id: int, database_url=None):
    """Обновляет время последней активности пользователя"""
    await execute_query(
        "UPDATE users SET last_seen = NOW() WHERE id = $1",
        user_id,
        database_url=database_url
    )

async def get_user_tariff(user_id: int, database_url=None) -> str:
    """Получает тариф пользователя с проверкой срока действия"""
    await update_user_activity(user_id, database_url)
    
    user = await execute_query(
        "SELECT tariff, is_admin, tariff_expires FROM users WHERE id = $1", 
        user_id,
        database_url=database_url
    )
    
    if not user:
        await execute_query(
            "INSERT INTO users (id, tariff) VALUES ($1, 'mini') ON CONFLICT DO NOTHING",
            user_id,
            database_url=database_url
        )
        return 'mini'
    
    import pytz
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    if user[0].get('is_admin'):
        return 'admin'
    
    # Проверяем срок действия тарифа
    tariff_expires = user[0].get('tariff_expires')
    if tariff_expires and tariff_expires < datetime.now(moscow_tz).date():
        # Тариф истек, возвращаем к минимуму
        await execute_query(
            "UPDATE users SET tariff = 'mini', tariff_expires = NULL, subscription_days = 0 WHERE id = $1",
            user_id,
            database_url=database_url
        )
        return 'mini'
    
    return user[0].get('tariff', 'mini')

async def update_user_subscription(user_id: int, tariff: str, days: int, database_url=None) -> bool:
    """Обновляет подписку пользователя (выдача или продление)"""
    try:
        import pytz
        moscow_tz = pytz.timezone('Europe/Moscow')
        today = datetime.now(moscow_tz).date()
        
        # Получаем текущую дату окончания
        user = await execute_query(
            "SELECT tariff_expires FROM users WHERE id = $1",
            user_id,
            database_url=database_url
        )
        
        if user and user[0].get('tariff_expires'):
            expires_date = user[0]['tariff_expires']
            if expires_date >= today:
                # Продлеваем существующую подписку
                new_expires = expires_date + timedelta(days=days)
            else:
                # Начинаем с сегодня
                new_expires = today + timedelta(days=days)
        else:
            # Новая подписка
            new_expires = today + timedelta(days=days)
        
        await execute_query('''
            UPDATE users 
            SET tariff = $1, 
                tariff_expires = $2,
                subscription_days = subscription_days + $3
            WHERE id = $4
        ''', tariff, new_expires, days, user_id, database_url=database_url)
        
        logger.info(f"✅ Подписка обновлена: user={user_id}, tariff={tariff}, days={days}, expires={new_expires}")
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления подписки: {e}")
        return False

async def get_user_subscription_info(user_id: int, database_url=None) -> Dict:
    """Получает информацию о подписке пользователя"""
    import pytz
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    user = await execute_query(
        "SELECT tariff, tariff_expires, subscription_days FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    
    if not user:
        return {'tariff': 'mini', 'expires': None, 'days': 0, 'expired': True, 'days_left': 0}
    
    data = user[0]
    tariff_expires = data.get('tariff_expires')
    
    if tariff_expires:
        expired = tariff_expires < datetime.now(moscow_tz).date()
        days_left = (tariff_expires - datetime.now(moscow_tz).date()).days if not expired else 0
    else:
        expired = True
        days_left = 0
    
    return {
        'tariff': data.get('tariff', 'mini'),
        'expires': tariff_expires,
        'days': data.get('subscription_days', 0),
        'expired': expired,
        'days_left': days_left
    }

async def check_trial_period(user_id: int, database_url=None) -> Tuple[bool, int]:
    """Проверяет триальный период пользователя (14 дней для Mini)"""
    user = await execute_query(
        "SELECT created_at, tariff FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    
    if not user:
        return False, 0
    
    user_data = user[0]
    created_at = user_data['created_at']
    current_tariff = user_data['tariff']
    
    # Если тариф не Mini, триал не нужен
    if current_tariff != 'mini':
        return False, 0
    
    # Вычисляем сколько дней прошло с регистрации
    days_since_registration = (datetime.now(pytz.UTC) - created_at).days
    
    # 14 дней триала
    trial_days = 14
    days_left = max(0, trial_days - days_since_registration)
    
    return days_left > 0, days_left

# ========== AI FUNCTIONS ==========
async def update_ai_usage_log(user_id: int, service_type: str, success: bool, 
                             api_key_index: int, model_name: str, 
                             prompt_length: int = 0, response_length: int = 0,
                             error_message: str = None, database_url=None):
    """Логирует использование AI сервисов"""
    await execute_query('''
        INSERT INTO ai_request_logs 
        (user_id, service_type, prompt_length, response_length, success, 
         error_message, api_key_index, model_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ''', user_id, service_type, prompt_length, response_length, 
        success, error_message, api_key_index, model_name,
        database_url=database_url)

async def get_user_ai_requests_today(user_id: int, database_url=None) -> int:
    """Получает количество AI запросов пользователя сегодня"""
    import pytz
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    user = await execute_query(
        "SELECT ai_requests_used, ai_reset_date FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    
    if not user:
        return 0
    
    today = datetime.now(moscow_tz).date()
    reset_date = user[0].get('ai_reset_date')
    
    if reset_date and reset_date < today:
        return 0
    
    return user[0].get('ai_requests_used', 0)

async def increment_ai_request(user_id: int, database_url=None) -> bool:
    """Увеличивает счетчик AI запросов пользователя"""
    try:
        import pytz
        moscow_tz = pytz.timezone('Europe/Moscow')
        today = datetime.now(moscow_tz).date()
        
        user = await execute_query(
            "SELECT ai_reset_date FROM users WHERE id = $1",
            user_id,
            database_url=database_url
        )
        
        if not user:
            return False
        
        if user[0].get('ai_reset_date') and user[0]['ai_reset_date'] < today:
            await execute_query('''
                UPDATE users 
                SET ai_requests_used = 1, ai_reset_date = CURRENT_DATE 
                WHERE id = $1
            ''', user_id, database_url=database_url)
        else:
            await execute_query('''
                UPDATE users 
                SET ai_requests_used = ai_requests_used + 1 
                WHERE id = $1
            ''', user_id, database_url=database_url)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка увеличения счетчика AI запросов: {e}")
        return False

async def check_ai_limit(user_id: int, database_url=None) -> Tuple[bool, int, int]:
    """Проверяет лимит AI запросов пользователя"""
    tariff_name = await get_user_tariff(user_id, database_url)
    tariff = TARIFFS.get(tariff_name, TARIFFS['mini'])
    
    # Проверяем триальный период для Mini
    if tariff_name == 'mini':
        in_trial, _ = await check_trial_period(user_id, database_url)
        if not in_trial:
            return False, 0, 0
    
    used = await get_user_ai_requests_today(user_id, database_url)
    limit = tariff['ai_requests_limit']
    
    return used < limit, used, limit

# ========== POSTS FUNCTIONS ==========
async def get_user_posts_today(user_id: int, database_url=None) -> int:
    """Получает количество постов пользователя сегодня"""
    import pytz
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    result = await execute_query(
        "SELECT posts_today, posts_reset_date FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    
    if not result:
        return 0
    
    user = result[0]
    today = datetime.now(moscow_tz).date()
    
    if user['posts_reset_date'] and user['posts_reset_date'] < today:
        return 0
    
    return user['posts_today'] or 0

async def increment_user_posts(user_id: int, database_url=None) -> bool:
    """Увеличивает счетчик постов пользователя"""
    try:
        import pytz
        moscow_tz = pytz.timezone('Europe/Moscow')
        today = datetime.now(moscow_tz).date()
        
        user = await execute_query(
            "SELECT posts_reset_date FROM users WHERE id = $1",
            user_id,
            database_url=database_url
        )
        
        if not user:
            return False
        
        if user[0].get('posts_reset_date') and user[0]['posts_reset_date'] < today:
            await execute_query('''
                UPDATE users 
                SET posts_today = 1, posts_reset_date = CURRENT_DATE 
                WHERE id = $1
            ''', user_id, database_url=database_url)
        else:
            await execute_query('''
                UPDATE users 
                SET posts_today = posts_today + 1 
                WHERE id = $1
            ''', user_id, database_url=database_url)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка увеличения счетчика постов: {e}")
        return False

async def save_scheduled_post(user_id: int, channel_id: int, post_data: Dict, scheduled_time: datetime, moscow_tz=None, database_url=None) -> Optional[int]:
    """Сохраняет запланированный пост"""
    try:
        import pytz
        
        if moscow_tz and scheduled_time.tzinfo is None:
            scheduled_time = moscow_tz.localize(scheduled_time)
        scheduled_time_utc = scheduled_time.astimezone(pytz.UTC)
        
        message_type = post_data.get('message_type', 'text')
        message_text = post_data.get('message_text', '')
        media_file_id = post_data.get('media_file_id', '')
        media_caption = post_data.get('media_caption', '')
        
        result = await execute_query('''
            INSERT INTO scheduled_posts 
            (user_id, channel_id, message_type, message_text, media_file_id, media_caption, scheduled_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        ''', 
        user_id,
        channel_id,
        message_type,
        message_text,
        media_file_id,
        media_caption,
        scheduled_time_utc,
        database_url=database_url
        )
        
        post_id = result[0]['id'] if result and isinstance(result, list) and len(result) > 0 else None
        
        if post_id and moscow_tz:
            logger.info(f"✅ Пост сохранен с ID: {post_id} на {scheduled_time.astimezone(moscow_tz).strftime('%d.%m.%Y %H:%M')} МСК")
        
        return post_id
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения поста: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def get_scheduled_posts(database_url=None) -> List[Dict]:
    """Получает все неотправленные запланированные посты"""
    return await execute_query(
        "SELECT * FROM scheduled_posts WHERE is_sent = FALSE AND scheduled_time > NOW()",
        database_url=database_url
    )

async def mark_post_sent(post_id: int, database_url=None) -> bool:
    """Отмечает пост как отправленный"""
    try:
        await execute_query(
            "UPDATE scheduled_posts SET is_sent = TRUE, sent_at = NOW() WHERE id = $1",
            post_id,
            database_url=database_url
        )
        return True
    except Exception as e:
        logger.error(f"Ошибка отметки поста #{post_id}: {e}")
        return False

async def update_post_error(post_id: int, error_message: str, retry_count: int, database_url=None) -> bool:
    """Обновляет информацию об ошибке поста"""
    try:
        await execute_query(
            "UPDATE scheduled_posts SET error_message = $1, retry_count = $2 WHERE id = $3",
            error_message[:500], retry_count, post_id,
            database_url=database_url
        )
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления ошибки поста #{post_id}: {e}")
        return False

# ========== CHANNELS FUNCTIONS ==========
async def get_user_channels(user_id: int, database_url=None) -> List[Dict]:
    """Получает каналы пользователя"""
    return await execute_query(
        "SELECT channel_id, channel_name FROM channels WHERE user_id = $1 AND is_active = TRUE",
        user_id,
        database_url=database_url
    )

async def add_user_channel(user_id: int, channel_id: int, channel_name: str, database_url=None) -> bool:
    """Добавляет канал пользователя"""
    try:
        await execute_query('''
            INSERT INTO channels (user_id, channel_id, channel_name, is_active)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (user_id, channel_id) DO UPDATE SET
            channel_name = EXCLUDED.channel_name,
            is_active = TRUE
        ''', user_id, channel_id, channel_name, database_url=database_url)
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления канала: {e}")
        return False

async def get_user_channels_count(user_id: int, database_url=None) -> int:
    """Получает количество каналов пользователя"""
    result = await execute_query(
        "SELECT COUNT(*) as count FROM channels WHERE user_id = $1 AND is_active = TRUE",
        user_id,
        database_url=database_url
    )
    return result[0]['count'] if result else 0

async def remove_user_channel(user_id: int, channel_id: int, database_url=None) -> bool:
    """Удаляет канал пользователя (деактивирует)"""
    try:
        await execute_query(
            "UPDATE channels SET is_active = FALSE WHERE user_id = $1 AND channel_id = $2",
            user_id, channel_id,
            database_url=database_url
        )
        return True
    except Exception as e:
        logger.error(f"Ошибка удаления канала: {e}")
        return False

# ========== TARIFF FUNCTIONS ==========
async def get_tariff_limits(user_id: int, database_url=None) -> Tuple[int, int, int]:
    """Получает лимиты тарифа пользователя"""
    tariff = await get_user_tariff(user_id, database_url)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    return (tariff_info['channels_limit'], 
            tariff_info['daily_posts_limit'],
            tariff_info['ai_requests_limit'])

async def create_tariff_order(user_id: int, tariff_id: str, database_url=None) -> bool:
    """Создает заказ тарифа"""
    try:
        await execute_query('''
            INSERT INTO tariff_orders (user_id, tariff, status)
            VALUES ($1, $2, 'pending')
        ''', user_id, tariff_id, database_url=database_url)
        return True
    except Exception as e:
        logger.error(f"Ошибка создания заказа: {e}")
        return False

async def get_tariff_orders(status: str = None, database_url=None) -> List[Dict]:
    """Получает заказы тарифов"""
    if status:
        return await execute_query(
            "SELECT * FROM tariff_orders WHERE status = $1 ORDER BY order_date DESC",
            status,
            database_url=database_url
        )
    else:
        return await execute_query(
            "SELECT * FROM tariff_orders ORDER BY order_date DESC",
            database_url=database_url
        )

async def update_order_status(order_id: int, status: str, admin_notes: str = None, database_url=None) -> bool:
    """Обновляет статус заказа"""
    try:
        if admin_notes:
            await execute_query('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW(), admin_notes = $2
                WHERE id = $3
            ''', status, admin_notes, order_id, database_url=database_url)
        else:
            await execute_query('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW()
                WHERE id = $2
            ''', status, order_id, database_url=database_url)
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса заказа: {e}")
        return False

# ========== STATS FUNCTIONS ==========
async def get_user_stats(user_id: int, database_url=None, ai_manager=None) -> Dict:
    """Получает полную статистику пользователя"""
    try:
        tariff_name = await get_user_tariff(user_id, database_url)
        tariff_info = TARIFFS.get(tariff_name, TARIFFS['mini'])
        
        posts_today = await get_user_posts_today(user_id, database_url)
        channels_count = await get_user_channels_count(user_id, database_url)
        ai_used = await get_user_ai_requests_today(user_id, database_url)
        
        scheduled_posts = await execute_query(
            "SELECT COUNT(*) as count FROM scheduled_posts WHERE user_id = $1 AND is_sent = FALSE",
            user_id,
            database_url=database_url
        )
        
        subscription_info = await get_user_subscription_info(user_id, database_url)
        in_trial, days_left = await check_trial_period(user_id, database_url)
        
        return {
            'tariff': tariff_info['name'],
            'tariff_key': tariff_name,
            'posts_today': posts_today,
            'posts_limit': tariff_info['daily_posts_limit'],
            'channels_count': channels_count,
            'channels_limit': tariff_info['channels_limit'],
            'ai_used': ai_used,
            'ai_limit': tariff_info['ai_requests_limit'],
            'scheduled_posts': scheduled_posts[0]['count'] if scheduled_posts else 0,
            'subscription_expires': subscription_info['expires'],
            'subscription_days_left': subscription_info['days_left'],
            'subscription_expired': subscription_info['expired'],
            'in_trial': in_trial,
            'trial_days_left': days_left
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return {}

async def get_all_users(database_url=None) -> List[Dict]:
    """Получает всех пользователей"""
    return await execute_query('''
        SELECT id, username, first_name, tariff, is_admin, created_at,
               tariff_expires, subscription_days, last_seen
        FROM users 
        ORDER BY created_at DESC
    ''', database_url=database_url)

async def get_active_users(days: int = 7, database_url=None) -> List[Dict]:
    """Получает активных пользователей за последние N дней"""
    return await execute_query('''
        SELECT id, username, first_name, tariff, last_seen
        FROM users 
        WHERE last_seen > NOW() - INTERVAL '$1 days'
        ORDER BY last_seen DESC
    ''', days, database_url=database_url)

async def get_user_by_id(user_id: int, database_url=None) -> Optional[Dict]:
    """Получает пользователя по ID"""
    result = await execute_query(
        "SELECT id, username, first_name, tariff, is_admin, created_at, tariff_expires, subscription_days FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    return result[0] if result else None

async def update_user_tariff(user_id: int, tariff: str, database_url=None) -> bool:
    """Обновляет тариф пользователя"""
    try:
        await execute_query('''
            UPDATE users SET tariff = $1 WHERE id = $2
        ''', tariff, user_id, database_url=database_url)
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления тарифа: {e}")
        return False

async def force_update_user_tariff(user_id: int, tariff: str, admin_id: int, database_url=None) -> Tuple[bool, str]:
    """Принудительно обновляет тариф пользователя (админ)"""
    try:
        user = await get_user_by_id(user_id, database_url)
        if not user:
            return False, f"❌ Пользователь с ID {user_id} не найден"
        
        old_tariff = user.get('tariff', 'mini')
        
        success = await update_user_tariff(user_id, tariff, database_url)
        if success:
            await execute_query('''
                INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
                VALUES ($1, $2, 'force_completed', $3)
            ''', user_id, tariff, f"Принудительное обновление админом {admin_id}", 
            database_url=database_url)
            
            return True, f"✅ Тариф пользователя {user_id} обновлен с {old_tariff} на {tariff}"
        else:
            return False, f"❌ Ошибка при обновлении тарифа"
    except Exception as e:
        logger.error(f"Ошибка принудительного обновления: {e}")
        return False, f"❌ Ошибка: {str(e)}"

# ========== LANGUAGE FUNCTIONS ==========
async def get_user_language(user_id: int, database_url=None) -> str:
    """Получает язык пользователя"""
    result = await execute_query(
        "SELECT language_code FROM user_languages WHERE user_id = $1",
        user_id,
        database_url=database_url
    )
    if result:
        return result[0]['language_code']
    return 'ru'

async def set_user_language(user_id: int, language_code: str, database_url=None) -> bool:
    """Устанавливает язык пользователя"""
    try:
        await execute_query('''
            INSERT INTO user_languages (user_id, language_code, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE SET
            language_code = EXCLUDED.language_code,
            updated_at = NOW()
        ''', user_id, language_code, database_url=database_url)
        return True
    except Exception as e:
        logger.error(f"Ошибка установки языка: {e}")
        return False

# ========== CLEANUP FUNCTIONS ==========
async def cleanup_old_sessions(days: int = 7, database_url=None):
    """Очищает старые сессии и неактивных пользователей"""
    try:
        # Очищаем старые запланированные посты
        await execute_query('''
            DELETE FROM scheduled_posts 
            WHERE is_sent = TRUE AND sent_at < NOW() - INTERVAL '30 days'
        ''', database_url=database_url)
        
        # Очищаем старые логи
        await execute_query('''
            DELETE FROM ai_request_logs 
            WHERE created_at < NOW() - INTERVAL '90 days'
        ''', database_url=database_url)
        
        # Очищаем завершенные заказы старше 60 дней
        await execute_query('''
            DELETE FROM tariff_orders 
            WHERE status IN ('completed', 'cancelled', 'force_completed') 
            AND processed_date < NOW() - INTERVAL '60 days'
        ''', database_url=database_url)
        
        logger.info("✅ Очистка старых данных выполнена")
    except Exception as e:
        logger.error(f"Ошибка очистки: {e}")
