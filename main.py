import asyncio
import logging
import random
import string
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass

import aiomysql
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from telebot.asyncio_helper import ApiException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('main_telebot')

@dataclass
class UserData:
    id: int
    owner_id: int
    tg_id: int
    code: int
    tg_usname: str
    player_name: Optional[str]
    type_name: Optional[str]

@dataclass
class CodeInfo:
    code: int
    expiry_time: datetime
    tg_id: int

class DatabaseManager:
    
    def __init__(self):
        self.db_config = {
            "host": "195.18.27.241",
            "user": "gs103649",
            "password": "phz3eitw",
            "db": "gs103649",
            "autocommit": True,
            "minsize": 1,
            "maxsize": 10
        }
        self.pool = None
    
    async def initialize(self):
        try:
            self.pool = await aiomysql.create_pool(**self.db_config)
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    @asynccontextmanager
    async def get_cursor(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    yield cursor
                finally:
                    await cursor.close()
    
    async def execute_query(self, query: str, params: tuple = None) -> Any:
        async with self.get_cursor() as cursor:
            await cursor.execute(query, params or ())
            return await cursor.fetchall()
    
    async def execute_update(self, query: str, params: tuple = None) -> int:
        async with self.get_cursor() as cursor:
            await cursor.execute(query, params or ())
            return cursor.rowcount
    
    async def get_user_by_tg_id(self, tg_id: int) -> Optional[UserData]:
        result = await self.execute_query(
            "SELECT id, owner_id, tg_id, code, tg_usname, player_name, type_name FROM telegram WHERE tg_id = %s",
            (tg_id,)
        )
        return UserData(**result[0]) if result else None
    
    async def update_user_code(self, tg_id: int, code: int, username: str) -> bool:
        result = await self.execute_update(
            "UPDATE telegram SET code = %s, tg_usname = %s WHERE tg_id = %s",
            (code, username, tg_id)
        )
        return result > 0
    
    async def insert_user(self, tg_id: int, code: int, username: str) -> bool:
        result = await self.execute_update(
            "INSERT INTO telegram (owner_id, tg_id, code, tg_usname) VALUES (0, %s, %s, %s)",
            (tg_id, code, username)
        )
        return result > 0
    
    async def delete_user(self, tg_id: int) -> bool:
        result = await self.execute_update(
            "DELETE FROM telegram WHERE tg_id = %s",
            (tg_id,)
        )
        return result > 0
    
    async def update_password(self, player_name: str, new_password: str) -> bool:
        result = await self.execute_update(
            "UPDATE accounts_1101 SET players_password = %s WHERE name = %s",
            (new_password, player_name)
        )
        return result > 0
    
    async def get_all_telegram_users(self) -> list:
        return await self.execute_query(
            "SELECT id, owner_id, tg_id, code, tg_usname, player_name, type_name FROM telegram"
        )

class CodeManager:
    
    def __init__(self):
        self.active_codes: Dict[int, CodeInfo] = {}
        self.user_states: Dict[int, Dict[str, Any]] = {}
        self.captcha_attempts: Dict[int, Dict[str, Any]] = {}
    
    def generate_code(self) -> int:
        return random.randint(100000, 999999)
    
    def generate_password(self, length: int = 8) -> str:
        chars = string.ascii_letters + string.digits
        while True:
            password = ''.join(random.choice(chars) for _ in range(length))
            if (any(c.islower() for c in password) and 
                any(c.isupper() for c in password) and 
                any(c.isdigit() for c in password)):
                return password
    
    def generate_captcha(self) -> Tuple[str, str]:
        num1 = random.randint(1, 10)
        num2 = random.randint(1, 10)
        operation = random.choice(['+', '-'])
        if operation == '+':
            answer = str(num1 + num2)
            question = f"{num1} + {num2}"
        else:
            answer = str(num1 - num2)
            question = f"{num1} - {num2}"
        return question, answer
    
    def add_code(self, code_id: int, tg_id: int, code: int, expiry_minutes: int = 1):
        expiry_time = datetime.now() + timedelta(minutes=expiry_minutes)
        self.active_codes[code_id] = CodeInfo(code, expiry_time, tg_id)
    
    def get_expired_codes(self) -> list:
        now = datetime.now()
        return [code_id for code_id, info in self.active_codes.items() 
                if now >= info.expiry_time]
    
    def remove_code(self, code_id: int):
        self.active_codes.pop(code_id, None)
    
    def set_user_state(self, user_id: int, state: str, data: Dict = None):
        self.user_states[user_id] = {'state': state, 'data': data or {}}
    
    def get_user_state(self, user_id: int) -> Optional[Dict]:
        return self.user_states.get(user_id)
    
    def clear_user_state(self, user_id: int):
        self.user_states.pop(user_id, None)
    
    def set_captcha(self, user_id: int, answer: str):
        self.captcha_attempts[user_id] = {'answer': answer, 'attempts': 0}
    
    def verify_captcha(self, user_id: int, user_answer: str) -> bool:
        if user_id not in self.captcha_attempts:
            return False
        
        captcha_data = self.captcha_attempts[user_id]
        captcha_data['attempts'] += 1
        
        if user_answer.strip() == captcha_data['answer']:
            self.captcha_attempts.pop(user_id, None)
            return True
        return False
    
    def get_captcha_attempts(self, user_id: int) -> int:
        return self.captcha_attempts.get(user_id, {}).get('attempts', 0)
    
    def remove_captcha(self, user_id: int):
        self.captcha_attempts.pop(user_id, None)

class TelegramBot:
    
    def __init__(self, token: str, db_manager: DatabaseManager, code_manager: CodeManager):
        self.bot = AsyncTeleBot(token)
        self.db = db_manager
        self.codes = code_manager
        self.last_user_data: Dict[int, UserData] = {}
        self.setup_handlers()
    
    def setup_handlers(self):
        self.bot.message_handler(commands=['start'])(self.start_command)
        self.bot.message_handler(commands=['addcode'])(self.addcode_command)
        self.bot.message_handler(commands=['recovery_password'])(self.recovery_password_command)
        
        self.bot.callback_query_handler(func=lambda call: call.data == 'start_recovery')(self.start_recovery)
        self.bot.callback_query_handler(func=lambda call: call.data == 'deltg')(self.handle_deltg_callback)
        self.bot.callback_query_handler(func=lambda call: call.data == 'addcode')(self.handle_addcode_callback)
        self.bot.callback_query_handler(func=lambda call: call.data == 'confirm_deltg')(self.handle_confirm_deltg)
        self.bot.callback_query_handler(func=lambda call: call.data == 'cancel_deltg')(self.handle_cancel_deltg)
        
        self.bot.message_handler(func=lambda message: self.codes.get_user_state(message.from_user.id) and self.codes.get_user_state(message.from_user.id)['state'] == 'waiting_captcha')(self.handle_captcha_answer)
    
    async def start_command(self, message):
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(text='Получить код', callback_data='addcode'))
        
        await self.bot.send_message(
            message.chat.id,
            '👨‍💼 При помощи телеграм-помощника вы сможете обезопасить аккаунт от взлома и восстановить аккаунт в случае утраты пароля.\n\n'
            'Для привязки игрового аккаунта, воспользуйтесь кнопкой «<b>Получить код</b>»\n\n'
            'Перед началом взаимодействия, не забудьте подписаться на наш новостной канал @fl1ckyy.',
            reply_markup=markup, 
            parse_mode='HTML'
        )
    
    async def addcode_command(self, message):
        await self.process_code_request(message)
    
    async def process_code_request(self, message_or_call):
        if hasattr(message_or_call, 'message'):
            chat_id = message_or_call.message.chat.id
            user_id = message_or_call.from_user.id
            try:
                await self.bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=message_or_call.message.message_id,
                    reply_markup=None
                )
            except ApiException as e:
                logger.warning(f"Could not edit message: {e}")
        else:
            chat_id = message_or_call.chat.id
            user_id = message_or_call.from_user.id
        
        username = getattr(message_or_call.from_user, 'username', None) or "NULL"
        code = self.codes.generate_code()
        
        try:
            user_data = await self.db.get_user_by_tg_id(user_id)
            
            if user_data and user_data.owner_id != 0:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(text='🗑 Отвязать профиль', callback_data='deltg'))
                
                player_name_display = (user_data.player_name or "Неизвестный").replace('_', ' ')
                await self.bot.send_message(
                    chat_id,
                    f"ℹ️ Вы уже <b>привязали</b> свой игровой аккаунт: <b>{player_name_display}</b> на <b>01</b> сервере.\n\n"
                    f"🔐 Если Вы желаете <b>отвязать</b> свой профиль, нажмите кнопку ниже 🗑",
                    parse_mode='HTML', 
                    reply_markup=markup
                )
                return
            
            if user_data:
                success = await self.db.update_user_code(user_id, code, username)
            else:
                success = await self.db.insert_user(user_id, code, username)
            
            if success:
                logger.info(f"Generated code {code} for user {user_id}")
                
                await self.bot.send_message(
                    chat_id,
                    f"✅ Ваш проверочный код - <b>{code}</b>\n\n"
                    "1. Выполните вход в свой игровой аккаунт, который желаете привязать.\n"
                    "2. В меню персонажа (/mn) выберите пункт настройки.\n"
                    "3. В настройках выберите пункт «Привязать Telegram».\n"
                    "4. Введите проверочный код и нажмите на кнопку «Подтвердить».\n"
                    "5. Переключите уведомления c Почты на Telegram.\n\n"
                    "Если вы привязали аккаунт корректно, то помощник пришел сообщение об успешной привязке.",
                    parse_mode='HTML'
                )
            else:
                raise Exception("Failed to update database")
                
        except Exception as e:
            logger.error(f"Error processing code request: {e}")
            await self.bot.send_message(
                chat_id, 
                "ℹ️ Произошла ошибка при генерации кода. Попробуйте позже."
            )
    
    async def recovery_password_command(self, message):
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(text='🔓 Восстановить доступ', callback_data='start_recovery'))
        markup.add(types.InlineKeyboardButton(text='📞 Перейти в поддержку', url='t.me/fl1ckyy'))

        await self.bot.send_message(
            message.chat.id,
            '🔐 Если Вы *потеряли доступ* к своему игровому аккаунту, Вы можете *восстановить доступ* к аккаунту с помощью телеграм-помощника.\n\n'
            '⚠️ Примечание: Для быстрого восстановления, Ваш аккаунт *должен быть привязан* к вашему Telegram.\n\n'
            '✔️ Если Вы *не связали Ваш игровой аккаунт с телеграм-помощником*, вам следует обратиться в техническую поддержку.',
            reply_markup=markup, 
            parse_mode='Markdown'
        )
    
    async def start_recovery(self, call):
        try:
            user_data = await self.db.get_user_by_tg_id(call.from_user.id)
            
            if not user_data or user_data.owner_id == 0:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(text='📞 Перейти в поддержку', url='t.me/fl1ckyy'))
                
                await self.bot.send_message(
                    call.message.chat.id,
                    "ℹ️ Игровой аккаунт не найден или не привязан к вашему профилю Telegram.\n\n"
                    "Пожалуйста, обратитесь в техническую поддержку.",
                    reply_markup=markup
                )
                return
            
            new_password = self.codes.generate_password()
            success = await self.db.update_password(user_data.player_name, new_password)
            
            if success:
                player_name_display = user_data.player_name.replace('_', ' ') if user_data.player_name else "Неизвестный"
                
                await self.bot.send_message(
                    call.message.chat.id,
                    f"✅ Доступ к игровому аккаунту <b>{player_name_display}</b> на <b>01</b> сервере восстановлен!\n"
                    f"🔑 Ваш новый пароль: <tg-spoiler><b><i>{new_password}</i></b></tg-spoiler>\n\n"
                    f"💾 Не забудьте <b>сохранить пароль</b> в надежном месте!",
                    parse_mode='HTML'
                )
                
                try:
                    await self.bot.edit_message_reply_markup(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        reply_markup=None
                    )
                except ApiException as e:
                    logger.warning(f"Could not edit message: {e}")
            else:
                raise Exception("Password update failed")
                    
        except Exception as e:
            logger.error(f"Password recovery error: {e}")
            await self.bot.send_message(
                call.message.chat.id,
                f"ℹ️ Произошла ошибка при восстановлении пароля: {str(e)}\n\n"
                "Пожалуйста, попробуйте позже или обратитесь в поддержку."
            )
    
    async def handle_deltg_callback(self, call):
        try:
            question, answer = self.codes.generate_captcha()
            self.codes.set_captcha(call.from_user.id, answer)
            self.codes.set_user_state(call.from_user.id, 'waiting_captcha')
            
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton(text='✅ Подтвердить', callback_data='confirm_deltg'),
                types.InlineKeyboardButton(text='❌ Отмена', callback_data='cancel_deltg')
            )
            
            await self.bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"🔒 <b>Подтверждение действия</b>\n\n"
                     f"Для отвязки профиля решите простой пример:\n"
                     f"<b>{question} = ?</b>\n\n"
                     f"Отправьте ответ числом в этот чат.",
                parse_mode='HTML',
                reply_markup=markup
            )
            
        except Exception as e:
            logger.error(f"Error starting deltg process: {e}")
            await self.bot.answer_callback_query(call.id, "❌ Произошла ошибка")
    
    async def handle_captcha_answer(self, message):
        user_id = message.from_user.id
        user_state = self.codes.get_user_state(user_id)
        
        if not user_state or user_state['state'] != 'waiting_captcha':
            return
        
        if self.codes.verify_captcha(user_id, message.text):
            self.codes.clear_user_state(user_id)
            await self.process_deltg_confirmation(user_id, message.chat.id)
        else:
            attempts = self.codes.get_captcha_attempts(user_id)
            if attempts >= 3:
                self.codes.remove_captcha(user_id)
                self.codes.clear_user_state(user_id)
                await self.bot.send_message(
                    message.chat.id,
                    "❌ Слишком много неверных попыток. Отвязка профиля отменена."
                )
            else:
                await self.bot.send_message(
                    message.chat.id,
                    f"❌ Неверный ответ. Попробуйте еще раз. Попытка {attempts}/3"
                )
    
    async def process_deltg_confirmation(self, user_id: int, chat_id: int):
        try:
            success = await self.db.delete_user(user_id)
            
            if success:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(text='🔐 Привязать новый аккаунт', callback_data='addcode'))
                
                await self.bot.send_message(
                    chat_id,
                    "✅ Ваш игровой аккаунт был успешно <b>отвязан</b> от Telegram.\n\n"
                    "🔐 Если Вы желаете <b>привязать новый аккаунт</b>, используйте <b>Меню</b> или кнопку ниже 🗳",
                    parse_mode='HTML', 
                    reply_markup=markup
                )
                
                logger.info(f"Profile unlinked for user {user_id}")
            else:
                raise Exception("Failed to delete user")
                
        except Exception as e:
            logger.error(f"Error unlinking profile: {e}")
            await self.bot.send_message(chat_id, "❌ Произошла ошибка при отвязке профиля")
    
    async def handle_confirm_deltg(self, call):
        await self.bot.answer_callback_query(call.id, "✍️ Отправьте ответ числом в чат")
    
    async def handle_cancel_deltg(self, call):
        self.codes.remove_captcha(call.from_user.id)
        self.codes.clear_user_state(call.from_user.id)
        
        try:
            await self.bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text="❌ Отвязка профиля отменена.",
                reply_markup=None
            )
        except ApiException as e:
            logger.warning(f"Could not edit message: {e}")
    
    async def handle_addcode_callback(self, call):
        await self.process_code_request(call)
    
    async def monitor_telegram_table(self):
        logger.info("Запущен мониторинг таблицы `telegram`")
        
        while True:
            try:
                current_data = await self.db.get_all_telegram_users()
                current_users = {
                    row['id']: UserData(**row) for row in current_data
                }
                
                for user_id, user_data in current_users.items():
                    if user_id in self.last_user_data:
                        old_data = self.last_user_data[user_id]
                        
                        if (old_data.code != user_data.code and user_data.code != 0):
                            await self.handle_code_change(user_id, user_data, old_data)
                        
                        if (old_data.owner_id == 0 and user_data.owner_id != 0):
                            await self.handle_account_binding(user_id, user_data)
                
                self.last_user_data = current_users.copy()
                
                expired_ids = set(self.last_user_data.keys()) - set(current_users.keys())
                for user_id in expired_ids:
                    self.last_user_data.pop(user_id, None)
                    self.codes.remove_code(user_id)
                
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"Ошибка в мониторе: {e}")
                await asyncio.sleep(10)
    
    async def handle_code_change(self, user_id: int, new_data: UserData, old_data: UserData):
        logger.info(f"Обнаружено изменение кода для id {user_id}: {old_data.code} -> {new_data.code}")
        
        self.codes.add_code(user_id, new_data.tg_id, new_data.code)
        logger.info(f"Код {new_data.code} будет активен 1 минуту")
        
        if new_data.owner_id != 0:
            try:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(text='Перейти в поддержку', url='t.me/fl1ckyy'))
                
                player_name = new_data.player_name.replace('_', ' ') if new_data.player_name else "Неизвестный"
                
                # Если type_name пустое или None, то пишем "Неизвестный тип"
                action_name = new_data.type_name if new_data.type_name else "Неизвестный тип"
                
                message_text = (
                    f"⚠️ С Вашего аккаунта *{player_name}* на *01* сервере поступил запрос на выполнение действия "
                    f"«{action_name}». *Код подтверждения: {new_data.code}*\n\n"
                    f"Никому не передавайте этот код! Даже администрации проекта. "
                    f"Если Вы не запрашивали это действие, обратитесь в техническую поддержку."
                )
                
                await self.bot.send_message(
                    new_data.tg_id, 
                    message_text, 
                    parse_mode='Markdown', 
                    reply_markup=markup
                )
                
                logger.info(f"✅ Отправлен код {new_data.code} пользователю {new_data.tg_id} (ID: {user_id})")
                
            except Exception as e:
                logger.error(f"❌ Ошибка отправки кода пользователю {new_data.tg_id}: {e}")
        else:
            logger.info(f"⏸️ Аккаунт не привязан (owner_id=0), код не отправлен")
    
    async def handle_account_binding(self, user_id: int, data: UserData):
        logger.info(f"Обнаружена привязка аккаунта для ID {user_id}: 0 -> {data.owner_id}")
        
        try:
            player_name = data.player_name.replace('_', ' ') if data.player_name else "Неизвестный"
            message_text = f"✅ Аккаунт {player_name} на 01 сервере *успешно привязан* к Телеграм помощнику."
            
            await self.bot.send_message(data.tg_id, message_text, parse_mode='Markdown')
            logger.info(f"✅ Уведомление о привязке отправлено пользователю {data.tg_id} (ID: {user_id})")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления о привязке: {e}")
    
    async def check_expired_codes(self):
        logger.info("Запущен мониторинг просроченных кодов")
        
        while True:
            try:
                expired_ids = self.codes.get_expired_codes()
                
                for code_id in expired_ids:
                    await self.expire_code(code_id)
                
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Ошибка в мониторе просроченных кодов: {e}")
                await asyncio.sleep(10)
    
    async def expire_code(self, code_id: int):
        try:
            success = await self.db.execute_update(
                "UPDATE telegram SET code = 0 WHERE id = %s", 
                (code_id,)
            )
            
            if success:
                logger.info(f"🔄 Код (ID: {code_id}) обнулен (истек срок действия)")
                self.codes.remove_code(code_id)
                
                if code_id in self.last_user_data:
                    self.last_user_data[code_id].code = 0
            else:
                logger.warning(f"Не удалось обнулить код для ID {code_id}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обнулении кода для ID {code_id}: {e}")
    
    async def init_monitor(self):
        try:
            users_data = await self.db.get_all_telegram_users()
            
            for user_row in users_data:
                user_data = UserData(**user_row)
                self.last_user_data[user_data.id] = user_data
                
                if user_data.code != 0:
                    self.codes.add_code(user_data.id, user_data.tg_id, user_data.code)
                    logger.info(f"Восстановлен код {user_data.code} для ID {user_data.id}")
            
            logger.info(f"✅ Загружено записей: {len(self.last_user_data)}")
            logger.info(f"Активных кодов: {len(self.codes.active_codes)}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации монитора: {e}")
    
    async def start_monitoring(self):
        await self.init_monitor()
        
        asyncio.create_task(self.monitor_telegram_table())
        asyncio.create_task(self.check_expired_codes())
        
        logger.info("Мониторинг запущен")
    
    async def run(self):
        await self.db.initialize()
        await self.start_monitoring()
        logger.info("Бот запущен")
        await self.bot.infinity_polling()

async def main():
    db_manager = DatabaseManager()
    code_manager = CodeManager()
    bot = TelegramBot('8313881273:AAF7OLED6eJK7ozhQ5tJL-kcIZE0cs-K-VU', db_manager, code_manager)
    
    try:
        await bot.run()
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        if db_manager.pool:
            db_manager.pool.close()
            await db_manager.pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())