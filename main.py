import io
import re
import time
import telebot
import locale

from collections import defaultdict
from openpyxl.workbook import Workbook
from sqlalchemy import func
from bot import admin_main_menu, client_main_menu, worker_main_menu, unknown_main_menu, supreme_leader_main_menu, audit_main_menu
from telebot import types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, InputFile, InputMediaAnimation
from database.config import *
from db.for_delivery import ForDelivery
from db.temp_reservations import TempReservations
from db.in_delivery import InDelivery
from db.temp_fulfilied import Temp_Fulfilled
from handlers.black_list import *
from handlers.clients_manage import *
from handlers.posts_manage import *
from handlers.reservations_manage import *
from types import SimpleNamespace
from handlers.reservations_manage import calculate_total_sum, calculate_processed_sum
from handlers.classess import *
from sqlalchemy import select, update, and_, func
from sqlalchemy.exc import IntegrityError
from datetime import datetime


# Настройка бота и кэш
bot = telebot.TeleBot(TOKEN)
user_messages = {}
user_pages = {}
PAGE_SIZE = 5
user_last_message_id = {}
last_bot_message = {}
user_data = {}
user_states = {}
temp_user_data = {}
temp_post_data = {}
last_start_time = {}
delivery_active = False
locale.setlocale(locale.LC_TIME, "ru_RU")
active_audit = {}



# Сохранение бронирования
def save_reservation(user_id, post_id, quantity=1, is_fulfilled=False):
    try:
        Reservations.insert(
            user_id=user_id,
            quantity=quantity,
            post_id=post_id,
            is_fulfilled=is_fulfilled,
        )
        return True, "Резервация успешно сохранена"
    except Exception:
        return False

# Получение неотправленных постов
@bot.message_handler(commands=["unsent_posts"])
def list_unsent_posts(message):
    user_id = message.chat.id
    role = get_client_role(user_id)

    # Проверяем роль пользователя
    if role not in ["admin", "worker", "supreme_leader", "audit"]:
        bot.send_message(user_id, "У вас нет прав доступа к этой функции.")
        return

    # Получаем неотправленные посты через метод класса
    try:
        unsent_posts = Posts.get_unsent_posts()
    except Exception:
        return

    # Формируем и отправляем ответ
    if unsent_posts:
        response = "📮 Неотправленные посты:\n"
        for post in unsent_posts:
            post_id, price, description, quantity = post
            response += f"ID: {post_id} | Цена: {price} ₽ | Описание: {description} | Количество: {quantity}\n"
        bot.send_message(user_id, response)
    else:
        bot.send_message(user_id, "✅ Все посты отправлены.")

# Инициализация старта бота
@bot.message_handler(commands=["start"])
def handle_start(message):
    user_id = message.chat.id

    # Получаем роль пользователя
    role = get_client_role(user_id)

    # Формируем персонализированное приветствие
    greetings = {
        "client": "Добро пожаловать в интерфейс бота, здесь вы можете просмотреть свою корзину или задать вопросы в чате поддержки.",
        "worker": "Давай за работу!",
        "audit": "Давай за работу!",
        "supreme_leader": "С возвращением, Повелитель!",
        "admin": "С возвращением в меню администратора",
    }
    greeting = greetings.get(role, "Привет, прошу пройти регистрацию")

    # Создаём инлайн-клавиатуру с кнопками
    inline_markup = InlineKeyboardMarkup()
    inline_markup.add(InlineKeyboardButton("В поддержку", url=support_link))
    inline_markup.add(InlineKeyboardButton("На канал", url=channel_link))
    inline_markup.add(InlineKeyboardButton("Правила", callback_data="rules"))  # Кнопка "Правила"

    # Определяем reply-клавиатуру по роли пользователя
    if role == "admin":
        reply_markup = admin_main_menu()
    elif role == "client":
        reply_markup = client_main_menu()
    elif role == "audit":
        reply_markup = audit_main_menu()
    elif role == "worker":
        reply_markup = worker_main_menu()
    elif role == "supreme_leader":
        reply_markup = supreme_leader_main_menu()
    else:
        reply_markup = unknown_main_menu()

    # Удаляем старые сообщения бота, если они существуют
    if user_id in last_bot_message:
        try:
            # Удаляем сообщение с приветствием
            bot.delete_message(
                chat_id=user_id, message_id=last_bot_message[user_id]["greeting"]
            )
        except Exception as e:
            print(
                f"Не удалось удалить старое приветственное сообщение для {user_id}: {e}"
            )
        try:
            # Удаляем сообщение с ресурсами
            bot.delete_message(
                chat_id=user_id, message_id=last_bot_message[user_id].get("resources")
            )
        except Exception:
            pass

    # Отправляем новое сообщение с приветствием
    try:
        sent_greeting = bot.send_message(user_id, greeting, reply_markup=reply_markup)

        # Если роль клиента, отправляем сообщение с ресурсами
        if role == "client":
            sent_resources = bot.send_message(
                user_id, "Посетите наши ресурсы:", reply_markup=inline_markup
            )
        else:
            sent_resources = None

        # Сохраняем ID новых сообщений
        last_bot_message[user_id] = {
            "greeting": sent_greeting.message_id,
            "resources": sent_resources.message_id if sent_resources else None,
        }
    except Exception as e:
        print(f"Ошибка при отправке сообщения для {user_id}: {e}")

    # Удаляем сообщение пользователя (команда /start)
    try:
        bot.delete_message(chat_id=user_id, message_id=message.message_id)
    except Exception:
        pass


# Обработчик нажатия на кнопку "Правила"
import threading


@bot.callback_query_handler(func=lambda call: call.data == "rules")
def show_rules(call):
    # Указываем текст правил
    rules_text = ("Правила Мега Скидок:\n1.В описании к посту всегда пишется дефект(если он имеется) и количество."
                  "\n2.Купленный товар возврату и обмену не подлежат."
                  "\n3.Одежда дешевле 1 500₽, не подошедшая по размеру возврату не подлежат."
                  "\n4.Администратор не знает что находится в корзине(Смотрите  Мои заказы)"
                  "\n5.Просьба доложить товар будет проигнорирована, товары обрабатываются в аблолютно случайном порядке"
                  "\n6.Бронь уходит первому нажавшему, и держится в течении некоторого времени"
                  "\n7.До обработки товара вы можете отказаться от товара, как только товар оказался у вас в корзине, отказаться уже нельзя, только полная расформировка"
                  "\n8.Доставка бесплатная от 2 000₽"
                  "\n9.Если не набралось данной суммы, можно осуществить платную доставку стоимостью в 350₽"
                  "\n10.Электрические товары, приобретенные у нас, имеют гарантию 7 дней."
                  "\n11.В случае, если товар пришел с дефектом, который не был указан, можете обратиться в поддержку."
                  "\nВо время доставки:"
                  "\n12.Курьер не может звонить заранее более чем за 5 минут в связи с загруженностью"
                  "\n13.Товар проверяется исключительно после оплаты"
                  "\n14.Курьер не знает что находится у вас в корзине(Смотрите  Заказы в доставке)")

    # Создаём разметку с кнопкой "Назад"
    markup = InlineKeyboardMarkup()
    back_button = InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start")  # Callback для возврата
    markup.add(back_button)

    try:
        # Редактируем текущее сообщение: добавляем текст и кнопку
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=rules_text,
            reply_markup=markup  # Кнопка для возврата
        )
    except Exception as e:
        print(f"Ошибка при изменении текста сообщения: {e}")


@bot.callback_query_handler(func=lambda call: call.data == "back_to_start")
def back_to_start(call):
    try:
        # Удаляем текущее сообщение с правилами
        bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)

        # Отправляем уведомление о возврате
        notification_message = bot.send_message(
            chat_id=call.message.chat.id,
            text="Вы вернулись в главное меню."
        )

        # Используем таймер для удаления уведомления через 5 секунд
        threading.Timer(5.0, bot.delete_message, args=(call.message.chat.id, notification_message.message_id)).start()

        # Отправляем приветственное сообщение, вызывая handle_start
        handle_start(call.message)

    except Exception as e:
        print(f"Ошибка при обработке возврата в главное меню: {e}")


# Хэндлер регистрации
@bot.message_handler(func=lambda message: message.text == "Регистрация")
def handle_registration(message):
    chat_id = message.chat.id

    # Проверяем, находится ли пользователь в черном списке
    if is_user_blacklisted(chat_id):
        bot.send_message(chat_id, "К сожалению, вы не можете зарегистрироваться, так как находитесь в черном списке.")
        return

    # Проверяем, зарегистрирован ли уже пользователь
    if Clients.get_row_by_user_id(chat_id):
        bot.send_message(chat_id, "Вы уже зарегистрированы!")
        handle_start(message)
        return

    # Сохраняем состояние для шага регистрации имени
    set_user_state(chat_id, Registration.REGISTERING_NAME)
    bot.send_message(chat_id, "Введите ваше имя:")

def resolve_user_id(user_id):
    """
    Возвращает user_id владельца корзины, если номер телефона зарегистрирован на другого пользователя.
    Если связь не найдена, возвращается текущий user_id.
    """
    try:
        # Получаем текущего пользователя
        current_user = Clients.get_row_by_user_id(user_id)
        if not current_user or not current_user.phone:
            # Если пользователя нет или у него нет телефона, используем его же user_id
            return user_id

        # Ищем владельца телефона
        owner = Clients.get_row_by_phone(current_user.phone)
        if not owner:
            # Если владельца телефона нет, возвращаем текущий user_id
            return user_id

        # Если телефон уже привязан к другому пользователю, возвращаем его user_id
        return owner.user_id
    except Exception as e:
        print(f"[ERROR]: Ошибка при определении владельца корзины для пользователя {user_id}: {e}")
        return user_id

def add_to_cart(user_id, post_id, quantity):
    """
    Добавление товара в корзину владельца телефона.
    """
    try:
        # Определяем владельца корзины
        actual_user_id = resolve_user_id(user_id)

        # Добавляем товар в корзину владельца телефона
        Reservations.insert(
            user_id=actual_user_id,
            post_id=post_id,
            quantity=quantity
        )

        print(f"✔️ Товар добавлен в корзину пользователя {actual_user_id} (первоначальный ID {user_id}).")
    except Exception as e:
        print(f"[ERROR]: Ошибка при добавлении товара в корзину: {e}")

def get_user_cart(user_id):
    """
    Возвращает содержимое корзины для всех пользователей, связанных с одним номером телефона.
    Если телефон или владелец не найден, возвращает пустой список.
    """
    try:
        # Получаем текущего пользователя
        current_user = Clients.get_row_by_user_id(user_id)
        if not current_user or not current_user.phone:
            return []  # Если пользователя нет или отсутствует привязанный номер, возвращаем пустую корзину

        # Получаем всех пользователей, связанных с этим номером
        with Session(bind=engine) as session:
            user_ids = session.query(Clients.user_id).filter(Clients.phone == current_user.phone).all()

        # Преобразуем список user_id для запроса заказов
        user_ids = [uid[0] for uid in user_ids]

        # Получаем заказы для всех связанных пользователей
        orders = []
        for uid in user_ids:
            user_orders = Reservations.get_row_by_user_id(uid)
            orders.extend(user_orders)

        return orders
    except Exception as e:
        print(f"[ERROR]: Ошибка при получении объединённой корзины: {e}")
        return []

def clear_cart(user_id):
    """
    Очистка корзины для пользователя.
    """
    try:
        # Определяем владельца корзины
        actual_user_id = resolve_user_id(user_id)

        # Очищаем корзину для владельца
        Reservations.delete_row(user_id=actual_user_id)
        print(f"✔️ Корзина очищена для пользователя {actual_user_id} (для {user_id}).")
    except Exception as e:
        print(f"[ERROR]: Ошибка при очистке корзины: {e}")

# Имя для регистрации
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == Registration.REGISTERING_NAME)
def handle_name_registration(message):
    chat_id = message.chat.id
    user_name = message.text.strip()

    # Проверяем валидность имени
    if len(user_name) < 2:
        bot.send_message(chat_id, "Имя слишком короткое. Пожалуйста, введите хотя бы 2 символа.")
        return

    # Сохраняем имя во временные данные
    if chat_id not in temp_user_data:
        temp_user_data[chat_id] = {}
    temp_user_data[chat_id]["name"] = user_name

    # Переход к следующему шагу - вводу номера
    set_user_state(chat_id, Registration.STARTED_REGISTRATION)
    bot.send_message(chat_id, "Введите ваш номер телефона:")

# Номер для регистрации
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == Registration.STARTED_REGISTRATION)
def handle_phone_registration(message):
    chat_id = message.chat.id
    raw_phone = message.text.strip()
    cleaned_phone = re.sub(r"\D", "", raw_phone)

    # Проверка валидности номер через регулярное выражение
    if is_phone_valid(cleaned_phone):
        if cleaned_phone.startswith("7"):
            cleaned_phone = "8" + cleaned_phone[1:]

        # Проверяем наличие номера в базе
        existing_client = Clients.get_row_by_phone(cleaned_phone)
        if existing_client:
            # Номер уже существует
            bot.send_message(
                chat_id,
                f"⚠️ Номер телефона {cleaned_phone} уже зарегистрирован. Вы уверены, что хотите его привязать к своему аккаунту? "
                "Эта операция уведомит текущего владельца номера.",
                reply_markup=create_yes_no_keyboard()  # Генерируем клавиатуру
            )
            # Сохраняем телефон во временные данные для подтверждения
            if chat_id not in temp_user_data:
                temp_user_data[chat_id] = {}
            temp_user_data[chat_id]["phone"] = cleaned_phone
            set_user_state(chat_id, Registration.REGISTERING_PHONE)  # Переходим в состояние подтверждения номера
        else:
            # Если номер уникальный, завершаем регистрацию
            complete_registration(chat_id, cleaned_phone)
    else:
        bot.send_message(chat_id, "❌ Введите корректный номер телефона. Например, +7XXXXXXXXXX")

# Валидация номера телефона
def is_phone_valid(phone):
    pattern = r"^(8|7|\+7)\d{10}$"
    return re.match(pattern, phone) is not None

# Подтверждение номера
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == Registration.REGISTERING_PHONE)
def confirm_phone_registration(message):
    chat_id = message.chat.id
    response = message.text.strip().lower()

    # Проверка временных данных (подстраховка)
    if chat_id not in temp_user_data or "phone" not in temp_user_data[chat_id]:
        bot.send_message(chat_id, "❌ Ошибка: временные данные регистрации потеряны. Попробуйте снова.")
        clear_user_state(chat_id)
        return

    # Получаем телефон и имя из временных данных
    phone = temp_user_data[chat_id]["phone"]
    name = temp_user_data[chat_id].get("name", "Неизвестный")

    if response == "да":
        # Проверяем, существует ли уже пользователь с таким номером телефона
        existing_client = Clients.get_row_by_phone(phone)
        if existing_client:
            # Если телефон уже существует, все равно добавляем нового пользователя с этим телефоном.
            # Корзина объединяется через `resolve_user_id` автоматически.

            # Добавляем нового пользователя
            Clients.insert(
                user_id=chat_id,
                name=name,
                phone=phone,  # Используем тот же номер телефона
                role="client"  # Роль по умолчанию
            )

            # Сообщаем текущему владельцу
            bot.send_message(
                existing_client.user_id,
                f"⚠️ Новый пользователь ({name}) зарегистрировался с номером телефона {phone}. "
                "Все заказы для этого телефона будут сохраняться в вашей корзине."
            )

            # Оповещаем нового пользователя
            bot.send_message(
                chat_id,
                f"✅ Вы успешно зарегистрированы! Корзина будет привязана к номеру {phone}, "
                "используемому несколькими пользователями.",
                reply_markup=types.ReplyKeyboardRemove()
            )

            # Завершаем регистрацию (важно!)
            clear_user_state(chat_id)
            handle_start(message)
        else:
            # Если номер уникальный, завершаем обычную регистрацию
            complete_registration(chat_id, phone)
    elif response == "нет":
        # Если пользователь отказался, предлагаем ввести новый номер телефона
        bot.send_message(chat_id, "❌ Регистрация номера отменена. Введите новый номер телефона:")
        set_user_state(chat_id, Registration.STARTED_REGISTRATION)
    else:
        # Если пользователь ввел что-то не то
        bot.send_message(
            chat_id,
            "⚠️ Пожалуйста, введите *'Да'* для подтверждения или *'Нет'* для отказа.",
            parse_mode="Markdown"
        )

def get_first_owner_by_phone(phone):
    """
    Ищет первого владельца номера телефона по id (минимальному значению).
    Если номера телефона нет, возвращает None.
    """
    try:
        # Ищем в таблице клиентов самого первого владельца номера по id
        with Session(bind=engine) as session:
            first_owner = (
                session.query(Clients)
                .filter(Clients.phone == phone)
                .order_by(Clients.id.asc())  # Сортировка по id для определения первого владельца
                .first()
            )
            return first_owner
    except Exception as e:
        print(f"[ERROR]: Ошибка при поиске первого владельца номера телефона: {e}")
        return None

# Завершение регистрации
def complete_registration(chat_id, phone):
    """
    Завершает регистрацию. Если номер уже существует, все равно добавляет нового пользователя в базу.
    """
    # Получаем имя из временных данных
    name = temp_user_data.get(chat_id, {}).get("name", "Неизвестный")

    try:
        # Устанавливаем роль (по умолчанию 'client')
        role = "supreme_leader" if chat_id == ADMIN_USER_ID else "client"

        # Проверяем существующего пользователя с таким телефоном
        existing_client = Clients.get_row_by_phone(phone)

        if existing_client:
            # Если номер телефона существует, добавляем нового пользователя с этим номером
            Clients.insert(
                user_id=chat_id,
                name=name,
                phone=phone,  # Используем уже существующий телефон
                role=role  # Роль по умолчанию
            )

            # Уведомляем нового пользователя
            bot.send_message(
                chat_id,
                f"✅ Вы успешно зарегистрированы. Ваш номер телефона {phone} уже используется, "
                "и ваша корзина будет объединена с текущими бронированиями.",
                reply_markup=types.ReplyKeyboardRemove()
            )

            # Уведомляем первого владельца телефона (если важно)
            bot.send_message(
                existing_client.user_id,
                f"⚠️ Новый пользователь ({name}) зарегистрировался под вашим номером телефона ({phone}). "
                "Бронирования будут объединены."
            )

        else:
            # Если номер уникальный, продолжаем обычную регистрацию
            Clients.insert(
                user_id=chat_id,
                name=name,
                phone=phone,
                role=role  # Роль по умолчанию
            )

            bot.send_message(
                chat_id,
                f"✅ Регистрация завершена! Ваш номер телефона {phone} сохранен.",
                reply_markup=types.ReplyKeyboardRemove()
            )

        # Завершаем процесс регистрации
        clear_user_state(chat_id)
        handle_start(SimpleNamespace(chat=SimpleNamespace(id=chat_id), message_id=None))

    except Exception as e:
        bot.send_message(chat_id, "❌ Во время регистрации произошла ошибка. Попробуйте позже.")
        print(f"[ERROR]: Ошибка завершения регистрации: {e}")

# Создание клавиатуры да или нет для подтверждения номера
def create_yes_no_keyboard():
    """Генерирует клавиатуру для подтверждения"""
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True, one_time_keyboard=True)
    markup.add(types.KeyboardButton("Да"), types.KeyboardButton("Нет"))
    return markup

# Проверка регистрации пользователя
def is_user_registered(phone: str) -> bool:
    try:
        with Session(bind=engine) as session:
            # Ищем номер в таблице клиентов
            return session.query(Clients).filter(Clients.phone == phone).first() is not None
    except Exception as e:
        print(f"Ошибка проверки пользователя: {e}")
        return False

# Обработчик запроса бронирования
@bot.callback_query_handler(func=lambda call: call.data.startswith("reserve_"))
def handle_reservation(call):
    post_id = int(call.data.split("_", 1)[1])
    user_id = call.from_user.id
    if is_user_blacklisted(user_id):
        return "Вы не можете бронировать товары, так как вы были заблокированы"
    if not is_registered(user_id):
        bot.answer_callback_query(
            callback_query_id=call.id,
            text="Вы не зарегистрированы! Для регистрации перейдите в бота",
            show_alert=True,
        )
        return
    with Session(bind=engine) as session:
        try:
            # Получаем текущий товар с блокировкой строки
            post = session.query(Posts).filter(Posts.id == post_id).with_for_update().first()
            if not post or post.quantity <= 0:
                # Проверка очереди
                user_in_queue = session.query(TempReservations).filter(
                    and_(
                        TempReservations.user_id == user_id,
                        TempReservations.post_id == post_id,
                        TempReservations.temp_fulfilled == False
                    )
                ).first()
                if user_in_queue:
                    bot.answer_callback_query(
                        callback_query_id=call.id,
                        text="Вы уже стоите в очереди за этим товаром!",
                        show_alert=True,
                    )
                    return
                # Добавляем в очередь
                temp_reservation = TempReservations(
                    user_id=user_id,
                    post_id=post_id,
                    quantity=1,
                    temp_fulfilled=False
                )
                session.add(temp_reservation)
                session.commit()
                bot.answer_callback_query(
                    callback_query_id=call.id,
                    text="Вы добавлены в очередь на этот товар.",
                    show_alert=True,
                )
                return
            # Если товар доступен, уменьшаем его количество
            post.quantity -= 1
            session.commit()
            # Сохраняем бронирование
            reservation = Reservations(
                user_id=user_id,
                post_id=post_id,
                quantity=1,
                is_fulfilled=False,
                old_price=post.price
            )
            session.add(reservation)
            session.commit()
            # Обновляем сообщение в канале
            new_caption = (
                f"Цена: {post.price} ₽\nОписание: {post.description}\nОстаток: {post.quantity}"
            )
            try:
                bot.edit_message_caption(
                    chat_id=CHANNEL_ID,
                    message_id=post.message_id,
                    caption=new_caption,
                    reply_markup=call.message.reply_markup,
                )
            except Exception as e:
                bot.send_message(
                    user_id, f"Не удалось обновить сообщение в канале. Ошибка: {e}"
                )

            # Отправляем личное сообщение с фото товара, описанием и кнопкой отмены
            cancel_button = InlineKeyboardMarkup()
            cancel_button.add(
                InlineKeyboardButton(
                    text="🚫 Это я не заказывал",
                    callback_data=f"cancel_reservation_{reservation.id}"

                )
            )
            try:
                bot.send_photo(
                    chat_id=user_id,
                    photo=post.photo,  # Ссылка на фото товара
                    caption=(
                        f"Вы забронировали товар!\n\n"
                        f"🏷️ Название: {post.description}\n"
                        f"💲 Цена: {post.price} ₽\n"
                        f"Если это была ошибка, нажмите кнопку ниже."
                    ),
                    reply_markup=cancel_button,
                )
            except Exception as e:
                bot.send_message(
                    chat_id=user_id,
                    text=f"Не удалось отправить информацию о бронировании. Ошибка: {e}",
                )

            # Уведомление пользователя
            if post.quantity == 0:
                bot.answer_callback_query(
                    callback_query_id=call.id,
                    text="Вы забронировали последний экземпляр товара!",
                    show_alert=True,
                )
            else:
                bot.answer_callback_query(
                    callback_query_id=call.id,
                    text="Вы забронировали товар!",
                    show_alert=True,
                )
        except IntegrityError:
            session.rollback()
            bot.answer_callback_query(
                callback_query_id=call.id,
                text="Произошла ошибка при бронировании. Попробуйте снова.",
                show_alert=True,
            )

# Получение бронирования пользователя
def get_user_reservations(user_id):
    """
    Получение всех заказов текущего пользователя, а также всех пользователей с таким же номером телефона.
    """
    # Получаем текущие данные пользователя
    client = Clients.get_row_by_user_id(user_id)
    if client is None:
        print("Пользователь не найден.")
        return []  # Пользователь не зарегистрирован

    # Находим всех пользователей с таким же номером телефона
    related_clients = Clients.get_row_by_phone_digits(phone_digits=client.phone[-4:])
    if not related_clients:
        print("Связанные пользователи по последним цифрам не найдены.")
        return []

    # Debug: какие пользователи найдены
    related_user_ids = [related_client.user_id for related_client in related_clients]

    # Собираем все бронирования для этих пользователей
    with Session(bind=engine) as session:
        reservations = session.query(Reservations).filter(
            Reservations.user_id.in_(related_user_ids)
        ).all()

    return reservations

# Обработчик моих забронированных товаров
@bot.message_handler(commands=["my_reservations"])
def show_reservations(message):
    user_id = message.chat.id
    query = Clients.get_row(user_id=user_id)
    # Проверка регистрации пользователя
    # if not is_registered(user_id):
    if query is None:
        msg = bot.send_message(
            user_id,
            "Вы не зарегистрированы! Для регистрации используйте команду /start register.",
        )
        user_messages[user_id] = [msg.message_id]
        return

    # Получаем заказы пользователя
    reservations = get_user_reservations(user_id)

    if reservations:
        for idx, (description, price, photo, quantity, is_fulfilled) in enumerate(
            reservations, start=1
        ):
            status = "✅ Положено" if is_fulfilled else "⏳ Ожидает выполнения"

            # Формируем описание
            caption = (
                f"{idx}. Описание: {description}\n"
                f"💰 Цена: {price}₽ x {quantity}\n"
                f"Статус: {status}"
            )

            if photo:
                try:
                    sent_photo = bot.send_photo(user_id, photo=photo, caption=caption)
                    user_messages.setdefault(user_id, []).append(sent_photo.message_id)
                except Exception as e:
                    bot.send_message(
                        user_id, f"Ошибка при показе фотографии: {e}"
                    )  # Показываем ошибку

# Хэндлер для обработки нажатий на заказ
@bot.callback_query_handler(func=lambda call: call.data.startswith("order_"))
def order_details(call):
    reservation_id = int(call.data.split("_")[1])

    try:
        # Получаем информацию о заказе через ORM
        order = Reservations.get_row_by_id(reservation_id)
        if not order:
            bot.answer_callback_query(call.id, "Заказ не найден.", show_alert=True)
            return

        # Получаем пост, связанный с этим заказом
        post = Posts.get_row_by_id(order.post_id)
        if not post:
            bot.answer_callback_query(call.id, "Товар не найден.", show_alert=True)
            return

        status = "✔️ Обработан" if order.is_fulfilled else "⌛ В обработке"
        caption = f"Цена: {post.price} ₽\nОписание: {post.description}\nСтатус: {status}"
        # Создаём кнопки возврата или отмены
        markup = InlineKeyboardMarkup()
        back_btn = InlineKeyboardButton("⬅️ Назад", callback_data="my_orders")
        markup.add(back_btn)
        # Добавляем кнопку отмены, если заказ ещё не обработан
        if not order.is_fulfilled:
            cancel_btn = InlineKeyboardButton("❌ Отказаться", callback_data=f"cancel_{reservation_id}")
            markup.add(cancel_btn)

        # Обновляем сообщение с деталями заказа
        bot.edit_message_media(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            media=InputMediaPhoto(media=post.photo, caption=caption),
            reply_markup=markup
        )
    except Exception as e:
        print(f"Ошибка отображения деталей заказа: {e}")
        bot.answer_callback_query(call.id, "Произошла ошибка.", show_alert=True)

# Отображает список заказов
@bot.callback_query_handler(func=lambda call: call.data == "my_orders")
def show_my_orders(call):
    message = call.message
    my_orders(message)  # Вызываем my_orders, передаём исходное сообщение
    bot.answer_callback_query(call.id)  # Подтверждаем обработку нажатия

# Обработчик функции Мои заказы
@bot.message_handler(func=lambda message: message.text == "🛒 Мои заказы")
def my_orders(message):
    user_id = message.chat.id

    # Сначала удаляем сообщение пользователя
    try:
        bot.delete_message(chat_id=user_id, message_id=message.message_id)
    except Exception:
        pass

    try:
        # Удаляем предыдущее сообщение бота, если оно есть
        if user_id in user_last_message_id:
            try:
                bot.delete_message(chat_id=user_id, message_id=user_last_message_id[user_id])
            except Exception:
                pass

        # Проверяем, зарегистрирован ли пользователь
        current_user = Clients.get_row_by_user_id(user_id)
        if not current_user:
            sent_message = bot.send_message(chat_id=user_id, text="❌ Вы не зарегистрированы.")
            user_last_message_id[user_id] = sent_message.message_id  # Сохраняем ID последнего сообщения
            return

        # Получаем заказы всех связанных пользователей
        orders = get_user_reservations(user_id)

        if orders:
            user_pages[user_id] = 0  # Устанавливаем текущую страницу на первую
            sent_message = send_order_page(user_id=user_id, message_id=None, orders=orders, page=user_pages[user_id])
            if sent_message:
                user_last_message_id[user_id] = sent_message.message_id  # Сохраняем ID последнего сообщения
        else:
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(InlineKeyboardButton(text="На канал", url=channel_link))
            sent_message = bot.send_message(
                chat_id=user_id,
                text="У вас пока нет заказов. Начните покупки, перейдя на наш канал.",
                reply_markup=keyboard,
            )
            user_last_message_id[user_id] = sent_message.message_id  # Сохраняем ID последнего сообщения
    except Exception as ex:
        print(f"Ошибка в обработке команды '🛒 Мои заказы': {ex}")


# Создает страницу с заказами
def send_order_page(user_id, message_id, orders, page):
    orders_per_page = 5  # Количество заказов на одной странице
    start = page * orders_per_page
    end = start + orders_per_page
    total_pages = (len(orders) - 1) // orders_per_page + 1
    selected_orders = orders[start:end]

    # Считаем общую сумму всех заказов
    total_sum_all = sum(
        Posts.get_row_by_id(order.post_id).price for order in orders if Posts.get_row_by_id(order.post_id)
    )

    # Считаем сумму только выполненных заказов
    total_sum_fulfilled = sum(
        Posts.get_row_by_id(order.post_id).price
        for order in orders
        if order.is_fulfilled and Posts.get_row_by_id(order.post_id)
    )

    # Формирование текста для страницы. Колонки: описание, цена, статус заказа.
    text = f"Ваши заказы (стр. {page + 1} из {total_pages}):\n\n"
    keyboard = InlineKeyboardMarkup(row_width=1)

    for order in selected_orders:
        post = Posts.get_row_by_id(order.post_id)  # Проверка и получение данных поста через ORM
        if post:
            status = "✅В корзине" if order.is_fulfilled else "⏳В обработке"
            keyboard.add(InlineKeyboardButton(
                text=f"({status})- {post.price} ₽ - {post.description}",
                callback_data=f"order_{order.id}"
            ))

    # Добавляем строки с общей суммой заказов и суммой выполненных заказов
    text += f"\nОбщая сумма заказов: {total_sum_all} ₽"
    text += f"\nОбщая сумма обработанных заказов: {total_sum_fulfilled} ₽\n"

    # Навигация по страницам
    if page > 0:
        keyboard.add(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"orders_page_{page - 1}"))
    if end < len(orders):
        keyboard.add(InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"orders_page_{page + 1}"))

    # Фото для страницы
    photo_path = "images/my_cart.jpg"
    with open(photo_path, "rb") as photo:
        if message_id:
            return bot.edit_message_media(
                chat_id=user_id,
                message_id=message_id,
                media=InputMediaPhoto(photo, caption=text),
                reply_markup=keyboard
            )
        else:
            return bot.send_photo(
                chat_id=user_id,
                photo=photo,
                caption=text,
                reply_markup=keyboard
            )

# Обработчик навигации между страницами
@bot.callback_query_handler(func=lambda call: call.data.startswith("orders_page_"))
def paginate_orders(call):
    user_id = call.message.chat.id
    message_id = call.message.message_id
    page = int(call.data.split("_")[2])

    # Получаем заказы пользователя и связанных клиентов
    orders = get_user_reservations(user_id)

    # Отправляем страницу с заказами
    try:
        new_message = send_order_page(user_id=user_id, message_id=message_id, orders=orders, page=page)
        user_last_message_id[user_id] = new_message.message_id  # Обновляем последний ID
    except Exception as e:
        print(f"Ошибка при попытке пагинации заказов: {e}")
    finally:
        bot.answer_callback_query(call.id)  # Подтверждаем успешную обработку

# Обработка отмены заказа
@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_"))
def cancel_reservation(call):
    print(f"Чета там чета там: {call.data}")  # Логируем callback_data для отладки
    try:
        # Универсальная обработка двух форматов данных
        if call.data.startswith("cancel_reservation_"):
            parts = call.data.split("_")
            if len(parts) == 3 and parts[2].isdigit():
                reservation_id = int(parts[2])
            else:
                raise ValueError(f"Некорректный формат callback_data: {call.data}")
        elif call.data.startswith("cancel_"):
            parts = call.data.split("_")
            if len(parts) == 2 and parts[1].isdigit():
                reservation_id = int(parts[1])
            else:
                raise ValueError(f"Некорректный формат callback_data: {call.data}")
        else:
            raise ValueError(f"Некорректный формат callback_data: {call.data}")

        # Извлекаем ID пользователя
        user_id = call.from_user.id  # Берём ID пользователя

        # Основная логика
        current_user = Clients.get_row_by_user_id(user_id)
        if not current_user:
            bot.answer_callback_query(call.id, "Вы не зарегистрированы.", show_alert=True)
            return

        related_clients = Clients.get_row_by_phone_digits(phone_digits=current_user.phone[-4:])
        related_user_ids = [client.user_id for client in related_clients]

        order = Reservations.get_row_by_id(reservation_id)
        if not order or order.user_id not in related_user_ids:
            bot.answer_callback_query(call.id, "Резерв не найден или не принадлежит вам.", show_alert=True)
            return

        if order.is_fulfilled:
            bot.answer_callback_query(call.id, "Невозможно отказаться от уже обработанного заказа.", show_alert=True)
            return

        post = Posts.get_row_by_id(order.post_id)
        if not post:
            bot.answer_callback_query(call.id, "Товар для отмены не найден.", show_alert=True)
            return

        success = Reservations.cancel_order_by_id(reservation_id)
        if not success:
            bot.answer_callback_query(call.id, "Ошибка отмены заказа.", show_alert=True)
            return

        with Session(bind=engine) as session:
            next_in_queue = session.query(TempReservations).filter(
                TempReservations.post_id == order.post_id,
                TempReservations.temp_fulfilled == False
            ).order_by(TempReservations.created_at).first()

            if next_in_queue:
                Reservations.insert(
                    user_id=next_in_queue.user_id,
                    post_id=order.post_id,
                    quantity=1,
                    is_fulfilled=False
                )
                next_in_queue.temp_fulfilled = True
                session.commit()

                bot.send_message(
                    chat_id=next_in_queue.user_id,
                    text="Ваш товар в очереди стал доступен и добавлен в вашу корзину."
                )

                bot.answer_callback_query(call.id, "Вы успешно отказались от товара. Он передан следующему в очереди.",
                                          show_alert=False)
                my_orders(call.message)
                return

        Posts.increment_quantity_by_id(order.post_id)

        if post.message_id:
            new_quantity = post.quantity + 1
            updated_caption = (
                f"Цена: {post.price} ₽\n"
                f"Описание: {post.description}\n"
                f"Остаток: {new_quantity}"
            )
            markup = InlineKeyboardMarkup()
            reserve_button = InlineKeyboardButton("🛒 Забронировать", callback_data=f"reserve_{post.id}")
            to_bot_button = InlineKeyboardButton("В Бота", url=f"{bot_link}?start=start")
            markup.add(reserve_button, to_bot_button)

            try:
                bot.edit_message_caption(
                    chat_id=CHANNEL_ID,
                    message_id=post.message_id,
                    caption=updated_caption,
                    reply_markup=markup,
                )
            except Exception as e:
                print(f"Ошибка обновления поста на канале: {e}")

        bot.answer_callback_query(call.id, "Вы успешно отказались от товара. Товар доступен в канале.",
                                  show_alert=False)
        my_orders(call.message)

    except ValueError as ve:
        print(f"Некорректные callback-данные: {ve}")
        bot.answer_callback_query(call.id, "Некорректные данные для отмены.", show_alert=True)
    except Exception as e:
        print(f"Ошибка при попытке отказаться от заказа: {e}")
        bot.answer_callback_query(call.id, "Произошла ошибка при обработке отмены.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("enqueue_"))
def handle_enqueue(call):
    user_id = call.message.chat.id
    post_id = int(call.data.split("_")[1])

    # Проверяем, существует ли запись уже в TempReservations
    with Session(bind=engine) as session:
        existing_entry = session.query(TempReservations).filter(
            TempReservations.user_id == user_id,
            TempReservations.post_id == post_id,
            TempReservations.temp_fulfilled == False
        ).first()

        if existing_entry:
            return

    # Добавляем в таблицу TempReservations
    TempReservations.insert(user_id=user_id, quantity=1, post_id=post_id, temp_fulfilled=False)
    bot.send_message(user_id, "Вы добавлены в очередь. Как только товар станет доступен, вы будете уведомлены.")

# Возврат в меню заказов
@bot.callback_query_handler(func=lambda call: call.data == "go_back")
def go_back_to_menu(call):
    # Если объект — сообщение (Message), то работаем с ним напрямую
    if isinstance(call, telebot.types.Message):
        chat_id = call.chat.id
    # Если объект — CallbackQuery, извлекаем его компонент message
    elif isinstance(call, telebot.types.CallbackQuery):
        chat_id = call.message.chat.id
        # Сразу подтверждаем callback_query
        try:
            bot.answer_callback_query(call.id)
        except Exception as e:
            print(f"Failed to answer callback query: {e}")
    else:
        print("Unsupported object type passed to go_back_to_menu")
        return

    # Отправляем сообщение пользователю
    bot.send_message(chat_id, "Вы вернулись в главное меню.")

# Обработчик функции 🚗 Заказы в доставке
@bot.message_handler(func=lambda message: message.text == "🚗 Заказы в доставке")
def show_delivery_orders(message):
    user_id = message.chat.id  # Получаем ID текущего пользователя

    try:
        # Получаем все записи из таблицы для текущего пользователя
        all_items = InDelivery.get_all_rows()

        # Фильтруем записи для конкретного user_id
        user_items = [item for item in all_items if item.user_id == user_id]

        # Проверяем сами данные

        # Создаём словарь для агрегации данных:
        aggregated_items = {}
        for item in user_items:
            if item.item_description not in aggregated_items:
                # Если описание ещё не добавлено, записываем его
                aggregated_items[item.item_description] = {
                    "quantity": item.quantity,  # Количество
                    "total_sum": item.quantity * item.price,  # Итоговая сумма
                }
            else:
                # Если описание уже есть, увеличиваем количество и итоговую сумму
                aggregated_items[item.item_description]["quantity"] += item.quantity
                aggregated_items[item.item_description]["total_sum"] += item.quantity * item.price

        # Преобразуем словарь обратно в список (для передачи на следующий этап)
        unique_items = [
            {
                "item_description": description,
                "quantity": data["quantity"],
                "total_sum": data["total_sum"],
            }
            for description, data in aggregated_items.items()
        ]

        # Если товаров нет, отправляем сообщение об этом
        if not unique_items:
            bot.send_message(
                chat_id=user_id,
                text="📭 У вас нет товаров в доставке.",
            )
            return

        # Отправляем список первым сообщением
        send_delivery_order_page(
            user_id=user_id,
            message_id=None,  # Потому что отправляется впервые
            orders=unique_items,
            page=0,
        )

    except Exception as e:
        # Если возникла ошибка — информируем пользователя
        bot.send_message(
            chat_id=user_id,
            text=f"❌ Ошибка при загрузке списка заказов: {str(e)}",
        )

# Создает страницу с заказами в доставке
def send_delivery_order_page(user_id, message_id, orders, page):
    orders_per_page = 5  # Количество товаров на странице
    start = page * orders_per_page
    end = start + orders_per_page
    total_pages = (len(orders) - 1) // orders_per_page + 1  # Всего страниц
    selected_orders = orders[start:end]  # Текущая страница товаров

    # Формируем сообщение для текущей страницы
    text = f"🚚 *Ваши товары в доставке* (страница {page + 1} из {total_pages}):\n\n"
    keyboard = InlineKeyboardMarkup(row_width=1)

    # Добавляем товары на страницу
    for idx, order in enumerate(selected_orders, start=start + 1):
        text += (
            f"*{idx})* {order['item_description']}\n"
            f"Количество: {order['quantity']}\n"
            f"Сумма: {order['total_sum']} руб.\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
        )

    # Добавляем кнопки для навигации
    if page > 0:
        keyboard.add(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"delivery_page_{page - 1}"))
    if end < len(orders):
        keyboard.add(InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"delivery_page_{page + 1}"))

    # Гифка для уведомления
    gif_path = "images/delivery_order.gif"
    with open(gif_path, "rb") as gif:
        if message_id:  # Если сообщение уже существует, обновляем его
            bot.edit_message_media(
                chat_id=user_id,
                message_id=message_id,
                media=InputMediaAnimation(gif, caption=text, parse_mode="Markdown"),
                reply_markup=keyboard,
            )
        else:  # Иначе отправляем новое сообщение
            bot.send_animation(
                chat_id=user_id,
                animation=gif,
                caption=text,
                reply_markup=keyboard,
                parse_mode="Markdown",
            )

# Хэндлер для команды "👔 Назначить работника"
@bot.message_handler(func=lambda message: message.text == "👔 Назначить работника")
def manage_user(message):
    # Проверяем, является ли пользователь администратором или лидером
    user_id = message.from_user.id
    if not (is_admin(user_id) or is_leader(user_id)):
        bot.send_message(message.chat.id, "У вас недостаточно прав для выполнения этой команды.")
        return

    # Если пользователь имеет доступ, продолжаем выполнение функции
    bot.send_message(
        message.chat.id,
        "Введите Имя пользователя и последние 4 цифры номера через пробел (например, Иван 1234):"
    )
    bot.register_next_step_handler(message, process_user_input)

# Обработка ввода имени и последних 4 цифр номера для поиска
def process_user_input(message):
    try:
        # Разбиваем данные на имя и последние цифры
        name, last_digits = message.text.split()
        last_digits = last_digits.strip()

        if not last_digits.isdigit() or len(last_digits) != 4:
            bot.send_message(message.chat.id, "Пожалуйста, введите корректные последние 4 цифры номера.")
            return

        # Поиск пользователя по имени и последним 4 цифрам номера
        user = find_user_by_name_and_last_digits(name, last_digits)

        if user:
            # Формируем сообщение с данными пользователя
            response = f"Данные пользователя:\nИмя: {user['name']}\nТекущая роль: {user['role']}"

            # Если роль из списка SPECIAL_ROLES, запрещаем изменение
            if user['role'] in SPECIAL_ROLES:
                response += "\nЭту роль нельзя изменить."
                bot.send_message(message.chat.id, response)
                return

            # Создаем кнопки для повышения/понижения роли
            keyboard = InlineKeyboardMarkup()
            keyboard.add(
                InlineKeyboardButton("Повысить", callback_data=f"promote_{user['user_id']}"),
                InlineKeyboardButton("Понизить", callback_data=f"demote_{user['user_id']}")
            )
            bot.send_message(message.chat.id, response, reply_markup=keyboard)
        else:
            bot.send_message(message.chat.id, "Пользователь не найден.")
    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите данные в формате 'Имя 1234'.")
    except Exception as e:
        bot.send_message(message.chat.id, "Произошла ошибка при обработке данных.")
        print(f"Ошибка: {e}")

# Обработчик изменения роли
@bot.callback_query_handler(func=lambda call: call.data.startswith("promote_") or call.data.startswith("demote_"))
def handle_role_change(call):
    try:
        # Получаем данные из callback (action, user_id)
        action, user_id = call.data.split("_")

        # Получаем пользователя через Clients
        user = Clients.get_row_by_user_id(int(user_id))  # Используем существующий метод get_row_by_user_id
        if not user:
            bot.answer_callback_query(call.id, "Пользователь не найден.")
            return

        current_role = user.role

        # Проверка корректности текущей роли
        if current_role not in ROLES:
            bot.answer_callback_query(call.id, "Некорректная роль пользователя.")
            return

        # Проверка, не относится ли пользователь к защищённым ролям
        if current_role in SPECIAL_ROLES:
            bot.answer_callback_query(call.id, "Эту роль нельзя менять.")
            return

        # Вычисление новой роли
        current_index = ROLES.index(current_role)
        if action == "promote" and current_index < len(ROLES) - 1:
            new_role = ROLES[current_index + 1]
        elif action == "demote" and current_index > 0:
            new_role = ROLES[current_index - 1]
        else:
            bot.answer_callback_query(call.id, "Дальнейшее изменение роли невозможно.")
            return

        # Используем метод для обновления роли пользователя
        success = Clients.update_row_for_work(user_id=user.user_id, updates={'role': new_role})

        if success:
            # Генерируем обновленную клавиатуру
            keyboard = InlineKeyboardMarkup()
            if new_role != ROLES[-1]:  # Проверяем, можно ли повысить
                keyboard.add(InlineKeyboardButton("Повысить", callback_data=f"promote_{user_id}"))
            if new_role != ROLES[0]:  # Проверяем, можно ли понизить
                keyboard.add(InlineKeyboardButton("Понизить", callback_data=f"demote_{user_id}"))

            # Обновляем сообщение с пользовательскими данными и клавиатурой
            try:
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text=f"Данные пользователя:\nИмя: {user.name}\nТекущая роль: {new_role}",
                    reply_markup=keyboard
                )
            except Exception as e:
                print(f"Ошибка обновления сообщения: {e}")
                bot.answer_callback_query(call.id, "Ошибка отображения новых данных, но роль изменена.")
                return

            # Уведомляем пользователя об успешном изменении роли
            bot.answer_callback_query(call.id, f"Роль изменена на {new_role}.")
        else:
            bot.answer_callback_query(call.id, "Ошибка при обновлении данных.")
    except Exception as e:
        bot.answer_callback_query(call.id, "Ошибка при обработке запроса.")
        print(f"Ошибка: {e}")

# Поиск пользователя по имени и последним 4 цифрам номера
def find_user_by_name_and_last_digits(name, last_digits):
    try:
        user = Clients.get_row_for_work_name_number(name=name, phone_ending=last_digits)
        if not user:
            print("Пользователь не найден.")  # отладка
            return None
        # Возвращаем user_id, чтобы использовать его далее
        return {
            'user_id': user.user_id,
            'name': user.name,
            'role': user.role,
        }
    except Exception as e:
        print(f"Ошибка при поиске пользователя: {e}")
        return None

# Обновление роли пользователя
def update_user_role(user_id, new_role):
    try:
        print(f"Обновление роли пользователя с user_id={user_id} на {new_role}")  # отладка
        success = Clients.update_row(user_id, {'role': new_role})
        if not success:
            print(f"Не удалось обновить роль пользователя с user_id={user_id}")
        return success
    except Exception as e:
        print(f"Ошибка при обновлении роли: {e}")
        return False

# Обработчик навигации между страницами для заказов в доставке
@bot.callback_query_handler(func=lambda call: call.data.startswith("delivery_page_"))
def paginate_delivery_orders(call):
    user_id = call.message.chat.id
    message_id = call.message.message_id
    page = int(call.data.split("_")[2])

    # Получаем заказы пользователя
    orders = InDelivery.get_all_rows()
    user_orders = [order for order in orders if order.user_id == user_id]

    try:
        # Отправка обновления страницы
        send_delivery_order_page(user_id=user_id, message_id=message_id, orders=user_orders, page=page)
    except Exception as e:
        print(f"Ошибка при попытке пагинации заказов в доставке: {e}")
    finally:
        bot.answer_callback_query(call.id)  # Подтверждаем успешную обработку

def confirm_delivery():
    """
    Перемещает обработанные заказы в in_delivery.
    """
    try:
        # Получаем всех клиентов, ожидающих доставки
        for_delivery_rows = ForDelivery.get_all_rows()

        for row in for_delivery_rows:
            user_id = row.user_id

            # Получаем обработанные на тот момент заказы пользователя
            reservations = Reservations.get_row_by_user_id(user_id)
            fulfilled_orders = [r for r in reservations if r.is_fulfilled]

            # Перемещаем обработанные заказы в in_delivery
            for order in fulfilled_orders:
                InDelivery.insert(
                    user_id=row.user_id,
                    item_description="Товар",  # Заполнить описанием из Posts
                    quantity=order.quantity,
                    total_sum=row.total_sum,
                    delivery_address=row.address
                )

            # После перемещения можно удалить из for_delivery
            ForDelivery.delete_all_rows()

        print("Все обработанные заказы перемещены в in_delivery.")
    except Exception as e:
        raise Exception(f"Ошибка при подтверждении доставки: {e}")

# Перессылка забронированного товара в группу Брони Мега Скидки
@bot.message_handler(func=lambda message: message.text == "📦 Заказы клиентов")
def send_all_reserved_to_group(message):
    user_id = message.chat.id
    role = get_client_role(user_id)  # Получение роли клиента
    # Проверка ролей
    if role not in ["supreme_leader", "admin"]:
        bot.send_message(user_id, f"У вас нет прав доступа к этой функции. Ваша роль: {role}")
        return
    try:
        # Получение всех резерваций
        reservations = Reservations.get_row_all()
        if not reservations:
            bot.send_message(user_id, "Нет забронированных товаров для отправки.")
            return
        # Фильтруем необработанные резервации
        reservations_to_send = [r for r in reservations if not r.is_fulfilled]
        if not reservations_to_send:
            bot.send_message(user_id, "Все текущие товары уже были обработаны.")
            return

        # Сортируем резервации: сначала по дате поста (created_at из Posts), затем по user_id
        sorted_reservations = sorted(
            reservations_to_send,
            key=lambda r: (
                Posts.get_row(r.post_id).created_at if Posts.get_row(r.post_id) and Posts.get_row(
                    r.post_id).created_at else datetime.max,
                r.user_id
            )
        )

        # Группируем заказы по user_id и post_id, суммируя количество
        grouped_orders = defaultdict(lambda: {"quantity": 0, "reservations": []})
        for reservation in sorted_reservations:
            key = (reservation.user_id, reservation.post_id)
            grouped_orders[key]["quantity"] += reservation.quantity
            grouped_orders[key]["reservations"].append(reservation)

        # Обрабатываем и отправляем сгруппированные заказы
        for (user_id, post_id), group in grouped_orders.items():
            try:
                quantity = group["quantity"]
                reservations = group["reservations"]
                # Получение данных о посте
                post_data = Posts.get_row(post_id)
                if not post_data:
                    continue
                # Получение данных о клиенте
                client_data = Clients.get_row(user_id)
                if not client_data:
                    bot.send_message(
                        user_id, f"⚠️ Клиент с ID {user_id} не найден. Пропускаем.")
                    continue
                # Формируем описание заказа
                caption = (
                    f"💼 Новый заказ:\n\n"
                    f"👤 Клиент: {client_data.name or 'Имя не указано'}\n"
                    f"📞 Телефон: {client_data.phone or 'Телефон не указан'}\n"
                    f"💰 Цена: {post_data.price or 'Не указана'}₽\n"
                    f"📦 Описание: {post_data.description or 'Описание отсутствует'}\n"
                    f"📅 Дата: {post_data.created_at.strftime('%d.%m') if post_data.created_at else 'Дата отсутствует'}\n"
                    f"📦 Количество: {quantity}"
                )
                # Создаем кнопку
                markup = InlineKeyboardMarkup()
                mark_button = InlineKeyboardButton(
                    text=f"✅ Положил {quantity} шт.",
                    callback_data=f"mark_fulfilled_group_{user_id}_{post_id}",
                )
                markup.add(mark_button)
                # Отправляем сообщение
                if post_data.photo:
                    message = bot.send_photo(
                        chat_id=TARGET_GROUP_ID,
                        photo=post_data.photo,
                        caption=caption,
                        reply_markup=markup,
                    )
                else:
                    message = bot.send_message(
                        chat_id=TARGET_GROUP_ID, text=caption, reply_markup=markup
                    )
                # Сохраняем ID сообщения для последующего удаления
                with Session(bind=engine) as session:
                    post = session.query(Posts).filter_by(id=post_id).first()
                    if post:
                        post.telegram_message_id = message.message_id  # Сохраняем ID сообщения
                        session.commit()
                # Задержка секунда перед отправкой следующего поста
                time.sleep(4)
            except Exception as e:
                bot.send_message(
                    user_id, f"⚠️ Ошибка при обработке заказа: {e}")
                print(f"⚠️ Ошибка при обработке заказа: {e}")
    except Exception as global_error:
        bot.send_message(user_id, f"Произошла ошибка: {global_error}")
        print(f"❌ Глобальная ошибка в send_all_reserved_to_group: {global_error}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("mark_fulfilled_group_"))
def mark_fulfilled_group(call):
    user_id = call.from_user.id
    role = get_client_role(user_id)
    if role not in ["admin", "supreme_leader"]:
        bot.answer_callback_query(
            call.id, "У вас нет прав доступа к этой функции.", show_alert=True
        )
        return
    try:
        # Извлекаем данные из callback_data
        _, target_user_id, post_id = call.data.split("_")[2:]
        target_user_id = int(target_user_id)
        post_id = int(post_id)
        with Session(bind=engine) as session:
            # Получаем актуальные необработанные резервации пользователя для данного поста
            reservations = (
                session.query(Reservations)
                .filter_by(user_id=target_user_id, post_id=post_id, is_fulfilled=False)
                .all()
            )
            if not reservations:
                bot.answer_callback_query(
                    call.id, "Резервации уже обработаны или отменены пользователем.", show_alert=True
                )
                return

            # Суммируем актуальное количество резервированных товаров
            total_required_quantity = sum(reservation.quantity for reservation in reservations)
            if total_required_quantity == 0:
                bot.answer_callback_query(
                    call.id, "Все товары из этого заказа были отменены пользователем.", show_alert=True
                )
                return

            # Получаем данные поста
            post = session.query(Posts).filter_by(id=post_id).first()
            if not post:
                bot.answer_callback_query(call.id, "Пост не найден.", show_alert=True)
                return

            # Получаем информацию о клиенте
            client = session.query(Clients).filter_by(user_id=target_user_id).first()
            if not client:
                bot.answer_callback_query(call.id, "Клиент не найден.", show_alert=True)
                return

            # Добавление записи в таблицу Temp_Fulfilled
            new_record = Temp_Fulfilled(
                post_id=post_id,
                user_id=target_user_id,
                user_name=client.name,
                item_description=post.description,
                quantity=total_required_quantity,
                price=post.price * total_required_quantity,  # Цена за все товары
            )
            session.add(new_record)

            # Отмечаем все резервации как выполненные
            for reservation in reservations:
                reservation.is_fulfilled = True
                session.merge(reservation)
            session.commit()

            # Проверяем оставшиеся резервации для данного поста
            remaining_reservations = session.query(Reservations).filter_by(
                post_id=post_id, is_fulfilled=False
            ).count()

            # Обновляем текст сообщения
            user_full_name = call.from_user.first_name or "Администратор"
            updated_text = (
                f"{call.message.caption or call.message.text}\n\n"
                f"✅ Этот заказ теперь обработан.\n"
                f"👤 Кто положил: {user_full_name}\n"
                f"📦 Нужно положить: {total_required_quantity}"
            )

            # Обновляем сообщение в чате
            if call.message.photo:
                try:
                    bot.edit_message_caption(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        caption=updated_text,
                    )
                except Exception:
                    pass
            else:
                try:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=updated_text,
                    )
                except Exception:
                    pass

            # Если больше нет активных резерваций, удаляем сообщение из канала
            if remaining_reservations == 0:
                def delete_channel_message():
                    try:
                        bot.delete_message(chat_id=CHANNEL_ID, message_id=post.message_id)
                    except Exception:
                        pass

                # Удаляем сообщение из канала через 5 секунд
                threading.Timer(5.0, delete_channel_message).start()
                bot.answer_callback_query(
                    call.id,
                    "Сообщение обновлено! Оно удалится из канала через 5 секунд.",
                )
            else:
                bot.answer_callback_query(call.id, "Заказ успешно обработан!")
    except Exception as global_error:
        bot.answer_callback_query(call.id, f"Ошибка: {global_error}", show_alert=True)

# Хэндлер для очистки корзины
@bot.callback_query_handler(func=lambda call: call.data.startswith("clear_cart_"))
def clear_cart(call):
    # Получаем ID клиента из callback данных
    client_id = int(call.data.split("_")[2])

    # Используем get_row, чтобы получить user_id из таблицы clients
    client = Clients.get_row("clients", {"id": client_id})

    if not client:
        bot.send_message(call.message.chat.id, "Клиент не найден.")
        return

    user_id = client["user_id"]

    # Используем update_row для удаления всех заказов клиента в таблице reservations
    Reservations.update_row("reservations", {"user_id": user_id},
               {"deleted": True})  # Например, здесь устанавливается поле deleted в True

    bot.send_message(call.message.chat.id, "Корзина клиента успешно расформирована.")

# Проверка на регистрацию(стэйты статуса)
def is_registered(user_id):
    """
    Проверяет, зарегистрирован ли пользователь в таблице clients.
    Использует метод get_row для получения данных.
    """
    client = Clients.get_row(user_id=user_id)
    return client is not None
def set_user_state(user_id, state):
    user_states[user_id] = state
def get_user_state(chat_id):
    state = user_states.get(chat_id, None)

    return state
def clear_user_state(user_id):
    if user_id in user_states:  # user_states, вероятно, это где хранится состояние пользователей
        del user_states[user_id]

# Обработчик кнопки "⚙️ Клиенты"
@bot.message_handler(func=lambda message: message.text == "⚙️ Клиенты")
def manage_clients(message):
    user_id = message.chat.id
    role = get_client_role(message.chat.id)
    # Проверяем, является ли пользователь администратором
    if role not in ["admin","supreme_leader"]:
        bot.send_message(user_id, "У вас недостаточно прав.")
        return

    # Создаем клавиатуру с кнопками
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("🗑 Удалить клиента 📞", "🧺 Просмотреть корзину", "🚚 Управление доставкой","❌ Брак", "⬅️ Назад")
    bot.send_message(message.chat.id, "Выберите действие:", reply_markup=markup)

# Обработчик для кнопки "❌ Брак"
@bot.message_handler(func=lambda message: message.text == "❌ Брак")
def defective_order(message):
    # Устанавливаем состояние пользователя
    set_user_state(message.chat.id, "awaiting_last_digits_defective")
    bot.send_message(message.chat.id, "Введите последние 4 цифры номера телефона для поиска пользователя:")

# Поиск пользователя по последним 4 цифрам телефона
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "awaiting_last_digits_defective")
def search_user_for_defective(message):
    last_digits = message.text.strip()

    # Ищем пользователя в Clients
    users = Clients.get_row_by_phone_digits(last_digits)

    if users:  # Если список пользователей найден
        user = users[0]  # Берем первого пользователя или делаем выбор из нескольких
        user_id = user.user_id
        user_name = user.name
        user_phone = user.phone

        # Отправляем информацию о пользователе и подтверждение
        keyboard = create_defective_confirmation_keyboard()
        bot.send_message(
            message.chat.id,
            f"Найден пользователь:\nИмя: {user_name}\nТелефон: {user_phone}\nВы хотите продолжить обработку для данного пользователя?",
            reply_markup=keyboard
        )

        # Сохраняем user_id для дальнейшей обработки
        temp_user_data[message.chat.id] = {"user_id": user_id}
        set_user_state(message.chat.id, "awaiting_defective_action")
    else:
        bot.send_message(message.chat.id, "Пользователи с такими цифрами номера не найдены. Попробуйте еще раз.")

# Обработка действия (подтверждения или отмены)
@bot.callback_query_handler(func=lambda call: get_user_state(call.message.chat.id) == "awaiting_defective_action")
def handle_defective_action(call):
    if call.data == "confirm_defective":
        set_user_state(call.message.chat.id, "awaiting_defective_sum")
        bot.send_message(call.message.chat.id, "Введите сумму брака:")
    elif call.data == "cancel_defective":
        bot.send_message(call.message.chat.id, "Операция отменена. Возвращаю вас в главное меню.")
        clear_user_state(call.message.chat.id)
        go_back_to_menu(call.message)

# Ввод суммы брака
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "awaiting_defective_sum")
def handle_defective_sum_entry(message):
    try:
        defective_sum = int(message.text.strip())
        user_id = temp_user_data[message.chat.id]["user_id"]  # Берем найденный user_id

        # Получаем заказы пользователя из таблицы Reservations
        reservations = Reservations.get_row_by_user_id(user_id)

        if reservations:
            # Указание места, где будет добавлена сумма брака
            keyboard = create_select_reservation_keyboard(reservations)
            bot.send_message(
                message.chat.id,
                "Выберите заказ, чтобы добавить сумму брака:",
                reply_markup=keyboard
            )
            set_user_state(message.chat.id, "select_reservation_for_defective")
            temp_user_data[message.chat.id]["defective_sum"] = defective_sum
        else:
            bot.send_message(message.chat.id, "Заказы у данного пользователя не найдены. Попробуйте еще раз.")
            clear_user_state(message.chat.id)
            go_back_to_menu(message)
    except ValueError:
        bot.send_message(message.chat.id, "Некорректное значение. Введите числовую сумму.")

# Обработка выбора заказа для дефектного товара
@bot.callback_query_handler(func=lambda call: get_user_state(call.message.chat.id) == "select_reservation_for_defective")
def handle_reservation_selection(call):
    # Отвечаем на callback_query сразу
    bot.answer_callback_query(call.id, text="Ваш выбор обрабатывается...")

    reservation_id = int(call.data.split("_")[1])  # Получаем ID заказа из callback_data
    defective_sum = temp_user_data[call.message.chat.id]["defective_sum"]

    # Обновляем return_order в базе данных
    with Session(bind=engine) as session:
        reservation = session.query(Reservations).filter_by(id=reservation_id).first()
        if reservation:
            reservation.return_order += defective_sum
            session.commit()
            bot.send_message(call.message.chat.id, f"Сумма брака {defective_sum} успешно добавлена в заказ.")
        else:
            bot.send_message(call.message.chat.id, "Ошибка: Заказ не найден.")

    # Завершаем процесс
    clear_user_state(call.message.chat.id)
    go_back_to_menu(call.message)  # Передаем только сообщение

# Клавиатура для выбора конкретного заказа
def create_select_reservation_keyboard(reservations):
    keyboard = types.InlineKeyboardMarkup()
    for reservation in reservations:
        btn = types.InlineKeyboardButton(
            text=f"Заказ ID {reservation.id} (Возврат: {reservation.return_order})",
            callback_data=f"select_{reservation.id}"
        )
        keyboard.add(btn)
    return keyboard

# Уникальная клавиатура подтверждения
def create_defective_confirmation_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    btn_confirm = types.InlineKeyboardButton("Подтвердить ❌ Брак", callback_data="confirm_defective")
    btn_cancel = types.InlineKeyboardButton("Отмена ❌ Брак", callback_data="cancel_defective")
    keyboard.add(btn_confirm, btn_cancel)
    return keyboard

# Обработчик нажатия на кнопку "Просмотреть корзину"
@bot.message_handler(func=lambda message: message.text == "🧺 Просмотреть корзину")
def request_phone_last_digits(message):
    bot.send_message(
        message.chat.id,
        "Введите последние 4 цифры номера телефона клиента:",
    )
    set_user_state(message.chat.id, "AWAITING_PHONE_LAST_4")

# Хэндлер для кнопки Управление доставкой
@bot.message_handler(func=lambda message: message.text == "🚚 Управление доставкой")
def handle_delivery_management(message):
    # Создаем клавиатуру с кнопками
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("📤 Отправить рассылку","👨‍🦯 Засунуть в доставку","✅ Подтвердить доставку", "🗄 Архив доставки", "⬅️ Назад")
    bot.send_message(message.chat.id, "Выберите действие:", reply_markup=markup)

# Хэедлнр для поиска по последним 4 цифрам номера
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "AWAITING_PHONE_LAST_4")
def handle_phone_input(message):
    input_text = message.text.strip()

    # Проверяем, что введены последние 4 цифры номера телефона
    if not input_text.isdigit() or len(input_text) != 4:
        bot.send_message(
            message.chat.id,
            "Введите корректные последние 4 цифры номера телефона (4 цифры).",
        )
        return

    # Показ корзины по последним 4 цифрам номера телефона
    show_cart_by_last_phone_digits(message, input_text)

# Получаем всех клиентов с такими последними цифрами телефона
def show_cart_by_last_phone_digits(message, last_4_digits):
    clients = Clients.get_row_by_phone_digits(last_4_digits)

    if not clients:
        bot.send_message(
            message.chat.id,
            "Пользователи с такими последними цифрами номера телефона не найдены.",
        )
        clear_user_state(message.chat.id)
        return

    # Для каждого найденного клиента
    for client in clients:
        # Рассчитать общую сумму заказов и обработанных заказов
        total_orders = calculate_total_sum(client.user_id)
        processed_orders = calculate_processed_sum(client.user_id)

        # Отправить сообщение с общей информацией
        bot.send_message(
            message.chat.id,
            f"Пользователь: {client.name}\n"
            f"Общая сумма заказов: {total_orders} руб.\n"
            f"Общая сумма обработанных заказов: {processed_orders} руб."
        )

        # Получить содержимое корзины
        reservations = Reservations.get_row_by_user_id(client.user_id)

        if not reservations:
            # Если корзина пуста
            bot.send_message(
                message.chat.id, f"Корзина пользователя {client.name} пуста."
            )
        else:
            # Если корзина не пуста, отправляем её содержимое
            send_cart_content(message.chat.id, reservations, client.user_id)

    # Очистить состояние пользователя
    clear_user_state(message.chat.id)

# Отображает содержимое корзины и добавляет кнопку для расформирования обработанных товаров
def send_cart_content(chat_id, reservations, user_id):
    for reservation in reservations:
        post = Posts.get_row_by_id(reservation.post_id)

        if post:
            # Отправляем фото и информацию о товаре
            if post.photo:
                bot.send_photo(
                    chat_id,
                    photo=post.photo,
                    caption=(
                        f"Описание: {post.description}\n"
                        f"Количество: {reservation.quantity}\n"
                        f"Статус: {'Выполнено' if reservation.is_fulfilled else 'В ожидании'}"
                    ),
                )
            else:
                bot.send_message(
                    chat_id,
                    f"Описание: {post.description}\n"
                    f"Количество: {reservation.quantity}\n"
                    f"Статус: {'Выполнено' if reservation.is_fulfilled else 'В ожидании'}",
                )
        else:
            bot.send_message(chat_id, f"Товар с ID {reservation.post_id} не найден!")

    # Добавляем кнопку "Расформировать обработанные"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Расформировать обработанные", callback_data=f"clear_processed_{user_id}"))
    bot.send_message(chat_id, "Выберите действие:", reply_markup=markup)

# Callback для кнопки "Расформировать обработанные"
@bot.callback_query_handler(func=lambda call: call.data.startswith("clear_processed_"))
def handle_clear_processed(call):
    user_id = int(call.data.split("_")[2])  # Извлекаем ID пользователя из callback_data

    # Удаляем только обработанные товары пользователя
    cleared_items = clear_processed(user_id)

    if cleared_items > 0:
        bot.send_message(call.message.chat.id,
                         f"Все обработанные товары (количество: {cleared_items}) были удалены из корзины.")
    else:
        bot.send_message(call.message.chat.id, "У пользователя нет обработанных товаров для удаления.")

# Удаляет обработанные товары из корзины пользователя
def clear_processed(user_id):
    # Получаем содержимое корзины пользователя
    reservations = Reservations.get_row_by_user_id(user_id)

    # Фильтруем только выполненные (обработанные) товары
    processed_items = [item for item in reservations if item.is_fulfilled]

    # Удаляем обработанные товары из БД
    for item in processed_items:
        Reservations.delete_row(item.id)

    # Возвращаем количество удаленных товаров
    return len(processed_items)

# Callback для инлайн-кнопок "Просмотреть корзину"
@bot.callback_query_handler(func=lambda call: call.data.startswith("view_cart_"))
def callback_view_cart(call):
    client_id = int(call.data.split("_")[2])  # Извлекаем ID клиента из callback_data

    # Получаем данные клиента
    client = Clients.get_row(client_id)

    if not client:
        bot.send_message(call.message.chat.id, "Пользователь не найден.")
        return

    # Информируем, чью корзину будем смотреть
    bot.send_message(call.message.chat.id, f"Корзина пользователя: {client.name}")

    # Получаем содержимое корзины
    reservations = Reservations.get_row_by_user_id(client.user_id)

    if not reservations:
        bot.send_message(call.message.chat.id, "Корзина пользователя пуста.")
    else:
        send_cart_content(call.message.chat.id, reservations)

# Удаление клиента по номеру телефона
@bot.message_handler(func=lambda message: message.text == "🗑 Удалить клиента 📞")
def delete_client_by_phone(message):
    user_id = message.chat.id
    role = get_client_role(message.chat.id)
    # Проверяем, является ли пользователь администратором
    if role not in ["admin","supreme_leader"]:
        bot.send_message(user_id, "У вас недостаточно прав.")
        return
    bot.send_message(message.chat.id, "Введите номер телефона клиента для удаления:")
    set_user_state(message.chat.id, "DELETE_CLIENT_PHONE")

# Функция для удаления клиента по номеру телефона
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "DELETE_CLIENT_PHONE")
def process_delete_client_phone(message):
    user_id = message.chat.id
    role = get_client_role(user_id)

    # Проверяем права пользователя
    if role not in ["admin","supreme_leader"]:
        bot.send_message(user_id, "У вас недостаточно прав.")
        return

    phone = message.text.strip()  # Убираем лишние пробелы

    try:
        # Получаем клиента по номеру телефона
        client = Clients.get_row_by_phone(phone)

        if client:
            client_user_id = client.user_id  # Извлекаем user_id клиента

            # Проверяем, не выполняются ли действия с защищённым пользователем
            if client_user_id == protected_user_id:
                bot.send_message(
                    user_id, f"Клиент с номером телефона {phone} защищен от удаления."
                )
                return

            # Добавляем клиента в черный список (защищенный пользователь не будет добавлен)
            if client_user_id != protected_user_id:
                BlackList.insert(user_id=client_user_id, phone=phone)

            # Удаляем клиента из таблицы reservations
            # Используем SQLAlchemy напрямую или другую существующую логику для удаления
            with Session(bind=engine) as session:
                deleted_reservations_count = session.query(Reservations).filter(
                    Reservations.user_id == client_user_id
                ).delete()
                session.commit()

            # Удаляем клиента из таблицы clients
            Clients.delete_row(client.id)

            bot.send_message(
                user_id,
                f"Клиент с номером телефона {phone} успешно удалён. "
                f"Связанных записей в таблице reservations удалено: {deleted_reservations_count}.",
            )
        else:
            bot.send_message(user_id, f"Клиент с номером телефона {phone} не найден.")
    except Exception as e:
        # Сообщаем об ошибке
        bot.send_message(user_id, f"Произошла ошибка при удалении данных: {e}")
    finally:
        clear_user_state(user_id)

# Возможность установить клиенту статус рабочего
@bot.callback_query_handler(func=lambda call: call.data.startswith("set_worker_") or call.data.startswith("set_client_"))
def handle_set_role(call):
    client_id = int(call.data.split("_")[2])
    new_role = "worker" if "set_worker" in call.data else "client"

    # Получаем клиента по ID (используем get_row)
    client = Clients.get_row("clients", {"id": client_id})

    if not client:
        bot.answer_callback_query(call.id, f"Клиент с ID {client_id} не найден.")
        return

    # Обновляем роль клиента (используем update_row)
    update_result = Clients.update_row("clients", {"role": new_role}, {"id": client_id})

    if update_result:
        bot.answer_callback_query(call.id, f"Роль успешно изменена на {new_role}.")
        bot.send_message(
            call.message.chat.id,
            f"Роль пользователя с ID {client_id} обновлена на {new_role}.",
        )
    else:
        bot.answer_callback_query(call.id, "Не удалось обновить роль, попробуйте позже.")

# Проверка на админа
def is_admin(user_id):
    """Проверяет, является ли пользователь администратором."""
    role = get_client_role(user_id)  # Предполагается, что эта функция получает роль из Clients
    return role and "admin" in role  # Если роль хранится как строка или список

def is_leader(user_id):
    """Проверяет, является ли пользователь администратором."""
    role = get_client_role(user_id)  # Предполагается, что эта функция получает роль из Clients
    return role and "supreme_leader" in role  # Если роль хранится как строка или список

def is_audit(user_id):
    """Проверяет, является ли пользователь Аудитом"""
    role = get_client_role(user_id)
    return role and "audit" in role

# Новый пост
@bot.message_handler(func=lambda message: message.text == "➕ Новый пост")
def create_new_post(message):
    user_id = message.chat.id
    role = get_client_role(user_id)

    if role not in ["worker", "admin", "supreme_leader", "audit"]:
        bot.send_message(user_id, "У вас нет прав доступа к этой функции.")
        return

    bot.send_message(
        message.chat.id, "Пожалуйста, отправьте фотографию для вашего поста."
    )
    temp_post_data[message.chat.id] = {}
    set_user_state(message.chat.id, CreatingPost.CREATING_POST)

# Фото
@bot.message_handler(content_types=["photo"])
def handle_photo(message):
    user_id = message.chat.id
    role = get_client_role(user_id)
    state = get_user_state(message.chat.id)
    if role not in ["worker", "admin","supreme_leader", "audit"]:
        bot.send_message(
            user_id, "Если у вас возникли вопросы, задайте их в чате поддержки"
        )
        return
    if state == CreatingPost.CREATING_POST:
        temp_post_data[message.chat.id]["photo"] = message.photo[-1].file_id
        bot.send_message(message.chat.id, "Теперь введите цену на товар.")
    else:
        bot.send_message(message.chat.id, "Неправильная последовательность действий")

# Описание
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == CreatingPost.CREATING_POST)
def handle_post_details(message):
    chat_id = message.chat.id
    if "photo" in temp_post_data[chat_id] and "price" not in temp_post_data[chat_id]:
        if not message.text.isdigit():
            bot.send_message(
                chat_id, "Ошибка: Цена должна быть числом. Попробуйте снова."
            )
            return
        temp_post_data[chat_id]["price"] = message.text
        bot.send_message(chat_id, "Введите описание товара.")
    elif (
            "price" in temp_post_data[chat_id]
            and "description" not in temp_post_data[chat_id]
    ):
        # Поле "description" сохраняем без проверки, но заменяем "*" на "x"
        description = message.text.replace("*", "x")
        temp_post_data[chat_id]["description"] = description
        bot.send_message(chat_id, "Введите количество товара.")
    elif (
            "description" in temp_post_data[chat_id]
            and "quantity" not in temp_post_data[chat_id]
    ):
        if not message.text.isdigit():
            bot.send_message(
                chat_id, "Ошибка: Количество должно быть числом. Попробуйте снова."
            )
            return
        temp_post_data[chat_id]["quantity"] = int(message.text)

        # Сохраняем пост
        data = temp_post_data[chat_id]
        save_post(
            chat_id, data["photo"], data["price"], data["description"], data["quantity"]
        )
        bot.send_message(chat_id, "Ваш пост успешно создан!")

        # Очищаем состояние пользователя после завершения
        clear_user_state(chat_id)

# Управление постами
@bot.message_handler(func=lambda message: message.text == "📄 Посты")
def manage_posts(message):
    user_id = message.chat.id
    message_id = message.message_id  # ID самого запроса

    # Удаляем запрос пользователя сразу же
    try:
        bot.delete_message(chat_id=user_id, message_id=message_id)
    except Exception as e:
        print(f"Не удалось удалить сообщение-запрос пользователя {user_id}: {e}")

    role = get_client_role(user_id)

    # Проверяем, имеет ли пользователь соответствующую роль
    if role not in ["admin", "worker", "supreme_leader", "audit"]:
        bot.send_message(user_id, "У вас нет прав доступа к этой функции.")
        return

    # Убедимся, что user_last_message_id[user_id] - это список
    if user_id not in user_last_message_id:
        user_last_message_id[user_id] = []
    elif not isinstance(user_last_message_id[user_id], list):
        user_last_message_id[user_id] = [user_last_message_id[user_id]]

    # Удаляем предыдущие сообщения, если они есть
    for msg_id in user_last_message_id[user_id]:
        try:
            bot.delete_message(chat_id=user_id, message_id=msg_id)
        except Exception as e:
            print(f"Не удалось удалить сообщение {msg_id} для пользователя {user_id}: {e}")

    # Очищаем список сообщений пользователя после удаления
    user_last_message_id[user_id] = []

    try:
        # Получаем посты в зависимости от роли пользователя
        if role in ["admin", "supreme_leader"]:
            posts = Posts.get_all_posts()  # Используем метод класса для получения всех постов
        else:
            posts = Posts.get_user_posts(
                user_id)  # Используем метод класса для получения постов только текущего пользователя

    except Exception as e:
        error_msg = bot.send_message(user_id, f"Ошибка получения постов: {e}")
        user_last_message_id[user_id].append(error_msg.message_id)
        return

    if not posts:
        no_posts_msg = bot.send_message(user_id, "Нет доступных постов.")
        user_last_message_id[user_id].append(no_posts_msg.message_id)
        return

    # Выводим информацию о каждом посте
    for post in posts:
        post_id = post.id
        description = post.description
        price = post.price
        quantity = post.quantity
        photo = post.photo  # Если фото есть

        # Создаем клавиатуру для управления постом
        markup = InlineKeyboardMarkup()
        edit_btn = InlineKeyboardButton(
            "✏️ Изменить", callback_data=f"edit_post_{post_id}"
        )
        delete_btn = InlineKeyboardButton(
            "🗑 Удалить", callback_data=f"delete_post_{post_id}"
        )
        markup.add(edit_btn, delete_btn)

        # Отправляем сообщение с фото или текстом
        try:
            if photo:
                msg = bot.send_photo(
                    chat_id=user_id,
                    photo=photo,
                    caption=f"**Пост #{post_id}:**\n"
                            f"📍 *Описание:* {description}\n"
                            f"💰 *Цена:* {price} ₽\n"
                            f"📦 *Количество:* {quantity}",
                    parse_mode="Markdown",
                    reply_markup=markup,
                )
            else:
                msg = bot.send_message(
                    chat_id=user_id,
                    text=f"**Пост #{post_id}:**\n"
                         f"📍 *Описание:* {description}\n"
                         f"💰 *Цена:* {price} ₽\n"
                         f"📦 *Количество:* {quantity}",
                    parse_mode="Markdown",
                    reply_markup=markup,
                )
            # Сохраняем ID отправленных сообщений
            user_last_message_id[user_id].append(msg.message_id)
        except Exception as e:
            error_msg = bot.send_message(user_id, f"Ошибка при отправке поста #{post_id}: {e}")
            user_last_message_id[user_id].append(error_msg.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_post_"))
def edit_post(call):
    post_id = int(call.data.split("_")[2])  # Получаем ID поста из callback_data
    user_id = call.from_user.id

    # Проверяем права на редактирование
    role = get_client_role(user_id)
    if role not in ["admin", "worker", "supreme_leader", "audit"]:
        bot.answer_callback_query(
            callback_query_id=call.id,
            text="У вас нет прав доступа к этой функции.",
            show_alert=True,
        )
        return

    # Сохраняем временные данные о посте, который редактируется
    temp_post_data[user_id] = {"post_id": post_id}

    # Отправляем инлайн-клавиатуру с вариантами редактирования
    markup = InlineKeyboardMarkup()
    edit_price_btn = InlineKeyboardButton("💰 Цена", callback_data=f"edit_price_{post_id}")
    edit_description_btn = InlineKeyboardButton("📍 Описание", callback_data=f"edit_description_{post_id}")
    edit_quantity_btn = InlineKeyboardButton("📦 Количество", callback_data=f"edit_quantity_{post_id}")
    markup.add(edit_price_btn, edit_description_btn, edit_quantity_btn)

    # Обновляем сообщение или отправляем новое с клавиатурой
    if call.message.text:
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text="Что вы хотите поменять?",
            reply_markup=markup
        )
    else:
        msg = bot.send_message(
            chat_id=call.message.chat.id,
            text="Что вы хотите поменять?",
            reply_markup=markup
        )
        user_last_message_id.setdefault(user_id, []).append(msg.message_id)  # Сохраняем ID сообщения

# Обработчик кнопки "Редактировать цену"
@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_price_"))
def handle_edit_price(call):
    user_id = call.from_user.id
    post_id = int(call.data.split("_")[2])  # Получаем ID поста

    # Устанавливаем состояние пользователя
    set_user_state(user_id, CreatingPost.EDITING_POST_PRICE)
    temp_post_data[user_id] = {"post_id": post_id}

    # Просим пользователя ввести новую цену
    bot.send_message(user_id, "Введите новую цену для поста:")

# Обработчик кнопки "Редактировать описание"
@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_description_"))
def handle_edit_description(call):
    user_id = call.from_user.id
    post_id = int(call.data.split("_")[2])  # Получаем ID поста

    # Устанавливаем состояние пользователя
    set_user_state(user_id, CreatingPost.EDITING_POST_DESCRIPTION)
    temp_post_data[user_id] = {"post_id": post_id}

    # Просим ввести новое описание
    bot.send_message(user_id, "Введите новое описание для поста:")

# Обработчик кнопки "Редактировать количество"
@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_quantity_"))
def handle_edit_quantity(call):
    user_id = call.from_user.id
    post_id = int(call.data.split("_")[2])  # Получаем ID поста

    # Устанавливаем состояние пользователя
    set_user_state(user_id, CreatingPost.EDITING_POST_QUANTITY)
    temp_post_data[user_id] = {"post_id": post_id}

    # Просим ввести новое количество
    bot.send_message(user_id, "Введите новое количество товара:")

# Обработчик ввода новой цены
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == CreatingPost.EDITING_POST_PRICE)
def edit_post_price(message):
    user_id = message.chat.id
    post_id = temp_post_data[user_id]["post_id"]  # Получаем ID поста

    # Проверка, что введено число
    if not message.text.isdigit():
        bot.send_message(user_id, "Ошибка: Цена должна быть числом. Попробуйте снова.")
        return

    new_price = int(message.text)
    temp_post_data[user_id]["price"] = new_price

    try:
        post = Posts.get_row_by_id(post_id)  # Получаем старые данные поста
        success, msg = Posts.update_row(
            post_id=post_id,
            price=new_price,
            description=post.description,
            quantity=post.quantity
        )
        if success:
            bot.send_message(user_id, "Цена успешно обновлена!")
        else:
            bot.send_message(user_id, f"Ошибка обновления цены: {msg}")
    except Exception as e:
        bot.send_message(user_id, f"Ошибка обновления цены: {e}")
    finally:
        clear_user_state(user_id)  # Сбрасываем состояние пользователя

# Обработчик ввода нового описания
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == CreatingPost.EDITING_POST_DESCRIPTION)
def edit_post_description(message):
    user_id = message.chat.id
    post_id = temp_post_data[user_id]["post_id"]  # Получаем ID поста

    new_description = message.text  # Новое описание
    temp_post_data[user_id]["description"] = new_description

    try:
        post = Posts.get_row_by_id(post_id)  # Получаем старые данные поста
        success, msg = Posts.update_row(
            post_id=post_id,
            price=post.price,
            description=new_description,
            quantity=post.quantity
        )
        if success:
            bot.send_message(user_id, "Описание успешно обновлено!")
        else:
            bot.send_message(user_id, f"Ошибка обновления описания: {msg}")
    except Exception as e:
        bot.send_message(user_id, f"Ошибка обновления описания: {e}")
    finally:
        clear_user_state(user_id)  # Сбрасываем состояние пользователя

# Обработчик ввода нового количества
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == CreatingPost.EDITING_POST_QUANTITY)
def edit_post_quantity(message):
    user_id = message.chat.id
    post_id = temp_post_data[user_id]["post_id"]  # Получаем ID поста

    # Проверяем, что ввод является числом
    if not message.text.isdigit():
        bot.send_message(user_id, "Ошибка: Количество должно быть числом. Попробуйте снова.")
        return

    new_quantity = int(message.text)
    temp_post_data[user_id]["quantity"] = new_quantity

    try:
        post = Posts.get_row_by_id(post_id)  # Получаем старые данные
        success, msg = Posts.update_row(
            post_id=post_id,
            price=post.price,
            description=post.description,
            quantity=new_quantity
        )
        if success:
            bot.send_message(user_id, "Количество успешно обновлено!")
        else:
            bot.send_message(user_id, f"Ошибка обновления количества: {msg}")
    except Exception as e:
        bot.send_message(user_id, f"Ошибка обновления количества: {e}")
    finally:
        clear_user_state(user_id)  # Очистка состояния

@bot.callback_query_handler(func=lambda call: call.data.startswith("delete_post_"))
def delete_post_handler(call):
    post_id = int(call.data.split("_")[2])  # Извлечение ID поста
    try:
        # Удалить пост из базы данных (если успешно)
        result, msg = Posts.delete_row(post_id=post_id)
        if result:
            # Сообщаем о результате
            bot.answer_callback_query(call.id, "Пост успешно удалён.")

            # Удаляем сообщение бота с постом и кнопками
            bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)

            # Удаляем сообщение пользователя (с его запросом)
            bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)
        else:
            # Возникает ошибка при удалении поста
            bot.answer_callback_query(call.id, f"Ошибка: {msg}")
    except Exception as e:
        # Обработка исключений, если что-то пошло не так
        bot.answer_callback_query(call.id, f"Ошибка: {e}")

# Кнопка назад
@bot.message_handler(func=lambda message: message.text == "⬅️ Назад")
def go_back(message):
    try:
        # Проверяем роль пользователя и возвращаем соответствующее меню
        if is_admin(message.chat.id):
            markup = admin_main_menu()  # Получаем меню администратора
            bot.send_message(
                message.chat.id, "Возвращаюсь в главное меню администратора.", reply_markup=markup
            )
        elif is_leader(message.chat.id):
            markup = supreme_leader_main_menu()  # Получаем меню лидера
            bot.send_message(
                message.chat.id, "Возвращаюсь в главное меню лидера.", reply_markup=markup
            )
        elif is_audit(message.chat.id):
            markup = audit_main_menu()
            bot.send_message(
                message.chat.id,"Возвращаюсь в главное меню", reply_markup=markup
            )
        else:
            markup = client_main_menu()  # Получаем меню клиента
            bot.send_message(
                message.chat.id, "Возвращаюсь в главное меню.", reply_markup=markup
            )
    except Exception as e:
        # При возникновении исключения отправляем сообщение об ошибке
        print(f"Ошибка при обработке команды '⬅️ Назад': {e}")
        bot.send_message(
            message.chat.id, "Произошла ошибка. Пожалуйста, попробуйте снова позже."
        )

# Отправка в канал
@bot.message_handler(func=lambda message: message.text == "📢 Отправить посты в канал")
def send_new_posts_to_channel(message):
    user_id = message.chat.id
    role = get_client_role(user_id)

    # Проверяем, есть ли права на отправку постов
    if role not in ["admin","supreme_leader"]:
        bot.send_message(user_id, "У вас нет прав доступа к этой функции.")
        return

    # Получаем посты, которые ещё не были отправлены в канал
    posts = Posts.get_unsent_posts()

    if posts:
        for post in posts:
            post_id = post.id
            photo = post.photo
            price = post.price
            description = post.description
            quantity = post.quantity

            # Используем user_id из Posts, чтобы найти имя создателя поста в Clients
            creator_user_id = post.chat_id
            creator_name = Clients.get_name_by_user_id(creator_user_id) or "Неизвестный автор"

            # Формируем описание поста для канала
            caption = f"Цена: {price} ₽\nОписание: {description}\nОстаток: {quantity}"

            # Добавляем кнопки
            markup = InlineKeyboardMarkup()
            reserve_btn = InlineKeyboardButton(
                "🛒 Забронировать", callback_data=f"reserve_{post_id}"
            )
            to_bot_button = InlineKeyboardButton(
                "В бота", url=f"{bot_link}?start=start"
            )
            markup.add(reserve_btn, to_bot_button)



            # Отправка поста в канал
            sent_message = bot.send_photo(
                CHANNEL_ID, photo=photo, caption=caption, reply_markup=markup
            )

            # Формируем сообщение для группы
            group_caption = (
                f"Пост был создан пользователем: {creator_name}\n\n{caption}"
            )
            bot.send_photo(ARCHIVE, photo=photo, caption=group_caption)

            # Обновляем статус публикации
            Posts.mark_as_sent(post_id=post_id, message_id=sent_message.message_id)

            # Задержка секунда перед отправкой следующего поста
            time.sleep(4)

        bot.send_message(
            user_id,
            f"✅ Все новые посты ({len(posts)}) успешно отправлены в канал и группу.",
        )
    else:
        bot.send_message(user_id, "Нет новых постов для отправки.")

# Для регистрации чета
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == Registration.REGISTERING_NAME)
def register_name(message):
    user_id = message.chat.id
    temp_user_data[user_id]["name"] = message.text
    bot.send_message(user_id, "Введите ваш номер телефона:")
    set_user_state(user_id, Registration.REGISTERING_PHONE)

# Статистика
@bot.message_handler(commands=['statistic'])
def handle_statistic(message):
    from datetime import datetime, timedelta

    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    last_monday = monday - timedelta(days=7)
    last_sunday = monday - timedelta(days=1)

    days_range = {
        'today': (today.date(), today.date()),
        'week': (monday.date(), today.date()),
        'last_week': (last_monday.date(), last_sunday.date())
    }

    statistics = {"today": {}, "week": {}, "last_week": {}}
    total_posts = {"week": 0, "last_week": 0}

    # Получение данных из базы данных
    all_posts = Posts.get_row_all()  # Получаем все посты
    all_clients = Clients.get_row_all()  # Получаем всех клиентов

    # Преобразование клиентов в словарь {user_id: name}
    clients_dict = {}
    if not all_clients:
        clients_dict = {}
    elif isinstance(all_clients, dict):
        clients_dict = {key: value.get("name", "Неизвестный пользователь") for key, value in all_clients.items()}
    elif isinstance(all_clients, list):
        if all(isinstance(client, dict) for client in all_clients):
            clients_dict = {client["user_id"]: client.get("name", "Неизвестный пользователь") for client in all_clients}
        else:
            clients_dict = {client.user_id: client.name for client in all_clients}
    else:
        raise TypeError(f"Unsupported data type for 'all_clients': {type(all_clients)}")

    # Генерация статистики постов
    for key, date_range in days_range.items():
        for post in all_posts:
            created_at_date = post.created_at.date()
            created_at_time = post.created_at.time()

            # Исключаем посты с временем "00:00:00"
            if created_at_time == datetime.min.time():
                continue

            if date_range[0] <= created_at_date <= date_range[1]:
                creator_name = clients_dict.get(post.chat_id, "Неизвестный пользователь")
                if creator_name not in statistics[key]:
                    statistics[key][creator_name] = 0
                statistics[key][creator_name] += 1

                # Считаем общее количество постов за неделю и прошлую неделю
                if key == "week":
                    total_posts["week"] += 1
                elif key == "last_week":
                    total_posts["last_week"] += 1

    # Формирование текста ответа
    response = "📊 Статистика постов:\n"
    for period, names_data in statistics.items():
        if period == "today":
            period_label = "Сегодня"
        elif period == "week":
            period_label = "На этой неделе"
        elif period == "last_week":
            period_label = "На прошлой неделе"
        else:
            period_label = "Неизвестный период"

        response += f"\n{period_label}:\n"

        for name, count in names_data.items():
            response += f"  - {name}: {count} постов\n"

    # Добавляем общую статистику за неделю и прошлую неделю
    response += f"\nОбщее количество постов:\n"
    response += f"  - На этой неделе: {total_posts['week']} постов\n"
    response += f"  - На прошлой неделе: {total_posts['last_week']} постов\n"

    if not statistics["today"] and not statistics["week"] and not statistics["last_week"]:
        response = "Нет статистики по постам за выбранные периоды."

    bot.send_message(message.chat.id, response)

# Обработчик для кнопки 'Отправить рассылку'.
@bot.message_handler(func=lambda message: message.text == "📤 Отправить рассылку")
def send_broadcast(message):
    user_id = message.from_user.id
    bot.send_message(chat_id=user_id, text="Начинаю рассылку подходящим пользователям...")
    try:
        # Получаем список клиентов для рассылки
        eligible_users = calculate_for_delivery()
        print(f"Найдено пользователей для рассылки: {eligible_users}")  # Для отладки

        if eligible_users:
            for user in eligible_users:
                try:
                    send_delivery_offer(bot, user["user_id"], user["name"])
                    time.sleep(1)  # Задержка между запросами
                except Exception as e:
                    print(f"Ошибка отправки пользователю {user['user_id']}: {str(e)}")
        else:
            bot.send_message(chat_id=user_id, text="Подходящих пользователей для рассылки не найдено.")
    except Exception as e:
        bot.send_message(chat_id=user_id, text=f"Ошибка при выполнении рассылки: {str(e)}")

def merge_carts_by_phone(primary_user_id, secondary_user_id):
    # Найти все товары secondary_user_id
    secondary_reservations = Reservations.get_row_by_user_id(secondary_user_id)

    # Перенос товаров от secondary_user_id к primary_user_id
    for reservation in secondary_reservations:
        update_fields = {
            "user_id": primary_user_id
        }
        Reservations.update_row(reservation.id, update_fields)

    print(f"Объединены товары: {secondary_user_id} -> {primary_user_id}")

# Обрабатывает ответ пользователя на предложение доставки с инлайн-клавиатуры.
@bot.callback_query_handler(func=lambda call: call.data in ["yes", "no"])
def handle_delivery_response_callback(call):
    # Получаем данные пользователя
    user_id = call.from_user.id
    message_id = call.message.message_id  # ID сообщения с кнопками
    response = call.data  # Получаем "yes" или "no" из callback data

    # Проверяем текущее время
    current_time = datetime.now().time()  # Текущее локальное время

    if response == "yes" and current_time.hour >= 16:
        # Если нажато "Да" после 14:00 — удаляем сообщение с кнопками
        bot.delete_message(chat_id=user_id, message_id=message_id)
        # Отправляем сообщение об отказе
        bot.send_message(chat_id=user_id,
                         text="Извините, но лист на доставку уже сформирован. Ожидайте следующую отправку.")
    elif response == "yes":
        # Если согласие до 14:00, запрашиваем адрес
        bot.send_message(chat_id=user_id, text="Пожалуйста, укажите город, адрес и подъезд")
        # Сохраняем состояние пользователя для дальнейшего ввода адреса
        set_user_state(user_id, "WAITING_FOR_ADDRESS")
    elif response == "no":
        # Если отказ, удаляем сообщение с кнопками и уведомляем об ожидании следующей доставки
        bot.delete_message(chat_id=user_id, message_id=message_id)
        bot.send_message(chat_id=user_id, text="Вы отказались от доставки. Оповестим вас при следующей доставке.")

    # Уведомляем Telegram, что callback обработан
    bot.answer_callback_query(call.id)

# Обрабатывает ввод адреса пользователя.
@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "WAITING_FOR_ADDRESS")
def handle_address_input(message):
    user_id = message.chat.id
    address = message.text
    print(f"[INFO] Пользователь с ID {user_id} ввел адрес: {address}")
    # Проверяем наличие данных о пользователе
    user_data = Clients.get_row_by_user_id(user_id)
    if not user_data:
        print(f"[WARNING] Данные пользователя {user_id} не найдены в базе.")
        bot.send_message(chat_id=user_id, text="Ошибка! Данные пользователя отсутствуют.")
        return
    name = user_data.name
    phone = user_data.phone
    print(f"[DEBUG] Получены данные пользователя: Имя={name}, Телефон={phone}")
    # Вычисление суммы заказов пользователя
    user_orders_sum = calculate_sum_for_user(user_id)
    print(f"[DEBUG] Сумма заказов пользователя {user_id}: {user_orders_sum}")
    # Поиск всех пользователей с таким же телефоном
    from db import Session, engine
    with Session(bind=engine) as session:
        same_phone_users = session.query(Clients).filter(Clients.phone == phone).all()
    if not same_phone_users:
        print(f"[WARNING] Других пользователей с телефоном {phone} не найдено")
        bot.send_message(chat_id=user_id, text="Ошибка! Не удалось найти других заказов с данным номером телефона.")
        return
    # Подсчет общей суммы всех заказов
    total_sum_by_phone = user_orders_sum
    all_user_orders_details = []
    for client in same_phone_users:
        client_sum = calculate_sum_for_user(client.user_id)
        all_user_orders_details.append({
            "name": client.name,
            "orders_sum": client_sum
        })
        if client.user_id != user_id:
            total_sum_by_phone += client_sum
    print(f"[DEBUG] Общая сумма заказов для телефона {phone}: {total_sum_by_phone}")
    # Генерация текста для подтверждения
    orders_details_text = f"Ваши заказы: {user_orders_sum}\n"
    for detail in all_user_orders_details:
        if detail["name"] != name:
            orders_details_text += f"{detail['name']}: {detail['orders_sum']}\n"
    orders_details_text += f"Общая сумма: {total_sum_by_phone}"
    # Отправляем подтверждающее сообщение
    bot.send_message(
        chat_id=user_id,
        text=f"Ваши данные:\nИмя: {name}\nТелефон: {phone}\nАдрес: {address}\n\n{orders_details_text}\n\nПодтверждаете?",
        reply_markup=keyboard_for_confirmation()
    )
    # Сохраняем данные во временном хранилище
    temp_user_data[user_id] = {
        "name": name,
        "phone": phone,
        "final_sum": user_orders_sum,
        "total_sum_by_phone": total_sum_by_phone,
        "address": address
    }
    print(f"[INFO] Временные данные пользователя {user_id} сохранены")
    # Вставляем данные пользователя в таблицу for_delivery
    try:
        ForDelivery.insert(
            user_id=user_id,
            name=name,
            phone=phone,
            address=address,
            total_sum=total_sum_by_phone
        )
        print(f"[INFO] Данные пользователя {user_id} добавлены в таблицу for_delivery")
    except Exception as e:
        print(f"[ERROR] Ошибка при добавлении данных пользователя {user_id} в таблицу for_delivery: {str(e)}")
        bot.send_message(chat_id=user_id, text="Ошибка при добавлении данных в базу. Попробуйте позже.")
        return
    # Устанавливаем состояние
    set_user_state(user_id, "WAITING_FOR_CONFIRMATION")

# Обрабатывает подтверждение доставки пользователем.
@bot.callback_query_handler(func=lambda call: call.data in ["confirm_yes", "confirm_no"])
def handle_delivery_confirmation_response(call):
    # Получаем данные пользователя
    user_id = call.from_user.id
    message_id = call.message.message_id  # ID сообщения с кнопками
    response = call.data  # Получаем "confirm_yes" или "confirm_no"

    # Если пользователь подтвердил (нажал "Да")
    if response == "confirm_yes":
        # Проверяем наличие временных данных пользователя
        if user_id not in temp_user_data:
            print(f"[WARNING] Временные данные для пользователя {user_id} не найдены!")
            bot.send_message(chat_id=user_id, text="Произошла ошибка. Ваши данные не найдены. Попробуйте заново.")
            return

        # Извлекаем временно сохранённые данные
        user_data = temp_user_data[user_id]
        name = user_data.get("name", "Не указано")
        phone = user_data.get("phone", "Не указан")
        total_sum = user_data.get("total_sum_by_phone", 0)
        address = user_data.get("address", "Не указан")

        # Формируем текст сообщения для отправки в канал
        delivery_channel = -1002909781356  # Замените своим ID
        message_for_channel = (
            f"📦 **Новый заказ на доставку:**\n"
            f"👤 Имя: {name}\n"
            f"📞 Телефон: {phone}\n"
            f"💰 Общая сумма заказов: {total_sum}\n"
            f"📍 Адрес доставки: {address}"
        )

        try:
            # Отправляем сообщение в канал
            bot.send_message(
                chat_id=delivery_channel,
                text=message_for_channel,
                parse_mode="Markdown"
            )
            print(f"[INFO] Сообщение пользователя {user_id} успешно отправлено в канал")

            # Уведомляем пользователя об успешной отправке
            bot.send_message(chat_id=user_id, text="Ваш заказ отправлен в обработку. Спасибо!")

            # Очищаем временные данные
            del temp_user_data[user_id]

        except Exception as e:
            # Логируем и уведомляем о возможной ошибке
            print(f"[ERROR] Ошибка при отправке данных для пользователя {user_id} в канал: {e}")
            bot.send_message(
                chat_id=user_id,
                text="К сожалению, произошла ошибка при обработке вашего заказа. Попробуйте позже."
            )

    # Если пользователь не подтвердил (нажал "Нет")
    elif response == "confirm_no":
        bot.send_message(chat_id=user_id, text="Вы отказались от доставки. Мы оповестим вас о следующей возможности.")
        print(f"[INFO] Пользователь {user_id} отказался от доставки: нажал 'Нет'")

    # Удаляем кнопки подтверждения (само сообщение)
    bot.delete_message(chat_id=user_id, message_id=message_id)

    # Уведомляем Telegram, что callback обработан
    bot.answer_callback_query(call.id)

@bot.message_handler(commands=["empty_delivery"])
def handle_empty_delivery_command(message):
    user_id = message.chat.id
    print(f"[INFO] Пользователь с ID {user_id} вызвал команду /empty_delivery")

    # Проверяем наличие данных
    if user_id in temp_user_data:
        del temp_user_data[user_id]
        print(f"[INFO] Данные пользователя {user_id} успешно удалены из временного хранилища.")
        bot.send_message(chat_id=user_id, text="Ваши данные на доставку были удалены.")
    else:
        print(f"[WARNING] Данных для удаления у пользователя {user_id} не найдено.")
        bot.send_message(chat_id=user_id, text="Нет данных для удаления.")

# Рассчитывает общую сумму заказов для указанного пользователя.
def calculate_sum_for_user(user_id):
    with Session(bind=engine) as session:
        result = session.query(
            func.sum(Posts.price - Reservations.return_order).label("final_sum")
        ).join(
            Reservations, Posts.id == Reservations.post_id
        ).filter(
            Reservations.user_id == user_id, Reservations.is_fulfilled == True
        ).first()

        return result.final_sum if result.final_sum else 0

@bot.message_handler(func=lambda message: message.text == "👨‍🦯 Засунуть в доставку")
def push_in_delivery(message):
    # Шаг 1. Запрос списка номеров у пользователя
    msg = bot.send_message(message.chat.id, "Введите номера телефонов, каждый с новой строки:")
    bot.register_next_step_handler(msg, process_numbers)


def process_numbers(message):
    try:
        # Шаг 2. Извлечение списка номеров телефонов
        numbers = message.text.splitlines()
        phone_numbers = [num.strip() for num in numbers if num.strip()]

        if not phone_numbers:
            bot.send_message(message.chat.id, "Список номеров пуст. Попробуйте снова.")
            return

        # Шаг 3. Обработка номеров телефонов
        successful_deliveries = []

        for phone in phone_numbers:
            with Session(bind=engine) as session:
                # Найти клиента по номеру телефона
                client = session.query(Clients).filter(Clients.phone == phone).first()
                if not client:
                    bot.send_message(message.chat.id, f"Клиент с номером {phone} не найден.")
                    continue

                # Найти выполненные заказы клиента
                reservations = session.query(Reservations).filter(
                    Reservations.user_id == client.user_id,
                    Reservations.is_fulfilled == True
                ).all()

                if not reservations:
                    bot.send_message(message.chat.id, f"У клиента {phone} нет выполненных заказов.")
                    continue

                # Рассчитать `total_sum` как сумму (quantity * price) для каждого заказа
                total_sum = 0
                for reservation in reservations:
                    post = session.query(Posts).filter(Posts.id == reservation.post_id).first()
                    if post:
                        total_sum += reservation.quantity * post.price

                # Добавление данных в таблицу ForDelivery
                if total_sum > 0:
                    try:
                        ForDelivery.insert(
                            user_id=client.user_id,
                            name=client.name,
                            phone=phone,
                            address="",  # Оставляем поле address пустым
                            total_sum=total_sum  # Рассчитанная сумма
                        )
                        successful_deliveries.append(phone)
                    except Exception as e:
                        bot.send_message(message.chat.id, f"Ошибка при добавлении данных клиента {phone}: {str(e)}")
                else:
                    bot.send_message(message.chat.id, f"У клиента {phone} нет товаров для добавления в доставку.")

        # Шаг 4. Уведомление о результатах
        if successful_deliveries:
            bot.send_message(
                message.chat.id,
                f"Заказы для следующих номеров успешно добавлены в доставку: {', '.join(successful_deliveries)}"
            )
        else:
            bot.send_message(message.chat.id, "Не удалось добавить заказы в доставку.")
    except Exception as e:
        bot.send_message(message.chat.id, f"Произошла ошибка: {str(e)}")


@bot.message_handler(func=lambda message: message.text == "🗄 Архив доставки")
def archive_delivery_to_excel(message):
    """
    Формирует Excel-файл с архивом доставок из таблицы in_delivery,
    отправляет его в канал delivery_archive, и очищает таблицу.
    """
    # Получение всех данных из таблицы InDelivery
    delivery_rows = InDelivery.get_all_rows()

    # Проверка: если нет данных, завершить выполнение
    if not delivery_rows:
        bot.send_message(message.chat.id, "Нет данных для архивации.")
        return None

    # Создание Excel файла в памяти
    wb = Workbook()
    ws = wb.active
    ws.title = "Архив доставок"

    # Добавление заголовков таблицы
    ws.append(["Телефон", "Имя", "Сумма", "Адрес доставки", "Че за товар"])

    # Получение данных и заполнение строк
    for row in delivery_rows:
        # Получение информации о клиенте по user_id из таблицы Clients
        client_data = Clients.get_row_by_user_id(row.user_id)

        # Заполнение строки для таблицы
        ws.append([
            client_data.phone if client_data else "Неизвестно",
            client_data.name if client_data else "Неизвестно",
            row.price,
            row.delivery_address,
            row.item_description
        ])

    # Сохранение файла в памяти
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)  # Перемещение курсора в начало файла

    # Указание имени файла через InputFile
    file_name = f"Архив_доставок_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    document =  InputFile(output, file_name=file_name)

    # Отправка файла в канал delivery_archive
    bot.send_document(chat_id=delivery_archive, document=document)

    # Уведомление пользователя об отправке
    bot.send_message(message.chat.id, "Архив доставок отправлен в канал!")

    # Очистка таблицы in_delivery
    InDelivery.clear_table()

    # Уведомление об успешной очистке
    bot.send_message(message.chat.id, "Все записи из таблицы in_delivery удалены.")

@bot.callback_query_handler(func=lambda call: get_user_state(call.from_user.id) == "WAITING_FOR_CONFIRMATION")
def handle_confirmation(call):
    """
    Обработка подтверждения данных. Считывается телефон пользователя из базы данных,
    и выполняется подсчёт общей суммы всех клиентов, связанных с этим телефоном.
    """
    user_id = call.from_user.id
    confirmation = call.data  # "confirm_yes" или "confirm_no"

    if confirmation == "confirm_yes":
        # Получаем временные данные пользователя (новые данные)
        user_temp_data = temp_user_data.get(user_id)

        if not user_temp_data:
            bot.send_message(
                chat_id=user_id,
                text="Ошибка! Временные данные отсутствуют. Попробуйте снова."
            )
            set_user_state(user_id, None)
            return

        # Извлекаем данные из временного хранилища
        name = user_temp_data.get("name", "Не указано")
        new_phone = user_temp_data.get("phone", "Не указан")  # Новый телефон, введённый пользователем
        address = user_temp_data.get("address", "Не указан")
        final_sum = user_temp_data.get("final_sum", 0)  # Сумма текущего заказа


        from db import Session, engine, Clients, ForDelivery

        # Подключаемся к базе данных
        with Session(bind=engine) as session:
            try:
                # Ищем клиента в базе по user_id (получаем данные из таблицы Clients)
                client = session.query(Clients).filter(Clients.user_id == user_id).first()
                if not client:
                    print(f"[ERROR] Клиент с user_id={user_id} не найден в таблице Clients.")
                    bot.send_message(
                        chat_id=user_id,
                        text="Ошибка! Клиент не найден в базе данных. Попробуйте снова.",
                    )
                    return

                # Телефон клиента из базы данных (актуальный)
                current_phone_in_db = client.phone

                # Находим всех клиентов с этим номером телефона
                related_clients = session.query(Clients).filter(Clients.phone == current_phone_in_db).all()

                # Собираем данные о клиентах и их заказах
                total_sum_by_phone = final_sum  # Начинаем с текущей суммы заказа
                all_names = [name]

                if related_clients:
                    for related_client in related_clients:
                        # Для всех связанных клиентов (кроме текущего)
                        if related_client.user_id != user_id:
                            all_names.append(related_client.name)
                            order_sum = calculate_sum_for_user(related_client.user_id)
                            total_sum_by_phone += order_sum
                else:
                    print(f"[DEBUG] Связанных клиентов для телефона {current_phone_in_db} не найдено.")

                # Составляем строку с именами клиентов
                all_names_str = ", ".join(all_names)

            except Exception as e:
                print(f"[ERROR] Ошибка при работе с базой данных: {e}")
                bot.send_message(
                    chat_id=user_id,
                    text="Произошла ошибка при обработке данных. Попробуйте снова.",
                )
                return

        # Сохранение подтверждённых данных в таблицу ForDelivery
        with Session(bind=engine) as session:
            try:
                delivery_entry = ForDelivery(
                    user_id=user_id,
                    name=name,
                    phone=new_phone,  # Новый телефон
                    address=address,  # Новый адрес
                    total_sum=total_sum_by_phone  # Итоговая сумма заказов
                )
                session.add(delivery_entry)
                session.commit()
            except Exception as e:
                print(f"[ERROR] Ошибка при записи в ForDelivery: {e}")
                bot.send_message(
                    chat_id=user_id,
                    text="Произошла ошибка при сохранении данных. Попробуйте снова.",
                )
                return

        # Уведомляем пользователя о подтверждении данных
        bot.edit_message_text(
            chat_id=user_id,
            message_id=call.message.message_id,
            text=(
                f"Ваш заказ подтверждён и будет доставлен на указанный адрес:\n"
                f"Связанные клиенты: {all_names_str}\n"
                f"Общая сумма заказов: {total_sum_by_phone}\n"
                f"Адрес доставки: {address}"
            )
        )

        # Удаляем временные данные и сбрасываем состояние пользователя
        if user_id in temp_user_data:
            del temp_user_data[user_id]
        set_user_state(user_id, None)

    elif confirmation == "confirm_no":
        # Если пользователь отказался подтверждать данные
        bot.edit_message_text(
            chat_id=user_id,
            message_id=call.message.message_id,
            text="Вы хотите изменить данные? Выберите вариант ниже:",
            reply_markup=keyboard_for_editing()
        )
        set_user_state(user_id, "WAITING_FOR_DATA_EDIT")

    # Завершаем callback
    bot.answer_callback_query(call.id)

def keyboard_for_editing():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Изменить адрес", callback_data="edit_address"))
    keyboard.add(types.InlineKeyboardButton("Изменить номер телефона", callback_data="new_phone"))
    keyboard.add(types.InlineKeyboardButton("Отказаться от доставки", callback_data="delivery_otmena"))
    return keyboard

@bot.callback_query_handler(func=lambda call: call.data == "delivery_otmena")
def handle_delivery_otmena(call):
    try:
        # Удаляем сообщение рассылки
        bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)

        # Отправляем уведомление пользователю
        bot.send_message(chat_id=call.message.chat.id,
                         text="Вы отказались от доставки. Оповестим вас при следующей доставке.")

        # Отвечаем на Callback, чтобы Telegram понял, что она обработана
        bot.answer_callback_query(callback_query_id=call.id)
    except Exception as e:
        print(f"Ошибка при обработке: {e}")

@bot.callback_query_handler(func=lambda call: get_user_state(call.from_user.id) == "WAITING_FOR_DATA_EDIT")
def handle_data_editing(call):
    user_id = call.from_user.id
    action = call.data


    if action == "new_phone":
        set_user_state(user_id, "WAITING_FOR_NEW_PHONE")
        bot.edit_message_text(
            chat_id=user_id,
            message_id=call.message.message_id,
            text="Введите новый номер телефона:"
        )
    elif action == "edit_address":
        set_user_state(user_id, "WAITING_FOR_NEW_ADDRESS")
        bot.edit_message_text(
            chat_id=user_id,
            message_id=call.message.message_id,
            text="Введите новый адрес доставки:"
        )
    else:
        print(f"DEBUG ERROR: Неизвестное значение 'call.data': {action}' для пользователя ID={user_id}")

@bot.message_handler(func=lambda message: get_user_state(message.from_user.id) == "WAITING_FOR_NEW_ADDRESS")
def handle_new_address(message):
    """
    Обработка нового адреса от пользователя.
    """
    user_id = message.from_user.id
    new_address = message.text
    temp_user_data[user_id]["address"] = new_address  # Сохранение нового адреса

    # Получаем временные данные пользователя
    name = temp_user_data[user_id].get("name", "Не указано")
    phone = temp_user_data[user_id].get("phone", "Не указан")
    final_sum = temp_user_data[user_id].get("final_sum", 0)

    # Получаем всех клиентов с таким же номером телефона
    from db import Session, engine
    with Session(bind=engine) as session:
        same_phone_users = session.query(Clients).filter(Clients.phone == phone).all()

    # Считаем общую сумму заказов и собираем имена всех клиентов
    total_sum_by_phone = final_sum
    all_names = [name]  # Добавляем текущее имя
    for client in same_phone_users:
        if client.user_id != user_id:  # Пропускаем текущего клиента
            all_names.append(client.name)
            total_sum_by_phone += calculate_sum_for_user(client.user_id)

    # Формируем строку с именами всех клиентов
    all_names_str = ", ".join(all_names)

    # Отправляем обновлённое сообщение с данными
    bot.send_message(
        chat_id=user_id,
        text=(
            f"Данные обновлены:\n"
            f"Имя: {name}\nТелефон: {phone}\nНовый адрес: {new_address}\n"
            f"Имена заказчиков: {all_names_str}\n"
            f"Общая сумма заказов: {total_sum_by_phone}.\n\n"
            f"Подтверждаете изменения?"
        ),
        reply_markup=keyboard_for_confirmation()
    )
    set_user_state(user_id, "WAITING_FOR_CONFIRMATION")

@bot.message_handler(func=lambda message: get_user_state(message.from_user.id) == "WAITING_FOR_NEW_PHONE")
def handle_new_phone(message):
    """
    Обработка нового номера телефона пользователя.
    Должен учитывать информацию по старому номеру телефона и временно сохранять новый номер.
    """
    user_id = message.from_user.id
    new_phone = message.text.strip()  # Убираем лишние пробелы

    # Временные данные текущего пользователя
    name = temp_user_data[user_id].get("name", "Не указано")
    current_phone = temp_user_data[user_id].get("phone", "Не указан")  # Это старый номер телефона
    address = temp_user_data[user_id].get("address", "Не указан")
    final_sum = temp_user_data[user_id].get("final_sum", 0)


    # Подключаемся к базе данных, чтобы найти тех, у кого такой же старый номер телефона (current_phone)
    from db import Session, engine, Clients
    with Session(bind=engine) as session:
        try:
            # Найти всех клиентов с текущим (старым) номером телефона
            same_phone_users = session.query(Clients).filter(Clients.phone == current_phone).all()


        except Exception as e:
            print(f"[ERROR] Ошибка при запросе к базе: {e}")
            same_phone_users = []

    # Подсчитываем общую сумму всех заказов и собираем имена
    total_sum_by_phone = final_sum  # Начинаем с суммы текущего пользователя
    all_names = [name]  # Добавляем название текущего клиента
    for client in same_phone_users:
        if client.user_id != user_id:  # Избегаем дублирования текущего пользователя
            all_names.append(client.name)
            order_sum = calculate_sum_for_user(client.user_id)  # Посчитать сумму заказов клиента
            total_sum_by_phone += order_sum

    # Формируем строку с именами всех клиентов
    all_names_str = ", ".join(all_names)

    # Сохраняем новый номер временно
    temp_user_data[user_id]["phone"] = new_phone

    # Отправляем итоговое сообщение
    bot.send_message(
        chat_id=user_id,
        text=(
            f"Обновление данных:\n"
            f"Текущий номер (старый): {current_phone}\n"
            f"Новый номер: {new_phone}\n"
            f"Имя: {name}\nАдрес: {address}\n"
            f"Имена заказчиков с текущим номером: {all_names_str}\n"
            f"Общая сумма заказов: {total_sum_by_phone}.\n\n"
            f"Подтверждаете изменения?"
        ),
        reply_markup=keyboard_for_confirmation()
    )

    # Переводим пользователя в состояние ожидания подтверждения
    set_user_state(user_id, "WAITING_FOR_CONFIRMATION")

def keyboard_for_confirmation():
    print("[INFO] Генерация клавиатуры для подтверждения действия")
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Да", callback_data="confirm_yes"))
    keyboard.add(types.InlineKeyboardButton("Нет", callback_data="confirm_no"))
    return keyboard

# Обработчик подтверждения или отмены изменений
@bot.callback_query_handler(func=lambda call: get_user_state(call.from_user.id) == "WAITING_FOR_CONFIRMATION")
def handle_confirmation(call):
    """
    Обработка подтверждения данных. Телефон и другая информация извлекаются:
    - Старый телефон — только из таблицы Clients.
    - Новые данные (телефон, адрес) — из temp_user_data.
    """
    user_id = call.from_user.id
    confirmation = call.data  # "confirm_yes" или "confirm_no"

    if confirmation == "confirm_yes":
        # Получаем временные данные пользователя (новые данные)
        user_temp_data = temp_user_data.get(user_id)

        if not user_temp_data:
            bot.send_message(
                chat_id=user_id,
                text="Ошибка! Временные данные отсутствуют. Попробуйте снова."
            )
            set_user_state(user_id, None)
            return

        # Извлекаем новые данные из временного хранилища
        name = user_temp_data.get("name", "Не указано")
        phone = user_temp_data.get("phone", "Не указан")  # Новый телефон
        address = user_temp_data.get("address", "Не указан")
        final_sum = user_temp_data.get("final_sum", 0)  # Сумма текущего заказа


        from db import Session, engine, Clients, ForDelivery

        # Подключаемся к базе для извлечения старого телефона из Clients
        with Session(bind=engine) as session:
            try:
                # Ищем клиента в таблице Clients по user_id
                client = session.query(Clients).filter(Clients.user_id == user_id).first()
                if not client:
                    # Если клиент отсутствует в таблице Clients, сообщаем об ошибке
                    print(f"[ERROR] Клиент с user_id={user_id} не найден в таблице Clients.")
                    bot.send_message(
                        chat_id=user_id,
                        text="Ошибка! Клиент не найден в базе данных. Попробуйте снова.",
                    )
                    return

                # Старый телефон: извлекаем его из записи в Clients
                old_phone = client.phone
                print(f"[DEBUG] Старый телефон из базы Clients: {old_phone}")

                # Инициализируем общую сумму и список связанных клиентов
                total_sum_by_phone = final_sum
                all_names = [name]

                # Если новый телефон отличается от старого, ищем связанные записи
                if old_phone != phone:
                    print(f"[DEBUG] Телефон изменен. Ищем клиентов с телефоном {old_phone}...")
                    same_phone_users = session.query(Clients).filter(Clients.phone == old_phone).all()

                    if same_phone_users:
                        print(
                            f"[DEBUG] Найдены клиенты с телефоном {old_phone}: {[client.name for client in same_phone_users]}")

                        # Вычисляем общую сумму заказов всех связанных клиентов
                        for other_client in same_phone_users:
                            if other_client.user_id != user_id:  # Исключаем текущего клиента
                                all_names.append(other_client.name)
                                order_sum = calculate_sum_for_user(other_client.user_id)
                                total_sum_by_phone += order_sum
                    else:
                        print(f"[DEBUG] Клиенты с телефоном {old_phone} не найдены.")
                else:
                    print(f"[DEBUG] Телефон не изменялся. Сумма остается: {final_sum}")

                # Формируем список имен клиентов
                all_names_str = ", ".join(all_names)

            except Exception as e:
                print(f"[ERROR] Ошибка при работе с базой данных: {e}")
                bot.send_message(
                    chat_id=user_id,
                    text="Произошла ошибка при обработке данных. Попробуйте снова.",
                )
                return

        # Сохраняем новые данные в таблицу ForDelivery
        with Session(bind=engine) as session:
            try:
                delivery_entry = ForDelivery(
                    user_id=user_id,
                    name=name,
                    phone=phone,  # Сохраняем новый телефон
                    address=address,  # Сохраняем новый адрес
                    total_sum=total_sum_by_phone,  # Итоговая сумма
                )
                session.add(delivery_entry)
                session.commit()
            except Exception as e:
                print(f"[ERROR] Ошибка записи в ForDelivery: {e}")
                bot.send_message(
                    chat_id=user_id,
                    text="Произошла ошибка при сохранении данных. Попробуйте снова.",
                )
                return

        # Отправляем подтверждающее сообщение пользователю
        bot.edit_message_text(
            chat_id=user_id,
            message_id=call.message.message_id,
            text=(
                f"Ваш заказ подтвержден и будет доставлен на указанный адрес:\n"
                f"Связанные клиенты: {all_names_str}\n"
                f"Общая сумма заказов: {total_sum_by_phone}\n"
                f"Адрес доставки: {address}"
            )
        )

        # Удаляем временные данные и сбрасываем состояние пользователя
        if user_id in temp_user_data:
            del temp_user_data[user_id]
        set_user_state(user_id, None)

    elif confirmation == "confirm_no":
        # Пользователь отказался подтверждать данные
        bot.edit_message_text(
            chat_id=user_id,
            message_id=call.message.message_id,
            text="Вы хотите изменить данные? Выберите вариант ниже:",
            reply_markup=keyboard_for_editing()
        )
        set_user_state(user_id, "WAITING_FOR_DATA_EDIT")

    # Завершаем callback
    bot.answer_callback_query(call.id)

# Клавиатура для доставки да или нет
def keyboard_for_delivery():
    """
        Создает новую inline-клавиатуру с кнопками "Да" и "Нет".
        """
    keyboard = InlineKeyboardMarkup()  # Создаем разметку для клавиатуры
    yes_button = InlineKeyboardButton(text="Да", callback_data="yes")  # Кнопка "Да"
    no_button = InlineKeyboardButton(text="Нет", callback_data="no")  # Кнопка "Нет"
    keyboard.add(yes_button, no_button)  # Добавляем кнопки в клавиатуру
    return keyboard

def calculate_for_delivery():
    """
    Вычисляет общую сумму обработанных заказов клиентов, объединяет заказы для клиентов с одинаковым номером телефона.
    Сообщение отправляется одному клиенту с минимальным ID. Логи содержат индивидуальную сумму, суммы других клиентов, и итоговую сумму.
    """

    # Шаг 1: Подготовка данных (загрузка из таблиц)
    from db import Session, engine
    with Session(bind=engine) as session:
        all_clients = session.query(Clients).all()

    if not all_clients:
        print("[WARNING] Данные о клиентах не найдены!")
        return []

    with Session(bind=engine) as session:
        # Добавляем фильтр для обработанных заказов
        all_reservations = session.query(Reservations).filter(Reservations.is_fulfilled == True).all()

    if not all_reservations:
        print("[WARNING] Данные о заказах не найдены!")
        return []

    with Session(bind=engine) as session:
        all_posts = session.query(Posts).all()

    if not all_posts:
        print("[WARNING] Данные о постах не найдены!")
        return []

    # Преобразуем списки клиентов и постов в словари для быстрого доступа
    clients_dict = {client.user_id: client for client in all_clients}
    clients_by_phone = {}
    for client in all_clients:
        phone = getattr(client, "phone", None)
        if phone:
            if phone not in clients_by_phone:
                clients_by_phone[phone] = []
            clients_by_phone[phone].append(client)

    posts_dict = {post.id: post for post in all_posts}

    # Шаг 2: Группировка заказов по user_id
    grouped_totals = {}
    for reservation in all_reservations:  # Здесь all_reservations содержит только обработанные заказы
        try:
            user_id = reservation.user_id
            post_id = reservation.post_id
            quantity = reservation.quantity
            return_order = reservation.return_order

            # Проверка: существует ли пользователь с данным user_id
            if user_id not in clients_dict:
                print(f"[WARNING] Пропуск заказа: не найден пользователь с user_id={user_id}.")
                continue

            # Проверка: существует ли пост (товар) с данным post_id
            if post_id not in posts_dict:
                print(f"[WARNING] Пропуск заказа: не найден пост с post_id={post_id}.")
                continue

            # Вычисление стоимости заказа
            post = posts_dict[post_id]
            price = post.price
            total_amount = (price * quantity) - return_order

            if user_id not in grouped_totals:
                grouped_totals[user_id] = 0
            grouped_totals[user_id] += total_amount

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке заказа: {str(e)}")
            continue

    # Шаг 3: Группировка заказов по телефону
    summed_by_phone = {}
    details_by_phone = {}  # Для хранения данных по отдельной сумме каждого клиента
    for user_id, total in grouped_totals.items():
        client = clients_dict[user_id]
        phone = getattr(client, "phone", None)

        if phone:
            if phone not in summed_by_phone:
                summed_by_phone[phone] = 0
                details_by_phone[phone] = []

            summed_by_phone[phone] += total
            details_by_phone[phone].append({
                "user_id": user_id,
                "name": client.name,
                "individual_total": total
            })

    # Шаг 4: Выбор клиента с минимальным ID и вывод данных логов
    delivery_users = []
    threshold = 1999  # Пороговое значение для рассылки

    for phone, total_amount in summed_by_phone.items():
        # Найти всех клиентов с этим номером телефона
        clients = clients_by_phone.get(phone, [])

        # Найти клиента с минимальным id
        if clients:
            clients.sort(key=lambda c: c.id)  # Сортируем по ID
            selected_client = clients[0]

            # Добавляем выбранного клиента в рассылку, если сумма превышает порог
            if total_amount > threshold:
                delivery_users.append({
                    "user_id": getattr(selected_client, "user_id"),
                    "name": getattr(selected_client, "name"),
                    "total_amount": total_amount,
                })
            else:
                print(
                    f"[INFO] Клиент с телефоном {phone} не добавлен в рассылку. Общая сумма заказов={total_amount} ниже порога={threshold}.")

    return delivery_users

# Отправка рассылки
def send_delivery_offer(bot, user_id, user_name):
    try:
        bot.send_message(
            chat_id=user_id,
            text=f"{user_name}, готовы ли Вы принять ближайшую доставку(пн,ср,пт) с 10:00 до 16:00?",
            reply_markup=keyboard_for_delivery()  # Используем новую клавиатуру
        )
        print(f"Сообщение успешно отправлено {user_id}")
    except Exception as e:
        print(f"Ошибка при отправке сообщения {user_id}: {e}")

# Обработка ответа пользователя на предложение доставки.
def handle_delivery_response(bot, user_id, response):
    if response.lower() == "да":
        bot.send_message(chat_id=user_id, text="Пожалуйста, укажите город, адрес и подъезд")
        # Здесь нужно сохранить состояние пользователя, чтобы дальше запросить данные.
        set_user_state(user_id, "WAITING_FOR_ADDRESS")
    else:
        bot.send_message(
            chat_id=user_id, text="Оповестим вас при следующей доставке."
        )

@bot.message_handler(func=lambda message: message.text == "✅ Подтвердить доставку")
def confirm_delivery(message):
    try:
        with Session(bind=engine) as session:
            # Получаем все записи из ForDelivery
            for_delivery_rows = session.query(ForDelivery).all()
            if not for_delivery_rows:
                bot.send_message(
                    message.chat.id,
                    "❌ Список доставки пуст. Нет данных для обработки."
                )
                return

            for current_for_delivery in for_delivery_rows:
                # Получаем данные клиента
                client = session.query(Clients).filter(
                    Clients.user_id == current_for_delivery.user_id
                ).first()
                if not client:
                    # Если клиент не найден, пропускаем
                    continue

                # Получаем выполненные заказы клиента из Reservations
                reservations = session.query(Reservations).filter(
                    Reservations.user_id == current_for_delivery.user_id,
                    Reservations.is_fulfilled == True
                ).all()

                if not reservations:
                    continue  # Пропускаем клиентов без выполненных заказов

                # Обработка каждого выполненного заказа как отдельной строки
                for reservation in reservations:
                    # Получаем связанный пост, чтобы извлечь описание товара
                    post = session.query(Posts).filter(Posts.id == reservation.post_id).first()
                    if not post:
                        continue

                    # Создаём отдельную запись в InDelivery для каждого товара
                    new_delivery = InDelivery(
                        post_id=reservation.post_id,  # ID поста
                        user_id=current_for_delivery.user_id,  # ID пользователя из ForDelivery
                        user_name=client.name,  # Имя клиента из Clients
                        item_description=post.description,  # Описание товара из Posts
                        quantity=reservation.quantity,  # Количество товара
                        price=reservation.quantity * post.price,  # Итоговая сумма за товар
                        delivery_address=current_for_delivery.address,  # Адрес доставки
                    )
                    session.add(new_delivery)

                    # Обновляем статус in_delivery для всех товаров в Temp_Fulfilled
                    session.query(Temp_Fulfilled).filter(
                        Temp_Fulfilled.user_id == current_for_delivery.user_id,
                        Temp_Fulfilled.post_id == reservation.post_id
                    ).update({"in_delivery": True}, synchronize_session=False)

                    # Удаляем обработанный заказ из Reservations
                    session.delete(reservation)

            # Удаляем обработанные записи из ForDelivery
            session.query(ForDelivery).delete(synchronize_session=False)

            # Подтверждаем транзакцию
            session.commit()
            bot.send_message(
                message.chat.id,
                "✅ Все заказы успешно обработаны и перемещены в InDelivery. Каждое наименование товара записано отдельно. "
                "Статусы обновлены в Temp_Fulfilled. Записи удалены из ForDelivery."
            )
    except Exception as e:
        bot.send_message(
            message.chat.id,
            f"❌ Ошибка при подтверждении доставки: {str(e)}"
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_"))
def handle_edit_choice(call):
    print(f"Получено callback_data: {call.data}")  # Логирование данных

    try:
        data_parts = call.data.split("_")  # Разделяем строку
        if len(data_parts) == 2:  # Для команд без ID (например, "edit_address")
            action = data_parts[0]  # Действие (edit)
            target = data_parts[1]  # Цель (address)

            if action == "edit" and target == "address":
                # Переход в состояние изменения адреса
                set_user_state(call.from_user.id, "EDITING_ADDRESS")
                bot.send_message(chat_id=call.from_user.id, text="Введите новый адрес:")
            else:
                bot.send_message(chat_id=call.from_user.id, text="Неизвестная команда.")
        elif len(data_parts) == 3:  # Для команд с ID (например, "edit_post_123")
            action = data_parts[0]
            target = data_parts[1]
            post_id = int(data_parts[2])  # ID поста

            if action == "edit" and target == "post":
                bot.send_message(chat_id=call.from_user.id, text=f"Вы выбрали редактирование поста с ID {post_id}")
            else:
                bot.send_message(chat_id=call.from_user.id, text="Неизвестная команда.")
        else:
            raise ValueError("Неверный формат callback_data")

    except ValueError as e:
        bot.send_message(chat_id=call.from_user.id, text="Ошибка: Неверный формат команды.")
        print(f"Ошибка обработки команды: {e}")
    except Exception as e:
        bot.send_message(chat_id=call.from_user.id, text="Произошла ошибка при обработке вашего выбора.")
        print(f"Общая ошибка: {e}")

# Для ревизии
@bot.message_handler(func=lambda message: message.text == "Ревизия")
def audit_menu(message):
    # Создаем клавиатуру
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)

    # Добавляем кнопки
    btn_do_audit = types.KeyboardButton("Сделать ревизию")
    btn_send_audit = types.KeyboardButton("В будущих обновлениях...")
    btn_back = types.KeyboardButton("⬅️ Назад")

    # Добавляем кнопки на клавиатуру
    keyboard.add(btn_do_audit, btn_send_audit, btn_back)

    # Отправляем сообщение с клавиатурой
    bot.send_message(message.chat.id, "Выберите действие:", reply_markup=keyboard)

@bot.message_handler(func=lambda message: message.text == "Сделать ревизию")
def manage_audit_posts(message):
    posts = Posts.get_row_all()

    if not posts:
        bot.send_message(message.chat.id, "Нет постов для ревизии.")
        return

    # Уникальные даты по постам
    unique_dates = sorted(list(set(post.created_at.date() for post in posts)))

    if not unique_dates:
        bot.send_message(message.chat.id, "Нет доступных дат для ревизии.")
        return

    # Клавиатура для выбора даты
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for date in unique_dates[:2]:  # Показываем максимум 2 даты
        # Форматируем дату в виде: "число месяц"
        formatted_date = date.strftime("%d %B")
        keyboard.add(types.KeyboardButton(formatted_date))

    keyboard.add(types.KeyboardButton("⬅️ Назад"))
    bot.send_message(message.chat.id, "Выберите дату для ревизии:", reply_markup=keyboard)

    # Сохраняем даты в temp_user_data
    temp_user_data[message.chat.id] = {
        "unique_dates": [date.strftime("%d %B") for date in unique_dates]
    }

@bot.message_handler(
    func=lambda message: message.text in temp_user_data.get(message.chat.id, {}).get("unique_dates", []))
def show_posts_by_date(message):
    global active_audit

    selected_date = message.text

    # Пробуем преобразовать дату пользователя в формат YYYY-MM-DD
    try:
        current_year = datetime.now().year  # Текущий год
        selected_date_with_year = f"{selected_date} {current_year}"  # Добавляем год
        selected_date_parsed = datetime.strptime(selected_date_with_year, "%d %B %Y").date()
        selected_date = str(selected_date_parsed)  # Преобразуем в строку формата YYYY-MM-DD
    except ValueError:
        bot.send_message(message.chat.id, "Неправильный формат даты. Пожалуйста, введите дату в формате '24 марта'.")
        return

    today_date = datetime.now().date()  # Сегодняшняя дата

    # Получаем и сразу обрабатываем посты с quantity = 0
    zero_quantity_posts = [
        post for post in Posts.get_row_all()
        if post.quantity == 0 and str(post.created_at.date()) == selected_date
    ]

    for post in zero_quantity_posts:
        post.created_at = datetime.combine(today_date, datetime.min.time())  # Устанавливаем сегодняшнюю дату
        post.is_sent = True  # Отмечаем как отправленные
        # Явно обновляем поля через ORM
        Posts.update_row(
            post.id,  # Обновляем по ID поста
            created_at=post.created_at,
            is_sent=post.is_sent
        )

    # Получаем посты с выбранной датой и quantity > 0
    posts = [
        post for post in Posts.get_row_all()
        if str(post.created_at.date()) == selected_date and post.quantity > 0
    ]

    if not posts:
        bot.send_message(message.chat.id, f"Нет постов за дату {selected_date}.")
        return

    # Устанавливаем ревизию как активную для пользователя
    active_audit[message.chat.id] = True

    # Добавляем кнопку отмены ревизии
    cancel_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    cancel_button = types.KeyboardButton("Отменить ревизию")
    cancel_keyboard.add(cancel_button)
    bot.send_message(message.chat.id, "Начинаю ревизию... Для отмены нажмите 'Отменить ревизию'.",
                     reply_markup=cancel_keyboard)

    # Отправляем посты
    for post in posts:
        # Проверяем, не была ли ревизия отменена
        if not active_audit.get(message.chat.id):
            bot.send_message(message.chat.id, "Ревизия отменена.")
            break

        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(text="Изменить цену", callback_data=f"audit_edit_price_{post.id}"))
        keyboard.add(
            types.InlineKeyboardButton(text="Изменить описание", callback_data=f"audit_edit_description_{post.id}"))
        keyboard.add(
            types.InlineKeyboardButton(text="Изменить количество", callback_data=f"audit_edit_quantity_{post.id}"))
        keyboard.add(types.InlineKeyboardButton(text="Удалить", callback_data=f"audit_delete_post_{post.id}"))
        keyboard.add(types.InlineKeyboardButton(text="Подтвердить", callback_data=f"audit_confirm_post_{post.id}"))

        # Отправляем сообщение с постом
        bot_message = bot.send_photo(
            chat_id=message.chat.id,
            photo=post.photo,
            caption=(
                f"📄 Пост #{post.id}\n\n"
                f"Описание: {post.description}\n"
                f"Цена: {post.price}\n"
                f"Количество: {post.quantity}\n"
                f"Дата создания: {post.created_at.strftime('%Y-%m-%d %H:%M')}"
            ),
            reply_markup=keyboard,
        )

        # Сохраняем сообщение для последующего обновления
        temp_post_data[post.id] = {"message_id": bot_message.message_id, "chat_id": message.chat.id}

        time.sleep(5)

    # Отключаем ревизию после обработки всех постов
    active_audit[message.chat.id] = False
    bot.send_message(message.chat.id, "Ревизия завершена.", reply_markup=types.ReplyKeyboardRemove())

@bot.message_handler(func=lambda message: message.text == "Отменить ревизию")
def cancel_audit(message):
    global active_audit

    # Проверяем, активна ли ревизия
    if not active_audit.get(message.chat.id):
        bot.send_message(message.chat.id, "Нет активной ревизии для отмены.")
        return

    # Завершаем ревизию
    active_audit[message.chat.id] = False
    bot.send_message(message.chat.id, "Ревизия успешно отменена.", reply_markup=types.ReplyKeyboardRemove())

@bot.callback_query_handler(func=lambda call: call.data.startswith("audit_edit_price_"))
def handle_edit_price_for_audit(call):
    user_id = call.from_user.id
    post_id = int(call.data.split("_")[3])  # ID поста после audit_edit_price_

    # Сохраняем состояние пользователя
    set_user_state(user_id, "EDITING_AUDIT_PRICE")
    temp_post_data[user_id] = {"post_id": post_id}

    # Получаем ID сообщения, чтобы редактировать его
    message_data = temp_post_data.get(post_id)

    try:
        if message_data:
            # Обновляем текст сообщения перед отправкой ввода
            bot.edit_message_caption(
                chat_id=message_data["chat_id"],
                message_id=message_data["message_id"],
                caption="✍️ Введите новую цену для этого поста:"
            )
        else:
            bot.send_message(user_id, "Произошла ошибка. Сообщение не найдено.")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Ошибка редактирования сообщения: {e}")

@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "EDITING_AUDIT_PRICE")
def edit_post_price_for_audit(message):
    user_id = message.chat.id
    post_id = temp_post_data[user_id]["post_id"]

    if not message.text.isdigit():  # Проверка на корректность ввода цены
        bot.send_message(user_id, "⛔ Ошибка: Цена должна быть числом. Попробуйте снова.")
        return

    new_price = int(message.text)

    try:
        # Получение поста из базы данных
        post = Posts.get_row_by_id(post_id)
        if not post:
            bot.send_message(user_id, "Пост не найден.")
            return

        # Обновление данных в базе
        success, msg = Posts.update_row(
            post_id=post_id,
            price=new_price,
            description=post.description,
            quantity=post.quantity,
            is_sent=False,
            created_at=post.created_at
        )

        # Если обновление успешно
        if success:
            # Обновляем данные и создаём новую клавиатуру
            post = Posts.get_row_by_id(post_id)  # Получаем актуальные данные
            message_data = temp_post_data.get(post_id)

            # Создаём клавиатуру с кнопками
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton(text="Изменить цену", callback_data=f"audit_edit_price_{post.id}"))
            keyboard.add(
                types.InlineKeyboardButton(text="Изменить описание", callback_data=f"audit_edit_description_{post.id}"))
            keyboard.add(
                types.InlineKeyboardButton(text="Изменить количество", callback_data=f"audit_edit_quantity_{post.id}"))
            keyboard.add(types.InlineKeyboardButton(text="Удалить", callback_data=f"audit_delete_post_{post.id}"))
            keyboard.add(types.InlineKeyboardButton(text="Подтвердить", callback_data=f"audit_confirm_post_{post.id}"))

            # Редактируем сообщение с обновлёнными данными
            bot.edit_message_caption(
                chat_id=message_data["chat_id"],
                message_id=message_data["message_id"],
                caption=(
                    f"📄 Пост #{post.id}\n\n"
                    f"Описание: {post.description}\n"
                    f"Цена: {post.price} руб.\n"
                    f"Количество: {post.quantity}\n"
                    f"Дата создания: {post.created_at.strftime('%Y-%m-%d %H:%M')}"
                ),
                reply_markup=keyboard
            )

            # Сообщение пользователю об успехе
            bot.send_message(user_id, "✅ Цена успешно обновлена!")
        else:
            bot.send_message(user_id, f"⛔ Ошибка при обновлении цены: {msg}")
    except Exception as e:
        bot.send_message(user_id, f"⛔ Произошла ошибка: {e}")
    finally:
        # Сбрасываем состояние пользователя
        clear_user_state(user_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("audit_edit_description_"))
def handle_edit_description_for_audit(call):
    user_id = call.from_user.id
    post_id = int(call.data.split("_")[3])  # ID поста

    # Устанавливаем состояние пользователя
    set_user_state(user_id, "EDITING_AUDIT_DESCRIPTION")
    temp_post_data[user_id] = {"post_id": post_id}

    # Редактируем сообщение
    message_data = temp_post_data.get(post_id)
    try:
        if message_data:
            bot.edit_message_caption(
                chat_id=message_data["chat_id"],
                message_id=message_data["message_id"],
                caption="✍️ Введите новое описание для этого поста:"
            )
        else:
            bot.send_message(user_id, "Ошибка: Сообщение не найдено.")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Ошибка редактирования сообщения: {e}")

@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "EDITING_AUDIT_DESCRIPTION")
def edit_post_description_for_audit(message):
    user_id = message.chat.id
    post_id = temp_post_data[user_id]["post_id"]

    new_description = message.text

    try:
        # Получаем пост
        post = Posts.get_row_by_id(post_id)
        if not post:
            bot.send_message(user_id, "Пост не найден.")
            return

        # Обновление в базе данных
        success, msg = Posts.update_row(
            post_id=post.id,
            price=post.price,
            description=new_description,
            quantity=post.quantity,
            is_sent=False,
            created_at=post.created_at
        )

        if success:
            # Загружаем актуальные данные поста
            post = Posts.get_row_by_id(post_id)
            message_data = temp_post_data.get(post_id)

            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton(text="Изменить цену", callback_data=f"audit_edit_price_{post.id}"))
            keyboard.add(
                types.InlineKeyboardButton(text="Изменить описание", callback_data=f"audit_edit_description_{post.id}"))
            keyboard.add(
                types.InlineKeyboardButton(text="Изменить количество", callback_data=f"audit_edit_quantity_{post.id}"))
            keyboard.add(types.InlineKeyboardButton(text="Удалить", callback_data=f"audit_delete_post_{post.id}"))
            keyboard.add(types.InlineKeyboardButton(text="Подтвердить", callback_data=f"audit_confirm_post_{post.id}"))

            # Редактируем сообщение
            bot.edit_message_caption(
                chat_id=message_data["chat_id"],
                message_id=message_data["message_id"],
                caption=(
                    f"📄 Пост #{post.id}\n\n"
                    f"Описание: {post.description}\n"
                    f"Цена: {post.price}\n"
                    f"Количество: {post.quantity}\n"
                    f"Дата создания: {post.created_at.strftime('%Y-%m-%d %H:%M')}"
                ),
                reply_markup=keyboard
            )

            bot.send_message(user_id, "✅ Описание успешно обновлено!")
        else:
            bot.send_message(user_id, f"⛔ Ошибка обновления описания: {msg}")
    except Exception as e:
        bot.send_message(user_id, f"⛔ Произошла ошибка: {e}")
    finally:
        clear_user_state(user_id)  # Очистка состояния

@bot.callback_query_handler(func=lambda call: call.data.startswith("audit_edit_quantity_"))
def handle_edit_quantity_for_audit(call):
    user_id = call.from_user.id
    post_id = int(call.data.split("_")[3])  # ID поста

    # Устанавливаем состояние пользователя
    set_user_state(user_id, "EDITING_AUDIT_QUANTITY")
    temp_post_data[user_id] = {"post_id": post_id}

    # Редактируем сообщение
    message_data = temp_post_data.get(post_id)
    try:
        if message_data:
            bot.edit_message_caption(
                chat_id=message_data["chat_id"],
                message_id=message_data["message_id"],
                caption="✍️ Введите новое количество для этого поста:"
            )
        else:
            bot.send_message(user_id, "Ошибка: Сообщение не найдено.")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Ошибка редактирования сообщения: {e}")

@bot.message_handler(func=lambda message: get_user_state(message.chat.id) == "EDITING_AUDIT_QUANTITY")
def edit_post_quantity_for_audit(message):
    user_id = message.chat.id
    post_id = temp_post_data[user_id]["post_id"]

    if not message.text.isdigit():
        bot.send_message(user_id, "⛔ Ошибка: Количество должно быть числом.")
        return

    new_quantity = int(message.text)

    try:
        post = Posts.get_row_by_id(post_id)
        if not post:
            bot.send_message(user_id, "Пост не найден.")
            return

        # Обновляем запись
        success, msg = Posts.update_row(
            post_id=post.id,
            price=post.price,
            description=post.description,
            quantity=new_quantity,
            is_sent=False,
            created_at=post.created_at
        )

        if success:
            post = Posts.get_row_by_id(post_id)
            message_data = temp_post_data.get(post_id)

            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton(text="Изменить цену", callback_data=f"audit_edit_price_{post.id}"))
            keyboard.add(
                types.InlineKeyboardButton(text="Изменить описание", callback_data=f"audit_edit_description_{post.id}"))
            keyboard.add(
                types.InlineKeyboardButton(text="Изменить количество", callback_data=f"audit_edit_quantity_{post.id}"))
            keyboard.add(types.InlineKeyboardButton(text="Удалить", callback_data=f"audit_delete_post_{post.id}"))
            keyboard.add(types.InlineKeyboardButton(text="Подтвердить", callback_data=f"audit_confirm_post_{post.id}"))

            # Редактируем сообщение
            bot.edit_message_caption(
                chat_id=message_data["chat_id"],
                message_id=message_data["message_id"],
                caption=(
                    f"📄 Пост #{post.id}\n\n"
                    f"Описание: {post.description}\n"
                    f"Цена: {post.price}\n"
                    f"Количество: {post.quantity}\n"
                    f"Дата создания: {post.created_at.strftime('%Y-%m-%d %H:%M')}"
                ),
                reply_markup=keyboard
            )

            bot.send_message(user_id, "✅ Количество успешно обновлено!")
        else:
            bot.send_message(user_id, f"⛔ Ошибка обновления количества: {msg}")
    except Exception as e:
        bot.send_message(user_id, f"⛔ Произошла ошибка: {e}")
    finally:
        clear_user_state(user_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("audit_delete_post_"))
def delete_post_handler_for_audit(call):
    post_id = int(call.data.split("_")[3])  # ID поста

    try:
        # Удаляем запись из базы данных
        Posts.delete_row(post_id=post_id)

        # Удаляем сообщение из чата
        bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)
        bot.answer_callback_query(call.id, "✅ Пост успешно удалён.")
    except Exception as e:
        bot.answer_callback_query(call.id, f"⛔ Ошибка удаления поста: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("audit_confirm_post_"))
def confirm_post(call):
    post_id = int(call.data.split("_")[-1])  # Получаем ID поста
    user_chat_id = call.from_user.id  # ID пользователя, сделавшего ревизию

    try:
        # Получаем пост из базы данных
        post = Posts.get_row_by_id(post_id)
        if not post:
            bot.answer_callback_query(call.id, "⛔ Пост не найден.")
            return

        # Обновляем is_sent, дату и chat_id
        success, msg = Posts.update_row(
            post_id=post.id,
            price=post.price,
            description=post.description,
            quantity=post.quantity,
            is_sent=False,  # Устанавливаем is_sent = False
            created_at=datetime.now(),  # Устанавливаем текущую дату и время
            chat_id=user_chat_id  # Устанавливаем chat_id пользователя, сделавшего ревизию
        )

        if success:
            # Удаляем сообщение из хранилища и чата
            if post_id in temp_post_data:
                message_data = temp_post_data.pop(post_id, None)
                if message_data:
                    bot.delete_message(
                        chat_id=message_data["chat_id"],
                        message_id=message_data["message_id"]
                    )
            # Отправляем подтверждение пользователю
            bot.answer_callback_query(call.id, "✅ Пост подтверждён. Дата обновлена, ревизор сохранён.")
        else:
            bot.answer_callback_query(call.id, f"⛔ Ошибка при подтверждении поста: {msg}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"⛔ Ошибка подтверждения поста: {e}")

@bot.message_handler(func=lambda message: message.text == "😞 У меня брак")
def defect(message):
    user_id = message.chat.id

    with Session(bind=engine) as session:
        # Получаем записи из Temp_Fulfilled с необходимыми условиями
        user_items = session.query(Temp_Fulfilled).filter_by(
            user_id=user_id,
            in_delivery=True,
            defect=False,
            skidka=False
        ).all()

        if not user_items:
            bot.send_message(user_id, "У вас нет товаров, которые подходят для возврата по браку.")
            return

        # Создаем клавиатуру с выбором товара
        markup = InlineKeyboardMarkup()
        for item in user_items:
            button = InlineKeyboardButton(
                text=f"{item.item_description} (x{item.quantity})",
                callback_data=f"select_defective_{item.id}"  # ID товара из Temp_Fulfilled
            )
            markup.add(button)

        # Отправляем сообщение с выбором товара
        bot.send_message(
            user_id,
            "Выберите товар, который хотите вернуть по браку:",
            reply_markup=markup
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith("select_defective_"))
def select_defective_order(call):
    user_id = call.from_user.id
    item_id = int(call.data.split("_")[2])  # ID записи в Temp_Fulfilled

    # Сохраняем состояние, чтобы отследить следующий шаг (ввод причины)
    set_user_state(user_id, {"action": "defect_reason", "item_id": item_id})

    # Показываем кнопку для перехода к вводу причины
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📋 Указать причину", callback_data="enter_defect_reason"))

    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text="Нажмите на кнопку ниже, чтобы указать причину возврата.",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data == "enter_defect_reason")
def request_defect_reason(call):
    user_id = call.from_user.id
    state = get_user_state(user_id)

    if not state or state.get("action") != "defect_reason":
        bot.answer_callback_query(call.id, "Ошибка! Попробуйте снова.", show_alert=True)
        return

    bot.send_message(
        user_id,
        "Пожалуйста, опишите проблему с товаром. Фотография не нужна, только текст"
    )
    set_user_state(user_id, {"action": "wait_defect_reason", "item_id": state["item_id"]})


@bot.message_handler(
    func=lambda message: get_user_state(message.chat.id)
                         and get_user_state(message.chat.id).get("action") == "wait_defect_reason"
)
def handle_defect_reason(message):
    user_id = message.chat.id
    state = get_user_state(user_id)

    if not state or "item_id" not in state:
        bot.send_message(user_id, "Ошибка! Попробуйте снова.")
        return

    reason = message.text
    item_id = state["item_id"]

    with Session(bind=engine) as session:
        # Получаем запись о товаре
        item = session.query(Temp_Fulfilled).filter_by(id=item_id).first()
        if not item:
            bot.send_message(user_id, "Ошибка! Товар не найден.")
            return

        # Отправляем сообщение администратору
        admin_users = session.query(Clients).filter(Clients.role.in_(["admin", "supreme_leader"])).all()

        # Получаем фото товара из таблицы Posts
        post = session.query(Posts).filter_by(id=item.post_id).first()
        if not post:
            bot.send_message(
                user_id,
                "Не удалось найти данные о товаре. Попробуйте позже."
            )
            return

        # Получаем номер телефона клиента из таблицы Clients
        client = session.query(Clients).filter_by(user_id=item.user_id).first()
        if not client:
            bot.send_message(
                user_id,
                "Не удалось найти данные о вашем профиле. Попробуйте позже."
            )
            return

        for admin in admin_users:
            # Считаем, сколько времени прошло с момента покупки
            time_since_purchase = datetime.now() - item.created_at
            days_since_purchase = time_since_purchase.days

            # Формируем текст сообщения
            message_text = (
                f"⚠️ Заявка на возврат брака:\n\n"
                f"👤 **Клиент:** {item.user_name}\n"
                f"📞 **Номер телефона:** {client.phone or 'Не указан'}\n"
                f"📦 **Товар:** {post.description}\n"
                f"❌ **Причина:** {reason}\n"
                f"🕒 **Время с покупки:** {days_since_purchase} дней назад\n"
                f"💰 **Сумма:** {item.price}₽\n"
                f"📅 **Дата покупки:** {item.created_at.strftime('%d.%m.%Y')}"
            )

            # Создаем inline клавиатуру с кнопками
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("✅ Брак", callback_data=f"defect_{item.id}"),
                InlineKeyboardButton("💸 Скидка", callback_data=f"discount_{item.id}"),
                InlineKeyboardButton("📞 Связаться", callback_data=f"contact_{item.user_id}")
            )

            # Если есть фото товара, отправляем фото с текстом
            if post.photo:
                bot.send_photo(
                    admin.user_id,
                    photo=post.photo,  # Фото из таблицы Posts
                    caption=message_text,
                    reply_markup=markup,
                    parse_mode="Markdown"  # Используем Markdown для форматирования
                )
            else:
                # Если фото отсутствует, отправляем только текст
                bot.send_message(
                    admin.user_id,
                    message_text,
                    reply_markup=markup,
                    parse_mode="Markdown"
                )

    bot.send_message(user_id, "Ваш запрос отправлен администратору. Спасибо!")
    clear_user_state(user_id)

@bot.callback_query_handler(
    func=lambda call: call.data.startswith("defect_") or call.data.startswith("discount_") or call.data.startswith(
        "contact_"))
def handle_inline_buttons(call):
    user_id = call.from_user.id
    action, item_id = call.data.split("_")
    item_id = int(item_id)

    if action == "defect":
        handle_defect_action(call, item_id)
    elif action == "discount":
        request_discount_amount(call, item_id)
    elif action == "contact":
        contact_client(call, item_id)

def handle_defect_action(call, item_id):
    with Session(bind=engine) as session:
        # Находим запись в Temp_Fulfilled
        item = session.query(Temp_Fulfilled).filter_by(id=item_id).first()
        if not item:
            bot.send_message(call.message.chat.id, "Не удалось найти запись.")
            return

        # Находим соответствующую запись в Reservations и добавляем сумму в return_order
        reservation = session.query(Reservations).filter_by(id=item.post_id).first()
        if reservation:
            reservation.return_order = (reservation.return_order or 0) + item.price
            session.commit()

        # Ставим статус "defect = True" в Temp_Fulfilled
        item.defect = True
        session.commit()

        # Получаем user_id клиента через Clients
        client = session.query(Clients).filter_by(user_id=item.user_id).first()
        if client:
            bot.send_message(
                client.user_id,  # ID клиента
                f"Ваш возврат оформлен!\n\n🔔 Товар: {item.item_description}\n💰 Сумма возврата: {item.price}₽"
            )

    # Уведомляем администратора
    bot.send_message(call.message.chat.id, "Возврат оформлен")

def request_discount_amount(call, item_id):
    # Сохраняем состояние для администратора
    set_user_state(call.from_user.id, {"action": "discount_request", "item_id": item_id, "admin_id": call.from_user.id})

    bot.send_message(
        call.message.chat.id,
        "Введите желаемую сумму скидки для клиента:"
    )

@bot.message_handler(
    func=lambda message: (state := get_user_state(message.chat.id)) and state.get("action") == "discount_request")
def handle_discount_amount(message):
    admin_id = message.chat.id  # ID администратора, который предложил скидку
    state = get_user_state(admin_id)

    if not state:
        bot.send_message(admin_id, "Произошла ошибка. Попробуйте ещё раз.")
        return

    try:
        discount_amount = int(message.text)
        if discount_amount <= 0:
            raise ValueError
    except ValueError:
        bot.send_message(admin_id, "Введите корректную сумму скидки (положительное число).")
        return

    # Получаем ID товара
    item_id = state["item_id"]

    with Session(bind=engine) as session:
        # Получаем информацию о товаре
        item = session.query(Temp_Fulfilled).filter_by(id=item_id).first()
        if not item:
            bot.send_message(admin_id, "Ошибка! Товар не найден.")
            return

        # Получаем данные клиента
        client = session.query(Clients).filter_by(user_id=item.user_id).first()
        if not client:
            bot.send_message(admin_id, "Ошибка! Не удалось найти клиента.")
            return

        # Сохраняем состояние для клиента
        set_user_state(
            client.user_id,
            {"action": "confirm_discount", "item_id": item_id, "discount_amount": discount_amount, "admin_id": admin_id}
        )

        # Уведомляем клиента о скидке
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("Да", callback_data=f"confirm_discount_{item_id}"),
            InlineKeyboardButton("Отказаться", callback_data=f"return_discount_{item_id}")
        )

        bot.send_message(
            client.user_id,
            f"Вам поступило предложение о скидке по вашему товару:\n\n"
            f"📦 Товар: {item.item_description}\n"
            f"💰 Размер скидки: {discount_amount}₽\n\n"
            f"Вы согласны на данную скидку?",
            reply_markup=markup
        )

    # Подтверждаем администратору
    bot.send_message(
        admin_id,
        f"Скидка в размере {discount_amount}₽ отправлена клиенту на подтверждение."
    )

@bot.callback_query_handler(
    func=lambda call: call.data.startswith("confirm_discount_") or call.data.startswith("return_discount_")
)
def handle_discount_confirmation(call):
    user_id = call.from_user.id
    try:
        action, item_id = call.data.rsplit("_", 1)  # Разделяем строку с конца
        item_id = int(item_id)  # Преобразуем item_id в число
    except ValueError:
        bot.answer_callback_query(call.id, "Ошибка: некорректные данные.")
        return

    state = get_user_state(user_id)
    if not state or state.get("item_id") != item_id:
        bot.answer_callback_query(call.id, "Ошибка! Товар не найден.")
        return

    discount_amount = state.get("discount_amount")
    admin_id = state.get("admin_id")  # Получаем ID администратора

    with Session(bind=engine) as session:
        # Получаем информацию о товаре
        item = session.query(Temp_Fulfilled).filter_by(id=item_id).first()
        if not item:
            bot.answer_callback_query(call.id, "Ошибка! Запись о товаре не найдена.")
            return

        if action == "confirm_discount":
            # Клиент согласен на скидку: Обновляем данные в базе
            item.skidka_price = discount_amount
            item.skidka = True
            session.commit()

            # Уведомляем клиента
            bot.answer_callback_query(call.id, "Скидка успешно активирована.")
            bot.send_message(
                call.message.chat.id,
                f"Скидка в размере {discount_amount}₽ успешно активирована! Спасибо за ваше решение!"
            )

            # Уведомляем администратора
            if admin_id:
                admin_message = (
                    f"Клиент согласился на скидку для товара:\n\n"
                    f"📦 Товар: {item.item_description}\n"
                    f"💰 Сумма скидки: {discount_amount}₽"
                )
                bot.send_message(admin_id, admin_message)

        elif action == "return_discount":
            # Клиент отказался от скидки: Отмечаем товар как "на возврат" и уведомляем
            item.defect = True  # Помечаем товар как "на возврат"
            session.commit()

            # Уведомляем клиента
            bot.answer_callback_query(call.id, "Хорошо, оформлен возврат.")
            bot.send_message(
                call.message.chat.id,
                "Хорошо, оформлен возврат. При следующей доставке товар будет возвращён."
            )

            # Уведомляем администратора
            if admin_id:
                admin_message = (
                    f"Клиент отказался от скидки, и товар был отмечен на возврат:\n\n"
                    f"📦 Товар: {item.item_description}\n"
                    f"💰 Предлагавшаяся скидка: {discount_amount}₽"
                )
                bot.send_message(admin_id, admin_message)

    clear_user_state(user_id)

def contact_client(call, user_id):
    with Session(bind=engine) as session:
        # Получаем данные клиента
        client = session.query(Clients).filter_by(user_id=user_id).first()
        if not client:
            bot.send_message(call.message.chat.id, "Не удалось найти данные клиента.")
            return

        # Отправляем ссылку на чат с клиентом администратору
        bot.send_message(
            call.from_user.id,
            f"[Нажмите, чтобы начать чат с клиентом](tg://user?id={client.user_id})",
            parse_mode="Markdown"  # Используем Markdown для создания кликабельной ссылки
        )



# Запуск бота
if __name__ == "__main__":
    bot.polling(none_stop=True)
