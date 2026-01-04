import os
from datetime import datetime, timedelta

from sqlalchemy import String, BIGINT, Boolean, DateTime, Integer, func
from sqlalchemy.orm import mapped_column, Session

from .db import AbstractModel, engine

class Posts(AbstractModel):
    __tablename__ = "posts"
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id = mapped_column(BIGINT, nullable=False)
    photo = mapped_column(String, nullable=False)
    price = mapped_column(Integer, nullable=False)
    description = mapped_column(String, nullable=False)
    message_id = mapped_column(BIGINT, nullable=True)
    quantity = mapped_column(Integer, nullable=False)
    is_sent = mapped_column(Boolean, nullable=False, default=0)
    created_at = mapped_column(DateTime, nullable=False, default=func.now())

    @staticmethod
    def insert(chat_id: int, photo: str, price: str, description: str, quantity: int):
        with Session(bind=engine) as session:
            posts = Posts(
                chat_id=chat_id,
                photo=photo,
                price=price,
                description=description,
                quantity=quantity
            )
            session.add(posts)
            session.commit()

    @staticmethod
    def get_row(post_id: int):
        with Session(bind=engine) as session:
            query = session.query(Posts).filter(Posts.id == post_id).first()
            return query

    @staticmethod
    def delete_row(post_id: int):
        with Session(bind=engine) as session:
            query = session.query(Posts).filter(Posts.id == post_id).first()
            if query is None:
                return False, "Пост не найден."  # Возвращаем False, если пост не найден

            try:
                if query.photo:
                    photo_path = query.photo
                    if os.path.exists(photo_path):
                        os.remove(photo_path)
            except Exception as e:
                return False, f"Ошибка при удалении фотографии: {str(e)}"

            try:
                session.delete(query)
                session.commit()
                return True, "Пост и связанные данные успешно удалены."  # Возвращаем True, если всё успешно удалено
            except Exception as e:
                return False, f"Ошибка при удалении поста: {str(e)}"

    @staticmethod
    def update_row(post_id: int, price: int = None, description: str = None, quantity: int = None,
                   is_sent: bool = None, created_at: datetime = None, chat_id: int = None):
        with Session(bind=engine) as session:
            post = session.query(Posts).filter(Posts.id == post_id).first()
            if not post:
                return False, "Пост не найден"

            # Обновляем только те значения, которые переданы
            if price is not None:
                post.price = price
            if description is not None:
                post.description = description
            if quantity is not None:
                post.quantity = quantity
            if is_sent is not None:
                post.is_sent = is_sent
            if created_at is not None:
                post.created_at = created_at
            if chat_id is not None:  # Добавлено обновление chat_id
                post.chat_id = chat_id

            session.commit()
            return True, "Данные успешно обновлены"


    @staticmethod
    def get_unsent_posts():
        with Session(bind=engine) as session:
            return session.query(Posts).filter(Posts.is_sent == False).all()

    @staticmethod
    def mark_as_sent(post_id, message_id):
        with Session(bind=engine) as session:
            post = session.query(Posts).filter(Posts.id == post_id).first()
            if post:
                post.is_sent = True
                post.message_id = message_id
                session.commit()

    @classmethod
    def increment_quantity_by_id(cls, post_id):
        with Session(bind=engine) as session:
            post = session.query(Posts).filter(Posts.id == post_id).first()
            if post:
                post.quantity += 1
                session.commit()

    @staticmethod
    def get_row_by_id(post_id: int):
        with Session(bind=engine) as session:
            query = session.query(Posts).filter(Posts.id == post_id).first()
            return query


    @staticmethod
    def get_posts_in_last_week(chat_id: int):
        """Получение постов за последние 7 дней."""
        now = datetime.utcnow()  # Текущее время
        last_week = now - timedelta(days=7)  # Временной интервал: последние 7 дней

        with Session(bind=engine) as session:
            posts = session.query(Posts).filter(
                Posts.chat_id == chat_id,
                Posts.created_at >= last_week
            ).all()

        return posts

    @staticmethod
    def get_row_all():
        with Session(bind=engine) as session:
            query = session.query(Posts).all()
            return query


    @staticmethod
    def get_all_posts():
            """Возвращает все посты, которые ещё не были отправлены на канал."""
            # Фильтруем из всех постов те, у которых is_sent равно False
            posts = Posts.get_row_all()  # Получение всех постов
            return [post for post in posts if not post.is_sent]

    @staticmethod
    def get_user_posts(user_id):
            """Возвращает посты, созданные конкретным пользователем, которые ещё не отправлены."""
            posts = Posts.get_row_all()  # Получаем все посты
            # Фильтруем по user_id и только те посты, которые ещё не отправлены
            return [post for post in posts if post.chat_id == user_id and not post.is_sent]

    @staticmethod
    def clone_post(post_id, **kwargs):
        original_post = Posts.get_row_by_id(post_id)

        if not original_post:
            print(f"❌ Ошибка: Оригинальный пост с ID {post_id} не найден.")
            return None, "Оригинальный пост не найден."

        try:
            print(f"Копируем пост ID={post_id} с параметрами: {kwargs}")

            # Создаём новый пост
            new_post_id = Posts.insert(
                chat_id=kwargs.get("chat_id", original_post.chat_id),
                photo=kwargs.get("photo", original_post.photo),
                price=kwargs.get("price", original_post.price),
                description=kwargs.get("description", original_post.description),
                quantity=kwargs.get("quantity", original_post.quantity)
            )

            if not new_post_id:
                print(f"❌ Ошибка при создании поста: insert вернул None.")
                return None, "Ошибка при сохранении нового поста."

            print(f"✅ Новый пост создан с ID: {new_post_id}")
            return new_post_id, None

        except Exception as e:
            print(f"❌ Ошибка при создании нового поста: {e}")
            return None, f"Ошибка: {e}"
