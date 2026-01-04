from sqlalchemy import (
    BIGINT,
    Boolean,
    Integer
)
from sqlalchemy.orm import mapped_column, Session

from . import Posts
# from main import user_data
from .db import AbstractModel, engine

class Reservations(AbstractModel):
    __tablename__ = "reservations"
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id = mapped_column(BIGINT, nullable=False)
    quantity = mapped_column(Integer, nullable=False)
    post_id = mapped_column(Integer, nullable=False)
    is_fulfilled = mapped_column(Boolean, nullable=False)
    return_order = mapped_column(Integer, default=0)
    old_price = mapped_column(Integer, nullable=False)

    @staticmethod
    def insert(user_id: int, quantity: int, post_id: int, is_fulfilled: bool):
        with Session(bind=engine) as session:
            reservations = Reservations(
            user_id=user_id,
            quantity=quantity,
            post_id=post_id,
            is_fulfilled=is_fulfilled,
            )
            session.add(reservations)
            session.commit()

    @staticmethod
    def get_row_by_user_id(user_id: int):
        with Session(bind=engine) as session:
            query = session.query(Reservations).filter(Reservations.user_id == user_id).all()
            return query

    @staticmethod
    def update_row(post_id: int, price: int, description: str, quantity: int):
        with Session(bind=engine) as session:
            post = session.query(Posts).filter(Posts.id == post_id).first()
            if not post:
                return False, "Пост не найден."

            # Обновляем данные
            post.price = price
            post.description = description
            post.quantity = quantity
            session.commit()
            return True, "Данные успешно обновлены."

    @staticmethod
    def get_row_by_id(reservation_id):
        with Session(bind=engine) as session:
            return session.query(Reservations).filter_by(id=reservation_id).first()

    @staticmethod
    def cancel_order_by_id(reservation_id: int):
        with Session(bind=engine) as session:
            order = session.query(Reservations).filter(Reservations.id == reservation_id).first()
            if order:
                session.delete(order)
                session.commit()
                return True
            return False

    @staticmethod
    def get_row_all(user_id=None):
        """
        Получить все бронирования. Если указан user_id — фильтровать по user_id.
        """
        from db.db import Session, engine
        with Session(bind=engine) as session:
            query = session.query(Reservations)
            if user_id is not None:
                query = query.filter(Reservations.user_id == user_id)
            return query.all()

    @staticmethod
    def delete_row(reservation_id: int):
        """Удаляет строку из таблицы reservations по id."""
        with Session(bind=engine) as session:
            try:
                # Ищем запись по reservation_id
                reservation = session.query(Reservations).filter_by(id=reservation_id).first()

                # Если запись найдена - удаляем
                if reservation:
                    session.delete(reservation)
                    session.commit()
                    return True  # Запись успешно удалена
                else:
                    return False  # Запись с таким id не найдена
            except Exception as e:
                session.rollback()  # В случае ошибки откатываем изменения
                raise Exception(f"Ошибка при удалении строки с id {reservation_id}: {e}")
