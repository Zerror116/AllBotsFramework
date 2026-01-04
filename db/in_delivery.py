import datetime
from ast import Bytes

from sqlalchemy import (
    String,
    ForeignKey,
    BIGINT,
    Boolean,
    DateTime,
    or_, Integer,
    BLOB
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import mapped_column, Session, Mapped, MappedColumn

from . import Posts
# from main import user_data
from .db import AbstractModel, engine

class InDelivery(AbstractModel):
    __tablename__ = "in_delivery"
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id = mapped_column(Integer, nullable=False)
    user_id = mapped_column(BIGINT, nullable=False)
    user_name = mapped_column(String, nullable=False)
    item_description = mapped_column(String, nullable=False)
    quantity = mapped_column(Integer, nullable=False)
    price = mapped_column(Integer, nullable=False)
    delivery_address = mapped_column(String, nullable=False)
    data = mapped_column(DateTime, nullable=False, default=datetime.datetime.now())

    @staticmethod
    def insert(post_id, user_id, user_name, item_description, quantity, price, delivery_address):
        """
        Добавляет запись в таблицу in_delivery.
        """
        with Session(bind=engine) as session:
            try:
                new_entry = InDelivery(
                    post_id=post_id,
                    user_id=user_id,
                    user_name=user_name,
                    item_description=item_description,
                    quantity=quantity,
                    price=price,
                    delivery_address=delivery_address
                )
                session.add(new_entry)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

    @staticmethod
    def get_all_rows():
        """
        Возвращает все записи из in_delivery.
        """
        with Session(bind=engine) as session:
            try:
                return session.query(InDelivery).all()
            except Exception as e:
                session.rollback()
                raise e

    @staticmethod
    def clear_table():
        """
        Очищает таблицу in_delivery.
        """
        with Session(bind=engine) as session:
            try:
                session.query(InDelivery).delete()
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
