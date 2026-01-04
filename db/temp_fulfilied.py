import datetime
from sqlalchemy import String, BIGINT, Boolean, DateTime, Integer, func
from sqlalchemy.exc import SQLAlchemyError
from .db import AbstractModel
from sqlalchemy.orm import mapped_column, Session


class Temp_Fulfilled(AbstractModel):
    __tablename__ = "temp_fulfilled"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id = mapped_column(Integer, nullable=False)
    user_id = mapped_column(BIGINT, nullable=False)
    user_name = mapped_column(String, nullable=False)
    item_description = mapped_column(String, nullable=False)
    quantity = mapped_column(Integer, nullable=False)
    in_delivery = mapped_column(Boolean, nullable=False, default=0)
    price = mapped_column(Integer, nullable=False)
    defect = mapped_column(Boolean, nullable=False, default=0)
    skidka = mapped_column(Boolean, nullable=False, default=0)
    skidka_price = mapped_column(Integer, nullable=False, default=0)
    created_at = mapped_column(DateTime, default=datetime.datetime.utcnow)

    @staticmethod
    def insert(session: Session, post_id: int, user_id: int, user_name: str,
               item_description: str, quantity: int, price: int) -> bool:
        """Добавляет запись в таблицу Temp_Fulfilied."""
        try:
            new_record = Temp_Fulfilled(
                post_id=post_id,
                user_id=user_id,
                user_name=user_name,
                item_description=item_description,
                quantity=quantity,
                price=price
            )
            session.add(new_record)
            session.commit()
            return True
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Ошибка при добавлении записи: {e}")
            return False

    @staticmethod
    def get_row(session: Session, record_id: int):
        """Возвращает запись по ID."""
        try:
            record = session.query(Temp_Fulfilled).filter(Temp_Fulfilled.id == record_id).first()
            return record
        except SQLAlchemyError as e:
            print(f"Ошибка при получении записи: {e}")
            return None

    @staticmethod
    def get_row_all(session: Session):
        """Возвращает все записи из таблицы."""
        try:
            records = session.query(Temp_Fulfilled).all()
            return records
        except SQLAlchemyError as e:
            print(f"Ошибка при получении записей: {e}")
            return None

    @staticmethod
    def delete_row(session: Session, record_id: int) -> bool:
        """Удаляет запись по ID."""
        try:
            record = session.query(Temp_Fulfilled).filter(Temp_Fulfilled.id == record_id).first()
            if record:
                session.delete(record)
                session.commit()
                return True
            else:
                print("Запись не найдена")
                return False
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Ошибка при удалении записи: {e}")
            return False

    @staticmethod
    def update_row(session: Session, record_id: int, **kwargs) -> bool:
        """Обновляет запись по ID."""
        try:
            record = session.query(Temp_Fulfilled).filter(Temp_Fulfilled.id == record_id).first()
            if record:
                for key, value in kwargs.items():
                    if hasattr(record, key):
                        setattr(record, key, value)
                session.commit()
                return True
            else:
                print("Запись не найдена")
                return False
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Ошибка при обновлении записи: {e}")
            return False

    @staticmethod
    def cleanup_old_records(session: Session) -> int:
        """Удаляет записи, которые старше 7 дней."""
        try:
            limit_date = datetime.datetime.utcnow() - datetime.timedelta(days=7)
            old_records = session.query(Temp_Fulfilled).filter(Temp_Fulfilled.created_at < limit_date).all()

            deleted_count = len(old_records)
            for record in old_records:
                session.delete(record)
            session.commit()
            return deleted_count
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Ошибка при удалении старых записей: {e}")
            return 0
