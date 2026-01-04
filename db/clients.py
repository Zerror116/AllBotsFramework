from sqlalchemy import (
    String,
    BIGINT,
    Integer
)
from sqlalchemy.orm import mapped_column, Session

# from main import user_data
from .db import AbstractModel, engine

class Clients(AbstractModel):
    __tablename__ = "clients"
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id = mapped_column(BIGINT, nullable=False)
    name = mapped_column(String, nullable=False)
    phone = mapped_column(String, nullable=False)
    role = mapped_column(String, nullable=False, default="client")
    # msg_ids если траблы с кешем можешь через бд пойти с удаление сообщ

    @staticmethod
    def insert(user_id: int, name: str, phone: str, role: str):
        with Session(bind=engine) as session:
            try:
                client = Clients(user_id=user_id, name=name, phone=phone, role=role)
                session.add(client)
                session.commit()
            except Exception:
                raise

    @staticmethod
    def get_row_all():
        """
        Получить всех клиентов из базы данных.
        """
        from db.db import Session, engine
        with Session(bind=engine) as session:
            query = session.query(Clients)
            return query.all()

    @staticmethod
    def get_row(user_id):
        """
        Возвращает клиента по user_id или None, если записи нет.
        """
        with Session(bind=engine) as session:
            result = session.query(Clients).filter(Clients.user_id == user_id).first()

            # Логируем результат проверки
            return result

    @staticmethod
    def get_row_by_phone(phone):
        """
        Возвращает клиента по номеру телефона или None, если записи нет.
        """
        with Session(bind=engine) as session:
            result = session.query(Clients).filter(Clients.phone == phone).first()  # Поиск клиента в базе
            return result

    @staticmethod
    def get_row_by_user_id(user_id):
        from db.db import Session, engine
        with Session(bind=engine) as session:
            return session.query(Clients).filter(Clients.user_id == user_id).first()

    @staticmethod
    def delete_row(client_id: int):
        with Session(bind=engine) as session:
            query = session.query(Clients).filter(Clients.id == client_id).first()
            if query is None:
                print(f"Клиент с ID {client_id} не найден для удаления.")
                return False
            print(f"Клиент {query.phone} ({query.name}) успешно удален.")
            session.delete(query)
            session.commit()

    @staticmethod
    def update_row(user_id: int, name: str, phone: str, role: str):
        with Session(bind=engine) as session:
            query = session.query(Clients).filter(Clients.user_id == user_id).first()
            if query is None:
                return False, "Пользователь не найден."

            # Обновляем данные
            query.name = name
            query.phone = phone
            query.role = role
            try:
                session.commit()  # Сохраняем изменения
                print(f"[update_row] Обновлены данные для user_id={user_id}.")
            except Exception as e:
                print(f"[update_row] Ошибка при обновлении: {e}")
                session.rollback()
                raise
            return True, "Данные пользователя обновлены успешно."

    @staticmethod
    def get_row_by_phone_digits(phone_digits):
        """Получение всех пользователей с совпадающими последними цифрами номера."""
        with Session(bind=engine) as session:
            query = session.query(Clients).filter(
                Clients.phone.like(f"%{phone_digits}")
            ).all()
            return query  # Возвращаем список объектов

    @staticmethod
    def get_name_by_user_id(user_id):
        client = Clients.get_row_by_user_id(user_id)
        if client:
            return client.name
        return None

    @staticmethod
    def get_row_for_work_name_number(name: str, phone_ending: str):
        """
        Поиск пользователя по имени и последним цифрам номера телефона.
        """
        with Session(bind=engine) as session:
            query = session.query(Clients).filter(
                Clients.name == name,
                Clients.phone.like(f"%{phone_ending}")
            ).first()
            return query

    @staticmethod
    def update_row_for_work(user_id, updates):
        try:
            # Пример обновления через SQLAlchemy
            with Session(bind=engine) as session:
                session.query(Clients).filter(Clients.user_id == user_id).update(updates)
                session.commit()  # Убедитесь, что изменения фиксируются
            return True
        except Exception as e:
            print(f"Error in update_row_for_work: {e}")
            session.rollback()  # Отменяем изменения при возникновении ошибки
            return False
