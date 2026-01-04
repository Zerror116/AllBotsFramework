from db.clients import Clients
from db import engine, Session, Posts


# Установка роли клиента
def set_client_role(user_id: int, role: str = "client"):
    """Назначение клиенту роли."""
    try:
        with Session(bind=engine) as session:
            client = session.query(Clients).filter(Clients.user_id == user_id).first()
            if not client:
                print("Клиент не найден.")
                return
            client.role = role
            session.add(client)
            session.commit()
            print(f"Роль '{role}' успешно назначена клиенту с ID {user_id}.")
    except Exception as e:
        print(f"Ошибка при назначении роли: {e}")



# Получение роли клиента
def get_client_role(user_id: int) -> str:
    """Возвращает роль клиента по его user_id."""
    try:
        with Session(bind=engine) as session:
            client = session.query(Clients).filter(Clients.user_id == user_id).first()
            if client:
                return client.role  # Возвращаем роль клиента
            return "unknown"  # По умолчанию, если клиент не найден
    except Exception as e:
        print(f"Ошибка при получении роли клиента: {e}")
        return "error"  # Возвращаем статус ошибки в случае сбоя



