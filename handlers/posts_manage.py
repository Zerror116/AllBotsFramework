from db import Posts, Clients
from datetime import datetime, timedelta


# Сохранение поста
def save_post(chat_id: int, photo: str, price: str, description: str, quantity: int):
    """Сохранение поста в базу данных."""
    Posts.insert(chat_id=chat_id, photo=photo, price=price, description=description, quantity=quantity)




