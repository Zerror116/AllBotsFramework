from db import engine, Session, BlackList

# Проверка на черный список
def is_user_blacklisted(user_id: int) -> bool:
    blacklisted_user = BlackList.get_row(user_id)
    return bool(blacklisted_user)