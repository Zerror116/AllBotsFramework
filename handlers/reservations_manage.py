from db import Reservations, Posts

# Вспомогательные функции для вычислений
def calculate_total_sum(user_id):
    """
    Рассчитывает общую сумму заказов пользователя.
    """
    reservations = Reservations.get_row_by_user_id(user_id)
    if not reservations:
        return 0

    total_sum = 0
    for reservation in reservations:
        post = Posts.get_row_by_id(reservation.post_id)
        if post:
            total_sum += post.price

    return total_sum

def calculate_processed_sum(user_id):
    """
    Рассчитывает сумму обработанных заказов пользователя.
    """
    reservations = Reservations.get_row_by_user_id(user_id)
    if not reservations:
        print(f"Нет бронирований пользователя с ID: {user_id}")
        return 0

    processed_sum = 0
    for reservation in reservations:
        if reservation.is_fulfilled:  # Если is_fulfilled — булево значение
            post = Posts.get_row_by_id(reservation.post_id)
            if post:
                processed_sum += post.price
            else:
                print(f"Нет данных в Posts для post_id: {reservation.post_id}")
        else:
            print(f"Бронирование ID {reservation.id} не обработано.")
    return processed_sum


