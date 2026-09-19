#!/usr/bin/env python3
"""
Consumer script - забирает элементы из очереди Redis
"""
import redis
import json
import time
import sys
from datetime import datetime

# Подключение к локальному Redis (по умолчанию host='localhost', port=6379, db=0)
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    # Проверка подключения
    r.ping()
except redis.ConnectionError as e:
    sys.exit(1)

# Имя очереди (должно совпадать с producer)
QUEUE_NAME = "my_task_queue"

def get_from_queue(blocking=True, timeout=0):
    """
    Забирает элемент из очереди Redis.
    
    Args:
        blocking (bool): Если True - ждет появления элементов (блокирующий режим)
        timeout (int): Таймаут ожидания в секундах (0 - бесконечно)
    
    Returns:
        Элемент из очереди или None
    """
    try:
        if blocking:
            # Блокирующий режим: BRPOP ждет появления элемента
            # Возвращает кортеж (имя_очереди, значение)
            result = r.brpop(QUEUE_NAME, timeout=timeout if timeout > 0 else 0)
            if result:
                queue_name, value = result
                return value
            return None
        else:
            # Неблокирующий режим: RPOP сразу возвращает или None
            return r.rpop(QUEUE_NAME)
    except Exception as e:
        print(f"❌ Ошибка при получении элемента: {e}")
        return None

def main():

            
    # Блокирующее получение элемента
    item = get_from_queue(blocking=True)
    print(item)
            
                

if __name__ == "__main__":
    main()
