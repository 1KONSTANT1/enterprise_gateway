#!/usr/bin/env python3
"""
Простой мониторинг очереди Redis с выводом всех элементов
"""
import redis
import time
from datetime import datetime

# Подключение к Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
QUEUE_NAME = "my_task_queue"

def monitor_queue():
    """Мониторинг размера очереди с выводом всех элементов"""
    try:
        while True:
            # Очистка экрана
            print("\033c", end="")
            
            # Текущее время
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем все элементы очереди
            all_items = r.lrange(QUEUE_NAME, 0, -1)
            size = len(all_items)
            
            print(f"=== Монитор очереди Redis === {current_time} ===")
            print(f"Имя очереди: {QUEUE_NAME}")
            print(f"Размер очереди: {size}")
            print("=" * 50)
            
            if size == 0:
                print("Очередь пуста")
            else:
                print(f"\nВсе элементы очереди (всего {size}):")
                print("-" * 50)
                
                # Выводим от самого старого к самому новому
                for i, item in enumerate(reversed(all_items)):
                    # Сокращаем длинные строки
                    display_item = item[:80] + "..." if len(item) > 80 else item
                    print(f"{i:3d}. {display_item}")
                
                print("-" * 50)
                
                # Информация о первом и последнем
                print(f"\nПервый в очереди (на обработку): {all_items[-1][:50]}...")
                print(f"Последний добавленный: {all_items[0][:50]}...")
            
            print("\nНажмите Ctrl+C для выхода")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n\n👋 Мониторинг остановлен")

if __name__ == "__main__":
    monitor_queue()
    