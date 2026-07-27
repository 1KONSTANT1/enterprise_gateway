#!/usr/bin/env python3
"""
Producer script - добавляет элементы в очередь Redis
"""
import redis
import sys
import subprocess

# Подключение к локальному Redis (по умолчанию host='localhost', port=6379, db=0)
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    # Проверка подключения
    r.ping()
except redis.ConnectionError as e:
    sys.exit(1)

# Имя очереди (списка в Redis)
QUEUE_NAME = "my_task_queue"

def add_to_queue(item):
    """
    Добавляет элемент в очередь Redis.
    Используем LPUSH для добавления в начало (слева).
    Для FIFO (первый вошел - первый вышел) получатель будет использовать RPOP.
    """
    try:
        
        r.lpush(QUEUE_NAME, item)
        return True
    except Exception as e:
        return False

def main():

    input = sys.argv[1]
   # subprocess.run([f'enroot remove --force {input}'],shell=True, check=True)
    #subprocess.run([f'sudo umount {sys.argv[2]}'], shell=True,check=True)
    #subprocess.run([f'enroot create --name {input} {sys.argv[3]}'],shell=True, check=True)
    print(f"Adding value {input}")
    add_to_queue(input)
    

if __name__ == "__main__":
    main()