#!/bin/bash

FILE="/tmp/change_$SLURM_JOB_ID"
if [ ! -f "$FILE" ]; then
    echo "Файл $FILE не найден. Скрипт завершает работу (нормально)."
    exit 0  # <--- Изменено с 1 на 0
fi
LINE=$(head -n 1 "$FILE")

# Разбиваем строку на слова (по пробелам) и берем первые 3
WORD1=$(echo "$LINE" | awk '{print $1}')
WORD2=$(echo "$LINE" | awk '{print $2}')
WORD3=$(echo "$LINE" | awk '{print $3}')


nohup setsid sudo --user=michman bash -c "
    enroot remove --force $WORD1
    enroot create --name $WORD1 $WORD3
    python3 /home/michman/dockerf/pr.py $WORD1
" > /dev/null 2>&1 &
#sudo --user=michman bash -c "(enroot remove --force $WORD1 &;enroot create --name $WORD1 $WORD3 &; python3 /home/michman/dockerf/pr.py $WORD1 &)"
#sudo -u michman enroot remove --force $WORD1
sudo umount $WORD2 
#sudo --user=michman bash -c "enroot create --name $WORD1 $WORD3 &"
#sudo --user=michman bash -c "python3 /home/michman/dockerf/pr.py $WORD1 &"
#sudo -u michman enroot create --name $WORD1 $WORD3
#sudo -u michman python3 /home/michman/dockerf/pr.py $WORD1

sudo rm /tmp/change_$SLURM_JOB_ID
