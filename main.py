import json
import uuid
import datetime
import time
import sys
import crcmod
import redis

r = redis.Redis(host='localhost', port=6379, db=0)  # тут поменять на нужный

reqest: bytearray = [0] * 8
confim: bytearray= [0]*6
password: bytearray=[0]*14
read_reqes: bytearray = [0]*15


def int_to_hex(buff, size):
    buff2:bytearray=[0]*size
    for i in range(size):


            hexval = hex(buff[i])
            hexstr = hexval[2:]
            if len(hexstr) == 1:
                buff2[i] = "0" + hexstr
            else:
                buff2[i] = hexstr

    out = ""
    for i in range(size):
        out += buff2[i]
    return out

def create_Packege_reqest():
    global reqest
    reqest[0] = 0x2f # /
    reqest[1] = 0x3f # ?
    reqest[2] = 0x00 # адрес
    reqest[3] = 0x00 # адрес
    reqest[4] = 0x00 # адрес
    reqest[5] = 0x21 # !
    reqest[6] = 0x0d # возврат каретки
    reqest[7] = 0x0a # перевод строки
    return reqest

def create_confirm_msg():
    global confim
    confim[0] = 0x06
    confim[1] = 0x30
    confim[2] = 0x35
    confim[3] = 0x31
    reqest[4] = 0x0d  # возврат каретки
    reqest[5] = 0x0a  # перевод строки
    return reqest

def create_password_msg():
    global password
    password[0] = 0x01
    password[1] = 0x50 # P
    password[2] = 0x31 # 1
    password[3] = 0x02
    password[4] = 0x28 # (
    password[5] = 0x37
    password[6] = 0x37
    password[7] = 0x37
    password[8] = 0x37
    password[9] = 0x37
    password[10] = 0x37
    password[11] = 0x29 # )
    password[12] = 0x03
    password[13] = 0x21 # !
    return password

def create_Read_msg(com,I1,I2,e):
    global read_reqes
    read_reqes[0] = 0x01
    read_reqes[1] = 0x52 # R
    read_reqes[2] = 0x31 # 1
    read_reqes[3] = 0x02
    read_reqes[4] = 0x45 # E
    if com == "day":
        read_reqes[5] = 0x44 # D - за сутки
    elif com == "month":
        read_reqes[5] = 0x4d # M - за месяц
    read_reqes[6] = 0x30 # 0
    if e == 0:
        read_reqes[7] = 0x50  # P - активная
        read_reqes[8] = 0x45  # E - потребленная
    elif e == 1:
        read_reqes[7] = 0x50  # P - активная
        read_reqes[8] = 0x49  # I - отпущенная
    elif e == 2:
        read_reqes[7] = 0x51  # Q - реактивная
        read_reqes[8] = 0x45  # E - потребленная
    elif e == 3:
        read_reqes[7] = 0x51  # Q - реактивная
        read_reqes[8] = 0x49  # I - отпущенная



    read_reqes[9] = 0x28 # (
    read_reqes[10] = ord(str(I1)) # номер запрашиваемого месяца (дня)
    read_reqes[11] = ord(str(I2))  # количество запрашиваемых месяцев (дней)
    read_reqes[12] = 0x29 # )
    read_reqes[13] = 0x03
    read_reqes[14] = 0x0d # пхд контрольная сумма
    return read_reqes





command = f'channel.commands'
#-- создаем пример запроса
json_create_cmd = {
    "channel": 'ktp6',  # название канала
    "cmd": 'day',  # название типа опроса day - показания на начало суток
    "run": 'ce6850',  # название вызываемого протокола
    "vm_id": 4,  # id прибора учёта
    "ph": 631,  # адрес под которым счетчик забит в успд
    "trf": '3',  # количество тарифов у счётчика
    "ki": 2,  # коэф тока
    "ku": 3,  # коэф трансформации
    "ago": 2,  # начало опроса 0 - текущий день 1 вчерашний и тд
    "cnt": 4,  # глубина опроса 1 за этот день 2 за этот и предыдущий и тп
    "overwrite": 0  # параметр дозаписи/перезаписи
}


json_string = json.dumps(json_create_cmd)

r.rpush(command, json_string) # добавляем его на редис
#---

# разбор полученного jsonа на данные
channel_command = r.lpop(f'channel.commands')

address = json.loads(channel_command)["ph"]
I1 = json.loads(channel_command)["ago"]
I2 = json.loads(channel_command)["cnt"]
com = json.loads(channel_command)["cmd"]
#trf = json.loads(channel_command)["trf"]



answer_key = str(uuid.uuid4())  # создание ключа
json_output = {"key": answer_key, "vmid": 4, "command": "day", "do": "send",
               "out": int_to_hex(create_Packege_reqest(),8),
               "protocol": "1",
               "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

json_string = json.dumps(json_output)
r.rpush('output', json_string)  # добавляем его на редис

json_output = {"key": answer_key, "vmid": 4, "command": "day", "do": "send",
               "out": int_to_hex(create_confirm_msg(),6),
               "protocol": "1",
               "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

json_string = json.dumps(json_output)
r.rpush('output', json_string)  # добавляем его на редис

json_output = {"key": answer_key, "vmid": 4, "command": "day", "do": "send",
               "out": int_to_hex(create_password_msg(),14),
               "protocol": "1",
               "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

json_string = json.dumps(json_output)
r.rpush('output', json_string)  # добавляем его на редис


for i in range (4):
    json_output = {"key": answer_key, "vmid": 4, "command": "day", "do": "send",
                   "out": int_to_hex(create_Read_msg(com,I1,I2,i),15),
                   "protocol": "1",
                   "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

    json_string = json.dumps(json_output)
    r.rpush('output', json_string)  # добавляем его на редис






#print(create_Read_msg(com,I1,I2))
# print(int_to_hex(create_Packege_reqest(),8))
# print(int_to_hex(create_Read_msg(com,I1,I2,1),14))






