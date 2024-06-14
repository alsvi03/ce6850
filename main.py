import json
import uuid
import datetime
from datetime import datetime, timedelta
import redis
import sys

def main():
    command = sys.argv[1:]  # получение команды

    r = redis.Redis(host='redis', port=6379, db=0)  # тут поменять на нужный
    # r.delete(f'output')
    # r.delete(f'dbwrite')

    reqest: bytearray = [0] * 8
    confim: bytearray= [0]*6
    password: bytearray=[0]*14
    read_reqes: bytearray = [0]*50

    def calculate_crc(data, count):
        crc = 0
        for i in range(count):
            crc += data[i]
        crc &= 0x7F
        return crc - 1




    def process_string(input_string):  # приведение ответа к формату буфера
        buffer = []
        for i in range(0, len(input_string), 2):
            if i+1 < len(input_string):
                two_chars = input_string[i:i+2]
                num = two_chars
                buffer.append(num)

        return buffer

    def hex_to_int(buff):
        int_buf = [0]*len(buff)
        for i in range(len(buff)):
            int_buf[i]=int(buff[i],16)
        return int_buf


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

    def create_Packege_reqest(address):
        global reqest

        address_bytes = [ord(char) for char in str(address)]

        for i in range(len(address_bytes)):
            reqest[i + 2] = address_bytes[i]

        reqest[0] = 0x2f # /
        reqest[1] = 0x3f # ?

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
        confim[4] = 0x0d  # возврат каретки
        confim[5] = 0x0a  # перевод строки
        return confim

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
        #POWЕX, параметры сети
        global read_reqes
        read_reqes[0] = 0x01
        read_reqes[1] = 0x52 # R
        read_reqes[2] = 0x31 # 1
        read_reqes[3] = 0x02
        if com =="min30":
            current_date = datetime.now()
            new_date = current_date - timedelta(days=I1)
            date_str = new_date.strftime("%d.%m.%y") #вычисляем дату, которую надо опросить
            i = min30(read_reqes,e,date_str)
        elif com == "day":
            i = day(read_reqes,e,I1,I2)
        elif com == "month":
            i = month(read_reqes,e,I1,I2)
        elif com == "instant":
            i = instant(read_reqes,e)
        elif com == "allen":
            i = allen(read_reqes,e,I1,I2)
        else:
            print("unknown command: "+ com)
            i=3

        read_reqes[i+1] = 0x03
        #read_reqes[14] = 0x0d # пхд контрольная сумма
        read_reqes[i+2] = calculate_crc(read_reqes, i+2)  # пхд контрольная сумма


        return read_reqes

    def allen(buff,e,I1,I2):
        #01 52 31 02    41 50 48 50 45 28 30 29    03 77  .R1.APHPE(0).w
        buff[4] = 0x41 # A
        buff[5] = 0x50 # P
        buff[6] = 0x48 # H
        if e == 0:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x45  # E - потребленная
        elif e == 1:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x49  # I - отпущенная
        elif e == 2:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x45  # E - потребленная
        elif e == 3:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x49  # I - отпущенная
        buff[9] = 0x28 # (
        buff[10] = ord(str(I1))  # номер запрашиваемого месяца (дня)
        buff[11] = ord(str(I2))  # количество запрашиваемых месяцев (дней)
        buff[12] = 0x29 # )
        return 12



    def instant (buff,e):
        if e == 0:  #.R1.POWEP().d
            buff[4] = 0x50 #P
            buff[5] = 0x4f #O
            buff[6] = 0x57 #W
            buff[7] = 0x45 #E
            buff[8] = 0x50 #P
            buff[9] = 0x28 #(
            buff[10] = 0x29 #)
        elif e == 1: #.R1.POWEQ().e
            buff[4] = 0x50 #P
            buff[5] = 0x4f #O
            buff[6] = 0x57 #W
            buff[7] = 0x45 #E
            buff[8] = 0x51 #Q
            buff[9] = 0x28 #(
            buff[10] = 0x29 #)
        elif e == 2: #.R1.POWES().g
            buff[4] = 0x50
            buff[5] = 0x4f
            buff[6] = 0x57
            buff[7] = 0x45
            buff[8] = 0x53
            buff[9] = 0x28
            buff[10] = 0x29
        elif e == 3: #.R1.VOLTA()._
            buff[4] = 0x56
            buff[5] = 0x4f
            buff[6] = 0x4c
            buff[7] = 0x54
            buff[8] = 0x41
            buff[9] = 0x28
            buff[10] = 0x29
        elif e == 4: #.R1.CURRE().Z
            buff[4] = 0x43
            buff[5] = 0x55
            buff[6] = 0x52
            buff[7] = 0x52
            buff[8] = 0x45
            buff[9] = 0x28
            buff[10] = 0x29
        elif e == 5: #.R1.COS_f()..
            buff[4] = 0x43
            buff[5] = 0x4f
            buff[6] = 0x53
            buff[7] = 0x5f #_
            buff[8] = 0x66
            buff[9] = 0x28
            buff[10] = 0x29
        elif e == 6: #.R1.SIN_f()..
            buff[4] = 0x53
            buff[5] = 0x49
            buff[6] = 0x4e
            buff[7] = 0x5f
            buff[8] = 0x66
            buff[9] = 0x28
            buff[10] = 0x29
        elif e == 7: #.R1.CORIU().[
            buff[4] = 0x43
            buff[5] = 0x4f
            buff[6] = 0x52
            buff[7] = 0x49
            buff[8] = 0x55
            buff[9] = 0x28
            buff[10] = 0x29
        elif e == 8: #.R1.FREQU().\
            buff[4] = 0x46
            buff[5] = 0x52
            buff[6] = 0x45
            buff[7] = 0x51
            buff[8] = 0x55
            buff[9] = 0x28
            buff[10] = 0x29


        #.R1.POWEP().d  01 52 31 02   50 4F 57 45 50 28 29   03 64
        #.R1.POWEQ().e  01 52 31 02   50 4F 57 45 51 28 29   03 65
        #.R1.POWES().g  01 52 31 02   50 4F 57 45 53 28 29   03 67
        #.R1.VOLTA()._  01 52 31 02   56 4F 4C 54 41 28 29   03 5F
        #.R1.CURRE().Z  01 52 31 02   43 55 52 52 45 28 29   03 5A
        #.R1.COS_f()..  01 52 31 02   43 4F 53 5F 66 28 29   03 03
        #.R1.SIN_f()..  01 52 31 02   53 49 4E 5F 66 28 29   03 08
        #.R1.CORIU().[  01 52 31 02   43 4F 52 49 55 28 29   03 5B
        #.R1.FREQU().\  01 52 31 02   46 52 45 51 55 28 29   03 5C
        return 10


    def min30 (buff,e,date):
        # 01 52 31 02 47 52 41 50 45   28  30 36 2E 30 36 2E 32 34 2E 31 2E 32 34  29   03 49
        # 01 52 31 02 47 52 41 51 49   28  31 30 2e 30 36 2e 32 34 2e 30 2e 32 34  29   03 48
        buff[4] = 0x47  # G
        buff[5] = 0x52  # R
        buff[6] = 0x41  # A

        if e == 0 or e == 1:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x45  # E - потребленная
        elif e == 2 or e == 3:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x49  # I - отпущенная
        elif e == 4 or e == 5:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x45  # E - потребленная
        elif e == 6 or e == 7:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x49  # I - отпущенная


        buff[9] = 0x28 # (
        index = 10
        for char in date: # вписываем дату
            ascii_value = ord(char)  # Получаем ASCII значение символа
            buff[index] = ascii_value
            index += 1
        buff[18] = 0x2e
        if e%2 == 0:
            buff[19] = 0x30 # c нулевой
            buff[20] = 0x30
        else:
            buff[19] = 0x32 # с 25
            buff[20] = 0x35
        buff[21] = 0x2e
        buff[22] = 0x32 # 24 шт (получасовки)
        buff[23] = 0x34
        buff[24] = 0x29 # )

        return 24

    def day(buff,e, I1, I2):
        buff[4] = 0x45  # E
        buff[5] = 0x44  # D - за сутки
        buff[6] = 0x30  # 0
        if e == 0:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x45  # E - потребленная
        elif e == 1:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x49  # I - отпущенная
        elif e == 2:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x45  # E - потребленная
        elif e == 3:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x49  # I - отпущенная
        buff[9] = 0x28  # (
        buff[10] = ord(str(I1+1))  # номер запрашиваемого месяца (дня)
        buff[11] = ord(str(I2))  # количество запрашиваемых месяцев (дней)
        buff[12] = 0x29  # )
        return 12

    def month(buff,e,I1,I2):
        buff[4] = 0x45  # E
        buff[5] = 0x4d  # M - за месяц
        buff[6] = 0x30  # 0
        if e == 0:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x45  # E - потребленная
        elif e == 1:
            buff[7] = 0x50  # P - активная
            buff[8] = 0x49  # I - отпущенная
        elif e == 2:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x45  # E - потребленная
        elif e == 3:
            buff[7] = 0x51  # Q - реактивная
            buff[8] = 0x49  # I - отпущенная
        buff[9] = 0x28  # (
        buff[10] = ord(str(I1+1))  # номер запрашиваемого месяца (дня)
        buff[11] = ord(str(I2))  # количество запрашиваемых месяцев (дней)
        buff[12] = 0x29  # )
        return 12


    def check_data(buffer):
        values = []
        e = 222 # нужен чтобы потом знать в какое поле записывать
        in_range = False
        current_string = ''
        for index, value in enumerate(buffer):
            if value == '28':
                prev_value1 = buffer[index - 2]  # Сохраняем первое предыдущее значение
                prev_value2 = buffer[index - 1]  # Сохраняем второе предыдущее значение
                prev_value3 = buffer[index - 3]  # Сохраняем второе предыдущее значение

                if prev_value1 == '50' and prev_value2 == '45':
                    e = 0
                elif prev_value1 == '50' and prev_value2 == '49':
                    e = 1
                elif prev_value1 == '51' and prev_value2 == '45':
                    e = 2
                elif prev_value1 == '51' and prev_value2 == '49':
                    e = 3
                elif prev_value1 == '45' and prev_value2 == '50':
                    e = 4
                elif prev_value1 == '45' and prev_value2 == '51':
                    e = 5
                elif prev_value1 == '45' and prev_value2 == '53':
                    e = 6
                elif prev_value1 == '54' and prev_value2 == '41':
                    e = 7
                elif prev_value1 == '52' and prev_value2 == '45':
                    e = 8
                elif prev_value1 == '5F' and prev_value2 == '66' and prev_value3 == '53':
                    e = 9
                elif prev_value1 == '5F' and prev_value2 == '66':
                    e = 10
                elif prev_value1 == '49' and prev_value2 == '55':
                    e = 11
                elif prev_value1 == '51' and prev_value2 == '55':
                    e = 12

                in_range = True
                current_string = ''

            elif value == '29':
                in_range = False
                # Переводим строку из ASCII символов в числа
                num_string = ''
                for char in current_string:
                    if char.isdigit():
                        num_string += char
                    elif char == ',':
                        num_string = ''
                    else:
                        num_string += '.'




                    #num_string += char if char.isdigit() else '.'

                values.append(num_string)

            elif in_range:
                current_string += chr(int(value, 16))  # Преобразуем ASCII код в символ и добавляем к текущей строке

        return values, e


    #command = f'channel.commands'
    #-- создаем пример запроса
        # json_create_cmd = {
        #     "channel": 'ktp6',  # название канала
        #     "cmd": 'allen',  # название типа опроса day - показания на начало суток
        #     "run": 'ce6850',  # название вызываемого протокола
        #     "vm_id": 4,  # id прибора учёта
        #     "ph": 573,  # адрес под которым счетчик забит в успд
        #     "trf": '3',  # количество тарифов у счётчика
        #     "ki": 2,  # коэф тока
        #     "ku": 3,  # коэф трансформации
        #     "ago": 0,  # начало опроса 0 - текущий день 1 вчерашний и тд
        #     "cnt": 1,  # глубина опроса 1 за этот день 2 за этот и предыдущий и тп
        #     "overwrite": 0  # параметр дозаписи/перезаписи
        # }
        #
        # json_string = json.dumps(json_create_cmd)
        # r.rpush(command, json_string) # добавляем его на редис
    #---

    # разбор полученного jsonа на данные
    channel_command = r.lpop(f'{command[0]}.commands')
    print(f'{command[0]}.commands')
    print(channel_command)
    address = json.loads(channel_command)["ph"]
    I1 = json.loads(channel_command)["ago"]
    I2 = json.loads(channel_command)["cnt"]
    com = json.loads(channel_command)["cmd"]
    trf = json.loads(channel_command)["trf"]
    vm_id = json.loads(channel_command)["vm_id"]
    overwrite = json.loads(channel_command)["overwrite"]



    answer_key = str(uuid.uuid4())  # создание ключа
    json_output = {"key": answer_key, "vmid": vm_id, "command": com, "do": "send",
                   "out": int_to_hex(create_Packege_reqest(address),8),
                   "protocol": "1",
                   "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

    json_string = json.dumps(json_output)
    r.rpush(f'{command[0]}.output', json_string)  # добавляем его на редис

    json_output = {"key": answer_key, "vmid": vm_id, "command": com, "do": "send",
                   "out": int_to_hex(create_confirm_msg(),6),
                   "protocol": "1",
                   "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

    json_string = json.dumps(json_output)
    r.rpush(f'{command[0]}.output', json_string)  # добавляем его на редис

    json_output = {"key": answer_key, "vmid": vm_id, "command": com, "do": "send",
                   "out": int_to_hex(create_password_msg(),14),
                   "protocol": "1",
                   "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

    json_string = json.dumps(json_output)
    r.rpush(f'{command[0]}.output', json_string)  # добавляем его на редис

    if com == "min30":
        for g in range(I2-I1): # чтобы запросить за каждый день
            for i in range(8):

                json_output = {"key": answer_key, "vmid": vm_id, "command": "min30", "do": "send",
                               "out": int_to_hex(create_Read_msg(com, I1+g, I2, i), 27),
                               "protocol": "1",
                               "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

                json_string = json.dumps(json_output)
                r.rpush(f'{command[0]}.output', json_string)  # добавляем его на редис
    elif com == "instant":
        for i in range(8):
            json_output = {"key": answer_key, "vmid": vm_id, "command": "instant", "do": "send",
                           "out": int_to_hex(create_Read_msg(com, I1, I2, i), 13),
                           "protocol": "1",
                           "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

            json_string = json.dumps(json_output)
            r.rpush(f'{command[0]}.output', json_string)  # добавляем его на редис
    else:
        for i in range (4):
            json_output = {"key": answer_key, "vmid": vm_id, "command": com, "do": "send",
                           "out": int_to_hex(create_Read_msg(com, I1, I2, i),15),
                           "protocol": "1",
                           "waitingbytes": 28}  # генерируем json с запросом и указываем ключ куда положить ответ

            json_string = json.dumps(json_output)
            r.rpush(f'{command[0]}.output', json_string)  # добавляем его на редис





    #----- создаем пример ответа
        # json_answer = {"in": "02415048504528372C302E3130303339290D0A0317", "state": "0"}
        # json_string = json.dumps(json_answer)
        # r.rpush(answer_key,json_string)
        #
        #
        # json_answer = {"in": "02415048504928372C302E3030303030290D0A030E", "state": "0"}
        # json_string = json.dumps(json_answer)
        # r.rpush(answer_key,json_string)

    #-----



    # c переодичностью в секунду проверяем:
    data_dict = {}
    if com == "min30" or com == "instant":
        i_max = 8
    else:
        i_max = 4
    while i < i_max:

        json_answer_list = r.lrange(answer_key, 0, -1)  # Получаем все элементы из списка

        for json_answer in json_answer_list:
            data, ind_com = check_data(process_string(json.loads(json_answer)["in"]))  # разбираем ответ на данные

            for index, number in enumerate(data, start=1):
                current_index = index
                field = ""

                if ind_com == 0:
                    field = "pp"
                elif ind_com == 1:
                    field = "pm"
                elif ind_com == 2:
                    field = "qp"
                elif ind_com == 3:
                    field = "qm"
                elif ind_com == 4:
                    field = "power_active"
                elif ind_com == 5:
                    field = "power_reactive"
                elif ind_com == 6:
                    field = "power_full"
                elif ind_com == 7:
                    field = "voltage"
                elif ind_com == 8:
                    field = "current_strength"
                elif ind_com == 9:
                    field = "power_coeff"
                elif ind_com == 10:
                    field = "SIN_f"
                elif ind_com == 11:
                    field = "current_voltage_angle"
                elif ind_com == 12:
                    field = "frequency"
                else:
                    field = "unknown"


                while current_index in data_dict and field in data_dict[current_index]:
                    current_index += 1

                if current_index not in data_dict:
                    data_dict[current_index] = {}

                data_dict[current_index][field] = str(float(number))

        r.ltrim(answer_key, len(json_answer_list), -1)  # Удаляем обработанные элементы из списка
        i += len(json_answer_list)  # Увеличиваем счетчик на количество обработанных элементов

    # Создаем итоговый JSON объект
    final_data = {
        "data": [
            {"index": key, **value}
            for key, value in data_dict.items()
        ],
        "date": datetime.now().strftime("%d-%m-%Y"),
        "time": datetime.now().strftime("%H:%M:%S"),
        "poll_date": datetime.now().strftime("%d-%m-%Y"),
        "poll_time": datetime.now().strftime("%H:%M:%S"),
        "command": com,
        "vm_id": vm_id,
        "ago": I1,
        "overwrite": overwrite,
    }

    json_string = json.dumps(final_data)
    r.rpush('dbwrite', json_string)  # кладем полученные данные в redis


    # for i in range(15):
    #     print(r.lpop('output'))

    # print("---")
    # print(r.lpop('dbwrite'))




main()
