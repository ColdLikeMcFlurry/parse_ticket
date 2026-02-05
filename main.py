import pprint
import time
import requests
import json
import ijson
import os
import pandas as pd
from datetime import date, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import random

# URL для проверки доступности сайта
site_url = "https://www.rzd.ru/"

# Полный набор headers, как у тебя
headers = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://ticket.rzd.ru/",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Host": "ticket.rzd.ru",
    "Origin": "https://ticket.rzd.ru",
    "Sec-Fetch-Site": "same-origin",
    "sec-fetch-mode": "cors",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0"
}


def check_connection():
    """Проверка доступности сайта"""
    try:
        response = requests.get(site_url, headers=headers, timeout=10)
        if response.status_code == 200:
            print("Доступ к сайту есть, можно работать с API")
        else:
            print(f"Ошибка при доступе к сайту: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Произошла ошибка при запросе: {e}")


def get_trains_info(st_from, st_to, orig, dest, dprt_dt):
    """Получение данных поездов через API"""
    api_url = "https://ticket.rzd.ru/api/v1/railway-service/prices/train-pricing"

    # Параметры запроса
    params = {
        "service_provider": "B2B_RZD",
        "getByLocalTime": "true",
        "carGrouping": "DontGroup",
        "origin": orig,
        "destination": dest,
        "departureDate": dprt_dt,
        # "2025-10-29T00:00:00",
        "specialPlacesDemand": "StandardPlacesAndForDisabledPersons",
        "carIssuingType": "Passenger",
        "getTrainsFromSchedule": "true",
        "adultPassengersQuantity": 1,
        "childrenPassengersQuantity": 0,
        "hasPlacesForLargeFamily": "false"
    }

    try:
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get(api_url, headers=headers, params=params, timeout=10)
        print(f"Статус запроса к API: {response.status_code}")
        response.raise_for_status()  # выбросит ошибку, если статус != 200
        print(f"ответ от сервера - {response.status_code} для маршрута {st_from} - {st_to} на {dprt_dt.split('T')[0]}")
        trains_info = response.json()
        # print(trains_info)
        print(f"Получил информацио о поездах на маршруте {st_from} - {st_to}")
        # добавляем дату и маршруты в те ответы, где поезда не курсируют
        if trains_info.get('errorInfo') and trains_info['errorInfo'].get('Code') == 310:
            try:
                trains_info['errorInfo']['dprt_dt'] = dprt_dt.split('T')[0]
                trains_info['errorInfo']['OriginName'] = st_from
                trains_info['errorInfo']['DestinationName'] = st_to
                trains_info['errorInfo']['OriginStationCode'] = orig
                trains_info['errorInfo']['DestinationStationCode'] = dest
            except Exception as e:
                print(f"Ошибка при форматировании даты: {e}")
        # print(trains_info['errorInfo']['Message'])
        return trains_info
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса к API: {e}")
        return None
    except json.JSONDecodeError:
        print("Не удалось декодировать JSON")
        return None


def get_trains_number(trains_info):
    if not trains_info:
        print("Нет данных для обработки")
        return

    train_numbers = []
    trains = trains_info.get("Trains", [])
    # pprint.pp(trains)
    # time.sleep(1000)
    for train in trains:
        number = train.get("TrainNumber")
        orig = train['OriginStationInfo'].get("StationCode")
        dest = train['DestinationStationInfo'].get("StationCode")
        dprt_dt = train.get("DepartureDateTime")

        if number:
            train_numbers.append({
                'number': number,
                'orig': int(orig),
                'dest': int(dest),
                'dprt_dt': dprt_dt.split('T')[0]
            })

    print("Номера поездов:")
    print(json.dumps(train_numbers, ensure_ascii=False, indent=4))
    return train_numbers


def get_info_in_train(trains):
    all_train_info = []
    errors_info = []
    # тут мы получаем подробную информацию по поезду
    api_url = "https://ticket.rzd.ru/apib2b/p/Railway/V1/Search/CarPricing"
    for train in trains:
        # Параметры запроса
        params = {
            "service_provider": "B2B_RZD",
            "OriginCode": int(train['orig']),
            "DestinationCode": int(train['dest']),
            "departureDate": train['dprt_dt'],
            "specialPlacesDemand": "StandardPlacesAndForDisabledPersons",
            "TrainNumber": train['number']
        }
        try:
            response = requests.post(api_url, headers=headers, json=params, timeout=(15, 30))
            status = response.status_code
            print(f"Запрос к API для поезда {train['number']} → статус {status}")
            response.raise_for_status()

            train_info = response.json()
            # print(train_info)
            # Проверяем, есть ли логическая ошибка в ответе API
            if train_info.get("ProviderError") or train_info.get("Message"):
                msg = train_info.get("Message", "Неизвестная ошибка API")
                print(f"Ошибка от API для {train['number']} ({train['dprt_dt']}): {msg}")
                errors_info.append({
                    "train": train['number'],
                    "date": train['dprt_dt'],
                    "origin": train['orig'],
                    "destination": train['dest'],
                    "api_message": msg
                })
            else:
                print(f"Получены данные по поезду {train['number']} на {train['dprt_dt']}")
                all_train_info.append(train_info)

        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса для поезда {train['number']}: {e}")
            errors_info.append({
                "train": train['number'],
                "date": train['dprt_dt'],
                "origin": train['orig'],
                "destination": train['dest'],
                "error": str(e)
            })

    return all_train_info, errors_info


def get_data_from_excel():
    df = pd.read_excel(fr'{os.getcwd()}\parameters.xlsx', sheet_name='routes')
    print('Читаю excel файл')
    print(df.head())
    return df.values


# тут берем инфу на 1  направление
def process_one_request(route, next_day):
    stFrom, stTo, orig_code, dest_code = route
    if next_day.weekday() in [1, 3]:  # вторник или четверг
        # return None
        dprt_dt = next_day.strftime("%Y-%m-%dT00:00:00")

        time.sleep(random.uniform(1.5, 2.8))

        return get_trains_info(stFrom, stTo, orig_code, dest_code, dprt_dt)


#  тут сохдаем многопоточность, например, 1 направление на 5 дат
def start_parse():
    print('Приступаю к парсингу')
    all_info = []

    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = []

        # Формируем все задачи
        for route in get_data_from_excel():
            for j in range(0, 120):
                next_day = start_date + timedelta(days=j)
                future = executor.submit(process_one_request, route, next_day)
                futures.append(future)

        # Сбор результатов по мере готовности
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_info.append(result)

    # Запись результата
    with open("all_info.json", "w", encoding="utf-8") as f:
        json.dump(all_info, f, ensure_ascii=False, indent=4)

    print("Перехожу к чтению JSON")


# def start_parse():
#     for route in get_data_from_excel():
#         stFrom, stTo, orig_code, dest_code = route[0], route[1], route[2], route[3]
#         for j in range(5, 10):
#             next_day = start_date + timedelta(days=j)
#             # обработнка нужных дней
#             if next_day.weekday() not in [1, 3]:
#                 continue
#             dprt_dt = next_day.strftime("%Y-%m-%dT00:00:00")
#
#             all_data = get_trains_info(stFrom, stTo, orig_code, dest_code, dprt_dt)
#             if not all_data:
#                 continue
#             all_info.append(all_data)
#
#             # trains_info = get_trains_number(all_data)
#             # if not trains_info:
#             #     continue
#             # detailed_info, errors = get_info_in_train(trains_info)
#             # train_info.extend(detailed_info)
#             # train_errors.extend(errors)
#
#     # тут мы записываем в json все направления
#     with open("all_info.json", "w", encoding="utf-8") as f:
#         json.dump(all_info, f, ensure_ascii=False, indent=4)
#     # # тут более точная информация по поезду
#     # with open("detailed_data.json", "w", encoding="utf-8") as f:
#     #     json.dump(train_info, f, ensure_ascii=False, indent=4)
#     # # тут создаем json с ошибками
#     # with open("train_errors.json", "w", encoding="utf-8") as f:
#     #     json.dump(train_errors, f, ensure_ascii=False, indent=4)


def read_json():
    print("Начинаю читать JSON")
    # тут читаем json-файл со всей информацией по направлению
    with open(f"all_info.json", "rb") as file_w:
        # directions = json.load(file_w)
        directions = ijson.items(file_w, 'item')
        all_items = []
        # проходимся по направлениям
        for direction in directions:
            # pprint.pprint(direction)
            # обработка ошибок, код 310
            if direction.get('errorInfo') and direction['errorInfo'].get('Code') == 310:
                errors = direction.get('errorInfo')
                data_errors = {
                    "date_search": datetime.today().strftime('%Y-%m-%d'),
                    "DepartureDateTime": errors.get("dprt_dt", '').split('T')[0],

                    "TrainNumber": errors.get("TrainNumber", 'Поезд не курсирует'),

                    "OriginName": errors.get("OriginName", None),
                    "DestinationName": errors.get("DestinationName", None),
                    # "OriginName": errors.get("OriginStationInfo", {}).get("StationName"),
                    # "DestinationName": errors.get("DestinationStationInfo", {}).get("StationName"),

                    "OriginStationCode": errors.get("OriginStationCode", None),
                    "DestinationStationCode": errors.get("DestinationStationCode", None),
                    # "OriginStationCode": errors.get("OriginStationInfo", {}).get("StationCode"),
                    # "DestinationStationCode": errors.get("DestinationStationInfo", {}).get("StationCode"),

                    "CarTypeName": errors.get("CarTypeName", 'Поезд не курсирует'),
                    "MinPrice": errors.get("MinPrice", 3000000),
                    "MaxPrice": errors.get("MaxPrice", 3000000),
                    "ServiceCosts": next(iter(errors.get("ServiceCosts", [])), 0),
                    "ServiceClasses": next(iter(errors.get("ServiceClasses", [])), 'Поезд не курсирует'),
                    "Carriers": next(iter(errors.get("Carriers", [])), 'Поезд не курсирует'),

                    "HasNonRefundableTariff": errors.get("HasNonRefundableTariff", False),
                    "HasPlacesForDisabledPersons": errors.get("HasPlacesForDisabledPersons", False),

                    "InitialStationName": errors.get("InitialStationName", None),
                    "FinalStationName": errors.get("FinalStationName", None),

                    "InitialTrainStationInfo": errors.get("InitialTrainStationInfo", {}).get('StationCode'),
                    "FinalTrainStationInfo": errors.get("FinalTrainStationInfo", {}).get('StationCode'),

                    "TrainDescription": errors.get("TrainDescription", 'Поезд не курсирует'),
                    "TrainBrandCode": errors.get("TrainBrandCode", 'Поезд не курсирует')
                }
                # pprint.pprint(errors, sort_dicts=False)
                all_items.append(data_errors)
            # берем все поезда по направлению
            trains = direction.get("Trains", [])
            # проходимся по поедам в по направлению
            for train in trains:
                # print(train['TrainNumber'])
                # берем все вагоны по направлению
                cars = train.get("CarGroups", [])
                # проходимся по вагонам в поезде
                for car in cars:
                    # if next(iter(car.get("Carriers", [])), None) not in ['ФПК', 'Поезд не курсирует']:
                    #     continue
                    data = {

                        "date_search": datetime.today().strftime('%Y-%m-%d'),
                        "DepartureDateTime": train.get("LocalDepartureDateTime", '').split('T')[0],

                        "TrainNumber": train.get("TrainNumber", ''),

                        # "OriginName": train["OriginName"],
                        # "DestinationName": train["DestinationName"],
                        "OriginName": direction.get("OriginStationInfo", {}).get("StationName"),
                        "DestinationName": direction.get("DestinationStationInfo", {}).get("StationName"),

                        # "OriginStationCode": train["OriginStationCode"],
                        # "DestinationStationCode": train["DestinationStationCode"],
                        "OriginStationCode": int(direction.get("OriginStationInfo", {}).get("StationCode")),
                        "DestinationStationCode": int(direction.get("DestinationStationInfo", {}).get("StationCode")),

                        "CarTypeName": car.get("CarTypeName", ''),
                        "MinPrice": int(car.get("MinPrice", 0)),
                        "MaxPrice": int(car.get("MaxPrice", 0)),
                        "ServiceCosts": int(next(iter(car.get("ServiceCosts", [])), None)),
                        "ServiceClasses": next(iter(car.get("ServiceClasses", [])), None),
                        "Carriers": next(iter(car.get("Carriers", [])), None),

                        "HasNonRefundableTariff": car.get("HasNonRefundableTariff", ),
                        "HasPlacesForDisabledPersons": car.get("HasPlacesForDisabledPersons", ),

                        "InitialStationName": train.get("InitialStationName", ''),
                        "FinalStationName": train.get("FinalStationName", ''),

                        "InitialTrainStationInfo": int(train.get("InitialTrainStationInfo", {}).get('StationCode')),
                        "FinalTrainStationInfo": int(train.get("FinalTrainStationInfo", {}).get('StationCode')),

                        "TrainDescription": train.get("TrainDescription", ''),
                        "TrainBrandCode": train.get("TrainBrandCode", '')

                    }
                    all_items.append(data)
                    pprint.pprint(data, sort_dicts=False)
        print('Перехожу к формирования файла excel')
        return all_items


def create_excel(all_price):
    print('Начинаю формировать файл excel')
    df = pd.DataFrame(all_price)
    df_sorted = df.sort_values(by=['DepartureDateTime', 'TrainNumber'], ascending=True)
    df_sorted.to_excel(f'{os.getcwd()}\тестовая выгрузка.xlsx', index=False)


if __name__ == "__main__":
    start = time.perf_counter()
    check_connection()
    start_date = date.today()
    # all_info = []
    # train_info = []
    # train_errors = []
    start_parse()
    data_from_json = read_json()
    create_excel(data_from_json)
    end = time.perf_counter()
    diff = end - start

    print(f"закончил собирать данные за: {diff / 60} минут")
