import pprint
import time
import requests
import json
import os
import pandas as pd
from datetime import date, timedelta, datetime

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
            print("✅ Доступ к сайту есть, можно работать с API")
        else:
            print(f"❌ Ошибка при доступе к сайту: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Произошла ошибка при запросе: {e}")


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
        response = requests.get(api_url, headers=headers, params=params, timeout=10)
        print(f"Статус запроса к API: {response.status_code}")
        response.raise_for_status()  # выбросит ошибку, если статус != 200
        print(f'ответ от сервера - {response.status_code} для маршрута {st_from} - {st_to} на {dprt_dt.split('T')[0]}')
        trains_info = response.json()
        print(f'Получил информацио о поездах на маршруте {st_from} - {st_to}')
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
        print(f"❌ Ошибка запроса к API: {e}")
        return None
    except json.JSONDecodeError:
        print("❌ Не удалось декодировать JSON")
        return None


def get_trains_number(trains_info):
    if not trains_info:
        print("❌ Нет данных для обработки")
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
            response = requests.post(api_url, headers=headers, json=params, timeout=10)
            status = response.status_code
            print(f"🔹 Запрос к API для поезда {train['number']} → статус {status}")
            response.raise_for_status()

            train_info = response.json()

            # Проверяем, есть ли логическая ошибка в ответе API
            if train_info.get("ProviderError") or train_info.get("Message"):
                msg = train_info.get("Message", "Неизвестная ошибка API")
                print(f"⚠️ Ошибка от API для {train['number']} ({train['dprt_dt']}): {msg}")
                errors_info.append({
                    "train": train['number'],
                    "date": train['dprt_dt'],
                    "origin": train['orig'],
                    "destination": train['dest'],
                    "api_message": msg
                })
            else:
                print(f"✅ Получены данные по поезду {train['number']} на {train['dprt_dt']}")
                all_train_info.append(train_info)

        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка запроса для поезда {train['number']}: {e}")
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


def start_parse():
    for route in get_data_from_excel():
        stFrom, stTo, orig_code, dest_code = route[0], route[1], route[2], route[3]
        for j in range(8, 15):
            next_day = start_date + timedelta(days=j)
            dprt_dt = next_day.strftime("%Y-%m-%dT00:00:00")
            time.sleep(7)

            all_data = get_trains_info(stFrom, stTo, orig_code, dest_code, dprt_dt)
            if not all_data:
                continue
            all_info.append(all_data)

            # trains_info = get_trains_number(all_data)
            # if not trains_info:
            #     continue
            # detailed_info, errors = get_info_in_train(trains_info)
            # train_info.extend(detailed_info)
            # train_errors.extend(errors)

    # тут мы записываем в json все направления
    with open("all_info.json", "w", encoding="utf-8") as f:
        json.dump(all_info, f, ensure_ascii=False, indent=4)
    # # тут более точная информация по поезду
    # with open("detailed_data.json", "w", encoding="utf-8") as f:
    #     json.dump(train_info, f, ensure_ascii=False, indent=4)
    # # тут создаем json с ошибками
    # with open("train_errors.json", "w", encoding="utf-8") as f:
    #     json.dump(train_errors, f, ensure_ascii=False, indent=4)


def read_json():
    # тут читаем json-файл со всей информацией по направлению
    with open(f"all_info.json", "r", encoding="utf-8") as file_w:
        directions = json.load(file_w)

        # проходимся по направлениям
        for direction in directions:
            # обработка ошибок
            if direction.get('errorInfo') and direction['errorInfo'].get('Code') == 310:
                errors = direction.get('errorInfo')
                pprint.pprint(errors, sort_dicts=False)
            # берем все поезда по направлению
            trains = direction.get("Trains", [])
            # проходимся по поедам в по направлению
            for train in trains:
                # print(train['TrainNumber'])
                # берем все вагоны по направлению
                cars = train.get("CarGroups", [])
                data = {

                    "TrainNumber": train["TrainNumber"],

                    "OriginName": train["OriginName"],
                    "DestinationName": train["DestinationName"],

                    "OriginStationCode": train["OriginStationCode"],
                    "DestinationStationCode": train["DestinationStationCode"],

                    "InitialStationName": train["InitialStationName"],
                    "FinalStationName": train["FinalStationName"],

                    "InitialTrainStationCode": train["InitialTrainStationCode"],
                    "FinalTrainStationInfo": train["FinalTrainStationInfo"]['StationCode'],

                    "TrainDescription": train["TrainDescription"],
                    "TrainBrandCode": train["TrainBrandCode"],

                    'date_search': datetime.today().strftime('%Y-%m-%d'),
                    "DepartureDateTime": train["DepartureDateTime"]

                }
                pprint.pprint(data, sort_dicts=False)
                # проходимся по вагонам в поезде
                # for car in cars:
                #     print(len(cars))


if __name__ == "__main__":
    # check_connection()
    start_date = date.today()
    all_info = []
    train_info = []
    train_errors = []
    # start_parse()
    read_json()
