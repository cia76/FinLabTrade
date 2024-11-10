import os.path  # Файлы и папки
from datetime import datetime  # Время исполнения скриптов
import csv  # CSV файл

import numpy as np
import pandas as pd


# Спецификация и данные тикера

board = 'TQBR'
symbol = 'SBER'
tf = 'D1'
# TODO Получить спецификацию тикера из брокера: размер лота, шаг цены, кол-во десятичных знаков и пр.
datapath = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Alor', '')  # Путь сохранения файла истории
file = f'{board}.{symbol}_{tf}'  # Имя файла истории
filename = f'{datapath}{file}.txt'  # Полное имя файла истории
delimiter = '\t'  # Разделитель значений в файле истории. По умолчанию табуляция
dt_format = '%d.%m.%Y %H:%M'  # Формат представления даты и времени в файле истории. По умолчанию русский формат

# Индикаторы на Series с параметрами
# TODO Если в индикаторе, например, в фильтре Ганна, есть разовые вычисления, то надо их вынести в отдельную функцию. Значит, индикаторы нужно делать в виде классов
# TODO Параметры могут быть разных типов. Не только целые значения как в периоде
# TODO Индикатор может на выходе выдавать более одного значения. В WL создавался индикатор для каждого выходного значения. В BT генерировались отдельные линии внутри индикатора
# TODO Индикатор не может получить свое предыдущее значение. Например, индикатор EMA

def hl2(high: pd.Series, low: pd.Series):
    """Средняя цена между максимумом и минимумом"""
    return (high.iloc[-1] + low.iloc[-1]) / 2

def sma(data: pd.Series, period: int):
    """Простая скользящая средняя"""
    return None if len(data) < period else data.rolling(window=period).mean().iloc[-1]

# Преобразования

def do_transforms(transforms, df, index):
    """Рассчитать все преобразования"""
    for transform in transforms:  # Пробегаемся по всем преобразованиям
        cmd = f'df._set_value(index, "{transform[0].__name__}", {transform[0].__name__}('  # Будем устанавливать значения индикаторов через генерацию функции _set_value
        if isinstance(transform[1], str):  # Одна колонка на вход индикатора
            cmd += f'df["{transform[1]}"]'
        else:  # Несколько колонок на вход индикатора
            cmd += ','.join(f'df["{col}"]' for col in transform[1])
        if len(transform) == 3:  # Если есть параметры
            if isinstance(transform[2], tuple):  # Несколько параметров
                cmd += ',' + ','.join(str(arg) for arg in transform[2]) + ')'
            else:  # Один параметр
                cmd += f',{transform[2]})'
        else:  # Если параметров нет
            cmd += ')'  # то закрываем скобку функции после колонок
        cmd += ')'  # Закрываем скобку функции _set_value
        # print(cmd)  # Для отладки
        exec(cmd)  # Запускаем функцию _set_value

def process_history(transforms):
    """Обработка истории"""
    df = pd.read_csv(filename,  # Имя файла
                     sep=delimiter,  # Разделитель значений
                     usecols=['datetime', 'open', 'high', 'low', 'close', 'volume'], # Для ускорения обработки задаем колонки, которые будут нужны для исследований
                     parse_dates=['datetime'],  # Колонку datetime разбираем как дату/время
                     dayfirst=True,  # В дате/времени сначала идет день, затем месяц и год
                     index_col='datetime')  # Индексом будет колонка datetime  # Дневки тикера
    for transform in transforms:  # Пробегаемся по всем преобразованиям
        df[transform[0].__name__] = np.nan  # Преобразования дадут нам новые столбцы в DataFrame. Пока значения их не определены
    for index, row in df.iterrows():  # Пробегаемся по каждой строке
        do_transforms(transforms, df, index)  # Рассчитываем значения индикаторов
    print(df)  # Для отладки

def new_bars_emulation(transforms):
    """Эмуляция прихода новых бар путем построчного считывания файла истории"""
    df = pd.DataFrame({column: pd.Series(dtype=type) for column, type in
                       {'datetime': 'datetime64[ns]',
                        'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
                        'volume': 'int'}.items()})
    df.set_index('datetime', inplace=True)
    for transform in transforms:  # Пробегаемся по всем преобразованиям
        df[transform[0].__name__] = np.nan  # Преобразования дадут нам новые столбцы в DataFrame. Пока значения их не определены
    with open(filename) as file:  # Открываем файл на последовательное чтение
        reader = csv.reader(file, delimiter=delimiter)  # Данные в строке разделены табуляцией
        next(reader, None)  # Пропускаем первую строку с заголовками
        for csv_row in reader:  # Последовательно получаем все строки файла
            index = pd.to_datetime(datetime.strptime(csv_row[0], dt_format))  # Переводим индекс времени в формат pandas
            df.loc[index] = dict(open=float(csv_row[1]), high=float(csv_row[2]), low=float(csv_row[3]), close=float(csv_row[4]), volume=int(csv_row[5]))  # Бар из файла заносим в поля OHLC
            do_transforms(transforms, df, index)  # Рассчитываем значения индикаторов
    print(df)  # Для отладки


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    transforms = ((hl2, ('high', 'low')),  # Средняя цена по High и Low
                  (sma, 'hl2', 26))  # Среднюю цену сглаживаем SMA
    dt_now = datetime.now()  # Текущее время для замера времени исполнения скрипта
    new_bars_emulation(transforms)  # Эмуляция прихода новых бар 8.21 с
    # process_history(transforms)  # История 2.15 с
    print(datetime.now() - dt_now)  # Время работы скрипта
