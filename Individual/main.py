from urllib.parse import urlencode
from urllib.request import urlopen
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, String, Float, Integer, MetaData
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.sql import select

class FinanceProvider:
    def __init__(self, shares):
        self.code = shares
    def get_data(self, start, end):
        #url = 'https://openexchangerates.org/api/latest.json?'
        start_date = datetime.strptime(start, "%d.%m.%Y").date()
        end_date = datetime.strptime(end, "%d.%m.%Y").date()
        url_start = 'http://export.finam.ru/' + self.code + '_' + start_date.strftime('%Y%m%d') + '_' + \
                    end_date.strftime('%Y%m%d') + '.csv?'
        params = urlencode([
            ('market', 45),
            ('em', '182400'),
            ('code', self.code),
            ('apply', 0),
            ('df', start_date.day + 1),  # Начальная дата, номер дня
            ('mf', start_date.month - 1),  # Начальная дата, номер месяца
            ('yf', start_date.year),  # Начальная дата, год
            ('from', start_date),  # Начальная дата полностью
            ('dt', end_date.day),  # Конечная дата, номер дня
            ('mt', end_date.month - 1),  # Конечная дата, номер месяца
            ('yt', end_date.year),  # Конечная дата, год
            ('to', end_date),  # Конечная дата
            ('p', 7),  # период обновления
            ('f', self.code + '_' + start_date.strftime('%Y%m%d') + '_' +  end_date.strftime('%Y%m%d')),
            ('e', ".csv"),
            ('cn', self.code),
            ('dtf', 1),
            ('tmf', 1),  # В каком формате брать время
            ('MSOR', 0),
            ('mstime', "on"),  # Московское время
            ('mstimever', 1),  # Коррекция часового пояса
            ('sep', 1),  # Разделитель полей
            ('sep2', 1),  # Разделитель разрядов
            ('datf', 1),  # Формат записи в файл.
            ('at', 0)])  # Нужны ли заголовки столбцов
        url = url_start + params
        print("Полный URL: " + url)
        data = urlopen(url).readlines()
        data_split = []
        for line in data:
            data_split.append(line.decode('utf-8').split(','))

        print("Данные с сервера получены.")
        return [
            {
                'ticker': data_split[line][0],
                'PER': data_split[line][1],
                'DATE': data_split[line][2],
                'TIME': data_split[line][3],
                'OPEN': data_split[line][4],
                'HIGH': data_split[line][5],
                'LOW': data_split[line][6],
                'CLOSE': data_split[line][7],
                'VOL': data_split[line][8].rstrip()
            }
            for line in range(len(data_split))
       ]


class Bdsql:
    def __init__(self, url_in, metadata_in, table_in):
        self.url_sql = url_in
        self.table = table_in
        self.metadata = metadata_in
        self.engine = create_engine(self.url_sql, echo = False)
        self.metadata.create_all(self.engine)
        self.tab = self.engine.connect()
        self.Session = sessionmaker(bind = self.engine)
        self.session = self.Session()
    def write_data(self, data):
        self.tab.execute(self.table.insert(), data)
    def read_data(self):
        return self.tab.execute(select([self.table])).fetchall()
    def to_print(self):
        for row in self.tab.execute(select([self.table])):
            print(row)

def main():
    print('Программа для анализа динамики курсов валют.')
    metadata = MetaData()
    finance = Table(
        'finance',
        metadata,
        Column('ticker', String),
        Column('PER', Integer),
        Column('DATE', String),
        Column('TIME', String),
        Column('OPEN', Float),
        Column('HIGH', Float),
        Column('LOW', Float),
        Column('CLOSE', Float),
        Column('VOL', Integer),
    )
    print('Подключение к бд...')
    bd = Bdsql('sqlite:///finance.sqlite3', metadata, finance)
    print('Успешно.\nСчитывание последней записи...')
    s = bd.read_data()
    last = s[-1].items()
    last_data = datetime.strptime(last[2][1], "%Y%m%d").date().strftime('%d.%m.%Y')
    print('Дата последней записи: ' + str(last_data))
    current_data = datetime.today().date().strftime('%d.%m.%Y')
    print('Текущая дата: ' + str(current_data))
    #provider = FinanceProvider('6387ca588f5746fe8054414b01dcb477')
    if last_data == current_data:
        print('Обновление не требуется.')
    else:
        print('Обновление БД...')
        provider = FinanceProvider('USD000UTSTOM')
        raw_data = provider.get_data(str(last_data), str(current_data))
        bd.write_data(raw_data)
    print('Вывод данных:\n<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n')
    bd.to_print()

main()
