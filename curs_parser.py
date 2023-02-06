# -*- coding: utf-8 -*-
from core.core_io import write2database, log_error, log_info
from datetime import datetime, timedelta
import json
import requests
from bs4 import BeautifulSoup as bs

def dmy2ymd(dt):
    d, m, y = dt.strip().split('.')
    if len(d) == 1: d = '0' + d
    if len(m) == 1: m = '0' + m
    return ''.join((y, m, d))

def load(url, debug = False):
    if not debug:
        resp = requests.get(source['url'])
        content = resp.text
    else:
        if 'kursf?json' in url: fname = 'kursf.json'
        elif 'exchange?json' in url: fname = 'exchange.json'
        elif 'index.minfin' in url: fname = 'wgb.html'
        else: fname = 'wgb.html'
        with open(fname, 'rt', encoding='utf-8') as f:
            content = f.read()
    try:
        if '?json' in url:
            data = json.loads(content)
        else:
            data = bs(content, features='lxml')
    except Exception as err:
        log_error(f'Error download {url}')
        data = []
    return data

def parse_soup(data, currency_list):
    target_table = ''
    for table in data.find_all('table'):
        title = table.find('caption')
        if 'жбанк' in title.text:
            target_table = table
            dt = title.text[-10:]
            break
    if not target_table: return []
    dt = dmy2ymd(dt)

    kurs = dict()
    parts = target_table.find_all('td')
    for n, part in enumerate(parts):
        if part.text in currency_list:
            kurs[part.text] = float(parts[n+2].text.replace(',', '.'))

    result = []
    for symbol in kurs:
        result.append([kurs[symbol], dt, symbol])
    return result

if __name__ == '__main__':

    table = 'cp_currency_curs'

    today = datetime.now()
    yesterday = today - timedelta(days = 1)
    target_date = f'{yesterday:%Y%m%d}'

    sources = (
               {'type': 0, 'url': f'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json', 'symbol': 'r030', 'currency': {'USD': 840, 'EUR': 978}, 'value': 'rate', 'date': 'exchangedate', 'key': lambda x: True, 'date_convert': dmy2ymd},
#               {'type': 1, 'url': f'https://bank.gov.ua/NBU_uonia?id_api=REF-SWAP_Swaps&json', 'symbol': '', 'currency': {'USD': '840', 'EUR': '978'}, 'value': '', 'date': '', 'key': lambda x: True, 'date_convert': lambda x: x},
               {'type': 1, 'url': f'https://index.minfin.com.ua/exchange/mb/', 'symbol': '', 'currency': {'USD': 'USD', 'EUR': 'EUR'}, 'value': '', 'date': '', 'key': lambda x: True, 'date_convert': lambda x: x},
               {'type': 2, 'url': f'https://bank.gov.ua/NBUStatService/v1/statdirectory/kursf?json&period=d&date={target_date}', 'symbol': 'r030', 'currency': {'USD': '840', 'EUR': '978'}, 'value': 'value', 'date': 'dt', 'key': lambda x: x['id_api'] == 'AvgKursBuy', 'date_convert': lambda x: x},
              )

    result = []
    for source in sources:

        currency_symbols = list(source['currency'].keys())
        currency_list = list(source['currency'].values())

#        data = load(source['url'], debug = True)
        data = load(source['url'])
        if not '?json' in source['url']:
            res = parse_soup(data, currency_list)
            for r in res:
                print(*r, source['type'])
                result.append([*r, source['type']])
            continue

        for d in data:
            if source['key'](d):
                if d[source['symbol']] in currency_list:
                    symbol = currency_symbols[currency_list.index(d[source['symbol']])]
                    dt = source['date_convert'](d[source['date']])
                    print(d[source['value']], dt, symbol, source['type'])
                    result.append([d[source['value']], dt, symbol, source['type']])

    res2write = []
    for r in result:
        dt = datetime.strptime(r[1], '%Y%m%d')
        log_info(str(r))
        res2write.append([r[0], dt, r[2], r[3]])

    #print(res2write)
    write2database(res2write, table)
