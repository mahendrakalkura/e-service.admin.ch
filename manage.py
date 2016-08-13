# -*- coding: utf-8 -*-

from contextlib import closing
from io import open
from pprint import pprint
from random import choice
from sys import argv

from backports import csv
from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
from psycopg2 import connect
from psycopg2.extensions import register_type, UNICODE, UNICODEARRAY
from psycopg2.extras import DictCursor
from requests import Session
from scrapy.selector import Selector
from tqdm import tqdm

from settings import INSTANTPROXIES_COM, POSTGRESQL

options = {
    'cache.data_dir': '.cache/data',
    'cache.lock_dir': '.cache/lock',
    'cache.type': 'file',
}
cache = CacheManager(**parse_cache_config_options(options))

register_type(UNICODE)
register_type(UNICODEARRAY)


@cache.cache('get_details', expire=86400)
def get_details(road, number, zip_code, city):
    session = Session()

    response = session.request(
        'GET',
        'https://www.e-service.admin.ch/eschkg/app/forward.do',
        params={
            'forward': 'zustaendigkeit',
            'navId': 'zustaendigkeit',
        },
        proxies=get_proxies(),
    )

    response = session.request(
        'GET',
        'https://www.e-service.admin.ch/eschkg/app/ajax_locality',
        params={
            'postCode': zip_code,
        },
        proxies=get_proxies(),
    )

    selector = Selector(text=response.text)
    city = selector.xpath(
        u'//option[contains(text(), "{city:s}")]/@value'.format(city=city),
    ).extract()
    if not city:
        return
    response = session.request(
        'POST',
        'https://www.e-service.admin.ch/eschkg/app/wizard/navigate.do',
        data={
            'countryCode': '',
            'enable_validation': 'off',
            'enable_validation': 'on',
            'isSwissAddress': 'true',
            'number': number,
            'onrp': city[0],
            'org.apache.struts.taglib.html.SUBMIT': 'zurueck',
            'postCode': zip_code,
            'street': road,
            'tabindex': '11',
        },
        proxies=get_proxies(),
    )

    details = {
        'address': [],
        'phone': '',
        'fax': '',
        'email': '',
    }

    selector = Selector(text=response.text)
    lines = selector.xpath(
        u'//td[@class="label "][@colspan="5"]/text()',
    ).extract()
    for line in lines:
        if 'Telefon:' in line:
            details['phone'] = line.replace('Telefon:', '').strip()
            continue
        if 'Telefax: ' in line:
            details['fax'] = line.replace('Telefax:', '').strip()
            continue
        if 'E-Mail: ' in line:
            details['email'] = line.replace('E-Mail:', '').strip()
            continue
        details['address'].append(line.strip())
    return details


def bootstrap():
    with closing(get_connection()) as connection:
        with closing(connection.cursor()) as cursor:
            query = 'DROP SCHEMA IF EXISTS public CASCADE'
            cursor.execute(query)

            query = 'CREATE SCHEMA IF NOT EXISTS public'
            cursor.execute(query)

            connection.commit()

        with closing(connection.cursor()) as cursor:
            query = '''
            CREATE TABLE IF NOT EXISTS records
                (
                    id INTEGER NOT NULL,
                    road CHARACTER VARYING(255) NOT NULL,
                    number CHARACTER VARYING(255) NOT NULL,
                    zip_code CHARACTER VARYING(255) NOT NULL,
                    city CHARACTER VARYING(255) NOT NULL,
                    contents jsonb NULL
                )
            '''
            cursor.execute(query)

            query = '''
            CREATE SEQUENCE records_id_sequence
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1
            '''
            cursor.execute(query)

            query = '''
            ALTER TABLE records
                ALTER COLUMN id
                SET DEFAULT nextval(\'records_id_sequence\'::regclass)
            '''
            cursor.execute(query)

            query = '''
            ALTER TABLE records
                ADD CONSTRAINT records_id_constraint
                PRIMARY KEY (id)
            '''
            cursor.execute(query)

            query = '''
            CREATE INDEX records_road ON records USING btree (road)
            '''
            cursor.execute(query)

            query = '''
            CREATE INDEX records_number ON records USING btree (number)
            '''
            cursor.execute(query)

            query = '''
            CREATE INDEX records_zip_code ON records USING btree (zip_code)
            '''
            cursor.execute(query)

            query = '''
            CREATE INDEX records_city ON records USING btree (city)
            '''
            cursor.execute(query)

            query = '''
            CREATE INDEX records_contents ON records USING btree (contents)
            '''
            cursor.execute(query)

            connection.commit()

        with open(
            'records.csv',
            'r',
            encoding='iso-8859-1',
            newline='',
        ) as resource:
            rows = csv.reader(resource, delimiter=u';')
            for row in tqdm(rows, total=1815462):
                with closing(connection.cursor()) as cursor:
                    query = '''
                    SELECT id
                    FROM records
                    WHERE
                        road = %(road)s
                        AND
                        number = %(number)s
                        AND
                        zip_code = %(zip_code)s
                        AND
                        city = %(city)s
                    '''
                    cursor.execute(
                        query,
                        {
                            'road': row[2],
                            'number': row[3],
                            'zip_code': row[0],
                            'city': row[1],
                        },
                    )
                    record = cursor.fetchone()
                    if record:
                        continue
                    query = '''
                    INSERT INTO records (road, number, zip_code, city)
                        VALUES (%(road)s, %(number)s, %(zip_code)s, %(city)s)
                    '''
                    cursor.execute(
                        query,
                        {
                            'road': row[2],
                            'number': row[3],
                            'zip_code': row[0],
                            'city': row[1],
                        },
                    )
                    connection.commit()


def get_connection():
    connection = connect(
        host=POSTGRESQL['host'],
        port=POSTGRESQL['port'],
        user=POSTGRESQL['user'],
        password=POSTGRESQL['password'],
        database=POSTGRESQL['database'],
        cursor_factory=DictCursor,
    )
    return connection


def get_proxies():

    @cache.cache('get_proxies', expire=86400)
    def get_proxies():
        session = Session()
        response = session.request(
            'POST',
            'http://admin.instantproxies.com/login_do.php',
            data={
                'username': INSTANTPROXIES_COM['username'],
                'password': INSTANTPROXIES_COM['password'],
            },
        )
        selector = Selector(text=response.text)
        textarea = selector.xpath(
            u'//textarea[@id="proxies-textarea"]/text()',
        ).extract()
        textarea = textarea[0]
        textarea = textarea.split('\n')
        textarea = map(lambda item: item.strip(), textarea)
        textarea = filter(None, textarea)
        return textarea

    proxies = get_proxies()
    proxy = choice(proxies)
    return {
        'http': 'http://{proxy:s}'.format(proxy=proxy),
        'https': 'http://{proxy:s}'.format(proxy=proxy),
    }

if __name__ == '__main__':
    if argv[1] == 'bootstrap':
        bootstrap()
    if argv[1] == 'get_details':
        details = get_details(u'Fang', 1, 3961, u'Vissoie')
        pprint(details)
    if argv[1] == 'get_proxies':
        proxies = get_proxies()
        pprint(proxies)
