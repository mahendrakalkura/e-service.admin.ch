# -*- coding: utf-8 -*-

from contextlib import closing
from io import open
from logging import basicConfig, WARN
from sys import argv

from backports import csv
from psycopg2.extensions import register_type, UNICODE, UNICODEARRAY
from pyres import ResQ
from pyres.horde import Khan
from simplejson import dumps
from tqdm import tqdm

from settings import PYRES
from utilities import (
    get_city,
    get_connection,
    get_details,
    get_sentry,
    get_total,
)

basicConfig(level=WARN)

register_type(UNICODE)
register_type(UNICODEARRAY)


class Record():

    queue = 'records'

    @staticmethod
    def perform(id):
        try:
            with closing(get_connection()) as connection:
                with closing(connection.cursor()) as cursor:
                    query = 'SELECT * FROM records WHERE id = %(id)s'
                    cursor.execute(
                        query,
                        {
                            'id': id,
                        },
                    )
                    record = cursor.fetchone()
                    if record['details']:
                        return
                    details = get_details(
                        record['road'],
                        record['number'],
                        record['zip_code'],
                        record['city_new'],
                    )
                    if not details:
                        return
                    details = dumps(
                        details,
                        ensure_ascii=False,
                        indent='    ',
                        sort_keys=True,
                    )
                    query = '''
                    UPDATE records
                    SET details = %(details)s
                    WHERE id = %(id)s
                    '''
                    cursor.execute(
                        query,
                        {
                            'details': details,
                            'id': id,
                        },
                    )
                    connection.commit()
        except Exception:
            sentry = get_sentry()
            sentry.captureException()


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
                    city_old CHARACTER VARYING(255) NOT NULL,
                    city_new CHARACTER VARYING(255) NULL,
                    details json NULL
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
            CREATE INDEX records_city_old ON records USING btree (city_old)
            '''
            cursor.execute(query)

            query = '''
            CREATE INDEX records_city_new ON records USING btree (city_new)
            '''
            cursor.execute(query)

            connection.commit()


def refresh():
    with closing(get_connection()) as connection:
        total = get_total('records.csv')
        with open(
            'records.csv',
            'r',
            encoding='iso-8859-1',
            newline='',
        ) as resource:
            rows = csv.reader(resource, delimiter=u';')
            for row in tqdm(rows, total=total):
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
                        city_old = %(city_old)s
                    '''
                    cursor.execute(
                        query,
                        {
                            'road': row[2],
                            'number': row[3],
                            'zip_code': row[0],
                            'city_old': row[1],
                        },
                    )
                    record = cursor.fetchone()
                    if record:
                        continue
                    query = '''
                    INSERT INTO records (road, number, zip_code, city_old)
                        VALUES (%(road)s, %(number)s, %(zip_code)s, %(city_old)s)
                    '''
                    cursor.execute(
                        query,
                        {
                            'road': row[2],
                            'number': row[3],
                            'zip_code': row[0],
                            'city_old': row[1],
                        },
                    )
                    connection.commit()


def process_1():
    with closing(get_connection()) as connection:
        with closing(connection.cursor()) as cursor:
            items = {}
            query = '''
            SELECT DISTINCT zip_code, city_old
            FROM records
            WHERE city_new IS NULL
            '''
            cursor.execute(query)
            records = cursor.fetchall()
            for record in records:
                if record['zip_code'] not in items:
                    items[record['zip_code']] = []
                items[record['zip_code']].append(record['city_old'])
            for zip_code, cities_old in tqdm(items.items()):
                cities = get_city(zip_code, cities_old)
                if not cities:
                    continue
                for city_old, city_new in cities.items():
                    query = '''
                    UPDATE records
                    SET city_new = %(city_new)s
                    WHERE zip_code = %(zip_code)s AND city_old = %(city_old)s
                    '''
                    cursor.execute(
                        query,
                        {
                            'city_new': city_new,
                            'zip_code': zip_code,
                            'city_old': city_old,
                        },
                    )
                connection.commit()


def process_2():
    r = ResQ()
    with closing(get_connection()) as connection:
        total = 0
        with closing(connection.cursor()) as cursor:
            query = '''
            SELECT COUNT(id) AS count
            FROM records
            WHERE city_new IS NOT NULL AND details IS NULL
            '''
            cursor.execute(query)
            total = cursor.fetchone()['count']
        with closing(connection.cursor('cursor')) as cursor:
            query = '''
            SELECT *
            FROM records
            WHERE city_new IS NOT NULL AND details IS NULL
            '''
            cursor.execute(query)
            for record in tqdm(cursor, total=total):
                r.enqueue(Record, record['id'])


def workers():
    workers = Khan(pool_size=PYRES, queues=['records'])
    workers.work()

if __name__ == '__main__':
    try:
        if argv[1] == 'bootstrap':
            bootstrap()
        if argv[1] == 'refresh':
            refresh()
        if argv[1] == 'process_1':
            process_1()
        if argv[1] == 'process_2':
            process_2()
        if argv[1] == 'workers':
            workers()
    except KeyboardInterrupt:
        pass
    except Exception:
        sentry = get_sentry()
        sentry.captureException()
