# -*- coding: utf-8 -*-

from contextlib import closing
from io import open
from sys import argv

from backports import csv
from psycopg2.extensions import register_type, UNICODE, UNICODEARRAY
from tqdm import tqdm

from tasks import celery as celery_
from utilities import get_connection, get_sentry, get_total

register_type(UNICODE)
register_type(UNICODEARRAY)

sentry = get_sentry()


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
            CREATE INDEX records_city ON records USING btree (city)
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


def process():
    with closing(get_connection()) as connection:
        total = 0
        with closing(connection.cursor()) as cursor:
            query = '''
            SELECT COUNT(id) AS count
            FROM records
            WHERE details IS NULL
            '''
            cursor.execute(query)
            total = cursor.fetchone()['count']

        with closing(connection.cursor('cursor')) as cursor:
            query = 'SELECT * FROM records WHERE details IS NULL'
            cursor.execute(query)
            for record in tqdm(cursor, total=total):
                celery_.send_task(
                    'tasks.process',
                    (record['id'],),
                    queue='e-service.admin.ch',
                    serializer='json',
                )

if __name__ == '__main__':
    try:
        if argv[1] == 'bootstrap':
            bootstrap()
        if argv[1] == 'refresh':
            refresh()
        if argv[1] == 'process':
            process()
    except KeyboardInterrupt:
        pass
    except Exception:
        sentry.captureException()
