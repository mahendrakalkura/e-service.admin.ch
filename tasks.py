# -*- coding: utf-8 -*-

from contextlib import closing
from logging import WARN

from celery import Celery
from kombu import Exchange, Queue
from raven.contrib.celery import register_signal, register_logger_signal
from simplejson import dumps

from settings import CELERY
from utilities import get_connection, get_details, get_sentry

celery = Celery(__name__)
celery.conf.update(
    BROKER=CELERY,
    BROKER_POOL_LIMIT=0,
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_ACKS_LATE=True,
    CELERY_QUEUES=(
        Queue(
            'e-service.admin.ch',
            Exchange('e-service.admin.ch'),
            routing_key='e-service.admin.ch',
        ),
    ),
    CELERY_IGNORE_RESULT=True,
    CELERY_RESULT_SERIALIZER='json',
    CELERY_ROUTES={
        'e-service.admin.ch': {
            'queue': 'e-service.admin.ch',
        },
    },
    CELERY_TASK_SERIALIZER='json',
    CELERYD_LOG_FORMAT='[%(asctime)s: %(levelname)s] %(message)s',
    CELERYD_POOL_RESTARTS=True,
    CELERYD_PREFETCH_MULTIPLIER=1,
    CELERYD_TASK_SOFT_TIME_LIMIT=30,
    CELERYD_TASK_TIME_LIMIT=60,
)

sentry = get_sentry()
register_logger_signal(sentry, loglevel=WARN)
register_signal(sentry)


@celery.task
def process(id):
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
                record['city'],
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
