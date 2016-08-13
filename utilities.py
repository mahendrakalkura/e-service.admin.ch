# -*- coding: utf-8 -*-

from io import open
from mmap import mmap
from random import choice

from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
from psycopg2 import connect
from psycopg2.extras import DictCursor
from raven import Client
from requests import Session
from scrapy.selector import Selector

from settings import INSTANTPROXIES_COM, POSTGRESQL, SENTRY

options = {
    'cache.data_dir': '.cache/data',
    'cache.lock_dir': '.cache/lock',
    'cache.type': 'dbm',
}
cache = CacheManager(**parse_cache_config_options(options))


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
        proxies=get_proxies(False),
    )

    response = session.request(
        'GET',
        'https://www.e-service.admin.ch/eschkg/app/ajax_locality',
        params={
            'postCode': zip_code,
        },
        proxies=get_proxies(False),
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
        proxies=get_proxies(False),
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


def get_proxies(invalidate):

    @cache.cache('get_proxies')
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

    if invalidate:
        cache.invalidate(get_proxies, 'get_proxies')

    proxies = get_proxies()
    proxy = choice(proxies)
    return {
        'http': 'http://{proxy:s}'.format(proxy=proxy),
        'https': 'http://{proxy:s}'.format(proxy=proxy),
    }


def get_sentry():
    sentry = Client(SENTRY)
    return sentry


def get_total(file):
    lines = 0
    with open(file, 'r+') as resource:
        buffer = mmap(resource.fileno(), 0)
        while buffer.readline():
            lines += 1
    return lines
