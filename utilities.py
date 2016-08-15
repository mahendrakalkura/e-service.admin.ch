# -*- coding: utf-8 -*-

from io import open
from mmap import mmap
from random import randint

from fake_useragent import UserAgent
from psycopg2 import connect
from psycopg2.extras import DictCursor
from raven import Client
from requests import Session
from scrapy.selector import Selector

from settings import POSTGRESQL, PROXY, SENTRY


def get_details(road, number, zip_code, city):
    proxies = get_proxies()

    user_agent = UserAgent()

    session = Session()

    response = session.request(
        'GET',
        'https://www.e-service.admin.ch/eschkg/app/forward.do',
        headers={
            'User-Agent': user_agent.random,
        },
        params={
            'forward': 'zustaendigkeit',
            'navId': 'zustaendigkeit',
        },
        proxies=proxies,
    )

    response = session.request(
        'GET',
        'https://www.e-service.admin.ch/eschkg/app/ajax_locality',
        headers={
            'User-Agent': user_agent.random,
        },
        params={
            'postCode': zip_code,
        },
        proxies=proxies,
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
        headers={
            'User-Agent': user_agent.random,
        },
        proxies=proxies,
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


def get_proxy():
    if not PROXY:
        return
    return '{hostname:s}:{port:d}'.format(hostname=PROXY['hostname'], port=randint(*PROXY['ports']))


def get_proxies():
    proxy = get_proxy()
    if not proxy:
        return
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
