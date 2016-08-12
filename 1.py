# -*- coding: utf-8 -*-

from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
from requests import Session
from scrapy.selector import Selector

options = {
    'cache.data_dir': '.cache/data',
    'cache.lock_dir': '.cache/lock',
    'cache.type': 'file',
}
cache = CacheManager(**parse_cache_config_options(options))


def get_proxies():
    proxies = {}
    return proxies


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
    city = selector.xpath(u'//option[contains(text(), "{city:s}")]/@value'.format(city=city)).extract()
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
    lines = selector.xpath(u'//td[@class="label "][@colspan="5"]/text()').extract()
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

details = get_details(u'Fang', 1, 3961, u'Vissoie')

from pprint import pprint
pprint(details)
