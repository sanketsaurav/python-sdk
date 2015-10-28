import logging
import re
import requests
import urllib.parse


API_TOKEN = '58f4afb7059681c5f5b7dc744116af1a6a8a3e87'
#API_TOKEN = 'c945f743e2a2cecf0ede0db230533614f481176a'
API_BASE_URL = 'https://platform.rescale.com/'
MAX_PAGES = 1
PAGE_SIZE = 100

logging.basicConfig(level=logging.INFO)


def get_session():
    session = requests.Session()
    session.headers.update({'Authorization': 'Token {0}'.format(API_TOKEN)})
    return session


def get_url(path):
    return urllib.parse.urljoin(API_BASE_URL, path)


def upload_file(local_file, session):
    logging.info('Uploading {0}'.format(local_file))
    result = session.post(get_url('api/v3/files/contents/'),
                          files={'file': open(local_file, 'rb')})
    result.raise_for_status()
    logging.info('{0}: {1}'.format(local_file, result))
    return result.json()


def get_paginated_results(path, session):
    records = []
    next = get_url(path + '?page=1&page_size{0}'.format(PAGE_SIZE))
    for i in range(MAX_PAGES):
        if not next:
            break
        logging.debug('Paginated {0}'.format(next))
        result = session.get(next)
        result.raise_for_status()
        json = result.json()
        next = json['next']
        records += json['results']

    return records


def strip_suffix(filename):
    compress_suffixes = ['\.tar\.gz$', '\.zip$', '\.tar\.xz$', '\.tar\.bz2$']
    for suffix in compress_suffixes:
        base = re.sub(suffix, '', filename)
        if base != filename:
            return base
    return filename
