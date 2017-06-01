import argparse
import ConfigParser
import json
import logging
import time
import os

import requests

try:
    # python 3 required
    import urllib.parse
except ImportError:
    # monkeypatch for python 2 compat
    import urllib
    import urlparse
    urlparse.urlencode = urllib.urlencode
    urllib.parse = urlparse

API_CONFIG_FILE = '~/.config/rescale/apiconfig'
DEFAULT_API_URL = 'https://platform.rescale.com/api/v3/'


class RescaleConfig(object):

    def __init__(self, profile='default'):
        parser = argparse.ArgumentParser()
        parser.add_argument('--profile', default=profile)
        args = parser.parse_args()
        self.profile = args.profile
        self.config = ConfigParser.ConfigParser()
        self.config.read([os.path.expanduser(API_CONFIG_FILE)])
        if not self.config.has_section(self.profile):
            raise ValueError('Unknown profile name: ' + self.profile)

    def apikey(self):
        try:
            return os.environ['RESCALE_API_KEY']
        except:
            try:
                return self.config.get(self.profile, 'apikey')
            except:
                return None

    def apiurl(self):
        try:
            return os.environ['RESCALE_API_URL']
        except:
            try:
                url = self.config.get(self.profile, 'apiurl')
                return url if url else DEFAULT_API_URL
            except:
                return DEFAULT_API_URL


class RescaleConnect(object):

    def __init__(self):
        config = RescaleConfig()
        self.api_key = config.apikey()
        self._root_url = config.apiurl()
        self._page_size = 100

    def __repr__(self):
        return json.dumps(self._raw_data, sort_keys=True,
                          indent=4, separators=(',', ': '))

    def _populate(self, json_data):
        self._raw_data = json_data

        for attribute_name, attribute_data in json_data.items():
            setattr(self, attribute_name, attribute_data)

    # Looks like this will not work properly if people are deleting things.
    def _paginate(self, url):
        connector = '&' if '?' in url else '?'
        response = self._request('GET', '{url}{connector}page_size={page_size}'.format(
            url=url, connector=connector, page_size=self._page_size)).json()
        while True:
            for r in response['results']:
                yield r
            if not response['next']:
                return
            response = self._request('GET', response['next']).json()

    def _request(self, method, relative_url,
                 **kwargs):
        headers = {'Authorization': 'Token ' + self.api_key}
        if 'files' not in kwargs:
            headers['Content-Type'] = 'application/json'

        response = requests.request(method,
                                    urllib.parse.urljoin(
                                        self._root_url, relative_url),
                                    headers=headers,
                                    **kwargs)
        try:
            response.raise_for_status()
        except Exception as e:
            logging.error(response.content)
            raise e
        return response

    @staticmethod
    def get_core_types():
        return [{'name': ct['name'], 'code': ct['code']} for ct in
                RescaleConnect()._paginate('coretypes/')]


class RescaleFile(RescaleConnect):

    def __init__(self, api_key=None, id=None, file_path=None, json_data=None):
        super(RescaleFile, self).__init__()
        self.api_key = api_key or self.api_key

        if id is not None:
            json_data = self._request('GET', 'files/{id}'.format(id=id)).json()

        if file_path is not None:
            json_data = self._upload_file(file_path)
            self.name = os.path.basename(file_path)

        if json_data is not None:
            self._populate(json_data)

    def _upload_file(self, file_path):
        # will fail on large files:
        # http://docs.python-requests.org/en/latest/user/quickstart/#post-a-multipart-encoded-file
        with open(file_path, 'rb') as fp:
            json_data = self._request('PUT',
                                      'files/contents/', files={'file': fp}).json()
            return json_data

    def download(self, target=None):  # TODO: can be async with a class counter
        response = self._request('GET', 'files/{file_id}/contents/'.format(file_id=self.id),
                                 stream=True)
        if not target:
            target = self.name
        with open(target, 'wb') as fp:
            for chunk in response.iter_content(8192):
                fp.write(chunk)

    @staticmethod
    def search(name):
        query = urllib.parse.urlencode((('search', name),))
        for json_data in RescaleConnect()._paginate('files/?{0}'.format(query)):
            yield RescaleFile(json_data=json_data)

    @staticmethod
    def get_newest_by_name(name):
        return next(RescaleFile.search(name), None)


class RescaleJob(RescaleConnect):

    def __init__(self, api_key=None, id=None, json_data=None):
        super(RescaleJob, self).__init__()
        self.api_key = api_key or self.api_key

        if id is not None:
            self._populate(self._request(
                'GET', 'jobs/{id}'.format(id=id)).json())

        if json_data is not None:
            self._populate(self._request('POST',
                                         'jobs/', data=json.dumps(json_data)).json())

    def get_statuses(self):
        return self._paginate('jobs/{job_id}/statuses/'.format(job_id=self.id))

    def get_latest_status(self):
        return next(self.get_statuses(), None)

    def get_files(self):
        for json_data in self._paginate('jobs/{job_id}/files/'.format(job_id=self.id)):
            yield RescaleFile(self.api_key, json_data=json_data)

    def get_file(self, name):
        query = urllib.parse.urlencode((('search', name),))
        results = self._paginate('jobs/{job_id}/files/?{query}'
                                 .format(job_id=self.id, query=query))
        return next(results, None)

    def submit(self):
        return self._request('POST', 'jobs/{job_id}/submit/'.format(job_id=self.id))

    def wait(self, refresh_rate=60):
        while not self.get_latest_status()['status'] == 'Completed':
            time.sleep(refresh_rate)
