import urllib.parse
import json
import time
import os

import requests


class RescaleConnect(object):
    try:
        api_key = os.environ['RESCALE_API_KEY']
    except:
        api_key = None
    _root_url = 'https://platform.rescale.com/api/v3/'
    _page_size = 100

    def __repr__(self):
        return json.dumps(self._raw_data, sort_keys=True,
                          indent=4, separators=(',', ': '))

    def _populate(self, json_data):
        self._raw_data = json_data

        for attribute_name, attribute_data in json_data.items():
            setattr(self, attribute_name, attribute_data)

    # Looks like this will not work properly if people are deleting things.
    def _paginate(self, url):
        response = self._request('GET', '{url}?page_size={page_size}'.format(
            url=url, page_size=self._page_size)).json()
        while True:
            for r in response['results']:
                yield r
            if not response['next']:
                return
            response = self._request('GET', response['next']).json()

    def _request(self, method, relative_url,
                 **kwargs):
        headers = {'Authorization': 'Token ' + self.api_key}
        if not 'files' in kwargs:
            headers['Content-Type'] = 'application/json'

        response = requests.request(method,
                                    urllib.parse.urljoin(
                                        self._root_url, relative_url),
                                    headers=headers,
                                    **kwargs)
        response.raise_for_status()
        return response


class RescaleFile(RescaleConnect):

    def __init__(self, api_key=None, id=None, file_path=None, json_data=None):
        self.api_key  = api_key or RescaleConnect.api_key
        
        if id is not None:
            json_data = self._request('GET', 'files/{id}'.format(id=id)).json()

        if file_path is not None:
            json_data = self._upload_file(file_path)

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


class RescaleJob(RescaleConnect):

    def __init__(self, api_key=None, id=None, json_data=None):
        self.api_key = api_key or RescaleConnect.api_key

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

    def submit(self):
        return self._request('POST', 'jobs/{job_id}/submit/'.format(job_id=self.id))

    def wait(self, refresh_rate=60):
        while not self.get_latest_status()['status'] == 'Completed':
            time.sleep(refresh_rate)
