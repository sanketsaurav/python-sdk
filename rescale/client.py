from datetime import datetime
try:
    import ConfigParser as configparser
except ImportError:
    import configparser
import dateutil.parser
import json
import logging
import math
import pytz
import time
import os
import re

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

logger = logging.getLogger(__name__)

API_CONFIG_FILE = '~/.config/rescale/apiconfig'
DEFAULT_API_URL = 'https://platform.rescale.com/api/v3/'


class RescaleConfig(object):

    def __init__(self, profile=None):
        if not profile:
            profile = 'default'
        self.profile = profile
        self.config = configparser.ConfigParser()
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

    def __init__(self, config=None, attempts=None):
        if config is None:
            config = RescaleConfig()
        self.config = config
        self.api_key = config.apikey()
        self._root_url = config.apiurl()
        self._page_size = 100
        self._attempts = attempts or 1
        self._retry_delay = 30

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
            for r in response.get('results', response.get('result')):
                yield r
            if not response['next']:
                return
            response = self._request('GET', response['next']).json()

    def _request(self, method, relative_url,
                 **kwargs):
        headers = {'Authorization': 'Token ' + self.api_key}
        if 'files' not in kwargs:
            headers['Content-Type'] = 'application/json'

        response = None
        last_error = None
        for i in range(self._attempts):
            response = requests.request(method,
                                        urllib.parse.urljoin(
                                        self._root_url, relative_url),
                                        headers=headers,
                                        **kwargs)
            try:
                response.raise_for_status()
            except Exception as e:
                logger.exception('Error on attempt %s, %s',
                                 i, response.content)
                last_error = e
                time.sleep(30)
            else:
                last_error = None
                break

        if last_error:
            raise last_error

        return response

    @staticmethod
    def get_core_types():
        return [{'name': ct['name'], 'code': ct['code']} for ct in
                RescaleConnect()._paginate('coretypes/')]


class RescaleFile(RescaleConnect):

    def __init__(self, api_key=None, id=None, file_path=None, json_data=None, config=None):
        super(RescaleFile, self).__init__(config=config)
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
    def search(name, config=None):
        query = urllib.parse.urlencode((('search', name),))
        for json_data in RescaleConnect(config=config)._paginate('files/?{0}'.format(query)):
            yield RescaleFile(json_data=json_data)

    @staticmethod
    def get_newest_by_name(name, config=None):
        return next(RescaleFile.search(name, config=config), None)


class RescaleJob(RescaleConnect):

    def __init__(self, api_key=None, id=None, json_data=None, config=None, **kwargs):
        super(RescaleJob, self).__init__(config=config, **kwargs)
        self.api_key = api_key or self.api_key

        if id is not None:
            self._populate(self._request(
                'GET', 'jobs/{id}/'.format(id=id)).json())

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

    def get_clusters(self):
        clusters = []
        response = self._paginate('jobs/{job_id}/clusters/'.format(job_id=self.id)),

        for r in response:
            for json_data in r:
                cluster = RescaleCluster(api_key=self.api_key)
                cluster.id = json_data.get('id')
                clusters.append(
                    cluster
                )
        return clusters

    def get_file(self, name):
        query = urllib.parse.urlencode((('search', name),))
        results = self._paginate('jobs/{job_id}/files/?{query}'
                                 .format(job_id=self.id, query=query))
        return next(results, None)

    def submit(self):
        return self._request('POST', 'jobs/{job_id}/submit/'.format(job_id=self.id))

    def delete(self):
        return self._request('DELETE', 'jobs/{job_id}/'.format(job_id=self.id))

    def wait(self, refresh_rate=60):
        self.wait_for_completed()

    def userlogs(self):
        uri = 'jobs/{job_id}/logs/?limit=10000'.format(job_id=self.id)
        return self._request('GET', uri).json()

    def connection_info(self):
        SSH_LOG_RE = re.compile('.*Command: ssh -i </path/to/key> (\S+);.*')
        info = set()
        for log in self.userlogs():
            m = SSH_LOG_RE.match(log['message'])
            if m:
                info.add(m.group(1))
        return info

    def get_all_metrics(self, period=300):
        statuses = self.get_statuses()
        start_date = min({dateutil.parser.parse(s['statusDate'])
                          for s in statuses})
        if 'Completed' in [s['status'] for s in statuses]:
            end_date = max({dateutil.parser.parse(s['statusDate'])
                            for s in statuses})
        else:
            end_date = datetime.now(tz=pytz.utc)
        offset = math.ceil((end_date - start_date).total_seconds() / 3600)

        task = self._request('GET', 'jobs/{job_id}/servers/load/'
                             '?offset={offset}&period={period}'
                             .format(job_id=self.id,
                                     offset=offset,
                                     period=period)).json()
        tasks = [task['token']]
        print('fetching {0} pages of metrics'.format(task['totalPages']))
        for page in range(1, task['totalPages']):
            tasks.append(self._request('GET', 'jobs/{job_id}/servers/load/'
                                       '?offset={offset}'
                                       '&period={period}'
                                       '&p={page}'
                                       .format(job_id=self.id,
                                               offset=offset,
                                               page=page,
                                               period=period)).json()['token'])
        results = []
        for task in tasks:
            print('getting task {0}'.format(task))
            status = {'ready': False}
            while not status['ready']:
                status = self._request('GET',
                                       'tasks/{taskid}/'
                                       .format(taskid=task)).json()
                time.sleep(10)
            results += status['result']
        return results

    def wait_for_status(self, status, refresh_seconds=30):
        latest_status = self.get_latest_status()
        while latest_status is None or \
                latest_status['status'] != status:
            time.sleep(refresh_seconds)
            latest_status = self.get_latest_status()
        return self

    def wait_for_executing(self):
        return self.wait_for_status('Executing')

    def wait_for_completed(self):
        return self.wait_for_status('Completed')

    def wait_for_cluster_shutdown(self):
        clusters = self.get_clusters()
        while not all(cluster.is_completed for cluster in clusters):
            time.sleep(30)
            logger.info('Waiting for cluster(s) to terminate.')


class RescaleCluster(RescaleConnect):

    def __init__(self, api_key=None, id=None, json_data=None, config=None):
        super(RescaleCluster, self).__init__(config=config)
        self.api_key = api_key or self.api_key

        if id is not None:
            self._populate(self._request(
                'GET', 'clusters/{id}/'.format(id=id)).json())

        if json_data is not None:
            self._populate(self._request('POST',
                                         'clusters/',
                                         data=json.dumps(json_data)).json())

    def get_statuses(self):
        return self._paginate('clusters/{cluster_id}/statuses/'
                              .format(cluster_id=self.id))

    def get_latest_status(self):
        return next(self.get_statuses(), None)

    @property
    def is_completed(self):
        latest_status = self.get_latest_status()
        if latest_status is not None:
            return latest_status['status'] == 'Stopped'
        else:
            return False


class RescaleStorageDevice(RescaleConnect):

    def __init__(self, api_key=None, id=None, json_data=None, config=None):
        super(RescaleStorageDevice, self).__init__(config=config)
        self.api_key = api_key or self.api_key
        if id is not None:
            self._populate(self._request(
                'GET', 'storage-devices/{id}/'.format(id=id)).json())

        if json_data is not None:
            self._populate(self._request('POST',
                                         'storage-devices/',
                                         data=json.dumps(json_data)).json())

    def get_statuses(self):
        return self._paginate('storage-devices/{id}/statuses/'
                              .format(id=self.id))

    def submit(self):
        self._request('POST',
                      'storage-devices/{id}/submit/'.format(id=self.id))
        return self

    def wait_for_started(self):
        statuses = self.get_statuses()
        while not any(status['status'] == 'Started' for status in statuses):
            statuses = self.get_statuses()
            time.sleep(30)
        return self.refresh()

    def copy_files_to_job(self, job, file_paths=None):
        data = {'storage_device': {'id': self.id}}
        if file_paths:
            input_files = {
                'jobanalyses': [{
                    'order': 0,
                    'input_files': [{
                        'input_file_type': 'COPY',
                        'source_path': path,
                        'name': os.path.basename(path),
                        'decompress': True,
                        'output_path': ''
                    } for path in file_paths]
            }]}
            data.update(input_files)
        self._request('POST',
                'jobs/{job_id}/storage-devices/'.format(job_id=job.id),
                data=json.dumps(data))
        return self

    def upload_cloud_file(self, rescale_file, dest_path):
        if isinstance(rescale_file, str):
            rescale_file = RescaleFile(id=rescale_file, config=self.config)
        data = {'files': [{'id': rescale_file.id}],
                'outputDir': dest_path}
        print(data)
        self._request(
            'POST',
            'storage-devices/{id}/file-downloads/'.format(id=self.id),
            data=json.dumps(data))
        return self

    def upload_local_file(self, local_path, dest_path):
        rescale_file = RescaleFile(file_path=local_path, config=self.config)
        self.upload_cloud_file(rescale_file, dest_path)
        return self

    def refresh(self):
        self._populate(self._request('GET',
                                     'storage-devices/{id}/'
                                     .format(id=self.id)).json())
        return self


def create_storage_device(config,
                          name,
                          size_mb=1000,
                          walltime=4,
                          run_low_pri=False,
                          cores=18):
    sd_def = {'name': name,
              'storage_size_mb': size_mb,
              'walltime': walltime,
              'cores_per_slot': cores}
    return RescaleStorageDevice(json_data=sd_def, config=config)


def list_running_jobs(config=None):
    connection = RescaleConnect(config=config)
    return connection._paginate('jobs/?t=1')


def list_running_clusters(config=None):
    connection = RescaleConnect(config=config)
    return connection._paginate('clusters/')


def list_running_hps(config=None):
    connection = RescaleConnect(config=config)
    return (RescaleStorageDevice(id=hpsjson['id'], config=config)
            for hpsjson in connection._paginate('storage-devices/?active=true'))

