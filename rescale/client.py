import urllib.parse
import json
import time

import requests

class RescaleConnect(object):
    def __init__(self, api_key, root_url='https://platform.rescale.com/api/'):
        self.api_key  = api_key
        self.root_url = root_url
    
    def _paginate(self, url): # Looks like this will not work properly if people are deleting things.
        response = self._request('GET', url).json()
        while True:
            for r in response['results']:
                yield r
            if not response['next']:
                return
            response = self._request('GET', response['next']).json()

    def _request(self, method, relative_url, content_type='application/json',
                 **kwargs):
        headers = {'Authorization': 'Token ' + self.api_key}
        if not 'files' in kwargs:
            headers['Content-Type'] = content_type

        response = requests.request(method, urllib.parse.urljoin(self.root_url, relative_url),
                                    headers=headers, **kwargs)
        response.raise_for_status()
        return response

class RescaleFile(RescaleConnect):
    def __init__(self, api_key, root_url, json_data=None, id=None):
        self.api_key  = api_key
        self.root_url = root_url
        
        if json_data is not None:
            self._populate(json_data)
        elif id is not None:
            pass #TODO: fetch data and populate
        else:
            raise Exception
        
    def _populate(self, json_data):
        self.raw_data = json_data
        
        for attribute in ['id', 'name', 'decryptedSize', 'path']:
            setattr(self, attribute, json_data[attribute])
        
    def download(self, target=None):  #TODO: can be async with a class counter
        response = self._request('GET', 'files/{file_id}/contents/'.format(file_id=self.id),
                                 stream=True)

        if not target:
            target = self.name
        with open(target, 'wb') as fp:
            for chunk in response.iter_content(8192):
                fp.write(chunk)
        
class RescaleJob(RescaleConnect):
    def __init__(self, api_key, root_url, json_data=None, id=None):
        self.api_key  = api_key
        self.root_url = root_url
        
        if json_data is not None:
            self._populate(json_data)
        elif id is not None:
            pass #TODO: fetch data and populate
        else:
            raise Exception
        
    def _populate(self, json_data):
        self.raw_data = json_data
        
        for attribute in ['id', 'name']:
            setattr(self, attribute, json_data[attribute])
    
    def get_statuses(self):
        return self._paginate('jobs/{job_id}/statuses/'.format(job_id=self.id))
    
    def get_latest_status(self):
        return next(self.get_statuses(), None)

    def get_files(self):
        for json_data in self._paginate('jobs/{job_id}/files/'.format(job_id=self.id)):
            yield RescaleFile(self.api_key, self.root_url, json_data)        

    def submit_job(self):
        return self._request('POST', 'jobs/{job_id}/submit/'.format(job_id=self.id))
    
    def wait(self, refresh_rate=60):
        while True:
            time.sleep(refresh_rate)
            self.get_latest_status()['status'] == 'Completed'
            return
             




class RescaleClient(RescaleConnect):
    def upload_file(self, file_path):
        # will fail on large files: http://docs.python-requests.org/en/latest/user/quickstart/#post-a-multipart-encoded-file
        with open(file_path, 'rb') as fp:
            json_data = self._request('PUT',
                                      'files/contents/', files={'file': fp}).json()
            return RescaleFile(self.api_key, self.root_url, json_data)
        
    def create_job(self, data):
        json_data = self._request('POST', 'jobs/', data=json.dumps(data)).json()
        return RescaleJob(self.api_key, self.root_url, json_data)
    
    