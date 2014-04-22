import requests
import urlparse
import json


class RescaleClient(object):
    def __init__(self, api_key, url='https://platform.rescale.com'):
        self.api_key = api_key
        self.url = url

    def upload_file(self, fp):
        return self._request(
            'PUT', 'api/files/contents/', files={'file': fp}).json()

    def download_file(self, file_id):
        return self._request(
            'GET', 'api/files/{file_id}/contents/'.format(file_id=file_id),
            stream=True)

    def create_job(self, data):
        return self._request('POST', 'api/jobs/', data=json.dumps(data)).json()

    def submit_job(self, job_id):
        return self._request(
            'POST', 'api/jobs/{job_id}/submit/'.format(job_id=job_id))

    def get_status(self, job_id):
        return self._paginate(
            'api/jobs/{job_id}/statuses/'.format(job_id=job_id))

    def get_files(self, job_id):
        return self._paginate(
            'api/jobs/{job_id}/files/'.format(job_id=job_id))

    def _paginate(self, url):
        response = self._request('GET', url).json()
        while True:
            for r in response['results']:
                yield r
            if not response['next']:
                return
            response = self._request('GET', response['next']).json()

    def _request(self, method, path, content_type='application/json',
                 **kwargs):
        headers = {'Authorization': 'Token ' + self.api_key}
        if not 'files' in kwargs:
            headers['Content-Type'] = content_type

        response = requests.request(method, urlparse.urljoin(self.url, path),
                                    headers=headers, **kwargs)
        response.raise_for_status()
        return response
