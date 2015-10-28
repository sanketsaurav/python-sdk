#!/usr/bin/env python3

import logging
import os
import re
import sys
import testlib
from rescale.client import RescaleConnect, RescaleFile, RescaleJob

BASE_JOB_RE = re.compile('^build[0-9\.]+-testcase[0-9\.]+$')
DRY_RUN = True


def get_base_test_job_ids():
    job_results = RescaleConnect()._paginate('jobs/')
    return {job['name']: job['id'] for job in job_results
            if BASE_JOB_RE.match(job['name'])}


def get_job(job_id):
    job_result = RescaleConnect()._request('GET', 'jobs/{0}'.format(job_id))
    return job_result.json()


def create_delta_job(base_name, delta_name, job_id, delta_id):
    new_name = '{0}-{1}'.format(base_name, delta_name)
    job = get_job(job_id)

    for job_analysis in job['jobanalyses']:
        job_analysis['inputFiles'].append({'id': delta_id})

    job['name'] = new_name

    return RescaleJob(json_data=job)


if __name__ == '__main__':
    logging.info('Starting delta test runs')
    if len(sys.argv) < 2:
        print('Usage: {0} DELTA_ARCHIVE'.format(sys.argv[0]))
        sys.exit(1)

    build_delta_archive = sys.argv[1]
    if not os.path.isfile(build_delta_archive):
        print('Build delta archive does not exist or is not a file')
        sys.exit(1)

    delta_info = RescaleFile(file_path=build_delta_archive)

    for base_name, job_id in get_base_test_job_ids().items():
        print(base_name)
        new_job = create_delta_job(base_name,
                                   testlib.strip_suffix(delta_info.name),
                                   job_id,
                                   delta_info.id)
        if not DRY_RUN:
            new_job.submit()
