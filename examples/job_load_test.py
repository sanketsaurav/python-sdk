#!/usr/bin/env python

from collections import defaultdict
from datetime import datetime
from dateutil import parser
import logging
import pandas as pd
import pytz
from rescale import cli, client
import sys
import time


COLUMNS = ['id', 'PendingToExecutingMins', 'ExecutingToCompletedMins',
           'Pending', 'Queued', 'Waiting For Cluster', 'Validated', 'Started',
           'Executing', 'Stopping', 'Completed']
JOB_TEMPLATES = {
    'nasa_overflow': {
            "name": "overflow-{timestamp}",
            "jobanalyses": [
            {
                "analysis": {
                    "code": "nasa_overflow",
                    "version": "2.2n.dp.little.impi"
                },
                "command": "echo test > test; sleep {joblength}",
                "flags": {
                    "igCv": True
                },
                "hardware": {
                    "coresPerSlot": 1,
                    "slots": 1,
                    "coreType": {
                        "code": "hpc-3"
                    },
                    "walltime": 1,
                    "type": "compute"
                },
                "inputFiles": [],
                "onDemandLicenseSeller": None,
                "envVars": None
            }
            ],
            "paramFile": None,
            "jobvariables": [],
            "isLowPriority": True,
            "isTemplateDryRun": False,
            "projectId": None,
            "inputFileParseTask": "",
            "archiveFilters": []
        },
    'fun_3d': {
            "name": "fun3d-{timestamp}",
            "jobanalyses": [
                {
                    "analysis": {
                        "code": "fun_3d",
                        "version": "13.1-intelmpi-icc-15.0.2"
                    },
                    "command": "echo test > test; sleep {joblength}",
                    "flags": {
                        "igCv": True
                    },
                    "hardware": {
                        "coresPerSlot": 1,
                        "slots": 1,
                        "coreType": {
                            "code": "hpc-3"
                        },
                        "walltime": 1,
                        "type": "compute"
                    },
                    "inputFiles": [],
                    "onDemandLicenseSeller": None,
                    "envVars": None
                }
            ],
            "paramFile": None,
            "jobvariables": [],
            "isLowPriority": True,
            "isTemplateDryRun": False,
            "projectId": None,
            "inputFileParseTask": "",
            "archiveFilters": []
        }
}

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


def start_job(config, template):
    job = client.RescaleJob(json_data=template, config=config, attempts=3)
    job.submit()
    return job


def calc_job_metrics(job):
    jobstatuses = {'id': job.id}
    for js in job.get_statuses():
        jobstatuses[js['status']] = parser.parse(js['statusDate'])
        if js['status'] == 'Completed':
            jobstatuses['CompletionReason'] = js['statusReason']
    if 'Pending' in jobstatuses and 'Executing' in jobstatuses:
        jobstatuses['PendingToExecutingMins'] = \
            (jobstatuses['Executing'] - jobstatuses['Pending']).total_seconds() / 60
    if 'Executing' in jobstatuses and 'Completed' in jobstatuses:
        jobstatuses['ExecutingToCompletedMins'] = \
            (jobstatuses['Completed'] - jobstatuses['Executing']).total_seconds() / 60
    return jobstatuses


if __name__ == '__main__':
    start_time = datetime.now(pytz.utc)
    args = cli.get_cli_args(args=(('--job-count', {'type': int}),
                                  ('--job-length-secs', {'type': int}),
                                  ('--start-delay', {'type': int}),
                                  ('--cores-per-slot', {'type': int, 'default': 1}),
                                  ('--delete-all', {'action': 'store_true'})))
    config = client.RescaleConfig(profile=args.profile)
    jobs_by_code = defaultdict(list)
    for i in range(args.job_count):
        jobtemplate = list(JOB_TEMPLATES.items())[i % 2]
        template = jobtemplate[1].copy()
        template['name'] = template['name'].format(timestamp=start_time.isoformat())
        command = template['jobanalyses'][0]['command'].format(joblength=args.job_length_secs)
        template['jobanalyses'][0]['command'] = command
        template['jobanalyses'][0]['hardware']['coresPerSlot'] = args.cores_per_slot
        job = start_job(config, template)
        jobs_by_code[jobtemplate[0]].append(job)
        logger.info('%s submitted, %s jobs so far', job.id, i + 1)
        time.sleep(args.start_delay)

    logger.info('All %s jobs queued, waiting for them to complete', args.job_count)
    job_times = []
    for code, jobs in jobs_by_code.items():
        for job in jobs:
            job.wait_for_completed()
            logger.info('Job %s completed', job.id)
            job_times.append(calc_job_metrics(job))
    df = pd.DataFrame(job_times, columns=COLUMNS)
    fname = '{0}-job-load-test.csv'.format(start_time.isoformat())
    df.to_csv(fname, index=False, columns=COLUMNS)

    if args.delete_all:
        if df.shape[0] == args.job_count:
            for jobs in jobs_by_code.values():
                [j.delete() for j in jobs]
        else:
            logger.warn('Not all jobs have data %s < %s',
                        df.shape[0], args.job_count)
