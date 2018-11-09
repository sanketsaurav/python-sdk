#!/usr/bin/env python

import logging
from rescale import cli, client
import sys

JOB_TEMPLATE = {
    "name": "storage-job-test",
    "jobanalyses": [
        {
            "analysis": {
                "code": "user_included",
                "version": "0"
            },
            "command": "sleep 3000",
            "flags": {
                "igCv": True
            },
            "hardware": {
                "coresPerSlot": 1,
                "slots": 1,
                "coreType": {
                    "code": "hpc-3"
                },
                "walltime": 4,
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


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


def start_storage(config):
    storage = client.create_storage_device(config, 'teststorage')
    storage.submit()
    storage.wait_for_started()
    logger.info('Storage started at %s', storage.sshConnection)
    return storage


def connect_job_to_storage(storage, config):
    job = client.RescaleJob(json_data=JOB_TEMPLATE, config=config)
    storage.connect_to_job(job)
    job.submit()
    job.wait_for_executing()
    logger.info('Job started at %s', job.connection_info())
    return job


if __name__ == '__main__':
    args = cli.get_cli_args()
    config = client.RescaleConfig(profile=args.profile)
    storage = start_storage(config)
    job = connect_job_to_storage(storage, config)
