#!/usr/bin/env python

import copy
import logging
import os.path
import rescale.client
import rescale.cli
import sys
import tempfile
import time

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

SDK_BRANCH = 'cloner-test'
JOB_TEMPLATE = {
    "jobanalyses": [
        {
            "analysis": {
                "code": "user_included",
                "version": "ubuntu"
            },
            "command": "mv .config job_cloner.py ~/; cd ~; virtualenv venv; pip install git+https://github.com/rescale/python-sdk.git@{branch}; python job_cloner.py --from-profile {from_profile} --to-profile {to_profile} --jobid {jobid}",
            "flags": {
                "igCv": True
            },
            "hardware": {
                "coresPerSlot": 2,
                "slots": 1,
                "coreType": {
                    "code": "titanium"
                },
                "walltime": 24,
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

class JobCloner(object):

    def __init__(self, from_profile, to_profile):
        self.input_temp = tempfile.mkdtemp()
        self.from_config = rescale.client.RescaleConfig(profile=from_profile)
        self.to_config = rescale.client.RescaleConfig(profile=to_profile)

    def clone_job(self, jobid):
        from_job = rescale.client.RescaleJob(id=jobid, config=self.from_config)
        logger.info('Retrieved job {0}'.format(from_job.name))

        to_job_data = from_job._raw_data
        target_jobanalyses = []
        for jobanalysis in to_job_data['jobanalyses']:
            target_jobanalysis = jobanalysis
            input_ids = []
            for input_file in jobanalysis['inputFiles']:
                from_file = rescale.client.RescaleFile(config=self.from_config,
                                                       id=input_file['id'])
                logger.info('Transferring file {0}'.format(from_file.name))
                target = '{0}/{1}'.format(self.input_temp, from_file.name)
                from_file.download(target)
                to_file = rescale.client.RescaleFile(config=self.to_config,
                                                     file_path=target)
                input_ids.append(to_file.id)
            target_jobanalysis['inputFiles'] = [{'id': id} for id in input_ids]
            target_jobanalyses.append(target_jobanalysis)

        to_job_data['jobanalyses'] = target_jobanalyses
        to_job = rescale.client.RescaleJob(config=self.to_config,
                                           json_data=to_job_data)
        print('Target job ID: {0}'.format(to_job.id))


def make_template(keys_file, cloner_file, jobid_to_copy, from_profile, to_profile):
    template = copy.deepcopy(JOB_TEMPLATE)
    jobanalysis = template['jobanalyses'][0]
    jobanalysis['inputFiles'] = [{'id': keys_file.id},
                                 {'id': cloner_file.id}]
    template['name'] = 'Cloning job {0}'.format(jobid_to_copy)
    jobanalysis['command'] = jobanalysis['command'].format(from_profile=from_profile,
                                                           to_profile=to_profile,
                                                           jobid=jobid_to_copy,
                                                           branch=SDK_BRANCH)
    return template


if __name__ == '__main__':

    args = rescale.cli.get_cli_args([('--from-profile', {}),
                                     ('--to-profile', {}),
                                     ('--cloner-profile', {}),
                                     ('--jobid', {})])
    if args.cloner_profile:
        cloner_config = rescale.client.RescaleConfig(profile=args.cloner_profile)
        keys_input = \
            rescale.client.RescaleFile.get_newest_by_name('rescale_keys.tar.gz', config=cloner_config)
        cloner_file = os.path.realpath(__file__)
        cloner_input = rescale.client.RescaleFile(config=cloner_config,
                                                  file_path=cloner_file)
        job_data = make_template(keys_input,
                                 cloner_input,
                                 args.jobid,
                                 args.from_profile,
                                 args.to_profile)
        logger.info('Creating job to clone job {0}'.format(args.jobid))
        clone_job = rescale.client.RescaleJob(config=cloner_config, json_data=job_data)
        clone_job.submit()
        logger.info('Waiting for cloning job to complete')
        clone_job.wait()
        time.sleep(60)
        clone_job.delete()
    else:
        JobCloner(args.from_profile, args.to_profile).clone_job(args.jobid)
