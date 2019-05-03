
import copy

from rescale import client

JOB_TEMPLATE = {
    'archiveFilters': [],
    'autoTerminateCluster': True,
    'caseFile': None,
    'inputFileParseTask': '',
    'isTemplateDryRun': False,
    'jobanalyses': [
        {
            'envVars': {},
            'postProcessScript': None,
            'templateTasks': [],
            'postProcessScriptCommand': '',
            'onDemandLicenseSeller': None,
            'useRescaleLicense': False,
            'preProcessScriptCommand': '',
            'analysis': {},
            'hardware': {
                'isReusable': False,
                'type': 'compute',
                'coreType': {'code': 'emerald'}
            },
            'preProcessScript': None,
            'flags': {},
            'inputFiles': []
        }
    ]
}


class MisconfiguredBulider(Exception):

    pass


class ConfiguredBuilder(object):

    def __init__(self):
        self.profile_name = None
        self.config = None

    def with_profile(self, profile_name):
        self.profile_name = profile_name
        return self

    def with_config(self, config):
        self.config = config
        return self

    def configure(self):
        if self.config is None:
            self.config = client.RescaleConfig(profile=self.profile_name)


class JobBuilder(ConfiguredBuilder):

    def __init__(self, name,
                 analysis='user_included',
                 analysis_version='0',
                 cores_per_slot=1,
                 slots=1,
                 core_type='emerald',
                 walltime=4,
                 low_priority=False,
                 command='sleep 300',
                 **kwargs):
        super(JobBuilder, self).__init__(**kwargs)
        self.name = name
        self.analysis = analysis
        self.analysis_version = analysis_version
        self.cores_per_slot = cores_per_slot
        self.slots = slots
        self.core_type = core_type
        self.walltime = walltime
        self.command = command
        self.hps_id = None
        self.low_priority = low_priority
        self.input_file_ids = set()
        self.input_file_paths = set()
        self.input_file_names = set()
        self.hps_file_paths = []

    def with_hps(self, hps, file_paths=None):
        if file_paths:
            self.hps_file_paths = file_paths
        if isinstance(hps, int):
            self.hps_id = hps
        else:
            self.hps_id = hps.id
        return self

    def with_latest_hps(self, file_paths=None):
        hps = next(client.list_running_hps(config=client.RescaleConfig(profile=self.profile_name)),
                   None)
        self.with_hps(hps, file_paths=file_paths)
        return self

    def with_existing_input_file_id(self, file_id):
        self.input_file_ids.append(file_id)
        return self

    def with_existing_input_file_name(self, file_name):
        self.input_file_names.append(file_name)
        return self

    def with_local_input_file(self, file_path):
        self.input_file_paths.append(file_path)
        return self

    def build(self):
        super(JobBuilder, self).configure()

        job_def = copy.deepcopy(JOB_TEMPLATE)
        job_def['name'] = self.name
        job_def['isLowPriority'] = self.low_priority
        jobanalysis = job_def['jobanalyses'][0]
        jobanalysis['command'] = self.command
        jobanalysis['analysis'] = {'code': self.analysis,
                                   'version': self.analysis_version}
        jobanalysis['hardware']['coresPerSlot'] = self.cores_per_slot
        jobanalysis['hardware']['slots'] = self.slots
        jobanalysis['hardware']['coreType']['code'] = self.core_type
        jobanalysis['hardware']['walltime'] = self.walltime

        input_files = [client.RescaleFile(id=file_id)
                       for file_id in self.input_file_ids] + \
                [client.RescaleFile(file_path=file_path)
                 for file_path in self.input_file_paths] + \
                [client.RescaleFile.get_newest_file_by_name(file_name)
                 for file_name in self.input_file_names]
        jobanalysis['inputFiles'] = [{'id': input_file.id}
                                     for input_file in input_files]

        job = client.RescaleJob(json_data=job_def,
                                config=self.config,
                                attempts=3)
        if self.hps_id:
            hps = client.RescaleStorageDevice(id=self.hps_id,
                                              config=self.config)\
                .wait_for_started()\
                .copy_files_to_job(job,
                                   file_paths=self.hps_file_paths)

        job.submit()
        return job


class HpsBuilder(ConfiguredBuilder):

    def __init__(self, name, size_mb,
                 walltime=24,
                 low_priority=False,
                 cores=18,
                 core_type='hps-3',
                 **kwargs):
        super(HpsBuilder, self).__init__(**kwargs)
        self.name = name
        self.size_mb = size_mb
        self.walltime = walltime
        self.low_priority = low_priority
        self.cores = cores
        self.core_type = core_type
        self.profile_name = 'default'
        self.config = None
        self.file_paths = []

    def build(self):
        super(HpsBuilder, self).configure()

        hps_def = {'name': self.name,
                   'storage_size_mb': self.size_mb,
                   'walltime': self.walltime,
                   'cores_per_slot': self.cores,
                   'core_type': self.core_type,
                   'run_low_priority': self.low_priority}

        hps = client.RescaleStorageDevice(json_data=hps_def,
                                          config=self.config)
        hps.submit()
        return hps

    def with_local_files(self, file_paths):
        self.file_paths += file_paths

