#!/usr/bin/env python3

import datetime
import logging
import os.path
import rescale.client

# Remember to set RESCALE_API_KEY env variable to your Rescale API key
# on platform.rescale.com (in Settings->API)

SHORT_TEST_ARCHIVE = 'inputs/all_short_tests.tar.gz'
LONG_TEST_FORMAT = 'inputs/long_test_{i}.tar.gz'
LONG_TEST_COUNT = 1
BUILD_ARCHIVE = 'inputs/echoware0.1.tar.gz'
POST_COMPARE_SCRIPT = 'inputs/compare_results.sh'

TEST_COMMAND = """
for testcase in $(find . -name "test[0-9]*" -type d); do
    ./echoware/bin/echo.sh $testcase
done
"""
POST_RUN_COMPARE_COMMAND = """
for testcase in $(find . -name "test[0-9]*" -type d); do
    ./compare_results.sh $testcase
done
"""
STDOUT_LOG = 'process_output.log'

logging.basicConfig(level=logging.INFO)

def get_or_upload(file_path):
    input_file = rescale.client.RescaleFile.get_newest_by_name(os.path.basename(file_path))
    if not input_file:
        input_file = rescale.client.RescaleFile(file_path=file_path)
    return input_file

def create_job(name, build_input, test_input, post_process, core_type, core_count):
    input_files = [build_input, test_input]
    job_definition = {
        'name': name,
        'isLowPriority': True,
        'jobanalyses': [
            {
                'analysis': {
                    'code': 'custom'
                },
                'hardware': {
                    'coresPerSlot': 1,
                    'slots': core_count,
                    'coreType': {
                        'code': core_type
                    }
                },
                'inputFiles': [{'id': inp.id} for inp in input_files],
                'command': TEST_COMMAND,
                'postProcessScript': {'id': post_process.id},
                'postProcessScriptCommand': POST_RUN_COMPARE_COMMAND
            }
        ],
    }
    return rescale.client.RescaleJob(json_data=job_definition)


def main():
    logging.info('Uploading test job input files')

    short_test_bundle = get_or_upload(SHORT_TEST_ARCHIVE)

    long_test_inputs = [get_or_upload(LONG_TEST_FORMAT.format(i=i))
                        for i in range(LONG_TEST_COUNT)]

    build_input = rescale.client.RescaleFile(file_path=BUILD_ARCHIVE)
    post_process_file = get_or_upload(POST_COMPARE_SCRIPT)


    # create all test jobs
    short_test_job = create_job('echoware0.1-all-short-tests',
                                build_input,
                                short_test_bundle,
                                post_process_file,
                                'standard-plus',
                                1)
    long_test_jobs = [create_job('echoware0.1-long-test-{0}'.format(i),
                                 build_input,
                                 long_test,
                                 post_process_file,
                                 'standard-plus',
                                 1)
                      for i, long_test in enumerate(long_test_inputs)]

    # submit all
    short_test_job.submit()
    [long_test_job.submit() for long_test_job in long_test_jobs]

    # wait for all to complete
    short_test_job.wait()
    [long_test_job.wait() for long_test_job in long_test_jobs]

    # get results
    short_test_job.get_file(STDOUT_LOG)\
        .download(target='{0}.out'.format(short_test_job.name))
    [job.get_file(STDOUT_LOG).download(target='{0}.out'.format(job.name))
     for job in [short_test_job] + long_test_jobs]

if __name__ == '__main__':
    main()
