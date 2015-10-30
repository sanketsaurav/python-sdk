#!/usr/bin/env python3

import datetime
import logging
import rescale.client

# Remember to set RESCALE_API_KEY env variable to your Rescale API key
# on platform.rescale.com (in Settings->API)

logging.basicConfig(level=logging.INFO)

def main():
    TEST_ARCHIVE = 'inputs/all_tests.tar.gz'
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
    JOB_NAME = 'echoware0.1-all-tests'

    logging.info('Uploading test job input files')
    input_files = [rescale.client.RescaleFile(file_path=TEST_ARCHIVE),
                   rescale.client.RescaleFile(file_path=BUILD_ARCHIVE),
                  ]
    post_process_file = \
        rescale.client.RescaleFile(file_path=POST_COMPARE_SCRIPT)


    job_definition = {
        'name': JOB_NAME,
        'isLowPriority': True,
        'jobanalyses': [
            {
                'analysis': {
                    'code': 'custom'
                },
                'hardware': {
                    'coresPerSlot': 1,
                    'slots': 1,
                    'coreType': {
                        'code': 'standard-plus'
                    }
                },
                'inputFiles': [{'id': inp.id} for inp in input_files],
                'command': TEST_COMMAND,
                'postProcessScript': {'id': post_process_file.id},
                'postProcessScriptCommand': POST_RUN_COMPARE_COMMAND
            }
        ],
    }


    logging.info('Launching %s', JOB_NAME)
    job = rescale.client.RescaleJob(json_data=job_definition)
    job.submit()

    logging.info('WAITING FOR JOB COMPLETION (this may take up to 10 minutes)')
    job.wait()

    logging.info('Downloading test results from completed job %s', job.id)
    test_log_files = [f for f in job.get_files()
                       if f.name == 'process_output.log']
    assert len(test_log_files) == 1
    test_log = test_log_files[0]
    test_log.download(target=test_log.name)

    logging.info('Finished! Test results in %s', test_log.name)


if __name__ == '__main__':
    main()
