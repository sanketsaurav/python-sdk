#!/usr/bin/env python3

import logging
import os
import sys
import testlib
from rescale.client import RescaleFile, RescaleJob

TEST_COMMAND = './build*/bin/runtest.sh'
POST_PROCESS_COMMAND = './diff.sh'

logging.basicConfig(level=logging.INFO)


def upload_tests(test_dir):
    archives_uploaded = []
    for test_archive in os.listdir(test_dir):
        path = os.path.join(test_dir, test_archive)
        archives_uploaded.append(RescaleFile(file_path=path))
    return archives_uploaded


def get_job_name(build_archive, test_archive):
    build_base = testlib.strip_suffix(build_archive)
    test_base = testlib.strip_suffix(test_archive)
    return '{0}-{1}'.format(build_base, test_base)


def create_job_json(name, input_files, post_process_file):
    return {
        'name': name,
        'jobanalyses': [
            {
                'analysis': {
                    'version': '0',
                    'code': 'custom'
                },
                'useRescaleLicense': True,
                'hardware': {
                    'coresPerSlot': 1,
                    'slots': 1,
                    'coreType': {
                        'code': 'hpc-plus'
                    }
                },
                'inputFiles': [{'id': inp.id} for inp in input_files],
                'command': TEST_COMMAND,
                'postProcessScriptCommand': POST_PROCESS_COMMAND,
                'postProcessScript': {'id': post_process_file.id}
            }
        ],
        'isLowPriority': True,
    }


if __name__ == '__main__':
    logging.info('Starting base test case setup')
    if len(sys.argv) < 4:
        print('Usage: {0} BUILD_ARCHIVE POST_PROCESS_SCRIPT TEST_CASE_ARCHIVE_DIR'.format(
            sys.argv[0]))
        sys.exit(1)

    build_archive = sys.argv[1]
    post_script = sys.argv[2]
    test_case_dir = sys.argv[3]

    if not os.path.isfile(build_archive):
        print('Build archive does not exist or is not a file')
        sys.exit(1)
    if not os.path.isfile(post_script):
        print('Post-processing script does not exist or is not a file')
        sys.exit(1)
    if not os.path.isdir(test_case_dir):
        print('Test case archive dir does not exist or is not a directory')
        sys.exit(1)

    build_input = RescaleFile(file_path=build_archive)
    post_input = RescaleFile(file_path=post_script)

    archives_uploaded = upload_tests(test_case_dir)

    for test_input in archives_uploaded:
        name = get_job_name(build_input.name, test_input.name)
        logging.info(
            'Creating job {0} for cloning (not submitting)'.format(name)
        )
        RescaleJob(json_data=create_job_json(name,
                                             [build_input, test_input],
                                             post_input))
