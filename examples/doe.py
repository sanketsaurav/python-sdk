import os
import sys
import time
from rescale.client import RescaleClient


def upload(client, path):
    with open(path) as fp:
        return client.upload_file(fp)


def wait_for_completion(client, job):
    while True:
        response = client.get_status(job['id'])
        latest_status = next(response, None)
        if latest_status and latest_status['status'] == 'Completed':
            print latest_status
            return

        time.sleep(30)


def run_job(api_key):
    c = RescaleClient(api_key)

    # Upload files
    input_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             'input')

    template_file = upload(c, os.path.join(input_dir, 'run_fmm'))
    input_files = [upload(c, os.path.join(input_dir, p))
                   for p in ['hello.txt.zip', 'world.txt.zip']]

    # Create parameter sweep
    job_data = {
        'name': 'DOE Test Job',
        'jobvariables': [
            {'name': 'index',
             'variableType': 'Param',
             'valueGeneratorType': 'FixedRange',
             'valueGeneratorSettings': {'minvalue': 1,
                                        'maxvalue': 10,
                                        'increment': 1}}
        ],
        'jobanalyses': [
            {'analysis': {'code': 'user_included'},
             'command': './run_fmm',
             'hardware': {'coresPerSlot': 1, 'coreType': 'Standard'},
             'templateTasks': [
                {'processedFilename': 'run_fmm',
                 'templateFile': {'id': template_file['id']}}
             ],
             'inputFiles': [{'id': i['id']} for i in input_files]
            }
        ]
    }

    job = c.create_job(job_data)
    # Submit
    c.submit_job(job['id'])

    # Poll and wait for completion
    wait_for_completion(c, job)

    # Download results as flat list
    output_files = c.get_files(job['id'])
    for output in output_files:
        # TODO: Return back the friendly-path from the API... This is gross.
        parent = os.path.dirname(output['path'])
        path_segments = ['output'] + parent.split('/')[5:] + [output['name']]
        dest = os.path.join(*path_segments)

        parent_dir = os.path.dirname(dest)
        if parent_dir and not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)

        response = c.download_file(output['id'])
        with open(dest, 'wb') as fp:
            for chunk in response.iter_content(8192):
                fp.write(chunk)
        print 'Wrote {dest} ({size})'.format(
            dest=dest, size=output['decryptedSize'])


if __name__ == '__main__':
    run_job(sys.argv[1])
