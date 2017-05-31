#!/usr/bin/env python

import rescale.client


def get_running_jobs():
    for json_data in rescale.client.list_running_jobs():
        print('{0}: {1}'.format(json_data['id'], json_data['name']))
        job = rescale.client.RescaleJob(id=json_data['id'])
        for connection_info in job.connection_info():
            print('\t{0}'.format(connection_info))


if __name__ == '__main__':
    get_running_jobs()
