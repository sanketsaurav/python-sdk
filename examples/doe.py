import os
import sys
import time
from rescale.client import RescaleFile, RescaleJob

def main():
    
    # get relative path to auxiliary files
    input_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             'input')

    # upload files
    template_file = RescaleFile(file_path=os.path.join(input_dir, 'run_fmm'))
    input_files = []
    for file in ['hello.txt.zip', 'world.txt.zip']:
        input_files.append( RescaleFile( file_path=os.path.join(input_dir, file) ) )
    
    # create parameter sweep dictionary
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
             'command': './{command}'.format(command=template_file.name),
             'hardware': {'coresPerSlot': 1, 'coreType': 'standard'},
             'templateTasks': [
                {'processedFilename': template_file.name,
                 'templateFile': {'id': template_file.id}}
             ],
             'inputFiles': [{'id': rescale_file.id} for rescale_file in input_files]
            }
        ]
    }
    
    # create parameter sweep job from dictionary
    job = RescaleJob(json_data=job_data)
    
    # submit job
    job.submit()
    
    # wait for job to complete
    job.wait()

    # download all files to a 'output' folder
    for file in job.get_files():

        file_path = os.path.join('output',*file.path.split('/')[4:])
        file_dir = os.path.dirname(file_path)
        
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)
        
        file.download(target=file_path)

if __name__ == '__main__':
    main()
