python-sdk
==========

Python bindings for the Rescale API

Set `RESCALE_API_KEY` to your Rescale user API key found in the
Settings->API section of the platform web portal.

Classes in rescale/client.py wrap Rescale REST API calls, for file
upload and download and job status, creation, and submission.

## DOE Example ##

Creates a simple Design-of-Experiments job and runs it, uploading
example input files in the process.

View the details in `examples/doe.py`

## ScaleX Developer Example ##

Automation for running a regression test suite with incremental build
updates on Rescale. Contained in `examples/scalex_developer`.

The workflow for this example is as follows:

- Create a "base" job for each regression test case, containing the
  software build to test and reference test inputs and outputs
- Base job also has a post-processing step to compare job output with
  reference output for test
- Clone each base job and add an incremental build delta as an additional
  inputs
- Submit these delta jobs for execution
- Collect output from job post-processing steps to see what tests
  passed and failed

There are 2 scripts that need to be run in this case:

`regression_test_base_job_setup.py` is run only once to upload a full
software build along with reference test cases to Rescale. It will
create a job for every archive in the `TEST_CASE_ARCHIVE_DIR`. The
naming scheme for these base jobs is: `<build file basename>-<test
case archive basename>`. *This script does not actually run any jobs*,
it just stages input files and creates unsubmitted jobs to be used by
the next script.

`regression_test_create_run_delta_tests.py` is to be run every time
there is a new build available and test jobs should be run. A new job
will be cloned from the base jobs and then submitted to run. The
DELTA_ARCHIVE file will be unpacked on top of the base full build tree
in the job work directory before running.

Potential enhancements to be made to this example:

- Wait for test job completions and then download the stdout
  process_output.log files for each job to check test results
- Delete old test jobs older than N runs of that test
- Allow specification of different core types and counts per test
  (right now each test is run on a 1-core Nickel cluster)
- Support additional methods for updating base build with incremental
  deltas
