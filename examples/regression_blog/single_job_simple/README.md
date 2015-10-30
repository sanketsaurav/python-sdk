```
cd examples/regression_blog/single_job_simple
mkvirtualenv -p /usr/bin/python3 blogdemo
pip install -r requirements.txt
PYTHONPATH=$PYTHONPATH:../../.. ./run_simple_test_job.py
```
