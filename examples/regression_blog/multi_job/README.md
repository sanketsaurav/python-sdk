```
cd examples/regression_blog/multi_job
mkvirtualenv -p /usr/bin/python3 blogdemo
pip install -r requirements.txt
PYTHONPATH=$PYTHONPATH:../../.. ./run_multi_job_test.py
```
