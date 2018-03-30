
import argparse

def get_cli_args(args=None):
    if args is None:
        args = []
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', default='default')
    for argname, kwargs in args:
        parser.add_argument(argname, **kwargs)
    return parser.parse_args()
