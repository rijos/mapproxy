import sys
from os.path import dirname as d
from os.path import abspath, join
root_dir = d(d(abspath(__file__)))
sys.path.append(root_dir)

def pytest_configure(config):
    import sys
    sys._called_from_pytest = True

def pytest_unconfigure(config):
    import sys
    del sys._called_from_pytest
