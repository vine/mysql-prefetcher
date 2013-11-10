#!/usr/bin/env python
import os
from setuptools import setup, find_packages

# Utility function to read the README.md file from main directory, used for 
# the long_description.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='myprefetch',
    version='0.2',
    description='MySQL Replication Prefetcher',
    packages=find_packages(),
    long_description=read('README.md'),
    license=read('LICENSE'),
    url='https://github.com/vine/mysql-prefetcher',
    install_requires=[
        'MySQL-python<=1.2.3',
    ],
    include_package_data=True,
)
