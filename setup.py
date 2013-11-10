#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='myprefetch',
    version='0.1',
    description='MySQL Replication Prefetcher',
    packages=find_packages(),
    long_description=open('README.md').read(),
    license=open('LICENSE').read(),
    url='https://github.com/vine/mysql-prefetcher',
    install_requires=[
        'MySQL-python',
    ],
)
