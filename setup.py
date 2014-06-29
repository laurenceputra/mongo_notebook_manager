#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup


with open('README.rst') as file:
    long_description = file.read()

setup(
    name='MongoNotebookManager',
    version='0.1.3',
    description='A notebook manager for IPython with MongoDB as the backend.',
    long_description=long_description,
    author='Laurence Putra',
    author_email='laurenceputra@gmail.com',
    url = 'https://github.com/laurenceputra/mongo_notebook_manager',
    license = 'GPL v3',
    packages = ['mongo_notebook_manager'],
    package_dir = {'mongo_notebook_manager': 'src/mongo_notebook_manager'},
    keywords = 'mongo notebook manager ipython',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Database :: Front-Ends',
        'Framework :: IPython',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent'
    ],
    install_requires=[
        'pymongo'
    ],
)