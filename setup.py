#!/usr/bin/env python3

import sys
import os

from setuptools import setup, find_packages

import amqpy

name = 'amqpy'
description = 'an AMQP 0.9.1 client library for Python 2.7 & Python >= 3.2.0'
keywords = ['amqp', 'rabbitmq', 'qpid']

classifiers = [
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: Implementation :: CPython',
    'Programming Language :: Python :: Implementation :: PyPy',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'License :: OSI Approved :: MIT License',
    'Intended Audience :: Developers',
    'Topic :: Internet',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Topic :: System :: Networking'
]

package_data = {
    '': ['*.rst', '*.ini', 'AUTHORS', 'LICENSE'],
}


def long_description():
    if os.path.exists('README.rst'):
        with open('README.rst') as f:
            return f.read()
    else:
        return description


setup(
    name=name,
    description=description,
    long_description=long_description(),
    version=amqpy.__version__,
    author=amqpy.__author__,
    author_email=amqpy.__contact__,
    maintainer=amqpy.__maintainer__,
    url=amqpy.__homepage__,
    platforms=['any'],
    license='LGPL',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    package_data=package_data,
    install_requires=['six>=1.0'],
    tests_require=['pytest>=2.6'],
    classifiers=classifiers,
    keywords=keywords
)
