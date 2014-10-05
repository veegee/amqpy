#!/usr/bin/env python3

import sys
import os

from setuptools import setup, find_packages

import amqpy

if sys.version_info < (3, 2):
    raise Exception('amqpy requires Python 3.2 or higher')

name = 'amqpy'
description = 'an AMQP 0.9.1 client library for Python >= 3.2.0'
keywords = ['amqp', 'rabbitmq', 'qpid']

classifiers = [
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
    'Intended Audience :: Developers',
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
    tests_require=['pytest>=2.6'],
    classifiers=classifiers,
    keywords=keywords
)
