#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'pyarrow>=0.14.0',
    'pandas',
    'Click>=6.0',
    'requests>=2.21.0',
    'protobuf',
    'requests-futures',
    'confuse',
    'simplejson'
]

setup_requirements = []

test_requirements = []

setup(
    author="Ryan Murray",
    author_email='rymurr@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Python client for Dremio. See https://dremio.com",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='dremio_client',
    name='dremio_client',
    packages=find_packages(
        include=['dremio_client', "dremio_client.flight", 'dremio_client.auth', 'dremio_client.model',
                 'dremio_client.util', 'dremio_client.conf']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/rymurr/dremio_client',
    version='0.2.0',
    zip_safe=False,
    extras_require={
        ':python_version == "2.7"': ['futures']
    },
    entry_points={
        'console_scripts': [
            'dremio_client=dremio_client.cli:cli',
        ],
    },
)
