#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = ['codex-africanus[dask] >= 0.2.1',
                'dask-ms >= 0.2.3',
                'loguru']

extras_require = {
    'testing': ['pytest', 'pytest-flake8']
}

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Simon Perkins",
    author_email='sperkins@ska.ac.za',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Measurement Set Averager",
    entry_points={
        'console_scripts': ['xova=xova.apps.xova.app:main'],
    },
    extras_require=extras_require,
    install_requires=requirements,
    license="BSD license",
    long_description=readme,
    long_description_content_type='text/x-rst',
    include_package_data=True,
    keywords='xova',
    name='xova',
    packages=find_packages(),
    python_requires=">=3.6",
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/ska-sa/xova',
    version='0.1.0',
    zip_safe=False,
)
