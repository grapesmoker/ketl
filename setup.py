#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', 'furl==2.1.0', 'smart_open==4.0.0',
                'marshmallow==3.9.1', 'tqdm==4.55.1', 'pyarrow==2.0.0',
                'requests==2.25.1', 'sqlalchemy==1.3.20']

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Jerry Vinokurov",
    author_email='grapesmoker@gmail.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="komprehensive ETL",
    entry_points={
        'console_scripts': [
            'ketl=ketl.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='ketl',
    name='ketl',
    packages=find_packages(include=['ketl', 'ketl.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/grapesmoker/ketl',
    version='0.1.0',
    zip_safe=False,
)
