#!/usr/bin/env python

import sys

from setuptools import find_packages, setup


def setup_package():
    metadata = dict(
        name='knapsack',
        version='0.1.0',
        description='Greedy Knapsack Algorithm on PySpark',
        author='Darrell Ulm',
        license='Apache License, Version 2.0',
        url='https://github.com/drulm/Spark_Knapsack',
        packages=find_packages(),
        long_description=open('./README.rst').read(),
        install_requires=open('./requirements.txt').read().split()
    )

    setup(**metadata)


if __name__ == '__main__':
    setup_package()
