from setuptools import setup, find_packages
from cassandra_connect import VERSION

setup(
    name='cassandra_connect',
    version=VERSION,
    packages=find_packages(),
    url='',
    license='',
    author='tom wilson',
    author_email='tom.andrew.wilson@gmail.com',
    description='pandas dataframes to and from cassandra with or without spark'
)
