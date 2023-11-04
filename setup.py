from setuptools import setup

setup(
    name='etl_ap',
    version='0.1.2',
    description='ETL para Andina Pack - Elico Group',
    author='James Garcia',
    author_email='jamgarciavi@unal.edu.co',
    url='',
    packages=['etl_ap'],
    install_requires=[
        'pytz',
        'requests',
        'snap7',
    ],
)
