from setuptools import setup, find_packages

from kafka_rest import __version__

setup(
    name='kafka-rest',
    version=__version__,
    description="Async client for Confluent's Kafka REST Proxy",
    url='https://github.com/gamechanger/kafka-rest',
    author='GameChanger',
    author_email='travis@gc.io',
    packages=find_packages(),
    install_requires=['tornado>=4.0.0,<5.0.0'],
    tests_require=[],
    test_suite="nose.collector",
    zip_safe=False
)
