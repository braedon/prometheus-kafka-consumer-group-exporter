from setuptools import setup, find_packages

setup(
    name='prometheus-kafka-consumer-group-exporter',
    version='0.5.5',
    description='Kafka consumer group Prometheus exporter',
    url='https://github.com/Braedon/prometheus-kafka-consumer-group-exporter',
    author='Braedon Vickers',
    author_email='braedon.vickers@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Monitoring',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='monitoring prometheus exporter kafka consumer group',
    packages=find_packages(),
    install_requires=[
        # kafka-python 1.4.5 included a number of bugs and a severe drop
        # in consumer performance. 1.4.6 fixed the bugs, but the performance
        # issues remained. 1.4.7 fixed the performance issues.
        'kafka-python >= 1.3, != 1.4.5, != 1.4.6',
        'jog',
        'prometheus-client >= 0.6.0',
        'javaproperties'
    ],
    entry_points={
        'console_scripts': [
            'prometheus-kafka-consumer-group-exporter=prometheus_kafka_consumer_group_exporter:main',
        ],
    },
)
