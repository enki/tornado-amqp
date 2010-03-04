from setuptools import setup

setup(
    name = 'tornado-amqp',
    packages = ['tamqp'],
    version = '0.1',
    zip_safe = True,
    install_requires = [
        'tornado',
        'amqplib'
    ]
)
