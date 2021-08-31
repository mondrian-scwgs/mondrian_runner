import versioneer
from setuptools import setup, find_packages

setup(
    name='mondrian_runner',
    packages=find_packages(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='python utilities for mondrian',
    author='Diljot Grewal',
    author_email='diljot.grewal@gmail.com',
    entry_points={
        'console_scripts': [
            'mondrian_runner = mondrian_runner.main:main',
        ]
    },
    package_data={'': ['*.py']}
)
