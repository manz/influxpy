import io
import os
import re
from codecs import open

from setuptools import setup

this_dir = os.path.abspath(os.path.dirname(__file__))


def read(*names, **kwargs):
    with io.open(
            os.path.join(this_dir, *names),
            encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


with open(os.path.join(this_dir, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().splitlines()

# Get the long description from the README file
with open(os.path.join(this_dir, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='influxpy',
    version=find_version('influxpy/__init__.py'),
    description='Influxdb for humans and robots.',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/manz/influxpy',

    # Author details
    author='Emmanuel Peralta',
    author_email='manz@ringum.net',

    # Choose your license
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],

    # What does your project relate to?
    keywords='influxdb wrapper',

    packages=['influxpy'],

    install_requires=requirements,

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'docs': ['sphinx', 'sphinx_rtd_theme'],
        'test': ['coverage'],
    },
)
