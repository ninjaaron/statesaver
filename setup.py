from setuptools import setup

setup(
    name='statesaver',
    version='0.3.1',
    description= \
        'context managers and iterators for serializing state on failure',
    # long_description=open('README.rst').read(),
    url='https://github.com/ninjaaron/statesaver',
    author='Aaron Christianson',
    author_email='ninjaaron@gmail.com',
    keywords='shelf json state',
    pymodules=['statesaver'],
    classifiers=['Programming Language :: Python :: 3.5'],
)
