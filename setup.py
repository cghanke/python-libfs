from setuptools import setup
from os import path


here = path.abspath(path.dirname(__file__))


with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='libfs',
    version='0.1',
    description='Library Filesystem',
    long_description=long_description,
    author='Christof Hanke',
    author_email='christof.hanke@induhviduals.de',
    url='https://github.com/ya-induhvidual/libfs',
    packages=['Libfs'],
    license='MIT',
    install_requires=['llfuse', 'mutagenx'],
    test_suite="test/test_all.py",
    scripts=['scripts/libfs.py'],
    keywords='fuse multimedia',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: BSD :: FreeBSD',
        'Operating System :: POSIX :: Linux',
        'Topic :: System :: Filesystems'
    ],
)
