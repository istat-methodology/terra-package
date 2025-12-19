from setuptools import setup, find_packages

setup(
    name='terra_package',
    version='0.1.0',
    description='TERRA package for network analysis',
    author='ISTAT',
    author_email='giulio.massacci@istat.it',
    packages=find_packages(),
    install_requires=[
        'pandas>=1.0',
        'networkx>=2.0',
        'distinctiveness>=0.1.5',
    ],
    python_requires='>=3.8',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
