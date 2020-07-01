import io
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

with io.open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requires = [line for line in f.readlines() if not line.startswith("#")]

setup(
    name="cnswd",
    version="5.0.0",
    packages=find_packages(),
    long_description="""
    股票网络数据工具包
    """,
    install_requires=requires + ['python_version>="3.7"'],
    tests_require=["pytest", "parameterized"],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'stock = cnswd.scripts.command:stock',
        ],
    },
    author="LDF",
    author_email="liu.dengfeng@hotmail.com",
    description="Utilities for fetching Chinese stock webpage data",
    license="https://github.com/liudengfeng/cnswd/blob/master/LICENSE",
    keywords="china stock data tools",
)
