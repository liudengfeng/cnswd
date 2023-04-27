from setuptools import find_packages, setup


setup(
    name="cnswd",
    version="5.0.1",
    packages=find_packages(),
    long_description="""
    股票网络数据工具包
    """,
    tests_require=["pytest", "parameterized"],
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "stock = cnswd.scripts.command:stock",
        ],
    },
    author="LDF",
    author_email="liu.dengfeng@hotmail.com",
    description="Utilities for fetching Chinese stock webpage data",
    license="https://github.com/liudengfeng/cnswd/blob/master/LICENSE",
    keywords="china stock data tools",
)
