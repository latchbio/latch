import sys

from setuptools import find_packages, setup

MIN_PYTHON_VERSION = (3, 7)
CURRENT_PYTHON = sys.version_info[:2]

if CURRENT_PYTHON < MIN_PYTHON_VERSION:
    print(
        f"Latch SDK is only supported for Python version is {MIN_PYTHON_VERSION}+. Detected you are on"
        f" version {CURRENT_PYTHON}, installation will not proceed!"
    )
    sys.exit(-1)

setup(
    name="latch",
    version="0.5.1",
    author_email="kenny@latch.bio",
    description="latch sdk",
    packages=find_packages(),
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "latch=latch.cli.main:main",
        ]
    },
    install_requires=[
        "pyjwt>=0.2.0",
        "requests>=2.0",
        "click>=7.0",
        "docker>=5.0",
        "boto3>=1.2",
        "flaightkit==0.2.0",
        "flaightkitplugins-pod==0.0.1",
        "typing-extensions==4.0.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
