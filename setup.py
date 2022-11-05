import sys

from setuptools import find_packages, setup

MIN_PYTHON_VERSION = (3, 7)
CURRENT_PYTHON = sys.version_info[:2]

if CURRENT_PYTHON < MIN_PYTHON_VERSION:
    print(
        f"Latch SDK is only supported for Python version is {MIN_PYTHON_VERSION}+."
        f" Detected you are on version {CURRENT_PYTHON}, installation will not proceed!"
    )
    sys.exit(-1)

setup(
    name="latch",
    version="v2.5.2",
    author_email="kenny@latch.bio",
    description="The Latchbio SDK",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "latch=latch_cli.main:main",
        ]
    },
    install_requires=[
        "awscli==1.25.22",
        "kubernetes>=24.2.0",
        "pyjwt>=0.2.0",
        "requests>=2.0",
        "click>=8.0",
        "docker>=5.0",
        "paramiko>=2.11.0",
        "scp>=0.14.0",
        "boto3>=1.24.22",
        "tqdm>=4.63.0",
        "lytekit==0.13.4",
        "lytekitplugins-pods==0.3.1",
        "typing-extensions==4.3.0",
        "apscheduler==3.9.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
