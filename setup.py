import sys

from setuptools import find_packages, setup

cur_ver = sys.version_info[:2]
ver_str = ".".join(map(str, cur_ver))

if cur_ver < (3, 8) or cur_ver > (3, 11):
    raise RuntimeError(
        f"Python {ver_str} is unsupported. Please use a Python version between 3.8 and"
        " 3.11, inclusive."
    )

setup(
    name="latch",
    version="v2.44.0",
    author_email="kenny@latch.bio",
    description="The Latch SDK",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.8,<3.12",
    entry_points={
        "console_scripts": [
            "latch=latch_cli.main:main",
        ]
    },
    install_requires=[
        "kubernetes>=24.2.0",
        "pyjwt>=0.2.0",
        "requests>=2.28.1",
        "click>=8.0",
        "docker>=5.0",
        "paramiko>=3.2.0",
        "scp>=0.14.0",
        "boto3>=1.26.0",
        "tqdm>=4.63.0",
        "lytekit==0.15.7",
        "lytekitplugins-pods==0.6.2",
        "typing-extensions==4.7.1",
        "apscheduler==3.9.1",
        "gql==3.4.0",
        "graphql-core==3.2.3",
        "requests-toolbelt==0.10.1",
        "latch-sdk-gql==0.0.6",
        "latch-sdk-config==0.0.4",
        "python-dateutil>=2.8",
        # for old latch develop, to be removed
        "aioconsole==0.6.1",
        "asyncssh==2.13.2",
        "websockets==11.0.3",
        "watchfiles==0.19.0",
    ],
    extras_require={
        "snakemake": ["snakemake>=7.18.0, <7.30.2", "pulp >=2.0, <2.8"],
        "pandas": ["pandas>=2.0.0"],
    },
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
