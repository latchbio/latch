from setuptools import find_packages, setup

setup(
    name="latch",
    version="0.0.14",
    author_email="kenny@latch.bio",
    description="latch sdk",
    packages=find_packages(),
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
        "flaightkit==0.1.0",
    ],
)
