from setuptools import find_packages, setup

setup(
    name="latch",
    version="0.0.4",
    author_email="kenny@latch.bio",
    description="latch sdk",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "latch=latch.cli.main:main",
        ]
    },
)
