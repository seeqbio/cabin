import os
from setuptools import setup, find_packages

PROJECT_ROOT = os.path.dirname(__file__)

with open(os.path.join(PROJECT_ROOT, "README.md")) as file_:
    long_description = file_.read()

setup(
    name="Cabin",
    version="0.0.1",
    python_requires=">=3.5.0", # TODO
    description="TODO",
    long_description=long_description,
    url="TODO",
    author="TODO",
    author_email="TODO",
    license="TODO",
    classifiers=[
        "TODO",
    ],
    packages=['cabin'],
    install_requires=[ # TODO
        "networkx",
        "prettytable",
        "humanize",
        "lxml",

        "boto3",
        "awscli",
        "mysql-connector-python",
    ],
    tests_require=[
        "pytest",
        "flake8",
    ],
)
