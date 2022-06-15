#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk~=0.1", "requests", "httpx"]

setup(
    name="source_bronnoyregister",
    description="Source implementation for Bronnoyregister.",
    author="Airbyte",
    author_email="timo@deeptechconsulting.no",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json"]},
)
