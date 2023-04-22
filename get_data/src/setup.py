"""Minimal setup file for tasks project."""

from setuptools import setup, find_packages

setup(
    name="extract",
    version="0.1.0",
    license="proprietary",
    description="Minimal Project Task Management",
    author="Louis Gare",
    packages=find_packages(where="modules"),
    package_dir={"": "modules"},
    entry_points={
        "console_scripts": [
            "extract = extract.cli:extract_cli",
        ]
    },
)
