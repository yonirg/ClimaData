from setuptools import setup, find_packages

setup(
    name="climadata",
    version="0.1",
    packages=find_packages(),
    install_requires=[
      "celery", "pandas", "pyarrow"
    ]
)