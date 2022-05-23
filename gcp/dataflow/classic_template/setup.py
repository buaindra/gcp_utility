from setuptools import setup, find_packages

setup(name = "classic_template",
  version="0.1.0",
  packages=find_packages(),
  install_requires=["google-cloud-storage"]
)