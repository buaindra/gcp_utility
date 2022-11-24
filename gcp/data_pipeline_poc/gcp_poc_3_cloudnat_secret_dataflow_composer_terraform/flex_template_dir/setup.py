from setuptools import setup, find_packages

setup(name= "beam_package",
	version="0.0.1",
	packages=find_packages(),
	install_requires=["requests", "google-cloud-logging", "google-cloud-secret-manager"]
)