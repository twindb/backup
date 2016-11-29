.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help
define BROWSER_PYSCRIPT
import os, webbrowser, sys
try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT
BROWSER := python -c "$$BROWSER_PYSCRIPT"

PYTHON := $(shell rpm --eval '%{__python}')
PYTHON_LIB := $(shell rpm --eval '%{python_sitelib}')
RHEL := $(shell if test -z "${OS_VERSION}"; then rpm --eval '%{rhel}'; else echo ${OS_VERSION}; fi)
pwd := $(shell pwd)
build_dir = ${pwd}/build
top_dir = ${build_dir}/rpmbuild
version = $(shell python -c 'from twindb_backup import __version__; print(__version__)')
PY_MAJOR = $(shell python -c 'import sys; print(sys.version[:3])')

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts


clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

lint: ## check style with flake8
	flake8 twindb_backup tests

test-deps:
	pip install --upgrade -r requirements.txt
	pip install --upgrade -r requirements_dev.txt
	pip install -U setuptools

test: test-deps ## run tests quickly with the default Python
	py.test tests/unit

test-integration: test-deps ## run integration tests
	pip show twindb-backup || pip install -e .
	py.test -xv tests/integration

test-all: ## run tests on every Python version with tox
	tox

coverage: ## check code coverage quickly with the default Python
	coverage run --source twindb_backup py.test

		coverage report -m
		coverage html
		$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/twindb_backup.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ twindb_backup
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: clean ## package and upload a release
	python setup.py sdist upload
	python setup.py bdist_wheel upload

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

install: clean ## install the package to the active Python's site-packages
	if test -z "${DESTDIR}" ; \
	then $(PYTHON) setup.py install \
		--prefix /usr \
		--install-lib $(PYTHON_LIB); \
	else $(PYTHON) setup.py install \
		--prefix /usr \
		--install-lib $(PYTHON_LIB) \
		--root "${DESTDIR}" ; \
		mkdir -p "${DESTDIR}/etc/cron.d/" ; \
		install -m 644 -o root support/twindb-backup.cron "${DESTDIR}/etc/cron.d/twindb-backup" ; \
		mkdir -p "${DESTDIR}/etc/twindb/" ; \
		install -m 600 -o root support/twindb-backup.cfg "${DESTDIR}/etc/twindb" ; \
	fi

rpm: ## Build rpm
	rm -rf "${build_dir}"
	mkdir -p "${top_dir}/SOURCES"
	$(PYTHON) setup.py sdist --dist-dir "${top_dir}/SOURCES"
	rpmbuild --define '_topdir ${top_dir}' --define 'version ${version}' --define 'PY_MAJOR ${PY_MAJOR}' -ba support/twindb-backup.spec

rhel:
	echo ${RHEL}

docker-rpm: ## Build rpm in a docker container
	@sudo docker run -v `pwd`:/twindb-backup:rw -e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" -e "AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}" -e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" centos:centos${RHEL} /bin/bash /twindb-backup/support/bootstrap-docker.sh
	find ${build_dir}
