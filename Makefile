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

PYTHON := python
PYTHON_LIB := $(shell $(PYTHON) -c "from distutils.sysconfig import get_python_lib; import sys; sys.stdout.write(get_python_lib())" )


PLATFORM := $(shell if test -z "${PLATFORM}"; then echo "centos"; else echo ${PLATFORM}; fi)
pwd := $(shell pwd)
build_dir = ${pwd}/build
top_dir = ${build_dir}/rpmbuild
version = $(shell python -c 'from twindb_backup import __version__; print(__version__)')
PY_MAJOR = $(shell python -c 'import sys; print(sys.version[:3])')
LOG_LEVEL := info
OMNIBUS_BRANCH := $(shell if test -z "${OMNIBUS_BRANCH}"; then echo "master"; else echo ${OMNIBUS_BRANCH}; fi)
OMNIBUS_SOFTWARE_BRANCH := $(shell if test -z "${OMNIBUS_SOFTWARE_BRANCH}"; then echo "master"; else echo ${OMNIBUS_SOFTWARE_BRANCH}; fi)
DOCKER_IMAGE := $(shell if test -z "${DOCKER_IMAGE}"; then echo "centos:centos7"; else echo ${DOCKER_IMAGE}; fi)

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

.PHONY: virtualenv
virtualenv: ## create virtual environment typically used for development purposes
	virtualenv env --setuptools --prompt='(twindb_backup)'

.PHONY: rebuild-requirements
rebuild-requirements: ## Rebuild requirements files requirements.txt and requirements_dev.txt
	pip-compile --verbose --no-index --output-file requirements.txt requirements.in
	pip-compile --verbose --no-index --output-file requirements_dev.txt requirements_dev.in

.PHONY: upgrade-requirements
upgrade-requirements: ## Upgrade requirements
	pip-compile --upgrade --verbose --no-index --output-file requirements.txt requirements.in
	pip-compile --upgrade --verbose --no-index --output-file requirements_dev.txt requirements_dev.in

.PHONY: bootstrap
bootstrap: ## bootstrap the development environment
	pip install -U "setuptools==32.3.1"
	pip install -U "pip==9.0.1"
	pip install -U "pip-tools>=1.6.0"
	pip-sync requirements.txt requirements_dev.txt
	pip install --editable .

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -rf pkg/
	rm -rf cache/
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

clean-docker:
	@sudo docker rm twindb-backup-build-${PLATFORM}

lint: ## check style with flake8
	flake8 twindb_backup tests

test-deps:
	pip install --upgrade -r requirements.txt
	pip install --upgrade -r requirements_dev.txt
	pip install -U setuptools

test: clean bootstrap ## run tests quickly with the default Python
	pytest --cov=./twindb_backup tests/unit
	codecov

test-integration: test-deps ## run integration tests
	pip show twindb-backup || pip install -e .
	py.test -xsv tests/integration

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

docker-test: ## Test twindb-backup in a docker container
	@sudo docker run \
		-v `pwd`:/twindb-backup:rw \
		-e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
		-e "AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}" \
		-e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
		-e "CI"=${CI} \
		-e "TRAVIS"=${TRAVIS} \
		-e "TRAVIS_BRANCH"=${TRAVIS_BRANCH} \
		-e "TRAVIS_COMMIT"=${TRAVIS_COMMIT} \
		-e "TRAVIS_JOB_NUMBER"=${TRAVIS_JOB_NUMBER} \
		-e "TRAVIS_PULL_REQUEST"=${TRAVIS_PULL_REQUEST} \
		-e "TRAVIS_JOB_ID"=${TRAVIS_JOB_ID} \
		-e "TRAVIS_REPO_SLUG"=${TRAVIS_REPO_SLUG} \
		-e "TRAVIS_TAG"=${TRAVIS_TAG} \
		${DOCKER_IMAGE} /bin/bash /twindb-backup/support/docker-test-${PLATFORM}.sh

package: ## Build package - PLATFORM must be one of "centos", "debian", "ubuntu"
	rm -rf pkg

	mkdir -p pkg
	mkdir -p "cache/${PLATFORM}"

	@sudo docker run --name "twindb-backup-build-${PLATFORM}" \
		-e LOG_LEVEL=${LOG_LEVEL} \
		-e OMNIBUS_BRANCH=${OMNIBUS_BRANCH} \
		-e OMNIBUS_SOFTWARE_BRANCH=${OMNIBUS_SOFTWARE_BRANCH} \
		-v ${pwd}/pkg:/twindb-backup/omnibus/pkg \
		-v ${pwd}/keys:/keys \
		-v "${pwd}/cache/${PLATFORM}:/var/cache/omnibus" \
		"twindb/backup-omnibus-${PLATFORM}"
