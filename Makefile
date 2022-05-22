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
		print("%-40s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT
BROWSER := python -c "$$BROWSER_PYSCRIPT"

PYTHON := python
PYTHON_LIB := $(shell $(PYTHON) -c "from distutils.sysconfig import get_python_lib; import sys; sys.stdout.write(get_python_lib())" )


PLATFORM ?= centos
OS_VERSION ?= 7

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

.PHONY: pip
pip:
	pip install -U "pip ~= 22.0.0"

.PHONY: pip-tools
pip-tools: pip
	pip install -U "pip-tools ~= 6.6"

.PHONY: setuptools
setuptools: pip
	pip install -U "setuptools ~= 62.3.2"

.PHONY: upgrade-requirements
upgrade-requirements: pip-tools ## Upgrade requirements
	pip-compile --upgrade --verbose --output-file requirements.txt requirements.in
	pip-compile --upgrade --verbose --output-file requirements_dev.txt requirements_dev.in

.PHONY: bootstrap
bootstrap: pip pip-tools setuptools ## bootstrap the development environment
	pip-sync requirements.txt requirements_dev.txt
	pip install --editable .

clean: clean-build clean-pyc clean-test clean-docs ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -rf pkg/
	rm -rf omnibus/pkg/
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


clean-docs:
	rm -rf docs/_build

black: ## Fix code style errors
	black --line-length 80 twindb_backup

lint: ## check style with pylint
	yamllint .
	black --check --line-length 80 twindb_backup
	pycodestyle --max-line-length=80 twindb_backup
	pylint twindb_backup


test: ## Run tests quickly with the default Python
	pytest -xv --cov-report term-missing --cov=./twindb_backup tests/unit
	codecov


test-integration: ## Run integration tests. Must be run in vagrant
	py.test -xsv tests/integration/

coverage: ## check code coverage quickly with the default Python
	py.test --cov-report term-missing  --cov=twindb_backup tests/unit

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/twindb_backup.rst
	# rm -f docs/modules.rst
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
		-e "GC_CREDENTIALS_FILE=${GC_CREDENTIALS_FILE}" \
		-e "OS_VERSION"=${OS_VERSION} \
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


docker-start:
	@docker run \
		-v ${pwd}:/twindb-backup \
		-e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
		-e "AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}" \
		-e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
		-e "GC_CREDENTIALS_FILE=${GC_CREDENTIALS_FILE}" \
		-it \
		--name builder_xtrabackup \
		--rm \
		--dns 8.8.8.8 \
		--dns 208.67.222.222 \
		--env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
		--env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
		--env PLATFORM=${PLATFORM} \
		--env OS_VERSION=${OS_VERSION} \
		--env LC_ALL=C.UTF-8 \
		--env LANG=C.UTF-8 \
		"twindb/omnibus-${PLATFORM}:${OS_VERSION}" \
		bash -l

ifeq ($(OS_VERSION),6)
        PLATFORM = centos
endif
ifeq ($(OS_VERSION),7)
        PLATFORM = centos
endif
ifeq ($(OS_VERSION),focal)
        PLATFORM = ubuntu
endif
ifeq ($(OS_VERSION),xenial)
        PLATFORM = ubuntu
endif
ifeq ($(OS_VERSION),bionic)
        PLATFORM = ubuntu
endif
ifeq ($(OS_VERSION),cosmic)
        PLATFORM = ubuntu
endif

ifeq ($(OS_VERSION),stretch)
        PLATFORM = debian
endif
package: ## Build package - OS_VERSION must be one of: 6, 7, jessie, stretch, xenial, bionic, cosmic.
	@docker run \
		-v ${pwd}:/twindb-backup \
		--name builder_xtrabackup \
		--rm \
		--dns 8.8.8.8 \
		--dns 208.67.222.222 \
		--env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
		--env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
		--env PLATFORM=${PLATFORM} \
		--env OS_VERSION=${OS_VERSION} \
		"twindb/omnibus-${PLATFORM}:${OS_VERSION}" \
		bash -l /twindb-backup/omnibus/omnibus_build.sh

install_package:
	if [ "${PLATFORM}" == "centos" ]
	then
		yum install -y $(ls omnibus/pkg/*.rpm)
	else
		dpkg -i $(ls omnibus/pkg/*.deb) | apt-get install -f
	fi

safety: ## check for known security vulnerabilities
	docker run --rm -it -v ${pwd}:/twindb-backup pyupio/safety safety check -r /twindb-backup/requirements.txt
	docker run --rm -it -v ${pwd}:/twindb-backup pyupio/safety safety check -r /twindb-backup/requirements_dev.txt
