VERSION := $(shell head -n 1 debian/changelog | awk '{match( $$0, /\(.+?\)/); print substr( $$0, RSTART+1, RLENGTH-2 ) }' | cut -d- -f1 )

all:
	./setup.py build

install:
	./setup.py install --root=$(DESTDIR) --install-purelib=/usr/lib/python3/dist-packages/ --prefix=/usr --no-compile -O0

version:
	echo $(VERSION)

clean:
	./setup.py clean || true
	$(RM) -r build
	$(RM) dpkg
	$(RM) -r htmlcov
	dh_clean || true
	find -name *.pyc -delete
	find -name __pycache__ -delete

dist-clean: clean

.PHONY:: all install version clean dist-clean

test-blueprints:
	echo ubuntu-focal-base

test-requires:
	echo flake8 python3-pytest python3-pytest-cov python3-pytest-django python3-pytest-mock

lint:
	flake8 --ignore=E501,E201,E202,E111,E126,E114,E402 --statistics --exclude subcontractor_plugins/iputils/pyping .

test:
	py.test-3 -x --cov=subcontractor_plugins --cov-report html --cov-report term -vv subcontractor_plugins

.PHONY:: test-blueprints test-requres test

dpkg-blueprints:
	echo ubuntu-focal-base

dpkg-requires:
	echo dpkg-dev debhelper python3-dev python3-setuptools dh-python

dpkg:
	dpkg-buildpackage -b -us -uc
	touch dpkg

dpkg-file:
	echo $(shell ls ../subcontractor-plugins_*.deb):focal

.PHONY:: dpkg-requires dpkg dpkg-file
