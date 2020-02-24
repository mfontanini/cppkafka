PACKAGE_NAME?= cppkafka
VERSION?=      $(shell ../get_version.py ../../CMakeLists.txt)
BUILD_NUMBER?= 1
MOCK_CONFIG?=  default
RESULT_DIR?=   pkgs-$(VERSION)-$(BUILD_NUMBER)-$(MOCK_CONFIG)

all: rpm

SOURCES:
	mkdir -p SOURCES

archive: SOURCES
	cd ../../ && \
	git archive --prefix=$(PACKAGE_NAME)-$(VERSION)/ \
		-o packaging/rpm/SOURCES/$(PACKAGE_NAME)-$(VERSION).tar.gz HEAD


build_prepare: archive
	mkdir -p $(RESULT_DIR)
	rm -f $(RESULT_DIR)/$(PACKAGE_NAME)*.rpm


srpm: build_prepare
	/usr/bin/mock \
		-r $(MOCK_CONFIG) \
		--define "__version $(VERSION)" \
		--define "__release $(BUILD_NUMBER)" \
		--resultdir=$(RESULT_DIR) \
		--no-clean --no-cleanup-after \
		--buildsrpm \
		--spec=cppkafka.spec \
		--sources=SOURCES

rpm: srpm
	/usr/bin/mock \
		-r $(MOCK_CONFIG) \
		--define "__version $(VERSION)"\
		--define "__release $(BUILD_NUMBER)"\
		--resultdir=$(RESULT_DIR) \
		--no-clean --no-cleanup-after \
		--rebuild $(RESULT_DIR)/$(PACKAGE_NAME)*.src.rpm

copy-artifacts:
	cp $(RESULT_DIR)/*rpm ../../artifacts/

clean:
	rm -rf SOURCES
	/usr/bin/mock -r $(MOCK_CONFIG) --clean
