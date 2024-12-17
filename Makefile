SOURCES := $(shell find src -name '*.ts')

.PHONY: build test

build: dist

test: build
	yarn test

dist: $(SOURCES)
	yarn build
	touch $@

