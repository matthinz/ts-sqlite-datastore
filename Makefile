SOURCES := $(shell find src -name '*.ts') tsconfig.json

.PHONY: build test

build: dist

test: build
	@if [ "$(FILTER)" = "" ]; then \
		yarn test; \
	else \
		yarn test --test-name-pattern=$(FILTER); \
	fi

dist: node_modules $(SOURCES)
	yarn build
	touch $@

node_modules: package.json yarn.lock
	yarn install
	touch $@
