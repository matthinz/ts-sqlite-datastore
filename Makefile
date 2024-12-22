SOURCES := $(shell find src -name '*.ts')

.PHONY: build test

build: dist README.md

test: build
	@if [ "$(FILTER)" = "" ]; then \
		yarn test; \
	else \
		yarn test -t $(FILTER); \
	fi

dist: node_modules $(SOURCES)
	yarn build
	touch $@

node_modules: package.json yarn.lock
	yarn install
	touch $@

README.md: dist
	node dist/tools/build-readme.js src/sqlite-datastore.ts > $@
