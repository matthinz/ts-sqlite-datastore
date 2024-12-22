SOURCES := $(shell find src -name '*.ts')

.PHONY: build test

build: dist

test: build
	@if [ "$(FILTER)" = "" ]; then \
		yarn test; \
	else \
		yarn test -t $(FILTER); \
	fi

dist: $(SOURCES)
	yarn build
	touch $@

