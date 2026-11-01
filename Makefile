CARGO ?= cargo
RUN_ARGS ?=

.PHONY: build release run run-release clean

build:
	$(CARGO) build

release:
	$(CARGO) build --release

run:
	$(CARGO) run -- $(RUN_ARGS)

run-release:
	$(CARGO) run --release -- $(RUN_ARGS)

clean:
	$(CARGO) clean
