CARGO ?= cargo
RUN_ARGS ?=
IMAGE ?= ghcr.io/example/l4guard:0.1.0

.PHONY: build release run run-release clean docker-build manifests check

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

docker-build:
	docker build -t $(IMAGE) .

manifests:
	kubectl kustomize deploy/base
	kubectl kustomize deploy/backend

check:
	$(CARGO) fmt --check
	$(CARGO) clippy --all-targets --all-features -- -D warnings
	$(CARGO) test
