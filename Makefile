CARGO ?= cargo
RUN_ARGS ?=
IMAGE ?= ghcr.io/example/l4guard:0.1.0

.PHONY: build release run run-release clean docker-build manifests check e8-check e9-check e9-recovery

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

e8-check:
	./scripts/e8-tls-passthrough.py
	./scripts/e8-cometbft-sync.sh

e9-check:
	./scripts/e9-verify.py all

e9-recovery:
	./scripts/e9-cometbft-recovery.sh
