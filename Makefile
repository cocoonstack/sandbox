KERNEL_VERSION := $(shell cat boot/kernel/VERSION)
KERNEL_MIRROR ?= https://cdn.kernel.org/pub/linux/kernel
BOOT_IMAGE ?= sandbox-boot:$(KERNEL_VERSION)

SILKD_VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' silkd/Cargo.toml | head -1)
SILKD_IMAGE ?= sandbox-silkd:$(SILKD_VERSION)

# Match only v* (binary releases); sdk-*-v* tags belong to the SDK packages.
SANDBOXD_VERSION ?= $(shell git describe --tags --match 'v*' --always --dirty)

EXTRACT_IMAGE ?= $(BOOT_IMAGE)

# The parent workspace's go.work excludes these modules; GOWORK=off keeps
# local invocations identical to CI.
GO_MODULES := protocol/wire sandboxd sdk/go e2e mcp
GO_OSES := linux darwin

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool versions
GOLANGCILINT_VERSION ?= v2.12.2
GOLANGCILINT_ROOT := $(LOCALBIN)/golangci-lint-$(GOLANGCILINT_VERSION)
GOLANGCILINT := $(GOLANGCILINT_ROOT)/golangci-lint

.PHONY: help test lint sh-lint boot boot-debug extract extract-debug silkd-image base python images \
	sandboxd go-test go-lint bench cloc

## Tool download targets
.PHONY: golangci-lint
golangci-lint: $(GOLANGCILINT)
$(GOLANGCILINT):
	GOBIN=$(GOLANGCILINT_ROOT) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCILINT_VERSION)

cloc: ## Count lines of code excluding tests (requires cloc)
	cloc --exclude-dir=target,dist,node_modules --exclude-ext=json \
		--not-match-f='(_test\.go|_test\.py|\.test\.ts)$$' .

help: ## show this list
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "%-14s %s\n", $$1, $$2}'

test: ## Rust tests: boot/init + silkd
	cd boot/init && cargo test
	cd silkd && cargo test

lint: ## Rust fmt --check + clippy -D warnings: boot/init + silkd
	cd boot/init && cargo fmt --check && cargo clippy --all-targets -- -D warnings
	cd silkd && cargo fmt --check && cargo clippy --all-targets -- -D warnings

sh-lint: ## shellcheck every tracked shell script
	git ls-files '*.sh' | xargs shellcheck

sandboxd: ## build dist/sandboxd
	mkdir -p dist
	cd sandboxd && GOWORK=off go build -ldflags "-X main.version=$(SANDBOXD_VERSION)" -o ../dist/sandboxd .

go-test: ## go test -race across the Go modules
	for m in $(GO_MODULES); do (cd $$m && GOWORK=off go test -race ./...) || exit 1; done

bench: ## claim-tier + data-plane benchmarks on this node (see docs/benchmarks.md)
	bash scripts/bench.sh

go-lint: golangci-lint ## golangci-lint (run + fmt --diff) across the Go modules, GOOS linux+darwin
	for m in $(GO_MODULES); do \
		for os in $(GO_OSES); do \
			(cd $$m && GOWORK=off GOOS=$$os $(GOLANGCILINT) run ./...) || exit 1; \
		done; \
		(cd $$m && GOWORK=off $(GOLANGCILINT) fmt --diff ./...) || exit 1; \
	done

boot: ## kernel + initramfs artifact image (docker)
	docker build -t $(BOOT_IMAGE) --build-arg KERNEL_VERSION=$(KERNEL_VERSION) --build-arg KERNEL_MIRROR=$(KERNEL_MIRROR) boot

boot-debug: ## boot image with busybox + /bin/sh on fatal errors
	docker build -t $(BOOT_IMAGE)-debug --build-arg KERNEL_VERSION=$(KERNEL_VERSION) --build-arg KERNEL_MIRROR=$(KERNEL_MIRROR) --build-arg INITRD_DEBUG=1 boot

extract: ## dump /boot artifacts into dist/ for boot-bench.sh
	rm -rf dist && mkdir -p dist
	cid=$$(docker create $(EXTRACT_IMAGE)) && docker cp $$cid:/boot dist/boot && docker rm $$cid

extract-debug: ## extract from the boot-debug image
	$(MAKE) extract EXTRACT_IMAGE=$(BOOT_IMAGE)-debug

silkd-image: ## silkd release binary in a scratch carrier image
	docker build -t $(SILKD_IMAGE) -f silkd/Dockerfile silkd

base: silkd-image ## base VM image against the local boot + silkd images
	docker build -t sandbox-base:dev \
		--build-arg BOOT_IMAGE=$(BOOT_IMAGE) \
		--build-arg SILKD_IMAGE=$(SILKD_IMAGE) \
		--secret id=sandbox_install_agent,src=os-image/base/install-agent.sh \
		-f os-image/base/24.04/Dockerfile os-image/base

python: base ## python flavor image on top of base
	docker build -t sandbox-python:dev \
		--build-arg BASE_IMAGE=sandbox-base:dev \
		-f os-image/python/3.12/Dockerfile os-image/python

images: base python ## all VM images
