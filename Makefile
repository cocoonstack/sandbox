KERNEL_VERSION := $(shell cat boot/kernel/VERSION)
KERNEL_MIRROR ?= https://cdn.kernel.org/pub/linux/kernel
BOOT_IMAGE ?= sandbox-boot:$(KERNEL_VERSION)

SILKD_VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' silkd/Cargo.toml | head -1)
SILKD_IMAGE ?= sandbox-silkd:$(SILKD_VERSION)

EXTRACT_IMAGE ?= $(BOOT_IMAGE)

# The parent workspace's go.work excludes these modules; GOWORK=off keeps
# local invocations identical to CI.
GO_MODULES := sandboxd sdk/go e2e mcp
GO_OSES := linux darwin

.PHONY: help test lint boot boot-debug extract extract-debug silkd-image base python images \
	sandboxd go-test go-lint

help: ## show this list
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "%-14s %s\n", $$1, $$2}'

test: ## Rust tests: boot/init + silkd
	cd boot/init && cargo test
	cd silkd && cargo test

lint: ## Rust fmt --check + clippy -D warnings: boot/init + silkd
	cd boot/init && cargo fmt --check && cargo clippy --all-targets -- -D warnings
	cd silkd && cargo fmt --check && cargo clippy --all-targets -- -D warnings

sandboxd: ## build dist/sandboxd
	mkdir -p dist
	cd sandboxd && GOWORK=off go build -o ../dist/sandboxd .

go-test: ## go test -race across the Go modules
	for m in $(GO_MODULES); do (cd $$m && GOWORK=off go test -race ./...) || exit 1; done

go-lint: ## golangci-lint (run + fmt --diff) across the Go modules, GOOS linux+darwin
	for m in $(GO_MODULES); do \
		for os in $(GO_OSES); do \
			(cd $$m && GOWORK=off GOOS=$$os golangci-lint run ./...) || exit 1; \
		done; \
		(cd $$m && GOWORK=off golangci-lint fmt --diff ./...) || exit 1; \
	done

# --platform: the kernel build is x86-only (x86_64_defconfig, PVH); without
# the pin, arm64 hosts build an aarch64 stage that dies inside kbuild.
boot: ## kernel + initramfs artifact image (docker)
	docker build --platform linux/amd64 -t $(BOOT_IMAGE) --build-arg KERNEL_VERSION=$(KERNEL_VERSION) --build-arg KERNEL_MIRROR=$(KERNEL_MIRROR) boot

boot-debug: ## boot image with busybox + /bin/sh on fatal errors
	docker build --platform linux/amd64 -t $(BOOT_IMAGE)-debug --build-arg KERNEL_VERSION=$(KERNEL_VERSION) --build-arg KERNEL_MIRROR=$(KERNEL_MIRROR) --build-arg INITRD_DEBUG=1 boot

extract: ## dump /boot artifacts into dist/ for boot-bench.sh
	rm -rf dist && mkdir -p dist
	cid=$$(docker create $(EXTRACT_IMAGE)) && docker cp $$cid:/boot dist/boot && docker rm $$cid

extract-debug: ## extract from the boot-debug image
	$(MAKE) extract EXTRACT_IMAGE=$(BOOT_IMAGE)-debug

silkd-image: ## silkd release binary in a scratch carrier image
	docker build --platform linux/amd64 -t $(SILKD_IMAGE) -f silkd/Dockerfile silkd

base: silkd-image ## base VM image against the local boot + silkd images
	docker build --platform linux/amd64 -t sandbox-base:dev \
		--build-arg BOOT_IMAGE=$(BOOT_IMAGE) \
		--build-arg SILKD_IMAGE=$(SILKD_IMAGE) \
		--secret id=sandbox_install_agent,src=os-image/base/install-agent.sh \
		-f os-image/base/24.04/Dockerfile os-image/base

python: base ## python flavor image on top of base
	docker build --platform linux/amd64 -t sandbox-python:dev \
		--build-arg BASE_IMAGE=sandbox-base:dev \
		-f os-image/python/3.12/Dockerfile os-image/python

images: base python ## all VM images
