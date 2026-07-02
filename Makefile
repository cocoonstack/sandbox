KERNEL_VERSION := $(shell cat boot/kernel/VERSION)
KERNEL_MIRROR ?= https://cdn.kernel.org/pub/linux/kernel
BOOT_IMAGE ?= sandbox-boot:$(KERNEL_VERSION)

EXTRACT_IMAGE ?= $(BOOT_IMAGE)

.PHONY: test lint boot boot-debug extract extract-debug base python images

test:
	cd boot/init && cargo test

lint:
	cd boot/init && cargo fmt --check && cargo clippy --all-targets -- -D warnings

# --platform: the kernel build is x86-only (x86_64_defconfig, PVH); without
# the pin, arm64 hosts build an aarch64 stage that dies inside kbuild.
boot:
	docker build --platform linux/amd64 -t $(BOOT_IMAGE) --build-arg KERNEL_VERSION=$(KERNEL_VERSION) --build-arg KERNEL_MIRROR=$(KERNEL_MIRROR) boot

boot-debug:
	docker build --platform linux/amd64 -t $(BOOT_IMAGE)-debug --build-arg KERNEL_VERSION=$(KERNEL_VERSION) --build-arg KERNEL_MIRROR=$(KERNEL_MIRROR) --build-arg INITRD_DEBUG=1 boot

extract:
	rm -rf dist && mkdir -p dist
	cid=$$(docker create $(EXTRACT_IMAGE)) && docker cp $$cid:/boot dist/boot && docker rm $$cid

extract-debug:
	$(MAKE) extract EXTRACT_IMAGE=$(BOOT_IMAGE)-debug

base:
	docker build --platform linux/amd64 -t sandbox-base:dev \
		--build-arg BOOT_IMAGE=$(BOOT_IMAGE) \
		--secret id=sandbox_install_agent,src=os-image/base/install-agent.sh \
		-f os-image/base/24.04/Dockerfile os-image/base

python: base
	docker build --platform linux/amd64 -t sandbox-python:dev \
		--build-arg BASE_IMAGE=sandbox-base:dev \
		-f os-image/python/3.12/Dockerfile os-image/python

images: base python
