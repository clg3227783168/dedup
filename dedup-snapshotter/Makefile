.PHONY: all build install clean test

BINARY := dedup-snapshotter
INSTALL_PATH := /usr/local/bin
CONFIG_PATH := /etc/dedup-snapshotter
SYSTEMD_PATH := /etc/systemd/system

all: build

build:
	CGO_ENABLED=1 go build -o bin/$(BINARY) ./cmd

install: build
	install -D -m 0755 bin/$(BINARY) $(INSTALL_PATH)/$(BINARY)
	install -D -m 0755 scripts/install.sh /tmp/dedup-snapshotter-install.sh
	@echo "Installing dedup-snapshotter..."
	@/tmp/dedup-snapshotter-install.sh

install-config:
	mkdir -p $(CONFIG_PATH)
	@if [ ! -f $(CONFIG_PATH)/config.json ]; then \
		echo '{"root":"/var/lib/containerd/io.containerd.snapshotter.v1.dedup","enable_erofs":true,"enable_lazy":true,"enable_mem_dedup":true}' > $(CONFIG_PATH)/config.json; \
	fi

install-systemd:
	install -D -m 0644 scripts/dedup-snapshotter.service $(SYSTEMD_PATH)/dedup-snapshotter.service
	systemctl daemon-reload

clean:
	rm -rf bin/

test:
	go test -v ./...

.DEFAULT_GOAL := build
