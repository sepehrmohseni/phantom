CARGO ?= cargo
TRUNK ?= trunk
UPSTREAM ?= http://localhost:3000
DB_PATH ?= ./phantom.redb
BIN := target/release/phantom
UI_DIST := crates/phantom-ui/dist

.DEFAULT_GOAL := release
.PHONY: setup ui ui-dev build release run dev check clippy fmt fmt-check test ci clean install

setup:
	rustup target add wasm32-unknown-unknown
	$(CARGO) install --locked trunk

ui:
	$(TRUNK) build --release

ui-dev:
	$(TRUNK) serve

build: ui
	$(CARGO) build -p phantom-cli

release: ui
	$(CARGO) build --release -p phantom-cli
	@ls -lh $(BIN)

run: release
	$(BIN) start --upstream $(UPSTREAM) --db-path $(DB_PATH)

dev:
	$(CARGO) run -p phantom-cli -- start --upstream $(UPSTREAM) --db-path $(DB_PATH) --ui-dir $(UI_DIST)

check:
	$(CARGO) check --workspace --exclude phantom-ui --all-targets
	$(CARGO) check -p phantom-ui --target wasm32-unknown-unknown

clippy:
	$(CARGO) clippy --workspace --exclude phantom-ui --all-targets -- -D warnings
	$(CARGO) clippy -p phantom-ui --target wasm32-unknown-unknown -- -D warnings

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

test:
	$(CARGO) test --workspace --exclude phantom-ui

ci: fmt-check clippy test release

install: release
	install -m 0755 $(BIN) $(HOME)/.cargo/bin/phantom

clean:
	$(CARGO) clean
	rm -rf $(UI_DIST)
