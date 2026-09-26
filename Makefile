# The checks CI runs, in one place, so they are also one command locally.
#
#   make check    everything
#   make lint     fmt + clippy
#   make test     the test suite
#
# Installing the git hook that runs `make lint-quick` on every commit:
#
#   git config core.hooksPath .githooks

.PHONY: check lint lint-quick fmt test msrv deps doc clean

# Read the claimed floor from the manifest rather than repeating it here, so the
# two cannot drift apart.
MSRV := $(shell sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml)

check: lint test msrv deps doc

lint: fmt clippy

# What the pre-commit hook runs: fast, and catches the things most likely to be
# wrong in an edit.
lint-quick: fmt clippy

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --all-targets --all-features --locked

test:
	cargo test --all-features --locked

# Needs the toolchain once: rustup toolchain install $(MSRV)
msrv:
	@echo "checking against the declared MSRV ($(MSRV))"
	cargo +$(MSRV) check --all-targets --all-features --locked

# Needs the toolchain once: cargo install cargo-deny
deps:
	@command -v cargo-deny >/dev/null 2>&1 \
		|| { echo "cargo-deny is not installed: cargo install cargo-deny"; exit 1; }
	cargo deny check

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

clean:
	cargo clean
