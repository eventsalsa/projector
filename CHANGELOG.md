# Changelog

## [0.5.0](https://github.com/eventsalsa/projector/compare/v0.4.0...v0.5.0) (2026-10-07)


### Features

* **projector:** add detached projections and a registry builder ([#19](https://github.com/eventsalsa/projector/issues/19)) ([f17cd1a](https://github.com/eventsalsa/projector/commit/f17cd1ae5fffb4b1c56af9c83a3f69e79cf70ae3))
* **projector:** add error classification and per-projection failure policy ([#23](https://github.com/eventsalsa/projector/issues/23)) ([bfd156f](https://github.com/eventsalsa/projector/commit/bfd156ff39ccb95c009cd6abf6932b6f63aadb55))


### Bug Fixes

* **projector:** avoid racing the leader connection on forced shutdown ([#24](https://github.com/eventsalsa/projector/issues/24)) ([e2e6652](https://github.com/eventsalsa/projector/commit/e2e665212f5489f4d46e4760b52ad2b75915918b))

## [0.4.0](https://github.com/eventsalsa/projector/compare/v0.3.0...v0.4.0) (2026-08-22)


### Features

* **daemon:** refine lifecycle and coordination for errgroup compatibility ([8ffb003](https://github.com/eventsalsa/projector/commit/8ffb003ce1978d6f342020b7f64a5322b9cfa2fd))

## [0.3.0](https://github.com/eventsalsa/projector/compare/v0.2.0...v0.3.0) (2026-08-22)


### Features

* **observer:** introduce pluggable telemetry and lifecycle observer ([7dd0852](https://github.com/eventsalsa/projector/commit/7dd08526b389d56e08f729777e5d7cc93cfc8a28))

## [0.2.0](https://github.com/eventsalsa/projector/compare/v0.1.0...v0.2.0) (2026-08-21)


### Features

* **projector:** rename worker to projector and upgrade to store v0.2.0 ([#13](https://github.com/eventsalsa/projector/issues/13)) ([312f2fe](https://github.com/eventsalsa/projector/commit/312f2fe87d6717ad86f01cd7c34f9580fe4b40ac))

## [0.1.0](https://github.com/eventsalsa/worker/compare/v0.0.3...v0.1.0) (2026-08-20)


### ⚠ BREAKING CHANGES

* **worker:** replace aggregate terminology with stream across consumer contracts and event filtering

### Code Refactoring

* **worker:** align with stream terminology and upgrade store to v0.1.0 ([#11](https://github.com/eventsalsa/worker/issues/11)) ([e07b3c9](https://github.com/eventsalsa/worker/commit/e07b3c93f5e389b074a721b169a9d67119cacb9e))

## v0.0.2

### Added

- Added `cmd/migrate-gen`, a stable CLI entrypoint for generating worker infrastructure migrations with the same defaults and table-name overrides as `migrations.Config`.

### Documentation

- Documented the quick CLI flow and the package-level migration API in `README.md` and `migrations` package docs.
