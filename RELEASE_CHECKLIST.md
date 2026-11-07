# Release Checklist

- [ ] Update the version and compatibility matrix.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run warning-free Clippy on all targets and features.
- [ ] Run the complete workspace test suite on Linux.
- [ ] Run the `redis-cli` interoperability script against a release build.
- [ ] Verify snapshot restart and interrupted-temp-file recovery tests.
- [ ] Build all workspace crates in release mode.
- [ ] Re-run benchmarks on an idle host and publish raw samples, hardware,
      versions, configurations, and methodology.
- [ ] Review `SECURITY.md`, especially the no-auth/TLS warning.
- [ ] Confirm no documentation claims unsupported Redis features or production
      readiness.
- [ ] Tag the exact tested commit and attach checksums for release artifacts.

