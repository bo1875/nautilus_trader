# Nautilus Trader maintenance fork

This fork maintains a bounded patch set on Nautilus Trader 1.224.0, upstream commit `076fe374f769ab65654aa5d472f8970ae47607eb`.

The `maintenance/1.224.0` branch includes OKX status and endpoint corrections, opt-in signed reported CASH balances with atomic validation and account-scoped reload, and strict order/fill/position report APIs that retain usable evidence while reporting malformed rows and conflicting duplicates. Native affordability and borrowing permissions are unchanged.

Commit `9c78e575f2a4590b98ec01b5cd314a34bbe45a1d` reproduces the previously qualified source exactly. Its 4,078 input files have combined SHA-256 `58bea7fefcd713e51a1b0436c24aa0cf513b2aff9a1d52b1f4d963456b3b716d`. That qualification does not certify a newly built binary.

The next release candidate uses the distinct identity `1.224.0+fork.3`. CI builds a Linux x86-64 CPython 3.12 wheel in a clean Debian Bookworm container, runs Rust OKX tests and installed-native regression tests, and retains the wheel, hashes, toolchain details and test reports as artifacts. It does not publish a release or deploy automatically. Application compatibility and exact artifact acceptance precede release adoption.

Use immutable commits and wheel SHA-256 pins. Do not depend on the moving maintenance branch. Upstream LICENSE and copyright notices remain in force; this is an independently maintained fork, not an upstream release.

## Tests

`fork_tests` contains synthetic loopback native-report and reported-account contracts. Application-owned reservation policies are tested by consuming applications. Tests must execute against the installed wheel from outside the source checkout; run with `--runxfail` so unavailable native capabilities cannot count as success.


Place-request observations now use the existing pending-request owner for handler,
send, response and reconnect correlation. They retain the requested price and
size, order identity and connection generation. A malformed or contradictory
reply remains unresolved; transport acknowledgment does not establish order
application or durable persistence. Existing order-event behavior is unchanged.
The capture limits and loss counters continue to bound this additional evidence.
Installed-client regressions use a synthetic local websocket venue.
