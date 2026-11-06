# Relay Fleet

A Java 21 control plane and asynchronous Python agent for **simulated devices** with intermittent connectivity. Generated Protocol Buffers/gRPC clients communicate over mutual TLS. SQLite journals on both sides preserve telemetry and configuration state across process restarts.

This is a local, single-control-plane project. It has no physical MCU, radio, robot, satellite, medical-device integration, cloud deployment or production-fleet claim.

## Working evidence

- Certificate identities map to tenant, role and device. Request fields cannot change that identity.
- Telemetry commits before ACK. Replays have one database effect; changed content under an existing sequence is rejected.
- Bounded agent journals pause simulation sampling at capacity without evicting unacknowledged rows.
- Desired configuration uses compare-and-swap versions; applied version/digest ACKs cannot regress state.
- The failure drill closes live TCP sockets, fills three queues, SIGKILLs one agent and the Java server, restarts them and verifies all **480 generated samples**. A separate lost-ACK drill proves replay creates no second effect.
- The operator CLI registers devices, inspects desired/applied versions and clock skew, queries telemetry and revokes identities within its certificate's tenant. Fleet listings traverse bounded, tenant-scoped pages instead of truncating large fleets.

The current full-suite run passed **10 Java tests, 11 Python tests and nine failure/security checks**. Independent review exposed a listing defect for large fleets; the new [pagination regression](evidence/pagination-regression.json) covers 1,025 devices with maximum-size configurations and names, including complete CLI traversal. Latest suite results and raw benchmark latencies are in [the verification log](evidence/final-python.log) and [failure-experiment.json](evidence/failure-experiment.json). Loopback benchmark runs vary substantially with host activity; none is a production-capacity claim or stable SLO.

## Run it

Requirements: Java 21, Python 3.12, a POSIX host and initial dependency-download access. Gradle 8.14.4 is pinned by wrapper and distribution SHA-256. Java dependencies are locked; Python dependencies are pinned in `requirements.lock`. No Docker or shared database is needed.

```bash
cd relay-fleet
bash scripts/setup.sh
bash scripts/test-publication.sh
```

Setup generates both languages' bindings and builds the Java distribution. The bounded publication test command runs 10 Java and 9 Python spool/agent/transport tests using actual SQLite and fresh disposable mTLS credentials. It excludes the fixed failure experiment and large pagination evidence-writing test. The original full experiment command, `bash scripts/verify.sh`, remains available for an explicitly requested rerun in a separate checkout. It regenerates historical evidence and includes a benchmark. Private test keys/databases are local runtime output, never distributed. A fresh clone uses ordinary ignored `.venv`, `.gradle`, `build` and `.runtime` directories.

For an interactive session, keep this running in one terminal:

```bash
.venv/bin/python -m scripts.serve_demo
```

Then use another terminal:

```bash
.venv/bin/python -m agent.operator --pki .runtime/manual/pki register sim-a sim-a
.venv/bin/python -m agent.operator --pki .runtime/manual/pki config sim-a --expected 0 --interval-ms 50
.venv/bin/python -m agent --pki .runtime/manual/pki --identity sim-a --device sim-a --spool .runtime/manual/sim-a.db --samples 100
.venv/bin/python -m agent.operator --pki .runtime/manual/pki list
.venv/bin/python -m agent.operator --pki .runtime/manual/pki query sim-a --limit 10
.venv/bin/python -m agent.operator --pki .runtime/manual/pki revoke sim-a
```

The demo preserves credentials/database across restarts. Repeated registration returns `ALREADY_EXISTS`; config updates require the current desired version. For a fresh demo, supply a new `--directory` to `scripts.serve_demo` and use its printed PKI path. `operator-north` and `operator-south` certificates address separate tenants.

`list` follows every continuation token and prints one complete JSON result; `list --page-size 37` changes each RPC's page size without changing the result. The CLI keeps the ordinary 4 MiB gRPC receive limit. It accumulates the complete output in memory, so client memory grows with the fleet even though each server response is bounded.

## Architecture

```mermaid
sequenceDiagram
    participant P as Simulated sampler
    participant A as Python agent
    participant J as SQLite journal
    participant S as Java gRPC service
    participant D as Control database
    P->>J: Atomic sequence allocation + append
    A->>J: Oldest at most 32 samples
    A->>S: mTLS Sync(samples, applied config ACK)
    S->>D: Recheck identity; validate sequence/hash
    S->>D: Atomic telemetry + reported-state commit
    S-->>A: Cumulative ACK + desired configuration
    A->>J: Delete through validated ACK
    A->>J: Validate and persist newer configuration
    Note over A,S: Lost response causes durable replay
```

`Store` owns a SQLite JDBC connection with WAL, `synchronous=FULL`, foreign keys and explicit transactions. Synchronized methods serialize transactions in this single-process design. The telemetry primary key is `(tenant, device, sequence)`; SHA-256 of the parsed sample's protobuf encoding detects conflicting replays. Sequences start at 1 and remain contiguous. Device timestamps are diagnostic data, never ordering or expiry authority. Each batch commits all samples and applied-state changes or rolls everything back.

The agent has separate sampling/sending tasks and one dedicated executor that owns its SQLite connection. Disk operations stay off the asyncio loop. Each agent allows one in-flight frame. The server requests one inbound frame at a time, withholding new demand until its ACK can be sent. Server inbound messages are capped at 80 KiB, with at most 32 samples of 2,048 UTF-8 bytes per frame. Device streams expire after 15 seconds even without a caller deadline. Clients renew deadline-bound streams with exponential retry and jitter. Cancellation closes the RPC/channel and drains/closes the disk worker.

The service bounds active device streams at 64, concurrent RPCs per connection at 32, and executor workers at eight with a 128-task queue. Saturation applies caller backpressure. These application limits are not a complete internet-facing denial-of-service defense.

## Contract and security

The contract is [`relay.v1`](proto/fleet.proto), with explicit device `schema_version=1`. Both languages generate bindings from the same schema. Field numbers are never reused and field 6 is reserved. Unknown additive protobuf fields remain transport-compatible; unsupported application schema versions fail explicitly. The database is the initial v1 schema with an additive internal `service_settings` table for its cursor-signing key; no general upgrade/migration framework is implemented.

**Listing compatibility correction:** during this pre-release review, `ListDevices` changed from `Empty` to `ListDevicesRequest(page_size, page_token)`, and its response added `next_page_token` as field 2. This is an intentional breaking correction to the operator API, not a claim of transparent compatibility. Regenerate and deploy the Java service and Python operator client together. Legacy empty requests decode with `page_size=0` and receive `INVALID_ARGUMENT`, preventing an old client from mistaking one page for the whole fleet. The device `Sync` contract and existing device journals are unchanged. Future breaking changes after release require a new API package/version.

Listing requires `page_size` from 1 through 100 and uses immutable device IDs in SQLite BINARY order. `next_page_token` is empty only when no further row exists; otherwise it resumes strictly after the last returned ID. Signed versioned cursors bind the tenant and last ID with HMAC-SHA-256; the key persists in the control database so cursors survive restarts. Malformed, tampered, foreign-tenant and oversized tokens fail with `INVALID_ARGUMENT`. SQL always derives tenant from the authenticated operator, independently of the token. Pages never exceed 512 KiB, safely below the default 4 MiB gRPC client limit even with 100 allowed 4,096-byte configs and maximum-length names. The large-fixture regression measured a 430,722-byte maximum page. A listing is a keyset traversal, not a database snapshot: concurrent additions behind the cursor appear on the next fresh traversal; additions ahead may appear during the current one. Static fleets are returned exactly once in order.

TLS validates the client chain against the configured CA. The interceptor hashes the authenticated **leaf certificate** and looks up its exact registration. CNs, metadata and claimed IDs grant no authority. Operator APIs derive tenant from that registration. Every device frame rechecks registration/revocation, including on an existing stream.

Local test certificates expire after seven days; private keys have mode 0600 and are ignored by source control. Trust boundaries are the host filesystem, operator bootstrap, CA, authenticated operator/device and control database. Revocation is implemented; automatic renewal/rotation, hardware key storage, CA revocation distribution and compromise recovery are outside this version. A stolen device key permits impersonation until revoked. Certificates do not attest reading truth or physical actuation.

| Condition | Result |
|---|---|
| Trusted CA certificate without registration | `UNAUTHENTICATED` |
| Untrusted certificate | TLS failure; client sees `UNAVAILABLE` |
| Device spoof or device invoking operator API | `PERMISSION_DENIED` |
| Cross-tenant query | `NOT_FOUND` |
| Changed replay at an existing sequence | `ALREADY_EXISTS`; rollback |
| Sequence gap, unknown config ACK, unsupported schema | `FAILED_PRECONDITION` |
| Stale desired compare-and-swap | `ABORTED` |
| Invalid config, oversized sample, invalid page size/cursor or legacy empty list request | `INVALID_ARGUMENT` |
| Excess frame/sample/stream limits | `RESOURCE_EXHAUSTED` |
| Deadline or maximum stream lifetime | `DEADLINE_EXCEEDED`; journal retained |

Configuration deliberately controls one behavior: `sample_interval_ms` must be a JSON integer from 10 to 60,000. Optional extra fields permit additive evolution. Only a known config version and matching SHA-256 can advance applied state. “Applied” means the simulated sampling configuration is committed locally and available to the sampling task, not firmware installation or physical actuation.

## Evidence and limits

[The failure report](evidence/failure-experiment.json) contains actual timestamps, assertions, authorization statuses, clock-skew diagnostics, an operator snapshot and raw batch latencies. [Validation notes](docs/validation.md) explain red/green evidence and corrected failures; [the runbook](docs/runbook.md) gives an investigation sequence. Full-suite logs are `evidence/final-java.log`, `evidence/final-python.log` and `evidence/pytest.xml`.

The benchmark sends 100 sequential 32-sample batches through a warmed mTLS stream, including SQLite FULL commits and ACKs. It excludes startup, PKI and handshake warmup. This is not a WAN, sustained-load, multi-replica or hardware benchmark. Profiling is reported separately because it changes timings.

Durability means process-crash recovery on the tested filesystem. Physical power failure, disk corruption, controller caches and disk-full failure were not injected. Sampling pauses when the queue fills, so no measurements are generated during that pause. SQLite/WAL overhead adds to payload storage; the row limit is not an exact file-size quota. Server telemetry has no retention policy. A lost spool or cloned identity needs explicit recovery; silent sequence reset is forbidden. Use one agent owner per spool and one Java process per database. No replication, HA, automatic anomaly detection or physical-device validation is claimed.

The [project-local CI workflow](.github/workflows/ci.yml) builds and runs the bounded publication tests when this repository is pushed. Hosted CI has not been claimed as executed evidence. The fresh local publication check is separate from the October 2026 full-suite experiment; see [publication checks](evidence/publication-checks.txt) and [provenance](evidence/README.md).

The implementation and review were AI-assisted. [Independent automated review](docs/independent-employer-review.md) concerns the bounded software artifact, not employer endorsement or personal mastery. No project license has been added; the redistributed Gradle wrapper's existing license is in [third-party notices](THIRD_PARTY_NOTICES.md).

Primary implementation references: [Java generation](https://grpc.io/docs/languages/java/quickstart/), [Python generation](https://grpc.io/docs/languages/python/quickstart/), [AsyncIO](https://grpc.github.io/grpc/python/grpc_asyncio.html), [authentication](https://grpc.io/docs/guides/auth/), [flow control](https://grpc.io/docs/guides/flow-control/), [deadlines](https://grpc.io/docs/guides/deadlines/) and [gRPC 1.84.1](https://github.com/grpc/grpc/releases/tag/v1.84.1).
