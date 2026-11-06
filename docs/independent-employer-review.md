# Independent automated engineering review

Reviewed 2026-10-23 by a separate automated reviewer. This is an artifact assessment, not a hiring decision, a human employer endorsement or proof of the applicant's personal ownership.

**Verdict after independent recheck: accept as a strong bounded portfolio artifact.** Relay Fleet supplies Java/gRPC/mTLS and asynchronous-agent evidence within the stated simulated scope. Its recovery, tenant boundaries and corrected operator listing passed independent execution. The original large-list defect is fixed; no material acceptance blocker remains within the documented local, simulated-fleet scope.

## Material finding, fixed and independently rechecked

**Original P2 finding — valid fleet state could not be completely inspected through the shipped operator interface.** The original [Store.list](../src/main/java/relay/Store.java) used `LIMIT 1000` without a continuation field. [agent.operator](../agent/operator.py) uses the default 4 MiB gRPC receive limit. I independently registered 1,002 devices through real authenticated Java RPCs, including 1,000 with permitted 4,096-byte configuration JSON and 64-character IDs. The default Python client rejected the list with `RESOURCE_EXHAUSTED`: the server produced 4,246,000 bytes versus the 4,194,304-byte client limit. Increasing only the test client's limit to 8 MiB allowed delivery, but returned 1,000 of 1,002 registered devices with no indication of the missing rows and no way to request the next page. The omitted device remains registered; this is an operator visibility/contract failure, not data loss or tenant escape.

The exact probe and results are [independent-probe.py](independent-probe.py) and [independent-probe-initial.json](../evidence/independent-probe-initial.json). The fixture uses real mTLS, generated clients and actual SQLite. Its fingerprint-only registrations exercise the intentionally operator-controlled registration API; it does not pretend those 1,000 simulated credentials represent connected hardware.

**Original acceptance condition, now satisfied:** implement a bounded, tenant-scoped listing page with stable ordering and an explicit continuation; make the operator CLI retrieve all pages. Test allowed large configurations, more than 1,000 registrations, exact one-time coverage of all IDs and tenant isolation. Increasing the receive limit alone leaves silent truncation unresolved. Changing the protobuf request/response needs a documented compatibility strategy and regenerated clients.

## Pagination recheck

I inspected the new `PageCursor`, listing query, protobuf definitions, CLI traversal and compatibility notes, then independently reran the full suite: **10 Java tests, 11 Python tests, 9 experiment checks and 480 recovered samples**, exit 0. The Python suite took 32.81 seconds. All 42 source-manifest entries matched before execution.

The permanent regression successfully traversed 1,025 devices with maximum-length tenant/device names and allowed 4,096-byte configurations. Its largest response was 430,722 bytes, below the explicit 512 KiB contract and unchanged default 4 MiB receive limit. It checked malformed and tampered cursors, tenant binding, changed page sizes, restart persistence, legacy empty requests and the real CLI's complete output.

I also adapted my original independent failure probe to the new API, separately from that regression. With 73-device pages, it returned all **1,002 original-fixture IDs exactly once**, in order, across 14 pages; the largest response was **310,102 bytes**. The ordinary receive limit was unchanged. Legacy empty requests returned `INVALID_ARGUMENT`. This closes both the oversized-message failure and silent truncation. The recheck code and result are [independent-pagination-recheck.py](independent-pagination-recheck.py) and [independent-pagination-recheck.json](../evidence/independent-pagination-recheck.json).

The implementation uses bounded keyset traversal and an HMAC-authenticated, tenant-bound cursor; the signing key persists in the database. Authorization still comes from the authenticated operator. It explicitly documents that concurrent additions can appear ahead of the cursor and that additions behind it require a new traversal: this is not a snapshot-listing claim. Changing `Empty` to a required page request is documented as a coordinated pre-release breaking correction; old clients fail explicitly rather than treating the first page as complete. The CLI assembles all pages in memory, also disclosed.

The latest independent short benchmark was 16,114 samples/s and 3.11 ms batch p95. It remains a variable local observation; prior measurements below are retained as history, not conflicting capacity guarantees.

## Independent verification actually performed

I preserved the author's pre-review small evidence files in `~/.cache/job-apps-runtime/relay-fleet/review-before`, checked all 40 entries in the source manifest, and ran the complete documented `bash scripts/verify.sh`. It finished with exit 0. All 10 Java tests and 10 Python tests passed; the Python suite took 23.40 seconds. The independently repeated process/socket experiment passed all 9 checks and recovered 480 samples. `pip check` reported no broken requirements. All 40 manifest hashes still matched after regeneration/testing. The retained [initial reviewer record](../evidence/independent-review-initial.json) separates this execution from the author's previous result.

The full drill exercises real child processes, a transparent TCP cut, agent/server SIGKILL, full durable queues, restart, configuration convergence and revocation on an existing stream. I inspected its assertions and the database checks: each of three devices ended with exactly sequences 1–160, and all three spools drained. A separate encrypted-response-loss experiment verifies a committed record remains queued without an ACK and replays as a duplicate with one database effect. These claims concern process crashes and interrupted local transport, not physical power loss or replicated failover.

Additional probes I wrote independently passed:

- A batch containing a new telemetry sample and an invalid applied-configuration digest failed with `FAILED_PRECONDITION`, inserted zero telemetry rows, and accepted the same sample exactly once after supplying the correct digest. Configuration reporting and sample publication share one transaction.
- A valid protobuf `uint64` sequence of 2^63 was rejected with `INVALID_ARGUMENT`, leaving prior telemetry intact. The Java/SQLite signed-integer boundary fails safely.
- The large-list probe above exposed the material finding despite the ordinary suite passing.

The independent warmed 3,200-sample microbenchmark measured 9,791 samples/s and 7.02 ms batch p95, compared with the author's preceding 21,890 samples/s and 1.93 ms. That variation is consistent with the README's disclosed host sensitivity. Neither run establishes an SLO. Do not pick the best run as a general capacity claim. The useful evidence is commit/ACK correctness and raw methodology, with throughput kept as an explicitly short loopback observation. I inspected the earlier JFR observations but did not independently repeat profiling or a clean-machine dependency installation. Hosted CI was not run.

## Code assessment

The leaf-certificate hash is taken from the actual TLS session and resolved to a registered principal. Request metadata and device IDs do not grant authority. Device frames recheck the registration and revoked-device state, including existing streams. Operator queries derive tenant from the certificate binding. The Java service bounds device streams, inbound message size, frames, queue/workers and stream lifetime; flow control requests one frame after an outbound ACK can be sent. These are concrete controls, with no complete internet-facing DoS claim.

The synchronized Store owns one SQLite connection. Telemetry, cumulative sequence and applied-state changes commit before the ACK is returned. Conflicting content at an existing sequence and gaps roll back. The Python spool allocates sequence and row atomically, refuses an ACK beyond sent/allocated data, pauses sampling at capacity and never evicts unacknowledged readings. Its dedicated executor keeps SQLite work outside the asyncio loop. Cancellation and network failures were exercised by the real suite.

The remaining documented boundaries matter: one control process/database, one agent owner per spool, no server retention, no fleet HA, no physical device, no automatic certificate renewal and no disk-fault injection. `fetch` also counts all retained telemetry on request paths, so long-running performance needs separate study. That is a future scaling concern, not a falsification of the small benchmark. The operator-pagination issue was the only material blocker found in this review and is now closed after independent recheck.
