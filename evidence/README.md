# Historical evidence and public-package verification

The original October 2026 failure/security, pagination and benchmark records remain historical. Red/failed logs are retained beside subsequent corrections. Some text logs and JSON string paths replace the private home prefix with `$HOME`; recorded timings, failure status and measurements are unchanged. [Publication provenance](../docs/publication-provenance.json) identifies these exact transformed hashes. The raw JFR binary is excluded; its textual summary and observations remain.

`source-manifest.json` is the unchanged historical manifest of the accepted source, including documentation that this standalone package adapts. All 42 entries matched the original workspace before packaging. It is not a manifest of this public checkout. The Java/Python implementation, protobuf schema, generated clients and existing tests are byte-preserved.

`publication-checks.txt` records the separate fresh local build/tests and simple CLI flow. Publication did not rerun the fixed 480-sample incident drill, 3,200-sample timing experiment or historical pagination run. The unchanged `scripts/verify.sh` remains an opt-in historical full-verification command that writes evidence. Test PKI is generated afresh by `scripts/pki.py`; no certificates, keys, runtime databases, environments or build distributions are included.

Relay Fleet is standalone; it does not depend on Strata or Fleet Watch. Those projects can consume this service's public API. The project and its separate review used AI assistance; artifact acceptance does not establish human employer endorsement, production deployment or personal mastery.
