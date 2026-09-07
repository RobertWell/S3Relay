# S3Relay

A lightweight **S3-compatible caching gateway** (Kotlin + Quarkus) that sits in
front of a home-built MinIO (or any S3-compatible store) to add read resilience
and local read performance. MinIO stays the authoritative, persistent store;
S3Relay's cache is **disposable** and never treated as durable.

```
S3 client  ──►  S3Relay  ──►  local ephemeral cache (/cache)  +  upstream MinIO/S3
```

## Consistency guarantees (the load-bearing rules)

- **A PUT reports durable success only after the object is stored upstream.**
  The object is streamed to a temp file, atomically moved into the cache as
  `PENDING_REMOTE`, then pushed upstream; only on upstream success does it
  become `SYNCED` and the client get `200`. If upstream fails the client gets
  `503` with "not durable" — never a false success — and the object is kept
  locally and retried by the reconciler.
- **Only `SYNCED` objects are ever evicted.** An object whose sole copy is in
  the ephemeral cache is pinned, even under disk pressure. Losing the cache
  (pod replacement) therefore never loses an acknowledged object — MinIO has it.
- **Cached objects stay readable during an upstream outage.** A cache hit is
  served without contacting MinIO; a cache miss during an outage fails in a
  bounded way (circuit breaker + timeout), it does not hang.

Object lifecycle: `CACHING → PENDING_REMOTE → SYNCING → SYNCED`.

## Relay contract — every answer is an S3 answer (HEL-452)

S3Relay is a bridge between MinIO and S3-compatible clients, so it never lets a
raw failure reach the client. Three classes, kept apart end to end:

| what happened upstream / locally | what the client gets |
|---|---|
| upstream answered **NoSuchKey** | `404 NoSuchKey` (an answer, served as such) |
| upstream answered any other **4xx** (`NoSuchBucket`, `AccessDenied`, `InvalidArgument`, …) | **the same status and S3 error code, relayed as-is** — never dressed up as an outage or a 500. A rejected PUT drops its local copy (upstream refused it; pinning it would only fill the cache). |
| upstream **5xx / timeout / transport failure / open breaker** | `503 ServiceUnavailable` + `Retry-After: 5` — the only class the cache is allowed to paper over: cached reads and `HEAD` keep answering from the cache, uncached ones fail bounded |
| cache full of objects upstream has not confirmed | `507 InsufficientStorage` (never an overflow into the emptyDir cap) |
| declared length ≠ received bytes | `400 BadDigest` |
| range starts past the object | `416 InvalidRange` + `Content-Range: bytes */<total>` |
| a local fault the outcome types did not model (disk full, client hung up mid-upload, routing) | `500 InternalError` from the `RelayExceptionMapper`, S3 XML body, **request id on the wire (`x-amz-request-id`) and in the log next to the stack trace** — never a bare 500 page |

Timeouts are configuration, not code: reads/metadata `S3RELAY_UPSTREAM_TIMEOUT_MS`
(default 8 s), PUT and the full-object download on a miss
`S3RELAY_UPSTREAM_PUT_TIMEOUT_MS` (default 10 min). Ranges stream straight from the
cached file with a bounded channel copy — no slice file, no heap buffer, no 2 GiB
ceiling. The reconciler asks upstream (`HEAD`) before re-uploading a pending object:
identical copy → adopted, different copy → the stale local one is dropped (upstream is
authoritative once it holds an object), absent → uploaded; every failure is a log line
with bucket/key. Metrics: `s3relay_put_total{outcome}`, `s3relay_get_total{outcome}`,
`s3relay_cache_used_bytes`, `s3relay_cache_pinned_bytes`, `s3relay_cache_pending_objects`.

## S3 compatibility (initial scope)

`PutObject`, `GetObject` (+ `Range`), `HeadObject`, `DeleteObject`,
`ListObjectsV2`. Path-style addressing (`/{bucket}/{key}`).

**Known limitations (first version):**
- SigV4 request signatures are accepted but **not verified** — S3Relay runs on
  the trusted in-cluster network in front of MinIO. Do not expose it publicly.
- No multipart upload yet (assess when object sizes require it).
- No bucket operations (create/delete/list-buckets); buckets live in MinIO.
- `ListObjectsV2` is proxied to upstream (not served from cache).

## Cache management

Bounded by size and TTL, with high/low disk watermarks and LRU eviction of
`SYNCED` objects. A scheduler runs eviction every 30s and reconciles
`PENDING_REMOTE` objects to upstream every 20s. Emergency eviction triggers when
usage crosses the high watermark.

## Configuration

| env | default | meaning |
|---|---|---|
| `S3RELAY_UPSTREAM_ENDPOINT` | `http://localhost:9000` | upstream S3 endpoint (MinIO/AWS/R2/Garage) |
| `S3RELAY_UPSTREAM_REGION` | `us-east-1` | |
| `S3RELAY_UPSTREAM_ACCESS_KEY` / `_SECRET_KEY` | | upstream credentials |
| `S3RELAY_UPSTREAM_PATH_STYLE` | `true` | path-style (MinIO); `false` for AWS virtual-host |
| `S3RELAY_UPSTREAM_TIMEOUT_MS` | `8000` | per-call upstream timeout (also the circuit-breaker budget) |
| `S3RELAY_CACHE_DIR` | `/cache` | ephemeral cache root (k8s `emptyDir`) |
| `S3RELAY_CACHE_MAX_BYTES` | `10737418240` | 10 GiB |
| `S3RELAY_CACHE_TTL_SECONDS` | `86400` | |
| `S3RELAY_CACHE_HIGH_WATERMARK` / `_LOW_WATERMARK` | `0.90` / `0.70` | evict above high, down to low |

The upstream is configured purely by endpoint + credentials, so it is **not
MinIO-specific** — AWS S3, Cloudflare R2 and Garage work unchanged.

## Kubernetes deployment

Runs as a Deployment with a **disk-backed `emptyDir`** mounted at `/cache`
(`ephemeral-storage` request/limit + `emptyDir.sizeLimit`), never a
PersistentVolume — the cache must be safe to lose. Manifests live in
`ryanlin2-control/manifests/s3relay`. `/q/health/live` never depends on the
upstream, so a MinIO blip does not restart the pod.

## CI

GitLab (`root/s3relay`, mirror of the GitHub repo): `s3relay-tests` runs the suite
against a MinIO **service container** and fails if the MinIO-backed wire suite did
not execute; `build-image-s3relay` builds, pushes `:SHA`, reads the digest back
and emits a `pin=` line. Promotion is a reviewed digest pin in
`ryanlin2-control/manifests/s3relay` — never `:latest`, never a local build.

## Build & test

```
make build                 # runner jar
S3RELAY_TEST_ENDPOINT=http://localhost:9000 make test   # needs a MinIO
```

Tests: `CacheStoreTest` (eviction invariant), `GatewayOutageTest` (durability +
outage + reconcile against a fake upstream), `S3RelayTest` (the S3 wire surface
against a real MinIO, asserting each write reaches the upstream, not just cache).
