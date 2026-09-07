package com.pkgrove.s3relay

import io.micrometer.core.instrument.Metrics
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.eclipse.microprofile.faulttolerance.CircuitBreaker
import org.eclipse.microprofile.faulttolerance.Timeout
import org.jboss.logging.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

/**
 * Upstream access wrapped in fault tolerance (HEL-421): a bounded timeout, a
 * circuit breaker so an unhealthy MinIO doesn't make every read block for the
 * full timeout, and the failure classified as UpstreamUnavailable so callers
 * keep serving cache. The breaker counts OUTAGES only: a NoSuchKey and any
 * other 4xx (UpstreamError) are answers, not failures (HEL-452).
 *
 * Timeouts are NOT hard-coded (HEL-452): the values below are overridden per
 * method from application.properties through the MicroProfile FT config keys
 * `com.pkgrove.s3relay.Upstream/<method>/Timeout/value` — reads short, PUT long.
 */
@ApplicationScoped
class Upstream(private val storage: ObjectStorage) {
    @Timeout(value = 8000)
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 5000, successThreshold = 2,
        skipOn = [software.amazon.awssdk.services.s3.model.NoSuchKeyException::class, UpstreamError::class])
    fun getInto(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata? =
        storage.get(bucket, key, dest, range)

    @Timeout(value = 600000)
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 5000, successThreshold = 2,
        skipOn = [UpstreamError::class])
    fun put(bucket: String, key: String, source: Path, metadata: ObjectMetadata): String =
        storage.put(bucket, key, source, metadata)

    @Timeout(value = 8000)
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 5000, successThreshold = 2,
        skipOn = [software.amazon.awssdk.services.s3.model.NoSuchKeyException::class, UpstreamError::class])
    fun head(bucket: String, key: String): ObjectMetadata? = storage.head(bucket, key)

    @Timeout(value = 8000)
    fun delete(bucket: String, key: String) = storage.delete(bucket, key)

    @Timeout(value = 8000)
    fun list(bucket: String, prefix: String?, delimiter: String?, token: String?, maxKeys: Int): Listing =
        storage.list(bucket, prefix, delimiter, token, maxKeys)
}

/** A read result: the cached file to stream, its metadata, the RESOLVED byte range
 *  (absolute, inclusive) when the client asked for one, the object's total size,
 *  and whether it was a hit. The resource streams `range` straight from `path` —
 *  no slice file, no heap buffer (HEL-452). */
class GetResult(val path: Path, val metadata: ObjectMetadata, val fromCache: Boolean,
                val range: LongRange?, val totalLength: Long)

/**
 * The gateway logic — the caching + write-through semantics the ticket spells
 * out, independent of the HTTP/S3 wire layer (S3Resource) so it can be unit
 * tested directly. Every outcome a client can receive is a value here; the only
 * exceptions that escape are local infrastructure faults (disk), which the
 * resource's mapper turns into an S3 InternalError — never a bare 500.
 */
@ApplicationScoped
class Gateway(
    private val upstream: Upstream,
    @ConfigProperty(name = "s3relay.cache.dir") private val cacheDir: String,
    @ConfigProperty(name = "s3relay.cache.max-bytes") private val maxBytes: Long,
    @ConfigProperty(name = "s3relay.cache.ttl-seconds") private val ttlSeconds: Long,
    @ConfigProperty(name = "s3relay.cache.high-watermark") private val high: Double,
    @ConfigProperty(name = "s3relay.cache.low-watermark") private val low: Double,
) {
    private val log = Logger.getLogger(Gateway::class.java)
    lateinit var cache: CacheStore

    @PostConstruct fun init() {
        cache = CacheStore(Path.of(cacheDir), maxBytes, ttlSeconds, high, low)
        runCatching {
            Metrics.gauge("s3relay_cache_used_bytes", cache) { it.usedBytes().toDouble() }
            Metrics.gauge("s3relay_cache_pinned_bytes", cache) { it.pinnedBytes().toDouble() }
            Metrics.gauge("s3relay_cache_pending_objects", cache) { it.entries().count { e -> e.state != SyncState.SYNCED }.toDouble() }
        }
    }

    private fun count(name: String, vararg tags: String) = runCatching { Metrics.counter(name, *tags).increment() }

    /**
     * Write-through PUT. Stream to a temp file, validate the size, atomic-move into
     * the cache as PENDING_REMOTE, then push upstream. Durable success ONLY after
     * upstream stores it (SYNCED). Outage → PENDING_REMOTE (readable locally, NOT
     * evictable, retried by the reconciler) and the caller is told the durable
     * write did not complete — never a false success. An upstream 4xx is relayed
     * (Rejected) and the local copy is dropped: upstream refused it, pinning it
     * would just fill the cache with objects that can never sync.
     */
    fun put(bucket: String, key: String, body: java.io.InputStream, declaredLength: Long?, contentType: String?): PutOutcome {
        if (declaredLength != null && declaredLength > 0 && !cache.canAccept(declaredLength)) {
            maybeEvict()
            if (!cache.canAccept(declaredLength)) {
                count("s3relay_put_total", "outcome", "insufficient_storage")
                return PutOutcome.InsufficientStorage("cache has ${cache.usedBytes()} of $maxBytes bytes in use, " +
                    "${cache.pinnedBytes()} of them pinned (not yet confirmed upstream); cannot accept $declaredLength more")
            }
        }
        synchronized(cache.lock(bucket, key)) {
            val temp = cache.newTemp()
            val written = try {
                Files.newOutputStream(temp).use { body.copyTo(it) }
            } catch (e: Exception) {
                Files.deleteIfExists(temp)   // client disconnect / disk error: never leave a part file behind
                count("s3relay_put_total", "outcome", "body_failed")
                throw e
            }
            if (declaredLength != null && declaredLength >= 0 && written != declaredLength) {
                Files.deleteIfExists(temp)
                count("s3relay_put_total", "outcome", "bad_length")
                return PutOutcome.BadRequest("declared Content-Length $declaredLength != received $written")
            }
            cache.commit(bucket, key, temp, SyncState.PENDING_REMOTE, null, contentType, Instant.now())
            return try {
                cache.setState(bucket, key, SyncState.SYNCING)
                val etag = upstream.put(bucket, key, cache.get(bucket, key)!!.first, ObjectMetadata(written, contentType, null, null))
                cache.setState(bucket, key, SyncState.SYNCED, etag)
                maybeEvict()
                count("s3relay_put_total", "outcome", "stored")
                PutOutcome.Stored(etag, written)
            } catch (e: UpstreamError) {
                cache.remove(bucket, key)
                log.warnf("upstream rejected PUT %s/%s: %d %s — relayed, local copy dropped", bucket, key, e.status, e.code)
                count("s3relay_put_total", "outcome", "rejected")
                PutOutcome.Rejected(e.status, e.code, e.message ?: e.code)
            } catch (e: Exception) {
                cache.setState(bucket, key, SyncState.PENDING_REMOTE)
                log.warnf("upstream PUT failed for %s/%s; kept PENDING_REMOTE for retry: %s", bucket, key, e.message)
                count("s3relay_put_total", "outcome", "not_durable")
                PutOutcome.NotDurable(e.message ?: "upstream unavailable")
            }
        }
    }

    /** GET: cache first; miss → fetch upstream into the cache (SYNCED) then serve.
     *  During an upstream outage a cached object is still served. */
    fun get(bucket: String, key: String, range: LongRange?): GetOutcome {
        cache.get(bucket, key)?.let { (path, e) ->
            count("s3relay_get_total", "outcome", "hit")
            return result(path, ObjectMetadata(e.size, e.contentType, e.etag, e.lastModified), true, range)
        }
        val temp = cache.newTemp()
        return try {
            // Fetch the FULL object to cache (so future reads hit) and serve the range locally.
            val meta = upstream.getInto(bucket, key, temp, null)
                ?: run { Files.deleteIfExists(temp); count("s3relay_get_total", "outcome", "not_found"); return GetOutcome.NotFound }
            synchronized(cache.lock(bucket, key)) {
                cache.commit(bucket, key, temp, SyncState.SYNCED, meta.etag, meta.contentType, meta.lastModified)
            }
            maybeEvict()
            val path = cache.get(bucket, key)!!.first
            count("s3relay_get_total", "outcome", "miss")
            result(path, meta, false, range)
        } catch (e: software.amazon.awssdk.services.s3.model.NoSuchKeyException) {
            Files.deleteIfExists(temp); count("s3relay_get_total", "outcome", "not_found"); GetOutcome.NotFound
        } catch (e: UpstreamError) {
            Files.deleteIfExists(temp); count("s3relay_get_total", "outcome", "upstream_error")
            GetOutcome.UpstreamError(e.status, e.code, e.message ?: e.code)
        } catch (e: Exception) {
            Files.deleteIfExists(temp)
            log.warnf("upstream GET failed for %s/%s and no cache copy: %s", bucket, key, e.message)
            count("s3relay_get_total", "outcome", "unavailable")
            GetOutcome.Unavailable(e.message ?: "upstream unavailable")
        }
    }

    /** Resolve a requested range against the cached file: absolute inclusive bounds, or 416 when
     *  the first byte is past the end. Nothing is copied — the resource streams from `path`. */
    private fun result(path: Path, meta: ObjectMetadata, hit: Boolean, range: LongRange?): GetOutcome {
        val total = Files.size(path)
        if (range == null) return GetOutcome.Ok(GetResult(path, meta.copy(contentLength = total), hit, null, total), partial = false)
        if (total == 0L || range.first >= total || range.first < 0) return GetOutcome.RangeNotSatisfiable(total)
        val last = minOf(range.last, total - 1)
        val resolved = range.first..last
        return GetOutcome.Ok(GetResult(path, meta.copy(contentLength = last - range.first + 1), hit, resolved, total), partial = true)
    }

    fun head(bucket: String, key: String): HeadOutcome {
        cache.peek(bucket, key)?.let { e -> return HeadOutcome.Ok(ObjectMetadata(e.size, e.contentType, e.etag, e.lastModified), fromCache = true) }
        return try {
            upstream.head(bucket, key)?.let { HeadOutcome.Ok(it, fromCache = false) } ?: HeadOutcome.NotFound
        } catch (e: UpstreamError) { HeadOutcome.UpstreamError(e.status, e.code, e.message ?: e.code) }
        catch (e: Exception) {
            log.warnf("upstream HEAD failed for %s/%s and no cache copy: %s", bucket, key, e.message)
            HeadOutcome.Unavailable(e.message ?: "upstream unavailable")
        }
    }

    /** DELETE removes upstream first (authoritative), then the cache copy. */
    fun delete(bucket: String, key: String): DeleteOutcome =
        try { upstream.delete(bucket, key); cache.remove(bucket, key); DeleteOutcome.Ok }
        catch (e: UpstreamError) { DeleteOutcome.UpstreamError(e.status, e.code, e.message ?: e.code) }
        catch (e: Exception) { log.warnf("upstream DELETE failed for %s/%s: %s", bucket, key, e.message); DeleteOutcome.Unavailable(e.message ?: "upstream unavailable") }

    fun list(bucket: String, prefix: String?, delimiter: String?, token: String?, maxKeys: Int): ListOutcome =
        try { ListOutcome.Ok(upstream.list(bucket, prefix, delimiter, token, maxKeys)) }
        catch (e: UpstreamError) { ListOutcome.UpstreamError(e.status, e.code, e.message ?: e.code) }
        catch (e: Exception) { log.warnf("upstream LIST failed for %s: %s", bucket, e.message); ListOutcome.Unavailable(e.message ?: "upstream unavailable") }

    /**
     * Retry loop for objects the upstream has not confirmed (reconciler calls this).
     * Upstream is authoritative once it HAS a copy (HEL-452): before re-uploading, ask.
     *  - upstream has the same object (size match) → adopt as SYNCED, nothing sent
     *  - upstream has a DIFFERENT object → someone wrote there since; our stale
     *    pending copy is dropped, never allowed to overwrite it
     *  - upstream has nothing → upload
     *  - upstream unreachable → leave pending, try next round
     * Every failure is logged with bucket/key; nothing is swallowed silently.
     */
    fun reconcilePending(): ReconcileReport {
        var synced = 0; var adopted = 0; var dropped = 0; var failed = 0
        for (e in cache.entries().filter { it.state != SyncState.SYNCED }) {
            val hit = cache.get(e.bucket, e.key) ?: continue
            synchronized(cache.lock(e.bucket, e.key)) {
                try {
                    val remote = upstream.head(e.bucket, e.key)
                    when {
                        remote != null && remote.contentLength == e.size -> {
                            cache.setState(e.bucket, e.key, SyncState.SYNCED, remote.etag); adopted++
                        }
                        remote != null -> {
                            cache.remove(e.bucket, e.key); dropped++
                            log.warnf("reconcile %s/%s: upstream holds a different object (%d bytes vs local %d); local pending copy dropped, upstream is authoritative",
                                e.bucket, e.key, remote.contentLength, e.size)
                        }
                        else -> {
                            cache.setState(e.bucket, e.key, SyncState.SYNCING)
                            val etag = upstream.put(e.bucket, e.key, hit.first, ObjectMetadata(e.size, e.contentType, null, null))
                            cache.setState(e.bucket, e.key, SyncState.SYNCED, etag); synced++
                        }
                    }
                } catch (ex: UpstreamError) {
                    cache.remove(e.bucket, e.key); dropped++
                    log.warnf("reconcile %s/%s: upstream rejected it (%d %s); local pending copy dropped", e.bucket, e.key, ex.status, ex.code)
                } catch (ex: Exception) {
                    cache.setState(e.bucket, e.key, SyncState.PENDING_REMOTE); failed++
                    log.warnf("reconcile %s/%s: upstream still unavailable (%s); kept pending", e.bucket, e.key, ex.message)
                }
            }
        }
        count("s3relay_reconcile_total", "outcome", "synced").also { if (synced > 0) runCatching { Metrics.counter("s3relay_reconcile_objects_total", "outcome", "synced").increment(synced.toDouble()) } }
        return ReconcileReport(synced, adopted, dropped, failed)
    }

    fun maybeEvict() { if (cache.overHighWatermark()) cache.evictToLowWatermark() }
}

data class ReconcileReport(val synced: Int, val adopted: Int, val dropped: Int, val failed: Int) {
    val total get() = synced + adopted + dropped + failed
}

sealed interface PutOutcome {
    data class Stored(val etag: String, val length: Long) : PutOutcome
    data class NotDurable(val reason: String) : PutOutcome
    data class BadRequest(val reason: String) : PutOutcome
    /** Upstream answered 4xx; relayed as-is. */
    data class Rejected(val status: Int, val code: String, val message: String) : PutOutcome
    /** Cache full of objects that cannot be evicted (not confirmed upstream) — 507. */
    data class InsufficientStorage(val reason: String) : PutOutcome
}
sealed interface GetOutcome {
    data class Ok(val result: GetResult, val partial: Boolean) : GetOutcome
    object NotFound : GetOutcome
    data class RangeNotSatisfiable(val totalLength: Long) : GetOutcome
    data class UpstreamError(val status: Int, val code: String, val message: String) : GetOutcome
    data class Unavailable(val reason: String) : GetOutcome
}
sealed interface HeadOutcome {
    data class Ok(val metadata: ObjectMetadata, val fromCache: Boolean) : HeadOutcome
    object NotFound : HeadOutcome
    data class UpstreamError(val status: Int, val code: String, val message: String) : HeadOutcome
    data class Unavailable(val reason: String) : HeadOutcome
}
sealed interface DeleteOutcome {
    object Ok : DeleteOutcome
    data class UpstreamError(val status: Int, val code: String, val message: String) : DeleteOutcome
    data class Unavailable(val reason: String) : DeleteOutcome
}
sealed interface ListOutcome {
    data class Ok(val listing: Listing) : ListOutcome
    data class UpstreamError(val status: Int, val code: String, val message: String) : ListOutcome
    data class Unavailable(val reason: String) : ListOutcome
}
