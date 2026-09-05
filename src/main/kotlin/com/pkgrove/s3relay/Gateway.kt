package com.pkgrove.s3relay

import io.smallrye.faulttolerance.api.RateLimit
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
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
 * keep serving cache. Any exception the breaker sees is a failure EXCEPT a
 * genuine NoSuchKey (that is a valid answer, not an outage).
 */
@ApplicationScoped
class Upstream(
    private val storage: ObjectStorage,
    @ConfigProperty(name = "s3relay.upstream.timeout-ms") private val timeoutMs: Long,
) {
    @Timeout(value = 8000)
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 5000, successThreshold = 2,
        skipOn = [software.amazon.awssdk.services.s3.model.NoSuchKeyException::class])
    fun getInto(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata? =
        storage.get(bucket, key, dest, range)

    @Timeout(value = 8000)
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 5000, successThreshold = 2)
    fun put(bucket: String, key: String, source: Path, metadata: ObjectMetadata): String =
        storage.put(bucket, key, source, metadata)

    @Timeout(value = 8000)
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 5000, successThreshold = 2,
        skipOn = [software.amazon.awssdk.services.s3.model.NoSuchKeyException::class])
    fun head(bucket: String, key: String): ObjectMetadata? = storage.head(bucket, key)

    @Timeout(value = 8000)
    fun delete(bucket: String, key: String) = storage.delete(bucket, key)

    @Timeout(value = 8000)
    fun list(bucket: String, prefix: String?, delimiter: String?, token: String?, maxKeys: Int): Listing =
        storage.list(bucket, prefix, delimiter, token, maxKeys)
}

/** A read result: the file to stream plus its metadata and whether it was a hit. */
class GetResult(val path: Path, val metadata: ObjectMetadata, val fromCache: Boolean)

/**
 * The gateway logic — the caching + write-through semantics the ticket spells
 * out, independent of the HTTP/S3 wire layer (S3Resource) so it can be unit
 * tested directly.
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

    @PostConstruct fun init() { cache = CacheStore(Path.of(cacheDir), maxBytes, ttlSeconds, high, low) }

    /**
     * Write-through PUT. Stream to a temp file, checksum-validate the size,
     * atomic-move into the cache as PENDING_REMOTE, then push upstream. Return
     * durable success ONLY after upstream stores it (state → SYNCED). If
     * upstream fails, the object is left PENDING_REMOTE (readable locally, NOT
     * evictable, retried by the reconciler) and the caller is told the durable
     * write did not complete — never a false success.
     */
    fun put(bucket: String, key: String, body: java.io.InputStream, declaredLength: Long?, contentType: String?): PutOutcome {
        synchronized(cache.lock(bucket, key)) {
            val temp = cache.newTemp()
            val written = Files.newOutputStream(temp).use { body.copyTo(it) }
            if (declaredLength != null && declaredLength >= 0 && written != declaredLength) {
                Files.deleteIfExists(temp)
                return PutOutcome.BadRequest("declared Content-Length $declaredLength != received $written")
            }
            cache.commit(bucket, key, temp, SyncState.PENDING_REMOTE, null, contentType, Instant.now())
            return try {
                cache.setState(bucket, key, SyncState.SYNCING)
                val etag = upstream.put(bucket, key, cache.get(bucket, key)!!.first, ObjectMetadata(written, contentType, null, null))
                cache.setState(bucket, key, SyncState.SYNCED, etag)
                maybeEvict()
                PutOutcome.Stored(etag, written)
            } catch (e: Exception) {
                cache.setState(bucket, key, SyncState.PENDING_REMOTE)
                log.warnf("upstream PUT failed for %s/%s; kept PENDING_REMOTE for retry: %s", bucket, key, e.message)
                PutOutcome.NotDurable(e.message ?: "upstream unavailable")
            }
        }
    }

    /** GET: cache first; miss → fetch upstream into the cache (SYNCED) then serve.
     *  During an upstream outage a cached object is still served. */
    fun get(bucket: String, key: String, range: LongRange?): GetOutcome {
        cache.get(bucket, key)?.let { (path, e) ->
            return GetOutcome.Ok(GetResult(sliceIfNeeded(path, range), ObjectMetadata(sizeFor(path, range, e.size), e.contentType, e.etag, e.lastModified), true), range != null)
        }
        val temp = cache.newTemp()
        return try {
            // Fetch the FULL object to cache (so future reads hit) and slice locally.
            val meta = upstream.getInto(bucket, key, temp, null)
                ?: run { Files.deleteIfExists(temp); return GetOutcome.NotFound }
            synchronized(cache.lock(bucket, key)) {
                cache.commit(bucket, key, temp, SyncState.SYNCED, meta.etag, meta.contentType, meta.lastModified)
            }
            maybeEvict()
            val path = cache.get(bucket, key)!!.first
            GetOutcome.Ok(GetResult(sliceIfNeeded(path, range), ObjectMetadata(sizeFor(path, range, meta.contentLength), meta.contentType, meta.etag, meta.lastModified), false), range != null)
        } catch (e: software.amazon.awssdk.services.s3.model.NoSuchKeyException) {
            Files.deleteIfExists(temp); GetOutcome.NotFound
        } catch (e: Exception) {
            Files.deleteIfExists(temp)
            log.warnf("upstream GET failed for %s/%s and no cache copy: %s", bucket, key, e.message)
            GetOutcome.Unavailable(e.message ?: "upstream unavailable")
        }
    }

    fun head(bucket: String, key: String): ObjectMetadata? {
        cache.peek(bucket, key)?.let { e -> return ObjectMetadata(e.size, e.contentType, e.etag, e.lastModified) }
        return try { upstream.head(bucket, key) } catch (e: Exception) { null }
    }

    /** DELETE removes upstream first (authoritative), then the cache copy. */
    fun delete(bucket: String, key: String): Boolean =
        try { upstream.delete(bucket, key); cache.remove(bucket, key); true }
        catch (e: Exception) { log.warnf("upstream DELETE failed for %s/%s: %s", bucket, key, e.message); false }

    fun list(bucket: String, prefix: String?, delimiter: String?, token: String?, maxKeys: Int): Listing =
        upstream.list(bucket, prefix, delimiter, token, maxKeys)

    /** Retry loop for objects the upstream rejected earlier (reconciler calls this). */
    fun reconcilePending(): Int {
        var synced = 0
        for (e in cache.entries().filter { it.state == SyncState.PENDING_REMOTE }) {
            val hit = cache.get(e.bucket, e.key) ?: continue
            synchronized(cache.lock(e.bucket, e.key)) {
                runCatching {
                    cache.setState(e.bucket, e.key, SyncState.SYNCING)
                    val etag = upstream.put(e.bucket, e.key, hit.first, ObjectMetadata(e.size, e.contentType, null, null))
                    cache.setState(e.bucket, e.key, SyncState.SYNCED, etag); synced++
                }.onFailure { cache.setState(e.bucket, e.key, SyncState.PENDING_REMOTE) }
            }
        }
        return synced
    }

    fun maybeEvict() { if (cache.overHighWatermark()) cache.evictToLowWatermark() }

    private fun sizeFor(path: Path, range: LongRange?, full: Long): Long =
        if (range == null) full else (minOf(range.last, Files.size(path) - 1) - range.first + 1).coerceAtLeast(0)

    // For a cache hit with a range, produce a temp slice the resource streams.
    private fun sliceIfNeeded(path: Path, range: LongRange?): Path {
        if (range == null) return path
        val total = Files.size(path)
        val first = range.first.coerceIn(0, if (total == 0L) 0 else total - 1)
        val last = range.last.coerceIn(first, if (total == 0L) 0 else total - 1)
        val slice = cache.newTemp()
        Files.newByteChannel(path).use { ch ->
            ch.position(first)
            val buf = java.nio.ByteBuffer.allocate((last - first + 1).toInt().coerceAtLeast(0))
            while (buf.hasRemaining() && ch.read(buf) > 0) { /* fill */ }
            buf.flip(); Files.newByteChannel(slice, java.nio.file.StandardOpenOption.WRITE).use { it.write(buf) }
        }
        return slice
    }
}

sealed interface PutOutcome {
    data class Stored(val etag: String, val length: Long) : PutOutcome
    data class NotDurable(val reason: String) : PutOutcome
    data class BadRequest(val reason: String) : PutOutcome
}
sealed interface GetOutcome {
    data class Ok(val result: GetResult, val partial: Boolean) : GetOutcome
    object NotFound : GetOutcome
    data class Unavailable(val reason: String) : GetOutcome
}
