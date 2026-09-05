package com.pkgrove.s3relay

import org.jboss.logging.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The object's durability state (HEL-421). The whole point: the cache is
 * EPHEMERAL, so an object is only "durably stored" once its copy exists
 * upstream. Only SYNCED objects may be evicted — evicting anything else would
 * lose the sole copy.
 */
enum class SyncState { CACHING, PENDING_REMOTE, SYNCING, SYNCED }

data class CacheEntry(
    val bucket: String, val key: String,
    @Volatile var state: SyncState,
    @Volatile var size: Long,
    @Volatile var etag: String?,
    @Volatile var contentType: String?,
    @Volatile var lastModified: Instant?,
    @Volatile var cachedAt: Instant,
    @Volatile var lastAccess: Long,   // nanoTime for LRU
) {
    fun evictable() = state == SyncState.SYNCED
}

/**
 * The disk cache under `/cache`. One file per object at a content-addressed-ish
 * safe path; an in-memory index (rebuilt by scanning disk at startup, because
 * the cache is disposable and a lost index just means a cold cache). Writes are
 * temp-file-then-atomic-move so a crash never leaves a half object served as
 * whole. Thread-safe; per-key locks serialize concurrent writes to one key.
 */
class CacheStore(
    private val root: Path,
    val maxBytes: Long,
    private val ttlSeconds: Long,
    private val highWatermark: Double,
    private val lowWatermark: Double,
) {
    private val log = Logger.getLogger(CacheStore::class.java)
    private val index = ConcurrentHashMap<String, CacheEntry>()
    private val bytes = AtomicLong(0)
    private val keyLocks = ConcurrentHashMap<String, Any>()

    init {
        Files.createDirectories(objectsDir())
        Files.createDirectories(tmpDir())
        // Best-effort index rebuild from a previous life; safe to skip entirely.
        // We do NOT trust on-disk state as SYNCED (we can't prove upstream has
        // it), so a rebuilt entry is PENDING_REMOTE until proven — but since a
        // fresh emptyDir is empty, this only matters for a same-pod restart.
        runCatching {
            Files.list(objectsDir()).use { s -> s.forEach { p ->
                if (Files.isRegularFile(p)) {
                    val id = p.fileName.toString()
                    decode(id)?.let { (b, k) ->
                        val size = Files.size(p)
                        index[id] = CacheEntry(b, k, SyncState.PENDING_REMOTE, size, null, null,
                            Instant.ofEpochMilli(Files.getLastModifiedTime(p).toMillis()),
                            Instant.ofEpochMilli(Files.getLastModifiedTime(p).toMillis()), System.nanoTime())
                        bytes.addAndGet(size)
                    }
                }
            } }
        }
    }

    fun usedBytes() = bytes.get()
    fun entries(): Collection<CacheEntry> = index.values
    fun lock(bucket: String, key: String): Any = keyLocks.computeIfAbsent(id(bucket, key)) { Any() }

    fun get(bucket: String, key: String): Pair<Path, CacheEntry>? {
        val id = id(bucket, key)
        val e = index[id] ?: return null
        val p = objectsDir().resolve(id)
        if (!Files.exists(p)) { remove(bucket, key); return null }
        if (ttlSeconds > 0 && e.state == SyncState.SYNCED &&
            e.cachedAt.plusSeconds(ttlSeconds).isBefore(Instant.now())) { remove(bucket, key); return null }
        e.lastAccess = System.nanoTime()
        return p to e
    }

    fun peek(bucket: String, key: String): CacheEntry? = index[id(bucket, key)]

    /** A fresh temp file the caller streams the body into before commit. */
    fun newTemp(): Path = Files.createTempFile(tmpDir(), "up-", ".part")

    /** Commit a written temp file as (bucket,key) in the given state, atomically. */
    fun commit(bucket: String, key: String, temp: Path, state: SyncState,
               etag: String?, contentType: String?, lastModified: Instant?): CacheEntry {
        val id = id(bucket, key)
        val dest = objectsDir().resolve(id)
        val size = Files.size(temp)
        Files.move(temp, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        val prev = index[id]
        val e = CacheEntry(bucket, key, state, size, etag, contentType, lastModified ?: Instant.now(),
            Instant.now(), System.nanoTime())
        index[id] = e
        bytes.addAndGet(size - (prev?.size ?: 0))
        return e
    }

    fun setState(bucket: String, key: String, state: SyncState, etag: String? = null) {
        index[id(bucket, key)]?.let { it.state = state; if (etag != null) it.etag = etag }
    }

    fun remove(bucket: String, key: String) {
        val id = id(bucket, key)
        index.remove(id)?.let { bytes.addAndGet(-it.size) }
        runCatching { Files.deleteIfExists(objectsDir().resolve(id)) }
    }

    /**
     * Evict SYNCED objects, LRU first, until under the low watermark. Never
     * touches an object that is not confirmed upstream. Also drops any SYNCED
     * object past its TTL. Returns bytes freed.
     */
    fun evictToLowWatermark(): Long {
        val now = Instant.now()
        var freed = 0L
        // TTL sweep first (any SYNCED past ttl).
        if (ttlSeconds > 0) index.values.filter { it.evictable() && it.cachedAt.plusSeconds(ttlSeconds).isBefore(now) }
            .forEach { freed += it.size; remove(it.bucket, it.key) }
        val low = (maxBytes * lowWatermark).toLong()
        if (bytes.get() <= low) return freed
        val victims = index.values.filter { it.evictable() }.sortedBy { it.lastAccess }
        for (v in victims) {
            if (bytes.get() <= low) break
            freed += v.size; remove(v.bucket, v.key)
        }
        if (bytes.get() > (maxBytes * highWatermark).toLong())
            log.warnf("cache still above high watermark after eviction: %d/%d bytes; non-SYNCED objects are pinned", bytes.get(), maxBytes)
        return freed
    }

    fun overHighWatermark() = bytes.get() > (maxBytes * highWatermark).toLong()

    // (bucket,key) -> one filesystem-safe id, reversible so a disk scan can rebuild.
    private fun id(bucket: String, key: String): String {
        fun enc(s: String) = s.toByteArray(Charsets.UTF_8).joinToString("") { "%02x".format(it) }
        return "${enc(bucket)}_${enc(key)}"
    }
    private fun decode(id: String): Pair<String, String>? {
        val i = id.indexOf('_'); if (i < 0) return null
        fun dec(h: String) = runCatching { h.chunked(2).map { it.toInt(16).toByte() }.toByteArray().toString(Charsets.UTF_8) }.getOrNull()
        val b = dec(id.substring(0, i)) ?: return null
        val k = dec(id.substring(i + 1)) ?: return null
        return b to k
    }
    private fun objectsDir() = root.resolve("objects")
    private fun tmpDir() = root.resolve("tmp")
}
