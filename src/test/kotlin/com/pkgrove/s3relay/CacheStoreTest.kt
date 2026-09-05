package com.pkgrove.s3relay

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.nio.file.Files

/**
 * The load-bearing invariant (HEL-421), tested without Quarkus so it is exact:
 * ONLY SYNCED objects are evictable; anything the upstream has not confirmed is
 * pinned even under disk pressure, because its cache copy is the only copy.
 */
class CacheStoreTest {
    private fun store(max: Long) = CacheStore(Files.createTempDirectory("cs"), max, ttlSeconds = 0, highWatermark = 0.9, lowWatermark = 0.5)
    private fun write(c: CacheStore, bucket: String, key: String, n: Int, state: SyncState) {
        val t = c.newTemp(); Files.write(t, ByteArray(n) { 'x'.code.toByte() })
        c.commit(bucket, key, t, state, "e", "application/octet-stream", null)
    }

    @Test fun `only SYNCED objects are evicted, LRU first`() {
        val c = store(1000)
        write(c, "b", "synced-old", 300, SyncState.SYNCED); Thread.sleep(2)
        write(c, "b", "pending", 300, SyncState.PENDING_REMOTE); Thread.sleep(2)
        write(c, "b", "synced-new", 300, SyncState.SYNCED)
        c.get("b", "synced-new")  // touch → newer LRU than synced-old
        assertEquals(900, c.usedBytes())
        // over low watermark (500) → evict SYNCED, oldest first; pending is pinned
        c.evictToLowWatermark()
        assertNull(c.peek("b", "synced-old"), "oldest SYNCED evicted")
        assertNotNull(c.peek("b", "pending"), "PENDING_REMOTE must never be evicted")
        assertTrue(c.usedBytes() <= 600)
    }

    @Test fun `a cache full of unsynced objects cannot be evicted below the watermark`() {
        val c = store(1000)
        write(c, "b", "p1", 400, SyncState.PENDING_REMOTE)
        write(c, "b", "p2", 400, SyncState.SYNCING)
        val freed = c.evictToLowWatermark()
        assertEquals(0, freed, "nothing durable to evict")
        assertEquals(800, c.usedBytes())
        assertNotNull(c.peek("b", "p1")); assertNotNull(c.peek("b", "p2"))
    }

    @Test fun `commit is atomic and replaces cleanly`() {
        val c = store(10_000)
        write(c, "b", "k", 100, SyncState.SYNCED); assertEquals(100, c.usedBytes())
        write(c, "b", "k", 250, SyncState.SYNCED); assertEquals(250, c.usedBytes(), "size accounting on replace")
        assertEquals(250, Files.size(c.get("b", "k")!!.first))
    }
}
