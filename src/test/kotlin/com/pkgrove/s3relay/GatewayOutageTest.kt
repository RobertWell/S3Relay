package com.pkgrove.s3relay

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * The resilience guarantees (HEL-421), tested deterministically against a fake
 * upstream that can be switched DOWN — no container, no flakiness. These are the
 * acceptance criteria that matter most: never a false durable success; cached
 * reads survive an outage; a failed PUT is retried and heals.
 */
class GatewayOutageTest {
    /** An in-memory ObjectStorage that can be flipped down. */
    class FakeStorage : ObjectStorage {
        val store = ConcurrentHashMap<String, ByteArray>()
        @Volatile var down = false
        private fun k(b: String, k: String) = "$b/$k"
        private fun guard() { if (down) throw UpstreamUnavailable("fake is down") }
        override fun put(bucket: String, key: String, source: Path, metadata: ObjectMetadata): String {
            guard(); store[k(bucket, key)] = Files.readAllBytes(source); return "etag-${store[k(bucket,key)]!!.size}"
        }
        override fun get(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata? {
            guard(); val b = store[k(bucket, key)] ?: return null; Files.write(dest, b)
            return ObjectMetadata(b.size.toLong(), "application/octet-stream", "etag-${b.size}", Instant.now())
        }
        override fun head(bucket: String, key: String): ObjectMetadata? {
            guard(); val b = store[k(bucket, key)] ?: return null; return ObjectMetadata(b.size.toLong(), null, "etag-${b.size}", Instant.now())
        }
        override fun delete(bucket: String, key: String) { guard(); store.remove(k(bucket, key)) }
        override fun list(bucket: String, prefix: String?, delimiter: String?, continuationToken: String?, maxKeys: Int): Listing {
            guard(); return Listing(emptyList(), emptyList(), null, false)
        }
    }

    private fun gateway(fake: FakeStorage): Gateway {
        val dir = Files.createTempDirectory("gw").toString()
        val g = Gateway(Upstream(fake, 8000), dir, 1_000_000, 0, 0.9, 0.5)
        g.init()   // @PostConstruct, called directly in the unit test
        return g
    }
    private fun bytes(s: String) = s.toByteArray().inputStream()

    @Test fun `PUT is durable only after upstream stores it — while down it is NotDurable but cached`() {
        val fake = FakeStorage(); val g = gateway(fake)
        val ok = g.put("b", "k", bytes("hello"), 5, "text/plain")
        assertTrue(ok is PutOutcome.Stored); assertArrayEquals("hello".toByteArray(), fake.store["b/k"])
        assertEquals(SyncState.SYNCED, g.cache.peek("b", "k")!!.state)

        fake.down = true
        val nd = g.put("b", "k2", bytes("world"), 5, "text/plain")
        assertTrue(nd is PutOutcome.NotDurable, "must NOT claim durable success while upstream is down")
        assertNull(fake.store["b/k2"], "upstream never got it")
        assertEquals(SyncState.PENDING_REMOTE, g.cache.peek("b", "k2")!!.state, "kept locally, pinned")
        // readable locally despite the failed durable write
        val got = g.get("b", "k2", null)
        assertTrue(got is GetOutcome.Ok && Files.readString((got).result.path) == "world")
    }

    @Test fun `cached GET survives an outage — uncached GET fails bounded`() {
        val fake = FakeStorage(); val g = gateway(fake)
        g.put("b", "cached", bytes("keepme"), 6, null)
        fake.down = true
        val hit = g.get("b", "cached", null)
        assertTrue(hit is GetOutcome.Ok && (hit).result.fromCache && Files.readString((hit).result.path) == "keepme")
        assertTrue(g.get("b", "never", null) is GetOutcome.Unavailable, "uncached read during outage is Unavailable, not a hang")
    }

    @Test fun `the reconciler heals PENDING_REMOTE objects when upstream returns`() {
        val fake = FakeStorage(); val g = gateway(fake)
        fake.down = true
        g.put("b", "p", bytes("later"), 5, null)
        assertEquals(SyncState.PENDING_REMOTE, g.cache.peek("b", "p")!!.state)
        assertEquals(0, g.reconcilePending(), "cannot sync while down")
        fake.down = false
        assertEquals(1, g.reconcilePending(), "one object healed")
        assertEquals(SyncState.SYNCED, g.cache.peek("b", "p")!!.state)
        assertArrayEquals("later".toByteArray(), fake.store["b/p"])
    }

    @Test fun `range read on a cache hit returns the exact slice`() {
        val fake = FakeStorage(); val g = gateway(fake)
        g.put("b", "r", bytes("0123456789"), 10, null)
        val o = g.get("b", "r", 2L..5L)
        assertTrue(o is GetOutcome.Ok && (o).partial)
        assertEquals("2345", Files.readString((o as GetOutcome.Ok).result.path))
    }
}
