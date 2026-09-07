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
        // HEL-460 tube mode: the fake streams like the real thing (the body is consumed, never replayed)
        override fun putStream(bucket: String, key: String, body: java.io.InputStream, length: Long, contentType: String?): String {
            guard(); val b = body.readBytes(); store[k(bucket, key)] = b; return "etag-${b.size}"
        }
        override fun open(bucket: String, key: String, range: LongRange?): ObjectStream? {
            guard(); val b = store[k(bucket, key)] ?: return null
            val slice = range?.let { b.copyOfRange(it.first.toInt(), minOf(it.last, b.size - 1L).toInt() + 1) } ?: b
            val cr = range?.let { "bytes ${it.first}-${minOf(it.last, b.size - 1L)}/${b.size}" }
            return ObjectStream(ObjectMetadata(slice.size.toLong(), "application/octet-stream", "etag-${b.size}", Instant.now()), slice.inputStream(), cr, b.size.toLong())
        }
        override fun delete(bucket: String, key: String) { guard(); store.remove(k(bucket, key)) }
        override fun list(bucket: String, prefix: String?, delimiter: String?, continuationToken: String?, maxKeys: Int): Listing {
            guard(); return Listing(emptyList(), emptyList(), null, false)
        }
    }

    private fun gateway(fake: FakeStorage): Gateway {
        val dir = Files.createTempDirectory("gw").toString()
        val g = Gateway(Upstream(fake), dir, 1_000_000, 0, 0.9, 0.5)
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
        assertEquals(0, g.reconcilePending().synced, "cannot sync while down")
        fake.down = false
        assertEquals(1, g.reconcilePending().synced, "one object healed")
        assertEquals(SyncState.SYNCED, g.cache.peek("b", "p")!!.state)
        assertArrayEquals("later".toByteArray(), fake.store["b/p"])
    }

    @Test fun `range read on a cache hit returns the exact slice`() {
        val fake = FakeStorage(); val g = gateway(fake)
        g.put("b", "r", bytes("0123456789"), 10, null)
        val o = g.get("b", "r", 2L..5L)
        assertTrue(o is GetOutcome.Ok && (o).partial)
        val r = (o as GetOutcome.Ok).result
        assertEquals(2L..5L, r.range); assertEquals(10L, r.totalLength); assertEquals(4L, r.metadata.contentLength)
        assertEquals("2345", slice(r))
        // no slice file was created: the tmp dir stays empty after a range read (HEL-452 leak)
        val root0 = g.cache.javaClass.getDeclaredField("root").apply { isAccessible = true }.get(g.cache) as Path
        assertEquals(0, Files.list(root0.resolve("tmp")).use { it.count() })
    }

    /** What the resource does: stream the resolved range from the cached file. */
    private fun slice(r: GetResult): String {
        val range = r.range ?: return Files.readString(r.path)
        java.nio.channels.FileChannel.open(r.path).use { ch ->
            val buf = java.nio.ByteBuffer.allocate((range.last - range.first + 1).toInt()); ch.position(range.first)
            while (buf.hasRemaining() && ch.read(buf) > 0) {}
            return String(buf.array(), 0, buf.position())
        }
    }

    // ── HEL-452: the relay contract ─────────────────────────────────────────

    class RejectingStorage(private val status: Int, private val code: String) : ObjectStorage {
        override fun put(bucket: String, key: String, source: Path, metadata: ObjectMetadata): String = throw UpstreamError(status, code, "$code from upstream")
        override fun get(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata? = throw UpstreamError(status, code, "$code from upstream")
        override fun head(bucket: String, key: String): ObjectMetadata? = throw UpstreamError(status, code, "$code from upstream")
        override fun putStream(bucket: String, key: String, body: java.io.InputStream, length: Long, contentType: String?): String = throw UpstreamError(status, code, "$code from upstream")
        override fun open(bucket: String, key: String, range: LongRange?): ObjectStream? = throw UpstreamError(status, code, "$code from upstream")
        override fun delete(bucket: String, key: String) = throw UpstreamError(status, code, "$code from upstream")
        override fun list(bucket: String, prefix: String?, delimiter: String?, continuationToken: String?, maxKeys: Int): Listing = throw UpstreamError(status, code, "$code from upstream")
    }

    @Test fun `an upstream 4xx is relayed as itself on every verb and never pins a copy`() {
        val dir = Files.createTempDirectory("gw").toString()
        val g = Gateway(Upstream(RejectingStorage(403, "AccessDenied")), dir, 1_000_000, 0, 0.9, 0.5); g.init()
        val p = g.put("b", "k", bytes("x"), 1, null)
        assertTrue(p is PutOutcome.Rejected && p.status == 403 && p.code == "AccessDenied")
        assertNull(g.cache.peek("b", "k"), "a rejected object is not kept pinned in the cache")
        val get = g.get("b", "k", null); assertTrue(get is GetOutcome.UpstreamError && get.status == 403)
        val head = g.head("b", "k"); assertTrue(head is HeadOutcome.UpstreamError && head.code == "AccessDenied")
        val del = g.delete("b", "k"); assertTrue(del is DeleteOutcome.UpstreamError && del.status == 403)
        val list = g.list("b", null, null, null, 10); assertTrue(list is ListOutcome.UpstreamError && list.status == 403)
    }

    @Test fun `HEAD during an outage is Unavailable, not NotFound — a cached object still answers`() {
        val fake = FakeStorage(); val g = gateway(fake)
        g.put("b", "k", bytes("hello"), 5, "text/plain")
        fake.down = true
        val cached = g.head("b", "k"); assertTrue(cached is HeadOutcome.Ok && cached.fromCache && cached.metadata.contentLength == 5L)
        assertTrue(g.head("b", "other") is HeadOutcome.Unavailable, "an outage must not read as 'object absent'")
        assertTrue(g.delete("b", "k") is DeleteOutcome.Unavailable)
        assertTrue(g.list("b", null, null, null, 10) is ListOutcome.Unavailable)
    }

    @Test fun `a body that fails mid-upload leaves no part file and reaches the caller as an exception`() {
        val fake = FakeStorage(); val g = gateway(fake)
        val failing = object : java.io.InputStream() {
            var n = 0
            override fun read(): Int { if (n++ > 100) throw java.io.IOException("client hung up"); return 65 }
        }
        val root = g.cache.javaClass.getDeclaredField("root").apply { isAccessible = true }.get(g.cache) as Path
        assertThrows(java.io.IOException::class.java) { g.put("b", "broken", failing, null, null) }
        assertNull(g.cache.peek("b", "broken")); assertNull(fake.store["b/broken"])
        assertEquals(0, Files.list(root.resolve("tmp")).use { it.count() }, "temp file cleaned up")
    }

    @Test fun `a declared length that does not match the received bytes is BadRequest and nothing is kept`() {
        val fake = FakeStorage(); val g = gateway(fake)
        val o = g.put("b", "short", bytes("abc"), 5, null)
        assertTrue(o is PutOutcome.BadRequest, "$o"); assertNull(g.cache.peek("b", "short")); assertNull(fake.store["b/short"])
    }

    @Test fun `a cache full of pinned objects passes new writes through instead of refusing or overflowing (HEL-460)`() {
        val fake = FakeStorage(); val dir = Files.createTempDirectory("gw").toString()
        val g = Gateway(Upstream(fake), dir, 20, 0, 0.9, 0.5); g.init()   // 20-byte cache
        fake.down = true
        assertTrue(g.put("b", "a", bytes("0123456789"), 10, null) is PutOutcome.NotDurable)   // pinned, 10/20
        assertTrue(g.put("b", "b", bytes("0123456789"), 10, null) is PutOutcome.NotDurable)   // pinned, 20/20
        // no room and upstream down: the body is relayed (tube) and upstream refuses → NotStored, nothing cached, cache not overflowed
        val full = g.put("b", "c", bytes("0123456789"), 10, null)
        assertTrue(full is PutOutcome.NotStored, "tube mode with upstream down is an honest 503, not a 507: $full")
        assertEquals(20L, g.cache.usedBytes()); assertEquals(20L, g.cache.pinnedBytes()); assertEquals(0L, g.cache.reservedBytes()); assertNull(g.cache.peek("b", "c"))
        fake.down = false
        // no room but upstream up: the body is streamed straight to upstream — durable, uncached
        val tube = g.put("b", "d", bytes("0123456789"), 10, null)
        assertTrue(tube is PutOutcome.Stored && !tube.cached, "passed through: $tube")
        assertArrayEquals("0123456789".toByteArray(), fake.store["b/d"]); assertNull(g.cache.peek("b", "d")); assertEquals(20L, g.cache.usedBytes())
        assertEquals(2, g.reconcilePending().synced)
        val cached = g.put("b", "c", bytes("0123456789"), 10, null)
        assertTrue(cached is PutOutcome.Stored && cached.cached, "evictable again once synced: $cached")
    }

    @Test fun `an object bigger than the whole cache is streamed from upstream, whole and ranged, and never cached (HEL-460)`() {
        val fake = FakeStorage(); val dir = Files.createTempDirectory("gw").toString()
        val g = Gateway(Upstream(fake), dir, 20, 0, 0.9, 0.5); g.init()   // 20-byte cache
        fake.store["b/big"] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".toByteArray()   // 36 bytes
        val whole = g.get("b", "big", null)
        assertTrue(whole is GetOutcome.Passthrough && !whole.partial, "$whole")
        assertEquals("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", String((whole as GetOutcome.Passthrough).stream.body.readBytes())); whole.stream.close()
        val part = g.get("b", "big", 10L..19L)
        assertTrue(part is GetOutcome.Passthrough && part.partial, "$part")
        part as GetOutcome.Passthrough
        assertEquals("ABCDEFGHIJ", String(part.stream.body.readBytes())); assertEquals("bytes 10-19/36", part.stream.contentRange); assertEquals(10L, part.stream.metadata.contentLength); part.stream.close()
        assertNull(g.cache.peek("b", "big")); assertEquals(0L, g.cache.usedBytes()); assertEquals(0L, g.cache.reservedBytes())
        // a small object on the same gateway is fetched into the cache as before
        fake.store["b/small"] = "tiny".toByteArray()
        assertTrue(g.get("b", "small", null) is GetOutcome.Ok); assertEquals(SyncState.SYNCED, g.cache.peek("b", "small")!!.state)
    }

    @Test fun `room is reserved for in-flight bodies so concurrent PUTs cannot together overflow the cache (HEL-460)`() {
        val fake = FakeStorage(); val dir = Files.createTempDirectory("gw").toString()
        val g = Gateway(Upstream(fake), dir, 25, 0, 0.9, 0.5); g.init()   // 25-byte cache: two 10-byte bodies fit, three do not
        val gate = java.util.concurrent.CountDownLatch(1)
        class Slow(val bytes: ByteArray) : java.io.InputStream() {   // holds the first read until released, like a slow client
            var i = 0; var waited = false
            override fun read(): Int { if (!waited) { waited = true; gate.await() }; return if (i < bytes.size) bytes[i++].toInt() and 0xff else -1 }
        }
        val pool = java.util.concurrent.Executors.newFixedThreadPool(3)
        val futures = listOf("a", "b", "c").map { k -> pool.submit<PutOutcome> { g.put("b", k, Slow("0123456789".toByteArray()), 10, null) } }
        Thread.sleep(300); assertEquals(20L, g.cache.reservedBytes(), "two bodies reserved, the third passed through")
        gate.countDown()
        val outs = futures.map { it.get(10, java.util.concurrent.TimeUnit.SECONDS) }
        assertEquals(3, outs.count { it is PutOutcome.Stored }, "$outs")
        assertEquals(1, outs.count { it is PutOutcome.Stored && !it.cached }, "exactly one went through the tube: $outs")
        assertEquals(20L, g.cache.usedBytes()); assertEquals(0L, g.cache.reservedBytes())
        assertEquals(3, listOf("a", "b", "c").count { fake.store["b/$it"] != null }, "all three are durable upstream")
        pool.shutdown()
    }

    @Test fun `the reconciler asks upstream first — adopts an identical copy, drops a stale one, never overwrites`() {
        val fake = FakeStorage(); val g = gateway(fake)
        fake.down = true
        g.put("b", "same", bytes("hello"), 5, null); g.put("b", "stale", bytes("old-version"), 11, null)
        fake.down = false
        fake.store["b/same"] = "hello".toByteArray()            // upstream already has the identical object (e.g. same-pod restart)
        fake.store["b/stale"] = "NEWER upstream write".toByteArray()   // someone wrote upstream directly meanwhile
        val r = g.reconcilePending()
        assertEquals(1, r.adopted); assertEquals(1, r.dropped); assertEquals(0, r.synced)
        assertEquals(SyncState.SYNCED, g.cache.peek("b", "same")!!.state)
        assertNull(g.cache.peek("b", "stale"), "stale pending copy is dropped")
        assertArrayEquals("NEWER upstream write".toByteArray(), fake.store["b/stale"], "the newer upstream object was NOT overwritten")
    }

    @Test fun `ranges resolve without slicing — open-ended, clamped, unsatisfiable, and past 2 GiB`() {
        val fake = FakeStorage(); val g = gateway(fake)
        g.put("b", "r", bytes("0123456789"), 10, null)
        val open = g.get("b", "r", 7L..Long.MAX_VALUE) as GetOutcome.Ok
        assertEquals(7L..9L, open.result.range); assertEquals(3L, open.result.metadata.contentLength); assertEquals("789", slice(open.result))
        assertTrue(g.get("b", "r", 10L..12L) is GetOutcome.RangeNotSatisfiable)
        // a sparse 2.2 GB cached object: the old code allocated the range in heap and overflowed toInt()
        val big = g.cache.newTemp(); java.io.RandomAccessFile(big.toFile(), "rw").use { it.setLength(2_200_000_000L); it.seek(2_199_999_995L); it.write("TAIL!".toByteArray()) }
        g.cache.commit("b", "big", big, SyncState.SYNCED, "e", null, null)
        val tail = g.get("b", "big", 2_199_999_995L..Long.MAX_VALUE) as GetOutcome.Ok
        assertEquals(2_199_999_995L..2_199_999_999L, tail.result.range); assertEquals(5L, tail.result.metadata.contentLength)
        assertEquals("TAIL!", slice(tail.result))
        val whole = g.get("b", "big", null) as GetOutcome.Ok
        assertEquals(2_200_000_000L, whole.result.metadata.contentLength)
    }
}
