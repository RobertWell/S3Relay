package com.pkgrove.s3relay

import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.security.MessageDigest

/**
 * HEL-460 tube mode: the %test cache holds 2 MiB, so a 3 MiB object can never be cached — it must still flow.
 * A PUT is streamed to upstream (durable, not cached); a GET miss is streamed from upstream with one chunk in
 * flight, whole or ranged, with a real Content-Length; small objects keep being cached as before.
 */
@QuarkusTest
@QuarkusTestResource(MinioTestResource::class)
class S3RelayTubeTest {
    private val B = MinioTestResource.BUCKET
    private fun sha(b: ByteArray) = MessageDigest.getInstance("SHA-256").digest(b).joinToString("") { "%02x".format(it) }

    @Test
    fun `a 3 MiB PUT that cannot be cached is passed through to upstream and read back byte-exact, whole and ranged`() {
        val body = ByteArray(3 * 1024 * 1024).also { java.util.Random(11).nextBytes(it) }
        val put = given().contentType("application/octet-stream").body(body).put("/$B/tube-put.bin").then().statusCode(200)
            .header("ETag", notNullValue()).header("X-S3Relay-Cache", equalTo("PASSTHROUGH")).extract()
        // upstream has it, byte-for-byte; the relay kept no copy (a subsequent HEAD is answered from upstream)
        MinioTestResource.directClient().use { c ->
            assertEquals(body.size.toLong(), c.headObject(HeadObjectRequest.builder().bucket(B).key("tube-put.bin").build()).contentLength())
        }
        given().head("/$B/tube-put.bin").then().statusCode(200).header("X-S3Relay-Cache", equalTo("MISS")).header("Content-Length", equalTo(body.size.toString()))
        // a GET of something the cache cannot hold streams from upstream: Content-Length, never chunked, exact bytes
        val whole = given().get("/$B/tube-put.bin").then().statusCode(200).header("X-S3Relay-Cache", equalTo("PASSTHROUGH"))
            .header("Content-Length", equalTo(body.size.toString())).header("Transfer-Encoding", nullValue()).extract().asByteArray()
        assertEquals(sha(body), sha(whole))
        val lo = 1_000_000L; val hi = 2_100_000L
        val part = given().header("Range", "bytes=$lo-$hi").get("/$B/tube-put.bin").then().statusCode(206).header("X-S3Relay-Cache", equalTo("PASSTHROUGH"))
            .header("Content-Range", equalTo("bytes $lo-$hi/${body.size}")).header("Content-Length", equalTo((hi - lo + 1).toString())).extract().asByteArray()
        assertArrayEquals(body.copyOfRange(lo.toInt(), hi.toInt() + 1), part)
        val tail = given().header("Range", "bytes=${body.size - 10}-").get("/$B/tube-put.bin").then().statusCode(206).extract().asByteArray()
        assertArrayEquals(body.copyOfRange(body.size - 10, body.size), tail)
        given().header("Range", "bytes=${body.size + 5}-").get("/$B/tube-put.bin").then().statusCode(416)
        // the ETag the relay returned is upstream's
        assertEquals(put.header("ETag").trim('"'), MinioTestResource.directClient().use { c -> c.headObject(HeadObjectRequest.builder().bucket(B).key("tube-put.bin").build()).eTag().trim('"') })
    }

    @Test
    fun `an oversized object written directly upstream is served through the relay without being cached — a small one is still cached`() {
        val big = ByteArray(2_500_000).also { java.util.Random(12).nextBytes(it) }
        MinioTestResource.directClient().use { c ->
            c.putObject(PutObjectRequest.builder().bucket(B).key("tube-direct.bin").contentType("application/x-tube").build(), RequestBody.fromBytes(big))
        }
        val got = given().get("/$B/tube-direct.bin").then().statusCode(200).header("X-S3Relay-Cache", equalTo("PASSTHROUGH"))
            .header("Content-Type", equalTo("application/x-tube")).header("Content-Length", equalTo(big.size.toString())).extract().asByteArray()
        assertEquals(sha(big), sha(got))
        // second read: still not cached (does not fit), still exact
        assertEquals(sha(big), sha(given().get("/$B/tube-direct.bin").then().statusCode(200).header("X-S3Relay-Cache", equalTo("PASSTHROUGH")).extract().asByteArray()))
        // a small object takes the normal path: cached on PUT, HIT on GET
        given().contentType("text/plain").body("small").put("/$B/tube-small.txt").then().statusCode(200).header("X-S3Relay-Cache", equalTo("STORED"))
        given().get("/$B/tube-small.txt").then().statusCode(200).header("X-S3Relay-Cache", equalTo("HIT")).body(equalTo("small"))
        // readiness still reports the cache; a pass-through never counted against it
        given().get("/q/health/ready").then().statusCode(200)
    }
}
