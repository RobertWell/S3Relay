package com.pkgrove.s3relay

import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured
import io.restassured.RestAssured.given
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.net.URI
import java.security.MessageDigest

/**
 * HEL-462 — multipart uploads through the relay, driven by a real S3 SDK client the way `aws s3 cp` / boto3
 * `upload_file` drive them: create → parts → complete, plus list-parts, abort and the relayed NoSuchUpload.
 * Parts are ≥ 5 MiB (S3's minimum for non-final parts); the assembled 11 MiB object is bigger than the 2 MiB test
 * cache, so the read-back also exercises the tube (HEL-460).
 */
@QuarkusTest
@QuarkusTestResource(MinioTestResource::class)
class S3RelayMultipartTest {
    private val B = MinioTestResource.BUCKET
    private fun sha(b: ByteArray) = MessageDigest.getInstance("SHA-256").digest(b).joinToString("") { "%02x".format(it) }
    /** An SDK client that talks to the RELAY (the relay does no auth; anonymous credentials keep the SDK from signing). */
    private fun relay(): S3Client = S3Client.builder().endpointOverride(URI.create("http://localhost:${RestAssured.port}"))
        .region(Region.US_EAST_1).forcePathStyle(true).credentialsProvider(AnonymousCredentialsProvider.create()).build()

    @Test
    fun `create, three parts, list parts, complete — the object is upstream, byte-exact through the relay, and the ETag is a multipart ETag`() {
        val p1 = ByteArray(5 * 1024 * 1024).also { java.util.Random(21).nextBytes(it) }
        val p2 = ByteArray(5 * 1024 * 1024).also { java.util.Random(22).nextBytes(it) }
        val p3 = ByteArray(1 * 1024 * 1024).also { java.util.Random(23).nextBytes(it) }
        val whole = p1 + p2 + p3
        relay().use { c ->
            val up = c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket(B).key("mp/big.bin").contentType("application/x-mp").build())
            assertTrue(up.uploadId().isNotBlank())
            val etags = listOf(p1, p2, p3).mapIndexed { i, part ->
                c.uploadPart(UploadPartRequest.builder().bucket(B).key("mp/big.bin").uploadId(up.uploadId()).partNumber(i + 1).contentLength(part.size.toLong()).build(),
                    RequestBody.fromBytes(part)).eTag()
            }
            val listed = c.listParts(ListPartsRequest.builder().bucket(B).key("mp/big.bin").uploadId(up.uploadId()).build())
            assertEquals(listOf(1, 2, 3), listed.parts().map { it.partNumber() }); assertEquals(listOf(5L, 5L, 1L).map { it * 1024 * 1024 }, listed.parts().map { it.size() })
            val done = c.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(B).key("mp/big.bin").uploadId(up.uploadId())
                .multipartUpload(CompletedMultipartUpload.builder().parts(etags.mapIndexed { i, e -> CompletedPart.builder().partNumber(i + 1).eTag(e).build() }).build()).build())
            assertTrue(done.eTag().trim('"').endsWith("-3"), "multipart ETag from upstream: ${done.eTag()}")
        }
        // upstream has the assembled object; the relay serves it (too big for the test cache → tube), exact bytes
        MinioTestResource.directClient().use { c -> assertEquals(whole.size.toLong(), c.headObject(HeadObjectRequest.builder().bucket(B).key("mp/big.bin").build()).contentLength()) }
        val got = given().get("/$B/mp/big.bin").then().statusCode(200).header("Content-Length", equalTo(whole.size.toString()))
            .header("X-S3Relay-Cache", equalTo("PASSTHROUGH")).header("Content-Type", equalTo("application/x-mp")).extract().asByteArray()
        assertEquals(sha(whole), sha(got))
    }

    @Test
    fun `complete drops a stale cached copy, abort leaves nothing, a wrong upload id relays NoSuchUpload, malformed complete is 400`() {
        // a small cached object under the key, then a multipart upload replaces it upstream: the cache must not serve the old bytes
        given().contentType("text/plain").body("old version").put("/$B/mp/replace.txt").then().statusCode(200).header("X-S3Relay-Cache", equalTo("STORED"))
        given().get("/$B/mp/replace.txt").then().statusCode(200).header("X-S3Relay-Cache", equalTo("HIT"))
        val part = "new version via multipart".toByteArray()
        relay().use { c ->
            val up = c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket(B).key("mp/replace.txt").build())
            val e = c.uploadPart(UploadPartRequest.builder().bucket(B).key("mp/replace.txt").uploadId(up.uploadId()).partNumber(1).contentLength(part.size.toLong()).build(), RequestBody.fromBytes(part)).eTag()
            c.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(B).key("mp/replace.txt").uploadId(up.uploadId())
                .multipartUpload(CompletedMultipartUpload.builder().parts(CompletedPart.builder().partNumber(1).eTag(e).build()).build()).build())
            // the next read is a MISS that fetches the new object
            given().get("/$B/mp/replace.txt").then().statusCode(200).header("X-S3Relay-Cache", equalTo("MISS")).body(equalTo("new version via multipart"))
            // abort: the upload disappears from the list (MinIO lists in-progress uploads by exact object prefix); the key never appears
            val ab = c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket(B).key("mp/aborted.bin").build())
            assertTrue(c.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(B).prefix("mp/aborted.bin").build()).uploads().any { it.uploadId() == ab.uploadId() })
            c.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(B).key("mp/aborted.bin").uploadId(ab.uploadId()).build())
            assertTrue(c.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(B).prefix("mp/aborted.bin").build()).uploads().none { it.uploadId() == ab.uploadId() })
            given().head("/$B/mp/aborted.bin").then().statusCode(404)
            // a bogus upload id is upstream's answer, relayed as itself
            val ex = assertThrows(S3Exception::class.java) {
                c.uploadPart(UploadPartRequest.builder().bucket(B).key("mp/aborted.bin").uploadId("no-such-upload").partNumber(1).contentLength(3).build(), RequestBody.fromBytes("abc".toByteArray()))
            }
            assertEquals(404, ex.statusCode()); assertEquals("NoSuchUpload", ex.awsErrorDetails().errorCode())
            // the rejected part's unread body must not poison the keep-alive connection: the SAME client's next call works
            c.deleteObject(DeleteObjectRequest.builder().bucket(B).key("mp/replace.txt").build())
            given().head("/$B/mp/replace.txt").then().statusCode(404)
            // and a large rejected part (bigger than the drain limit) is answered with Connection: close, still followed by a working call
            val ex2 = assertThrows(S3Exception::class.java) {
                c.uploadPart(UploadPartRequest.builder().bucket(B).key("mp/aborted.bin").uploadId("no-such-upload").partNumber(1).contentLength(2_000_000).build(),
                    RequestBody.fromBytes(ByteArray(2_000_000)))
            }
            assertEquals(404, ex2.statusCode())
            assertTrue(c.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(B).prefix("mp/aborted.bin").build()).uploads().isEmpty())
        }
        // a Complete body that is not the expected XML is a 400 MalformedXML from the relay, never a 500
        given().contentType("application/xml").body("<CompleteMultipartUpload><Part><PartNumber>x</PartNumber></Part></CompleteMultipartUpload>")
            .post("/$B/mp/replace.txt?uploadId=whatever").then().statusCode(400).body(containsString("MalformedXML"))
        given().post("/$B/mp/replace.txt").then().statusCode(405)
    }
}
