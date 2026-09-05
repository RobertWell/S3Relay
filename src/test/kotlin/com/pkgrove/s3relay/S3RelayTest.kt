package com.pkgrove.s3relay

import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException

/**
 * The S3-compatible surface against a real MinIO. Ordered so the outage cases
 * (which stop the container) run last.
 */
@QuarkusTest
@QuarkusTestResource(MinioTestResource::class)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class S3RelayTest {
    private val B = MinioTestResource.BUCKET

    @Test @Order(1)
    fun `PutObject writes through to BOTH cache and upstream`() {
        val body = "hello-s3relay".toByteArray()
        given().contentType("text/plain").body(body).put("/$B/greeting.txt")
            .then().statusCode(200).header("ETag", notNullValue())
        // upstream has it (read with a direct client, bypassing S3Relay)
        MinioTestResource.directClient().use { c ->
            val got = c.getObjectAsBytes(GetObjectRequest.builder().bucket(B).key("greeting.txt").build()).asByteArray()
            assertArrayEquals(body, got, "the object must exist in MinIO, not only in the ephemeral cache")
        }
    }

    @Test @Order(2)
    fun `GetObject serves the cached copy after a write (HIT)`() {
        given().get("/$B/greeting.txt").then().statusCode(200)
            .header("X-S3Relay-Cache", equalTo("HIT")).body(equalTo("hello-s3relay"))
    }

    @Test @Order(3)
    fun `cache MISS falls back to upstream, populates the cache, then HITs`() {
        // put straight to MinIO so S3Relay has never seen it
        MinioTestResource.directClient().use { c ->
            c.putObject(PutObjectRequest.builder().bucket(B).key("direct.bin").build(), RequestBody.fromBytes("from-minio".toByteArray()))
        }
        given().get("/$B/direct.bin").then().statusCode(200).header("X-S3Relay-Cache", equalTo("MISS")).body(equalTo("from-minio"))
        given().get("/$B/direct.bin").then().statusCode(200).header("X-S3Relay-Cache", equalTo("HIT"))
    }

    @Test @Order(4)
    fun `HeadObject returns metadata without a body`() {
        given().head("/$B/greeting.txt").then().statusCode(200)
            .header("Content-Length", equalTo("13")).header("Accept-Ranges", equalTo("bytes"))
    }

    @Test @Order(5)
    fun `Range GET returns 206 and the exact slice`() {
        given().header("Range", "bytes=0-4").get("/$B/greeting.txt").then().statusCode(206).body(equalTo("hello"))
        given().header("Range", "bytes=6-12").get("/$B/greeting.txt").then().statusCode(206).body(equalTo("s3relay"))
    }

    @Test @Order(6)
    fun `ListObjectsV2 returns the keys as S3 XML`() {
        given().queryParam("list-type", "2").get("/$B").then().statusCode(200)
            .body(containsString("<Name>test</Name>"))
            .body(containsString("<Key>greeting.txt</Key>"))
            .body(containsString("<Key>direct.bin</Key>"))
            .body(containsString("<IsTruncated>false</IsTruncated>"))
    }

    @Test @Order(7)
    fun `DeleteObject removes from upstream and cache`() {
        given().delete("/$B/direct.bin").then().statusCode(204)
        given().get("/$B/direct.bin").then().statusCode(404).body(containsString("<Code>NoSuchKey</Code>"))
        MinioTestResource.directClient().use { c ->
            assertThrows(NoSuchKeyException::class.java) { c.headObject { it.bucket(B).key("direct.bin") } }
        }
    }

    @Test @Order(8)
    fun `unknown key is a NoSuchKey 404`() {
        given().get("/$B/nope.txt").then().statusCode(404).body(containsString("NoSuchKey"))
    }
}
