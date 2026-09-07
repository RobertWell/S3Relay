package com.pkgrove.s3relay

import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.Test

/**
 * HEL-452 — the relay contract on the wire, against a real MinIO: upstream 4xx pass
 * through as themselves (status + S3 code), unsatisfiable ranges are 416, every
 * error body is S3 XML with a request id, and ranges stream without slicing.
 */
@QuarkusTest
@QuarkusTestResource(MinioTestResource::class)
class S3RelayErrorTest {
    private val B = MinioTestResource.BUCKET

    @Test
    fun `an upstream NoSuchBucket is relayed as 404 NoSuchBucket, not as an outage`() {
        given().queryParam("list-type", "2").get("/no-such-bucket-hel452").then().statusCode(404)
            .header("x-amz-request-id", notNullValue())
            .body(containsString("<Code>NoSuchBucket</Code>")).body(containsString("<RequestId>"))
        given().get("/no-such-bucket-hel452/some-key").then().statusCode(404)
            .body(containsString("<Code>NoSuchBucket</Code>"))
    }

    @Test
    fun `a range past the end is 416 InvalidRange with the total — open-ended and tail ranges stream exactly`() {
        val body = ByteArray(70_000) { (it % 251).toByte() }   // bigger than one channel transfer chunk
        given().contentType("application/octet-stream").body(body).put("/$B/range-hel452.bin").then().statusCode(200)
        given().header("Range", "bytes=70000-70010").get("/$B/range-hel452.bin").then().statusCode(416)
            .header("Content-Range", equalTo("bytes */70000")).body(containsString("<Code>InvalidRange</Code>"))
        val tail = given().header("Range", "bytes=69990-").get("/$B/range-hel452.bin").then().statusCode(206)
            .header("Content-Range", equalTo("bytes 69990-69999/70000")).header("Content-Length", equalTo("10")).extract().asByteArray()
        assert(tail.contentEquals(body.copyOfRange(69990, 70000)))
        val mid = given().header("Range", "bytes=1000-65535").get("/$B/range-hel452.bin").then().statusCode(206)
            .header("Content-Range", equalTo("bytes 1000-65535/70000")).extract().asByteArray()
        assert(mid.contentEquals(body.copyOfRange(1000, 65536)))
        // a range whose end exceeds the object is clamped, per RFC 7233
        given().header("Range", "bytes=69000-99999").get("/$B/range-hel452.bin").then().statusCode(206)
            .header("Content-Range", equalTo("bytes 69000-69999/70000"))
    }

    @Test
    fun `HEAD on a missing key is 404 and on a cached key reports the cache`() {
        given().head("/$B/missing-hel452").then().statusCode(404)
        given().contentType("text/plain").body("x").put("/$B/head-hel452.txt").then().statusCode(200)
        given().head("/$B/head-hel452.txt").then().statusCode(200).header("X-S3Relay-Cache", equalTo("HIT")).header("Content-Length", equalTo("1"))
    }

}
