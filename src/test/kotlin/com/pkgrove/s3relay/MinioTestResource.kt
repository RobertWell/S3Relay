package com.pkgrove.s3relay

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import java.net.URI
import java.nio.file.Files

/**
 * Wires the app to a REAL MinIO started on the host by the test harness
 * (S3RELAY_TEST_* env) and a throwaway cache dir, and creates the bucket. No
 * Testcontainers — the host Docker's API version and the mounted-socket
 * negotiation made that unreliable, and starting the container in the harness
 * is simpler than embedding it. Real backend, not a mock.
 */
class MinioTestResource : QuarkusTestResourceLifecycleManager {
    override fun start(): MutableMap<String, String> {
        directClient().use { it.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()) }
        val cacheDir = Files.createTempDirectory("s3relay-cache").toString()
        return mutableMapOf(
            "s3relay.upstream.endpoint" to ENDPOINT,
            "s3relay.upstream.access-key" to USER,
            "s3relay.upstream.secret-key" to PASS,
            "s3relay.upstream.path-style" to "true",
            "s3relay.cache.dir" to cacheDir,
        )
    }
    override fun stop() {}

    companion object {
        val ENDPOINT: String = System.getenv("S3RELAY_TEST_ENDPOINT") ?: "http://localhost:9000"
        val USER: String = System.getenv("S3RELAY_TEST_USER") ?: "s3relaytest"
        val PASS: String = System.getenv("S3RELAY_TEST_PASS") ?: "s3relaytestsecret"
        val BUCKET: String = System.getenv("S3RELAY_TEST_BUCKET") ?: "test"
        fun directClient(): S3Client = S3Client.builder().endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1).forcePathStyle(true)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(USER, PASS))).build()
    }
}
