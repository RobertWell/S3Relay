package com.pkgrove.s3relay

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.net.URI
import java.nio.file.Path
import java.time.Instant

/**
 * The one upstream backend, over AWS SDK v2. Endpoint/region/credentials/path-style
 * are all configuration, so nothing here is MinIO-specific. A connection or timeout
 * failure surfaces as UpstreamUnavailable; a real NoSuchKey surfaces as null — the
 * two are NOT the same, and the cache layer treats them differently.
 */
@ApplicationScoped
class S3ObjectStorage(
    @ConfigProperty(name = "s3relay.upstream.endpoint") private val endpoint: String,
    @ConfigProperty(name = "s3relay.upstream.region") private val region: String,
    @ConfigProperty(name = "s3relay.upstream.access-key") private val accessKey: String,
    @ConfigProperty(name = "s3relay.upstream.secret-key") private val secretKey: String,
    @ConfigProperty(name = "s3relay.upstream.path-style") private val pathStyle: Boolean,
    @ConfigProperty(name = "s3relay.upstream.timeout-ms") private val timeoutMs: Long,
    @ConfigProperty(name = "s3relay.upstream.put-timeout-ms") private val putTimeoutMs: Long,
) : ObjectStorage {

    private fun builder() = S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .forcePathStyle(pathStyle)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))

    private val client: S3Client by lazy { builder().build() }

    /** Tube mode (HEL-460): the client for pass-through transfers — no retries (a request stream cannot be replayed
     *  and a half-sent response cannot be restarted) and no whole-call deadline (a slow tube is allowed to be slow;
     *  the HTTP client's socket timeout still catches a transfer that stops making progress). */
    private val streamClient: S3Client by lazy {
        builder().overrideConfiguration(software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.builder()
            .retryPolicy(software.amazon.awssdk.core.retry.RetryPolicy.none()).build()).build()
    }

    // Per-call SDK deadlines: reads get the short timeout (a stuck read must not hold a worker
    // thread), a PUT gets the long one — a 454 MB object cannot arrive in 8 s, and a PUT that is
    // timed out mid-flight is exactly the "pinned forever" failure HEL-452 found.
    private fun <B : software.amazon.awssdk.awscore.AwsRequest.Builder> B.deadline(ms: Long): B = apply {
        overrideConfiguration(software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration.builder()
            .apiCallTimeout(java.time.Duration.ofMillis(ms)).build())
    }

    override fun put(bucket: String, key: String, source: Path, metadata: ObjectMetadata): String = wrap {
        val req = PutObjectRequest.builder().bucket(bucket).key(key).deadline(putTimeoutMs)
            .apply { metadata.contentType?.let { contentType(it) } }
            .contentLength(metadata.contentLength).build()
        client.putObject(req, RequestBody.fromFile(source)).eTag()?.trim('"') ?: ""
    }

    override fun putStream(bucket: String, key: String, body: java.io.InputStream, length: Long, contentType: String?): String = wrap {
        val req = PutObjectRequest.builder().bucket(bucket).key(key).contentLength(length)
            .apply { contentType?.let { contentType(it) } }.build()
        streamClient.putObject(req, RequestBody.fromInputStream(body, length)).eTag()?.trim('"') ?: ""
    }

    override fun open(bucket: String, key: String, range: LongRange?): ObjectStream? = wrapNullable {
        val req = GetObjectRequest.builder().bucket(bucket).key(key)
            .apply { range?.let { range(if (it.last == Long.MAX_VALUE) "bytes=${it.first}-" else "bytes=${it.first}-${it.last}") } }.build()
        val rs = streamClient.getObject(req)
        val r = rs.response()
        // Content-Range "bytes a-b/total" carries the whole object's size on a ranged answer.
        val total = r.contentRange()?.substringAfterLast('/')?.toLongOrNull() ?: if (range == null) r.contentLength() else null
        ObjectStream(ObjectMetadata(r.contentLength(), r.contentType(), r.eTag()?.trim('"'), r.lastModified()), rs, r.contentRange(), total)
    }

    override fun get(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata? = wrapNullable {
        val req = GetObjectRequest.builder().bucket(bucket).key(key).deadline(putTimeoutMs)   // a download is as long as an upload
            .apply { range?.let { range("bytes=${it.first}-${it.last}") } }.build()
        // AWS SDK's toFile transformer refuses to overwrite; the cache handed us
        // a freshly-created temp file, so clear it first.
        java.nio.file.Files.deleteIfExists(dest)
        val resp = client.getObject(req, dest)
        ObjectMetadata(resp.contentLength(), resp.contentType(), resp.eTag()?.trim('"'),
            resp.lastModified())
    }

    override fun head(bucket: String, key: String): ObjectMetadata? = wrapNullable {
        val r = client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).deadline(timeoutMs).build())
        ObjectMetadata(r.contentLength(), r.contentType(), r.eTag()?.trim('"'), r.lastModified())
    }

    override fun delete(bucket: String, key: String) { wrap { client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).deadline(timeoutMs).build()) } }

    override fun list(bucket: String, prefix: String?, delimiter: String?, continuationToken: String?, maxKeys: Int): Listing = wrap {
        val r = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).deadline(timeoutMs)
            .apply { prefix?.let { prefix(it) }; delimiter?.let { delimiter(it) }; continuationToken?.let { continuationToken(it) } }
            .maxKeys(maxKeys).build())
        Listing(
            r.contents().map { ListedObject(it.key(), it.size(), it.eTag()?.trim('"'), it.lastModified()) },
            r.commonPrefixes().mapNotNull { it.prefix() },
            r.nextContinuationToken(), r.isTruncated == true)
    }

    // ── Multipart (HEL-462): parts stream on the no-retry client; the control calls take the read deadline ──
    override fun createMultipart(bucket: String, key: String, contentType: String?): String = wrap {
        client.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket(bucket).key(key).deadline(timeoutMs)
            .apply { contentType?.let { contentType(it) } }.build()).uploadId()
    }
    override fun uploadPart(bucket: String, key: String, uploadId: String, partNumber: Int, body: java.io.InputStream, length: Long): String = wrap {
        streamClient.uploadPart(UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).contentLength(length).build(),
            RequestBody.fromInputStream(body, length)).eTag()?.trim('"') ?: ""
    }
    override fun completeMultipart(bucket: String, key: String, uploadId: String, parts: List<Pair<Int, String>>): String = wrap {
        client.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).deadline(putTimeoutMs)
            .multipartUpload(CompletedMultipartUpload.builder().parts(parts.map { (n, e) -> CompletedPart.builder().partNumber(n).eTag(e).build() }).build())
            .build()).eTag()?.trim('"') ?: ""
    }
    override fun abortMultipart(bucket: String, key: String, uploadId: String) { wrap {
        client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).deadline(timeoutMs).build()) } }
    override fun listMultipartUploads(bucket: String, prefix: String?, keyMarker: String?, uploadIdMarker: String?, maxUploads: Int): MultipartUploads = wrap {
        val r = client.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(bucket).maxUploads(maxUploads).deadline(timeoutMs)
            .apply { prefix?.let { prefix(it) }; keyMarker?.let { keyMarker(it) }; uploadIdMarker?.let { uploadIdMarker(it) } }.build())
        MultipartUploads(r.uploads().map { MultipartUpload(it.key(), it.uploadId(), it.initiated()) }, r.nextKeyMarker(), r.nextUploadIdMarker(), r.isTruncated == true)
    }
    override fun listParts(bucket: String, key: String, uploadId: String, partNumberMarker: Int?, maxParts: Int): MultipartParts = wrap {
        val r = client.listParts(ListPartsRequest.builder().bucket(bucket).key(key).uploadId(uploadId).maxParts(maxParts).deadline(timeoutMs)
            .apply { partNumberMarker?.let { partNumberMarker(it) } }.build())
        MultipartParts(r.parts().map { MultipartPart(it.partNumber(), it.eTag()?.trim('"'), it.size(), it.lastModified()) }, r.nextPartNumberMarker(), r.isTruncated == true)
    }

    // Three classes, kept apart on purpose (HEL-452): a NoSuchKey is an ANSWER (null on the
    // nullable paths); any other 4xx is an ANSWER too and is relayed as UpstreamError with the
    // upstream's own status + S3 error code; a 5xx, a timeout or a transport failure is an OUTAGE
    // (UpstreamUnavailable) — the only class the cache is allowed to paper over.
    private fun <T> wrap(block: () -> T): T = try { block() } catch (e: NoSuchKeyException) {
        throw e
    } catch (e: S3Exception) {
        if (e.statusCode() in 500..599) throw UpstreamUnavailable("upstream ${e.statusCode()}", e)
        throw UpstreamError(e.statusCode(), e.awsErrorDetails()?.errorCode() ?: "InvalidRequest",
            e.awsErrorDetails()?.errorMessage() ?: e.message ?: "upstream ${e.statusCode()}", e)
    } catch (e: SdkClientException) {
        throw UpstreamUnavailable(e.message ?: "upstream client error", e)
    }

    // Only "the KEY is absent" is null. A 404 that says NoSuchBucket (or any other coded 4xx)
    // is an answer about something else and is relayed as itself (HEL-452 test caught this).
    private fun <T> wrapNullable(block: () -> T): T? = try { wrap(block) }
        catch (e: NoSuchKeyException) { null }
        catch (e: UpstreamError) { if (e.status == 404 && e.code in ABSENT_KEY_CODES) null else throw e }

    private companion object { val ABSENT_KEY_CODES = setOf("NoSuchKey", "NotFound", "InvalidRequest") }
}
