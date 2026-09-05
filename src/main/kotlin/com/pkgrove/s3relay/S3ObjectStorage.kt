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
) : ObjectStorage {

    private val client: S3Client by lazy {
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.of(region))
            .forcePathStyle(pathStyle)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .build()
    }

    override fun put(bucket: String, key: String, source: Path, metadata: ObjectMetadata): String = wrap {
        val req = PutObjectRequest.builder().bucket(bucket).key(key)
            .apply { metadata.contentType?.let { contentType(it) } }
            .contentLength(metadata.contentLength).build()
        client.putObject(req, RequestBody.fromFile(source)).eTag()?.trim('"') ?: ""
    }

    override fun get(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata? = wrapNullable {
        val req = GetObjectRequest.builder().bucket(bucket).key(key)
            .apply { range?.let { range("bytes=${it.first}-${it.last}") } }.build()
        // AWS SDK's toFile transformer refuses to overwrite; the cache handed us
        // a freshly-created temp file, so clear it first.
        java.nio.file.Files.deleteIfExists(dest)
        val resp = client.getObject(req, dest)
        ObjectMetadata(resp.contentLength(), resp.contentType(), resp.eTag()?.trim('"'),
            resp.lastModified())
    }

    override fun head(bucket: String, key: String): ObjectMetadata? = wrapNullable {
        val r = client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
        ObjectMetadata(r.contentLength(), r.contentType(), r.eTag()?.trim('"'), r.lastModified())
    }

    override fun delete(bucket: String, key: String) { wrap { client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build()) } }

    override fun list(bucket: String, prefix: String?, delimiter: String?, continuationToken: String?, maxKeys: Int): Listing = wrap {
        val r = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket)
            .apply { prefix?.let { prefix(it) }; delimiter?.let { delimiter(it) }; continuationToken?.let { continuationToken(it) } }
            .maxKeys(maxKeys).build())
        Listing(
            r.contents().map { ListedObject(it.key(), it.size(), it.eTag()?.trim('"'), it.lastModified()) },
            r.commonPrefixes().mapNotNull { it.prefix() },
            r.nextContinuationToken(), r.isTruncated == true)
    }

    // A NoSuchKey / NoSuchBucket is a real absence (null); anything else that is a
    // transport failure is an outage. A 4xx from S3 is authoritative, not an outage.
    private fun <T> wrap(block: () -> T): T = try { block() } catch (e: NoSuchKeyException) {
        throw e
    } catch (e: S3Exception) {
        if (e.statusCode() in 500..599) throw UpstreamUnavailable("upstream ${e.statusCode()}", e) else throw e
    } catch (e: SdkClientException) {
        throw UpstreamUnavailable(e.message ?: "upstream client error", e)
    }

    private fun <T> wrapNullable(block: () -> T): T? = try { wrap(block) }
        catch (e: NoSuchKeyException) { null }
        catch (e: S3Exception) { if (e.statusCode() == 404) null else throw e }
}
