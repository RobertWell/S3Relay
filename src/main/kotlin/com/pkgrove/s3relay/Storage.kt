package com.pkgrove.s3relay

import java.nio.file.Path
import java.time.Instant

/** Object metadata that survives the round trip to any S3-compatible backend. */
data class ObjectMetadata(
    val contentLength: Long,
    val contentType: String?,
    val etag: String?,          // opaque; MinIO/S3 hex md5 for single-part
    val lastModified: Instant?,
)

/** A readable handle to an object's bytes plus its metadata. `path` is a local
 *  file the caller may stream and MUST NOT delete (the cache owns it). */
data class ObjectHandle(val path: Path, val metadata: ObjectMetadata)

data class ListedObject(val key: String, val size: Long, val etag: String?, val lastModified: Instant?)
data class Listing(val objects: List<ListedObject>, val commonPrefixes: List<String>, val nextToken: String?, val truncated: Boolean)

/**
 * The storage abstraction (HEL-421). Deliberately backend-neutral — MinIO, AWS
 * S3, R2 and Garage are all just an endpoint + credentials behind this. Blocking
 * by contract: callers run it on Quarkus worker threads (@Blocking), which is
 * simpler and more robust here than suspend fns over the AWS SDK's own threads.
 */
interface ObjectStorage {
    /** Uploads the file at `source` as (bucket,key). Returns the stored ETag. */
    fun put(bucket: String, key: String, source: Path, metadata: ObjectMetadata): String
    /** Tube mode (HEL-460): uploads `body` as (bucket,key) straight from the request stream, `length` bytes, never
     *  buffered and never retried (the stream cannot be replayed). Returns the stored ETag. */
    fun putStream(bucket: String, key: String, body: java.io.InputStream, length: Long, contentType: String?): String
    /** Streams (bucket,key) into `dest`; null if absent. `range` = [first,last] inclusive, or null. */
    fun get(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata?
    /** Opens (bucket,key) as a stream the caller drains (to disk, or straight to a client — HEL-460); null if absent.
     *  The metadata describes the BODY that will flow (the range's length for a ranged open). */
    fun open(bucket: String, key: String, range: LongRange?): ObjectStream?
    fun head(bucket: String, key: String): ObjectMetadata?
    fun delete(bucket: String, key: String)
    fun list(bucket: String, prefix: String?, delimiter: String?, continuationToken: String?, maxKeys: Int): Listing
}

/**
 * An open upstream object: its bytes as a stream plus what the upstream said about them. `close()` releases the
 * connection; `abort()` drops it without draining (a client walked away mid-transfer). `contentRange` is the
 * upstream's `Content-Range` for a ranged open; `totalLength` the whole object's size when known.
 */
class ObjectStream(val metadata: ObjectMetadata, val body: java.io.InputStream, val contentRange: String?, val totalLength: Long?) : java.io.Closeable {
    override fun close() { runCatching { body.close() } }
    fun abort() { runCatching { (body as? software.amazon.awssdk.http.Abortable)?.abort() }; close() }
}

/** Thrown when the upstream is unreachable (distinct from a genuine 404), so the
 *  read path can decide to keep serving cached bytes instead of failing. */
class UpstreamUnavailable(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * The upstream ANSWERED with a 4xx (NoSuchBucket, AccessDenied, InvalidArgument, …).
 * A relay passes that answer through as itself — same status, same S3 error code —
 * never as an outage and never as a generic 500 (HEL-452).
 */
class UpstreamError(val status: Int, val code: String, message: String, cause: Throwable? = null) : RuntimeException(message, cause)
