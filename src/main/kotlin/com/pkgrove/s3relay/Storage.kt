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
    /** Streams (bucket,key) into `dest`; null if absent. `range` = [first,last] inclusive, or null. */
    fun get(bucket: String, key: String, dest: Path, range: LongRange?): ObjectMetadata?
    fun head(bucket: String, key: String): ObjectMetadata?
    fun delete(bucket: String, key: String)
    fun list(bucket: String, prefix: String?, delimiter: String?, continuationToken: String?, maxKeys: Int): Listing
}

/** Thrown when the upstream is unreachable (distinct from a genuine 404), so the
 *  read path can decide to keep serving cached bytes instead of failing. */
class UpstreamUnavailable(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
