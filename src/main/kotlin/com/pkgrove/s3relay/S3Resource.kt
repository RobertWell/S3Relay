package com.pkgrove.s3relay

import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.*
import jakarta.ws.rs.core.*
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider
import org.jboss.logging.Logger
import org.jboss.resteasy.reactive.PathPart
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * The S3-compatible wire layer (HEL-421): PutObject, GetObject, HeadObject,
 * DeleteObject, ListObjectsV2, and range GET. Path-style addressing
 * (`/{bucket}/{key...}`), which MinIO and every S3 client can speak. Auth
 * headers (SigV4) are accepted but NOT verified — S3Relay sits on the trusted
 * in-cluster network in front of MinIO; verifying signatures is a documented
 * non-goal of the first version.
 *
 * Relay contract (HEL-452): every answer is an S3 answer. Upstream 4xx pass
 * through with the upstream's status and error code; an outage/timeout/open
 * breaker is 503 ServiceUnavailable; a local fault is 500 InternalError with a
 * request id that is also in the log — never a bare stack trace.
 */
@ApplicationScoped
@Path("/")
class S3Resource(private val gateway: Gateway) {
    private val httpDate = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'").withZone(ZoneOffset.UTC)

    // ── ListObjectsV2: GET /{bucket}?list-type=2 ────────────────────────────
    @GET @Path("{bucket}")
    @Produces(MediaType.APPLICATION_XML)
    fun list(@PathParam("bucket") bucket: String,
             @QueryParam("list-type") listType: String?,
             @QueryParam("prefix") prefix: String?,
             @QueryParam("delimiter") delimiter: String?,
             @QueryParam("continuation-token") token: String?,
             @QueryParam("max-keys") @DefaultValue("1000") maxKeys: Int): Response =
        when (val o = gateway.list(bucket, prefix, delimiter, token, maxKeys.coerceIn(1, 1000))) {
            is ListOutcome.Ok -> Response.ok(Xml.listV2(bucket, prefix, delimiter, maxKeys, token, o.listing)).build()
            is ListOutcome.UpstreamError -> S3Errors.relay(o.status, o.code, o.message, "/$bucket")
            is ListOutcome.Unavailable -> S3Errors.unavailable(o.reason, "/$bucket")
        }

    // ── PutObject: PUT /{bucket}/{key} ──────────────────────────────────────
    @PUT @Path("{bucket}/{key:.+}")
    fun put(@PathParam("bucket") bucket: String, @PathParam("key") key: String,
            @HeaderParam("Content-Length") contentLength: Long?,
            @HeaderParam("Content-Type") contentType: String?,
            body: java.io.InputStream): Response =
        when (val o = gateway.put(bucket, key, body, contentLength, contentType)) {
            is PutOutcome.Stored -> Response.ok().header("ETag", "\"${o.etag}\"").build()
            is PutOutcome.NotDurable -> S3Errors.unavailable("object cached locally but upstream storage failed; not durable: ${o.reason}", "/$bucket/$key")
            is PutOutcome.BadRequest -> S3Errors.error(400, "BadDigest", o.reason, "/$bucket/$key")
            is PutOutcome.Rejected -> S3Errors.relay(o.status, o.code, o.message, "/$bucket/$key")
            is PutOutcome.InsufficientStorage -> S3Errors.error(507, "InsufficientStorage", o.reason, "/$bucket/$key")
        }

    // ── GetObject (+ Range): GET /{bucket}/{key} ────────────────────────────
    @GET @Path("{bucket}/{key:.+}")
    fun get(@PathParam("bucket") bucket: String, @PathParam("key") key: String,
            @HeaderParam("Range") rangeHeader: String?): Response {
        val range = parseRange(rangeHeader)
        return when (val o = gateway.get(bucket, key, range)) {
            is GetOutcome.Ok -> {
                val r = o.result; val m = r.metadata
                val offset = r.range?.first ?: 0L
                val count = r.range?.let { it.last - it.first + 1 } ?: r.totalLength
                val b = (if (o.partial) Response.status(206) else Response.ok())
                    .entity(PathPart(r.path, offset, count))
                    .header("Content-Length", count)
                    .header("Accept-Ranges", "bytes")
                    .header("X-S3Relay-Cache", if (r.fromCache) "HIT" else "MISS")
                m.contentType?.let { b.header("Content-Type", it) }
                m.etag?.let { b.header("ETag", "\"$it\"") }
                m.lastModified?.let { b.header("Last-Modified", httpDate.format(it)) }
                r.range?.let { b.header("Content-Range", "bytes ${it.first}-${it.last}/${r.totalLength}") }
                b.build()
            }
            GetOutcome.NotFound -> S3Errors.error(404, "NoSuchKey", "The specified key does not exist.", "/$bucket/$key")
            is GetOutcome.RangeNotSatisfiable -> S3Errors.error(416, "InvalidRange", "The requested range is not satisfiable", "/$bucket/$key")
                .let { Response.fromResponse(it).header("Content-Range", "bytes */${o.totalLength}").build() }
            is GetOutcome.UpstreamError -> S3Errors.relay(o.status, o.code, o.message, "/$bucket/$key")
            is GetOutcome.Unavailable -> S3Errors.unavailable(o.reason, "/$bucket/$key")
        }
    }

    /*
     * Data path (HEL-452 bench, 2026-09-07): the body is a PathPart, which Quarkus REST hands
     * to Vert.x `sendFile(path, offset, count)` — a Netty file region, i.e. kernel sendfile on
     * the connection's event loop with the socket's own flow control. Nothing is copied into
     * the JVM: no heap buffer, no Netty direct memory, no worker thread per download. Both
     * earlier shapes broke under 16 parallel 70 MiB downloads: FileChannel.transferTo into the
     * JAX-RS OutputStream queued 8 MiB slabs per connection, and 256 KiB chunked writes still
     * outran the socket because a worker thread's writes are queued as event-loop tasks that
     * the response's writeQueueFull() cannot see — direct memory filled to whatever cap it had.
     */

    // ── HeadObject: HEAD /{bucket}/{key} ────────────────────────────────────
    @HEAD @Path("{bucket}/{key:.+}")
    fun head(@PathParam("bucket") bucket: String, @PathParam("key") key: String): Response =
        when (val o = gateway.head(bucket, key)) {
            is HeadOutcome.Ok -> {
                val m = o.metadata
                val b = Response.ok().header("Content-Length", m.contentLength).header("Accept-Ranges", "bytes")
                    .header("X-S3Relay-Cache", if (o.fromCache) "HIT" else "MISS")
                m.contentType?.let { b.header("Content-Type", it) }
                m.etag?.let { b.header("ETag", "\"$it\"") }
                m.lastModified?.let { b.header("Last-Modified", httpDate.format(it)) }
                b.build()
            }
            HeadOutcome.NotFound -> Response.status(404).build()
            is HeadOutcome.UpstreamError -> Response.status(o.status).header("x-amz-error-code", o.code).build()   // HEAD has no body
            is HeadOutcome.Unavailable -> Response.status(503).header("Retry-After", "5").build()
        }

    // ── DeleteObject: DELETE /{bucket}/{key} ────────────────────────────────
    @DELETE @Path("{bucket}/{key:.+}")
    fun delete(@PathParam("bucket") bucket: String, @PathParam("key") key: String): Response =
        when (val o = gateway.delete(bucket, key)) {
            DeleteOutcome.Ok -> Response.noContent().build()
            is DeleteOutcome.UpstreamError -> S3Errors.relay(o.status, o.code, o.message, "/$bucket/$key")
            is DeleteOutcome.Unavailable -> S3Errors.unavailable("upstream delete failed: ${o.reason}", "/$bucket/$key")
        }

    private fun parseRange(h: String?): LongRange? {
        val m = Regex("^bytes=(\\d+)-(\\d*)$").find(h?.trim() ?: "") ?: return null
        val first = m.groupValues[1].toLongOrNull() ?: return null
        val last = m.groupValues[2].takeIf { it.isNotEmpty() }?.toLongOrNull() ?: Long.MAX_VALUE
        return if (last < first) null else first..last
    }
}

/** S3-style error responses with a request id that also lands in the log. */
object S3Errors {
    private val log = Logger.getLogger(S3Errors::class.java)

    fun error(status: Int, code: String, message: String, resource: String): Response {
        val rid = UUID.randomUUID().toString()
        if (status >= 500) log.warnf("rid=%s %d %s %s: %s", rid, status, code, resource, message)
        return Response.status(status).type(MediaType.APPLICATION_XML).header("x-amz-request-id", rid)
            .header("x-s3relay-request-id", rid).entity(Xml.error(code, message, resource, rid)).build()
    }

    /** An upstream 4xx, passed through as itself. */
    fun relay(status: Int, code: String, message: String, resource: String): Response = error(status, code, message, resource)

    fun unavailable(message: String, resource: String): Response =
        Response.fromResponse(error(503, "ServiceUnavailable", message, resource)).header("Retry-After", "5").build()
}

/**
 * Last line of defence (HEL-452): anything the outcome types did not model —
 * a disk that filled up while streaming a body, a client that hung up mid-upload,
 * a timeout or open breaker that escaped a path, a routing/parsing error — still
 * leaves as an S3 XML error with a request id, and the id is in the log next to
 * the stack trace. Never a bare 500 page, never a stack trace on the wire.
 */
@Provider
class RelayExceptionMapper : ExceptionMapper<Throwable> {
    private val log = Logger.getLogger(RelayExceptionMapper::class.java)
    override fun toResponse(e: Throwable): Response {
        val rid = UUID.randomUUID().toString()
        val (status, code, msg) = when (e) {
            is UpstreamError -> Triple(e.status, e.code, e.message ?: e.code)
            is UpstreamUnavailable, is org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException,
            is org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException ->
                Triple(503, "ServiceUnavailable", "upstream unavailable: ${e.message ?: e.javaClass.simpleName}")
            is WebApplicationException -> {
                val st = e.response?.status ?: 500
                Triple(st, when (st) { 404 -> "NoSuchKey"; 405 -> "MethodNotAllowed"; 400 -> "InvalidRequest"; 413 -> "EntityTooLarge"; else -> "InternalError" }, e.message ?: "request failed")
            }
            is java.io.IOException -> Triple(500, "InternalError", "local storage error (rid $rid)")
            else -> Triple(500, "InternalError", "internal error (rid $rid)")
        }
        if (status >= 500) log.errorf(e, "rid=%s %d %s: %s", rid, status, code, e.message) else log.warnf("rid=%s %d %s: %s", rid, status, code, e.message)
        return Response.status(status).type(MediaType.APPLICATION_XML).header("x-amz-request-id", rid).header("x-s3relay-request-id", rid)
            .apply { if (status == 503) header("Retry-After", "5") }
            .entity(Xml.error(code, msg, "", rid)).build()
    }
}

/** Minimal S3 XML — enough for standard clients to parse listings and errors. */
object Xml {
    private val iso = DateTimeFormatter.ISO_INSTANT
    private fun esc(s: String) = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    fun error(code: String, message: String, resource: String, requestId: String = "") =
        """<?xml version="1.0" encoding="UTF-8"?><Error><Code>${esc(code)}</Code><Message>${esc(message)}</Message><Resource>${esc(resource)}</Resource><RequestId>${esc(requestId)}</RequestId></Error>"""

    fun listV2(bucket: String, prefix: String?, delimiter: String?, maxKeys: Int, token: String?, l: Listing): String = buildString {
        append("""<?xml version="1.0" encoding="UTF-8"?>""")
        append("""<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">""")
        append("<Name>${esc(bucket)}</Name>")
        append("<Prefix>${esc(prefix ?: "")}</Prefix>")
        token?.let { append("<ContinuationToken>${esc(it)}</ContinuationToken>") }
        l.nextToken?.let { append("<NextContinuationToken>${esc(it)}</NextContinuationToken>") }
        append("<KeyCount>${l.objects.size + l.commonPrefixes.size}</KeyCount>")
        append("<MaxKeys>$maxKeys</MaxKeys>")
        delimiter?.let { append("<Delimiter>${esc(it)}</Delimiter>") }
        append("<IsTruncated>${l.truncated}</IsTruncated>")
        for (o in l.objects) {
            append("<Contents>")
            append("<Key>${esc(o.key)}</Key>")
            append("<LastModified>${iso.format(o.lastModified ?: Instant.EPOCH)}</LastModified>")
            o.etag?.let { append("<ETag>&quot;${esc(it)}&quot;</ETag>") }
            append("<Size>${o.size}</Size>")
            append("<StorageClass>STANDARD</StorageClass>")
            append("</Contents>")
        }
        for (p in l.commonPrefixes) append("<CommonPrefixes><Prefix>${esc(p)}</Prefix></CommonPrefixes>")
        append("</ListBucketResult>")
    }
}
