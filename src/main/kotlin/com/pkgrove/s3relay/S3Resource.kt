package com.pkgrove.s3relay

import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.*
import jakarta.ws.rs.core.*
import org.jboss.logging.Logger
import java.nio.file.Files
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * The S3-compatible wire layer (HEL-421 initial scope): PutObject, GetObject,
 * HeadObject, DeleteObject, ListObjectsV2, and range GET. Path-style addressing
 * (`/{bucket}/{key...}`), which MinIO and every S3 client can speak. Auth
 * headers (SigV4) are accepted but NOT verified — S3Relay sits on the trusted
 * in-cluster network in front of MinIO; verifying signatures is a documented
 * non-goal of the first version.
 */
@ApplicationScoped
@Path("/")
class S3Resource(private val gateway: Gateway) {
    private val log = Logger.getLogger(S3Resource::class.java)
    private val httpDate = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'").withZone(ZoneOffset.UTC)

    // ── ListObjectsV2: GET /{bucket}?list-type=2 ────────────────────────────
    @GET @Path("{bucket}")
    @Produces(MediaType.APPLICATION_XML)
    fun listOrHead(@PathParam("bucket") bucket: String,
                   @QueryParam("list-type") listType: String?,
                   @QueryParam("prefix") prefix: String?,
                   @QueryParam("delimiter") delimiter: String?,
                   @QueryParam("continuation-token") token: String?,
                   @QueryParam("max-keys") @DefaultValue("1000") maxKeys: Int): Response {
        return try {
            val l = gateway.list(bucket, prefix, delimiter, token, maxKeys.coerceIn(1, 1000))
            Response.ok(Xml.listV2(bucket, prefix, delimiter, maxKeys, token, l)).build()
        } catch (e: Exception) { error(503, "ServiceUnavailable", "upstream unavailable", "/$bucket") }
    }

    // ── PutObject: PUT /{bucket}/{key} ──────────────────────────────────────
    @PUT @Path("{bucket}/{key:.+}")
    fun put(@PathParam("bucket") bucket: String, @PathParam("key") key: String,
            @HeaderParam("Content-Length") contentLength: Long?,
            @HeaderParam("Content-Type") contentType: String?,
            body: java.io.InputStream): Response =
        when (val o = gateway.put(bucket, key, body, contentLength, contentType)) {
            is PutOutcome.Stored -> Response.ok().header("ETag", "\"${o.etag}\"").build()
            is PutOutcome.NotDurable -> error(503, "ServiceUnavailable",
                "object cached locally but upstream storage failed; not durable: ${o.reason}", "/$bucket/$key")
            is PutOutcome.BadRequest -> error(400, "BadDigest", o.reason, "/$bucket/$key")
        }

    // ── GetObject (+ Range): GET /{bucket}/{key} ────────────────────────────
    @GET @Path("{bucket}/{key:.+}")
    fun get(@PathParam("bucket") bucket: String, @PathParam("key") key: String,
            @HeaderParam("Range") rangeHeader: String?): Response {
        val range = parseRange(rangeHeader)
        return when (val o = gateway.get(bucket, key, range)) {
            is GetOutcome.Ok -> {
                val r = o.result; val m = r.metadata
                val stream = StreamingOutput { out -> Files.newInputStream(r.path).use { it.copyTo(out) } }
                val b = (if (o.partial) Response.status(206) else Response.ok()).entity(stream)
                    .header("Content-Length", m.contentLength)
                    .header("Accept-Ranges", "bytes")
                    .header("X-S3Relay-Cache", if (r.fromCache) "HIT" else "MISS")
                m.contentType?.let { b.header("Content-Type", it) }
                m.etag?.let { b.header("ETag", "\"$it\"") }
                m.lastModified?.let { b.header("Last-Modified", httpDate.format(it)) }
                if (o.partial && range != null) b.header("Content-Range", "bytes ${range.first}-${range.first + m.contentLength - 1}/*")
                b.build()
            }
            GetOutcome.NotFound -> error(404, "NoSuchKey", "The specified key does not exist.", "/$bucket/$key")
            is GetOutcome.Unavailable -> error(503, "ServiceUnavailable", o.reason, "/$bucket/$key")
        }
    }

    // ── HeadObject: HEAD /{bucket}/{key} ────────────────────────────────────
    @HEAD @Path("{bucket}/{key:.+}")
    fun head(@PathParam("bucket") bucket: String, @PathParam("key") key: String): Response {
        val m = gateway.head(bucket, key) ?: return Response.status(404).build()
        val b = Response.ok().header("Content-Length", m.contentLength).header("Accept-Ranges", "bytes")
        m.contentType?.let { b.header("Content-Type", it) }
        m.etag?.let { b.header("ETag", "\"$it\"") }
        m.lastModified?.let { b.header("Last-Modified", httpDate.format(it)) }
        return b.build()
    }

    // ── DeleteObject: DELETE /{bucket}/{key} ────────────────────────────────
    @DELETE @Path("{bucket}/{key:.+}")
    fun delete(@PathParam("bucket") bucket: String, @PathParam("key") key: String): Response =
        if (gateway.delete(bucket, key)) Response.noContent().build()
        else error(503, "ServiceUnavailable", "upstream delete failed", "/$bucket/$key")

    private fun parseRange(h: String?): LongRange? {
        val m = Regex("bytes=(\\d+)-(\\d*)").find(h?.trim() ?: "") ?: return null
        val first = m.groupValues[1].toLong()
        val last = m.groupValues[2].takeIf { it.isNotEmpty() }?.toLong() ?: Long.MAX_VALUE
        return first..last
    }

    private fun error(status: Int, code: String, message: String, resource: String): Response =
        Response.status(status).type(MediaType.APPLICATION_XML).entity(Xml.error(code, message, resource)).build()
}

/** Minimal S3 XML — enough for standard clients to parse listings and errors. */
object Xml {
    private val iso = DateTimeFormatter.ISO_INSTANT
    private fun esc(s: String) = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    fun error(code: String, message: String, resource: String) =
        """<?xml version="1.0" encoding="UTF-8"?><Error><Code>${esc(code)}</Code><Message>${esc(message)}</Message><Resource>${esc(resource)}</Resource></Error>"""

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
