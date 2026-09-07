package com.pkgrove.s3relay

import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.eclipse.microprofile.health.Readiness
import org.jboss.logging.Logger

/** Periodic cleanup + upstream reconciliation (HEL-421 cache management).
 *  Nothing here fails silently (HEL-452): a failing sweep is an ERROR line. */
@ApplicationScoped
class Maintenance(private val gateway: Gateway) {
    private val log = Logger.getLogger(Maintenance::class.java)

    @Scheduled(every = "30s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun evict() {
        runCatching { val freed = gateway.cache.evictToLowWatermark(); if (freed > 0) log.infof("evicted %d bytes", freed) }
            .onFailure { log.error("eviction sweep failed", it) }
    }

    // Retry objects the upstream rejected, so a transient MinIO outage during a
    // PUT is healed without the client re-uploading.
    @Scheduled(every = "20s", delayed = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun reconcile() {
        runCatching {
            val r = gateway.reconcilePending()
            if (r.total > 0) log.infof("reconcile: synced=%d adopted=%d dropped=%d still-failing=%d", r.synced, r.adopted, r.dropped, r.failed)
        }.onFailure { log.error("reconcile sweep failed", it) }
    }
}

/** Ready as soon as the cache dir is writable; liveness never depends on the
 *  upstream, so a MinIO outage does not kill the pod (cached reads keep working). */
@Liveness @ApplicationScoped
class LiveCheck : HealthCheck {
    override fun call(): HealthCheckResponse = HealthCheckResponse.up("s3relay-live")
}
@Readiness @ApplicationScoped
class ReadyCheck(private val gateway: Gateway) : HealthCheck {
    override fun call(): HealthCheckResponse {
        val c = gateway.cache
        val b = HealthCheckResponse.named("s3relay-ready")
            .withData("cache_used_bytes", c.usedBytes()).withData("cache_pinned_bytes", c.pinnedBytes())
            .withData("cache_max_bytes", c.maxBytes)
        return if (c.maxBytes > 0) b.up().build() else b.down().build()
    }
}
