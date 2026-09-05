package com.pkgrove.s3relay

import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.eclipse.microprofile.health.Readiness
import org.jboss.logging.Logger

/** Periodic cleanup + upstream reconciliation (HEL-421 cache management). */
@ApplicationScoped
class Maintenance(private val gateway: Gateway) {
    private val log = Logger.getLogger(Maintenance::class.java)

    @Scheduled(every = "30s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun evict() { runCatching { val freed = gateway.cache.evictToLowWatermark(); if (freed > 0) log.infof("evicted %d bytes", freed) } }

    // Retry objects the upstream rejected, so a transient MinIO outage during a
    // PUT is healed without the client re-uploading.
    @Scheduled(every = "20s", delayed = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun reconcile() { runCatching { val n = gateway.reconcilePending(); if (n > 0) log.infof("reconciled %d pending object(s) to upstream", n) } }
}

/** Ready as soon as the cache dir is writable; liveness never depends on the
 *  upstream, so a MinIO outage does not kill the pod (cached reads keep working). */
@Liveness @ApplicationScoped
class LiveCheck : HealthCheck {
    override fun call(): HealthCheckResponse = HealthCheckResponse.up("s3relay-live")
}
@Readiness @ApplicationScoped
class ReadyCheck(private val gateway: Gateway) : HealthCheck {
    override fun call(): HealthCheckResponse =
        if (gateway.cache.maxBytes > 0) HealthCheckResponse.up("s3relay-ready")
        else HealthCheckResponse.down("s3relay-ready")
}
