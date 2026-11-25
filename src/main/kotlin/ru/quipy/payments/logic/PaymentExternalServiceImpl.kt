package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

    }

    private val semaphore = Semaphore(properties.parallelRequests)
    private val outgoingRequests = meterRegistry.counter("requests.outgoing")
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val retriesCounter = meterRegistry.counter("payment.retries", "service", serviceName, "accountName", accountName)

    private val deadlineDroppedInitial = meterRegistry.counter("payment.deadline.dropped", "service", serviceName, "accountName", accountName, "stage", "initial")
    private val deadlineDroppedRetry = meterRegistry.counter("payment.deadline.dropped", "service", serviceName, "accountName", accountName, "stage", "retry")

    private val retriesRecovered = meterRegistry.counter("payment.retries.outcome", "service", serviceName, "accountName", accountName, "outcome", "success")
    private val retriesExhausted = meterRegistry.counter("payment.retries.outcome", "service", serviceName, "accountName", accountName, "outcome", "failed")

    private val minProcessingTime = requestAverageProcessingTime.toMillis() / 2

    private val networkMarginMillis = requestAverageProcessingTime.toMillis() / 6

    private val perCallTimeout = requestAverageProcessingTime.toMillis() + (requestAverageProcessingTime.toMillis() / 2)

    private val networkTimeout = Duration.ofMillis(minOf(requestAverageProcessingTime.toMillis(), Duration.ofSeconds(1).toMillis()))
    private val client = OkHttpClient.Builder()
        .connectTimeout(networkTimeout)
        .readTimeout(Duration.ofMillis(perCallTimeout))
        .writeTimeout(networkTimeout)
        .callTimeout(Duration.ofMillis(perCallTimeout + networkMarginMillis))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (now() + minProcessingTime > deadline) {
            logger.warn("[$accountName] Discarding payment $paymentId before first attempt: remaining time (${deadline - now()} ms) < required ($minProcessingTime ms)")
            deadlineDroppedInitial.increment()
            val transactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(success = false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Discarded: deadline will be exceeded")
            }
            return
        }

        doPerformPayment(paymentId, amount, paymentStartedAt, deadline)
    }

    private fun doPerformPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var needRetry = true
        var retryCount = 0

        while (needRetry) {
            needRetry = false

            if (minProcessingTime > deadline) {
                logger.warn("[$accountName] Discarding payment $paymentId before attempt ${retryCount + 1}: remaining time (${deadline - now()} ms) < required ($minProcessingTime ms)")
                if (retryCount == 0) deadlineDroppedInitial.increment() else deadlineDroppedRetry.increment()
                retriesExhausted.increment()
                return
            }

            val transactionId = UUID.randomUUID()
            var callSucceeded = false

            semaphore.acquire()
            try {
                rateLimiter.tickBlocking()

                if ( now() + minProcessingTime > deadline) {
                    logger.warn("[$accountName] Discarding payment $paymentId after rate limiter: remaining time (${deadline - now()} ms) < required ($minProcessingTime ms)")
                    if (retryCount == 0) deadlineDroppedInitial.increment() else deadlineDroppedRetry.increment()
                    retriesExhausted.increment()
                    return
                }

                outgoingRequests.increment()

                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                logger.info("[$accountName] Submit: $paymentId , txId: $transactionId (attempt ${retryCount + 1})")

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    callSucceeded = body.result
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException, is InterruptedIOException -> {
                        logger.warn("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId: ${e.message}")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout: ${e.message}")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                semaphore.release()
            }

            if (callSucceeded) {
                if (retryCount > 0) {
                    retriesRecovered.increment()
                }
            } else {
                retryCount++

                val backoffDelay = minOf( retryCount * Duration.ofSeconds(1).toMillis() / rateLimitPerSec, requestAverageProcessingTime.toMillis() / 3)

                val totalTimeNeeded = backoffDelay + minProcessingTime

                val maxRetries = (deadline - paymentStartedAt) / requestAverageProcessingTime.toMillis()
                if (retryCount >= maxRetries) {
                    logger.warn("[$accountName] Exhausted retries ($maxRetries) for payment $paymentId")
                    retriesExhausted.increment()
                    needRetry = false
                } else if (now() + totalTimeNeeded > deadline) {
                    logger.warn("[$accountName] Discarding retry for payment $paymentId: remaining time (${deadline - now()} ms) < required ($totalTimeNeeded ms)")
                    deadlineDroppedRetry.increment()
                    retriesExhausted.increment()
                    needRetry = false
                } else {
                    logger.info("[$accountName] Backing off for ${backoffDelay}ms before retry #$retryCount for payment $paymentId")
                    try {
                        Thread.sleep(backoffDelay)
                    } catch (ie: InterruptedException) {
                        Thread.currentThread().interrupt()
                        return
                    }

                    if (now() + minProcessingTime > deadline) {
                        logger.warn("[$accountName] Discarding retry for payment $paymentId after backoff: deadline will be exceeded")
                        deadlineDroppedRetry.increment()
                        retriesExhausted.increment()
                        needRetry = false
                    } else {
                        retriesCounter.increment()
                        needRetry = true
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun rateLimitPerSec() = properties.rateLimitPerSec

    override fun parallelRequests() = properties.parallelRequests

    override fun averageProcessingTime(): Duration = properties.averageProcessingTime

}

public fun now() = System.currentTimeMillis()
