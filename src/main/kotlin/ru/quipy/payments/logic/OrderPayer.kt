package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.exception.RateLimitExceededException
import java.time.Duration
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    @Autowired private val paymentService: PaymentService,
    @Autowired private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    @Autowired(required = false) private val paymentAccounts: List<PaymentExternalSystemAdapter>?,
    @Value("\${case.rps}")
    private val configuredTestRate: Int = DEFAULT_TEST_RATE_PER_SECOND,
    @Value("\${case.processing-time-millis}")
    private val configuredTestProcessingTime: Long = DEFAULT_TEST_PROCESSING_TIME_MILLIS,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        const val DEFAULT_TEST_RATE_PER_SECOND = 15
        const val DEFAULT_TEST_PROCESSING_TIME_MILLIS = 2500L

        private val WINDOW: Duration = Duration.ofSeconds(1)
    }

    private class PriorityRunnable(
        val deadline: Long,
        private val runnable: Runnable
    ) : Runnable, Comparable<PriorityRunnable> {
        override fun run() = runnable.run()
        override fun compareTo(other: PriorityRunnable): Int = this.deadline.compareTo(other.deadline)
    }

    private val activeAccounts = paymentAccounts?.filter { it.isEnabled() }?.ifEmpty { paymentAccounts } ?: emptyList()
    val accountRateLimit: Int = if (activeAccounts.isNotEmpty()) {
        activeAccounts.minOf { it.rateLimitPerSec() }
    } else {
        configuredTestRate
    }
    val parallelRequests: Int = if (activeAccounts.isNotEmpty()) {
        activeAccounts.maxOf { it.parallelRequests() }
    } else {
        20
    }
    val avgProcessingTimeMillis: Long = if (activeAccounts.isNotEmpty()) {
        activeAccounts.map { it.averageProcessingTime().toMillis() }.average().toLong()
    } else {
        1800L
    }

    val testRate: Int = if (configuredTestRate > 0) configuredTestRate else accountRateLimit
    val testProcessingTime: Long = if (configuredTestProcessingTime > 0) configuredTestProcessingTime else DEFAULT_TEST_PROCESSING_TIME_MILLIS

    val leakRate: Long = accountRateLimit.toLong()

    val tokenRate: Int = (accountRateLimit * 1.28).toInt()

    val queueLimit: Long = ((testProcessingTime / 1000.0) * accountRateLimit).toLong()

    val leakBucketSize: Int = (queueLimit * 2 / 3).toInt()

    val tokenBucketCapacity: Int = leakBucketSize / 2

    val retryAfterMillis: Long =  (1650L / accountRateLimit)

    val threads: Int =  (accountRateLimit * 1.28).toInt()

    init {
        logger.info(
            "OrderPayer initialized with calculated parameters: " +
            "accountRateLimit=$accountRateLimit, parallelRequests=$parallelRequests, avgProcessingTimeMillis=$avgProcessingTimeMillis, " +
            "testRate=$testRate, testProcessingTime=$testProcessingTime | " +
            "tokenRate=$tokenRate, tokenCapacity=$tokenBucketCapacity, leakRate=$leakRate, leakBucketSize=$leakBucketSize, " +
            "queueLimit=$queueLimit, threads=$threads, retryAfterMillis=$retryAfterMillis"
        )
    }

    private val rateLimiter: RateLimiter = CompositeRateLimiter(
        TokenBucketRateLimiter(
            rate = tokenRate,
            bucketMaxCapacity = tokenBucketCapacity,
            window = 1L,
            timeUnit = TimeUnit.SECONDS,
        ),
        LeakingBucketRateLimiter(
            rate = leakRate,
            window = WINDOW,
            bucketSize = leakBucketSize,
        )
    )

    private val queue = PriorityBlockingQueue<Runnable>(1000)

    private val paymentExecutor = ThreadPoolExecutor(
        threads,
        threads,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val remainingMillis = deadline - createdAt

        if (remainingMillis <= avgProcessingTimeMillis) {
            throw RateLimitExceededException(retryAfterMillis)
        }

        val estimatedWaitMillis = (queue.size.toDouble() / accountRateLimit * 1000.0).toLong() + avgProcessingTimeMillis
        if (estimatedWaitMillis > remainingMillis) {
            throw RateLimitExceededException(retryAfterMillis)
        }

        if (queue.size >= queueLimit) {
            throw RateLimitExceededException(retryAfterMillis)
        }

        if (!rateLimiter.tick()) {
            throw RateLimitExceededException(retryAfterMillis)
        }

        paymentExecutor.execute(PriorityRunnable(deadline) {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        })

        return createdAt
    }
}
