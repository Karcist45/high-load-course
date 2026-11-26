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
    private val testRate: Int,
    @Value("\${case.processing-time-millis}")
    private val testProcessingTime: Long,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private val WINDOW: Duration = Duration.ofSeconds(1)
    }

    private class PriorityRunnable(
        val deadline: Long,
        private val runnable: Runnable
    ) : Runnable, Comparable<PriorityRunnable> {
        override fun run() = runnable.run()
        override fun compareTo(other: PriorityRunnable): Int = this.deadline.compareTo(other.deadline)
    }

    private val activeAccounts = paymentAccounts!!.filter { it.isEnabled() }
    val accountRateLimit: Int = activeAccounts.minOf { it.rateLimitPerSec() }

    val parallelRequests: Int = activeAccounts.maxOf { it.parallelRequests() }

    val avgProcessingTimeMillis: Long = activeAccounts.map { it.averageProcessingTime().toMillis() }.average().toLong()

    val queueLimit: Long = (testProcessingTime * accountRateLimit) / Duration.ofSeconds(1).toMillis()

    val leakRate: Long = accountRateLimit.toLong()
    val leakBucketSize: Int = queueLimit.toInt()

    val tokenRate: Int = accountRateLimit
    val tokenBucketCapacity: Int = maxOf(parallelRequests, accountRateLimit)

    val retryAfterMillis: Long = Duration.ofSeconds(1).toMillis() / accountRateLimit

    val threads: Int = maxOf(parallelRequests, accountRateLimit)

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

        val estimatedWaitMillis = (queue.size.toDouble() / accountRateLimit * Duration.ofSeconds(1).toMillis()).toLong() + avgProcessingTimeMillis
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
