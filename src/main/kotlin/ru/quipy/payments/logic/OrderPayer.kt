package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
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
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)


        private const val TOKEN_RATE = 14
        private const val TOKEN_BUCKET_CAPACITY = 48

        private const val LEAK_RATE = 11L
        private const val LEAK_BUCKET_SIZE = 96

        private const val QUEUE_LIMIT = 150L
        const val RETRY_AFTER_MILLIS = 150L
        const val THREADS = 14
        private val WINDOW: Duration = Duration.ofSeconds(1)
    }

    private class PriorityRunnable(
        val deadline: Long,
        private val runnable: Runnable
    ) : Runnable, Comparable<PriorityRunnable> {
        override fun run() = runnable.run()
        override fun compareTo(other: PriorityRunnable): Int = this.deadline.compareTo(other.deadline)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val rateLimiter: RateLimiter = CompositeRateLimiter(
        TokenBucketRateLimiter(
            rate = TOKEN_RATE,
            bucketMaxCapacity = TOKEN_BUCKET_CAPACITY,
            window = 1L,
            timeUnit = TimeUnit.SECONDS,
        ),
        LeakingBucketRateLimiter(
            rate = LEAK_RATE,
            window = WINDOW,
            bucketSize = LEAK_BUCKET_SIZE,
        )
    )

    private val queue = PriorityBlockingQueue<Runnable>(1000)

    private val paymentExecutor = ThreadPoolExecutor(
        THREADS,
        THREADS,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (queue.size >= QUEUE_LIMIT) {
            throw RateLimitExceededException(RETRY_AFTER_MILLIS)
        }

        if (!rateLimiter.tick()) {
            throw RateLimitExceededException(RETRY_AFTER_MILLIS)
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