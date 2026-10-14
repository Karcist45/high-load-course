package ru.quipy.apigateway

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*


@RestController
class APIController(
    meterRegistry: MeterRegistry,
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    private val incomingRequestsCounter: Counter = meterRegistry.counter("requests.incoming")
    private val processedRequestsCounter: Counter = meterRegistry.counter("requests.processed")

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): PaymentSubmissionDto {
        incomingRequestsCounter.increment()
        val paymentId = UUID.randomUUID()
        val createdAt:Long
        orderRepository.findById(orderId)?.also {
            createdAt = orderPayer.processPayment(orderId, it.price, paymentId, deadline)
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
        } ?: throw IllegalArgumentException("No such order $orderId")

        return PaymentSubmissionDto(createdAt, paymentId).also {
            processedRequestsCounter.increment()  // todo
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}