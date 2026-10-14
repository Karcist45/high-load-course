package ru.quipy.payments.exception

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import java.time.Instant

@RestControllerAdvice
class RateLimitExceptionHandler (
    meterRegistry: MeterRegistry,
){
    private val returnedTooManyRequests = meterRegistry.counter("requests.too_many")

    @ExceptionHandler(RateLimitExceededException::class)
    fun handleTooManyRequests(ex: RateLimitExceededException): ResponseEntity<String> {
        returnedTooManyRequests.increment()
        return ResponseEntity
            .status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER,Instant.now().plusSeconds(ex.retryAfterSeconds).toEpochMilli().toString())
            .build()
    }
}