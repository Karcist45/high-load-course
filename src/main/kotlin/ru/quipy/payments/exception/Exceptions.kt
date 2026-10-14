package ru.quipy.payments.exception

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

class RateLimitExceededException(val retryAfterSeconds: Long) : RuntimeException()