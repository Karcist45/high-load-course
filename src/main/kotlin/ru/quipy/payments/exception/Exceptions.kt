package ru.quipy.payments.exception

class RateLimitExceededException(val retryAfterMillis: Long) : RuntimeException()