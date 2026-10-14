package ru.quipy.payments.exception

class RateLimitExceededException(val retryAfterSeconds: Long) : RuntimeException()