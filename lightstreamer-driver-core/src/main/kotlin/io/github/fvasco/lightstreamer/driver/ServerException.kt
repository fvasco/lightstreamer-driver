package io.github.fvasco.lightstreamer.driver

import java.io.IOException

/**
 * Server error
 */
class ServerException(val code: Int, message: String, cause: Throwable? = null) : IOException("$code: $message", cause)
