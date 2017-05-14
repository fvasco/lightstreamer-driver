package io.github.fvasco.lightstreamer.driver.helper

import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.produce

fun clientMessageBlcpEncoder(messageChannel: ReceiveChannel<ClientMessage>) = produce<String>(Unconfined) {
    for (message in messageChannel) {
        with(message) {
            val blcp = buildString {
                append(name)
                append("\r\n")
                var firstParameter = true
                for ((k, v) in parameters) {
                    if (v != null) {
                        if (firstParameter) {
                            firstParameter = false
                        } else {
                            append('&')
                        }
                        append(k)
                        append('=')
                        append(v.urlEncode())
                    }
                }
            }
            send(blcp)
        }
    }
}