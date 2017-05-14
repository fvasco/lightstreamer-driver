package io.github.fvasco.lightstreamer.driver

import io.github.fvasco.lightstreamer.driver.helper.ServerMessage
import io.github.fvasco.lightstreamer.driver.helper.urlDecode
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.produce
import java.util.concurrent.atomic.AtomicInteger

/**
 * Define a subscription
 */
class Subscription internal constructor(val mode: SubscriptionMode,
                                        val name: String,
                                        val dataAdapterName: String,
                                        itemNames: List<String>,
                                        val requestSnapshot: Boolean,
                                        private val job: Job) : Job by job {
    val id = idGenerator.incrementAndGet()
    val itemNames = itemNames.sortedWith(itemNameComparator)
    /**
     * Channel of received messages for current subscription
     */
    internal val serverMessageChannel = Channel<ServerMessage>()

    /**
     * Subscription update message channel.
     */
    val messageChannel: ReceiveChannel<SubscriptionMessage> = produce(CommonPool + job) {
        val messageConverter = Server2SubscriptionMessageConverter(this@Subscription)
        for (serverMessage in serverMessageChannel) {
            val subscriptionMessage = messageConverter(serverMessage)
            send(subscriptionMessage)
        }
    }

    private class Server2SubscriptionMessageConverter(private val subscription: Subscription) {
        private val values = arrayOfNulls<String>(subscription.itemNames.size)

        operator fun invoke(lightstreamerServerMessage: ServerMessage): SubscriptionMessage =
                when (lightstreamerServerMessage) {
                    is ServerMessage.SubscriptionOk -> onSubscriptionOk()
                    is ServerMessage.Update -> onUpdate(lightstreamerServerMessage)
                    is ServerMessage.ClearSnapshot -> onClearSnapshot()
                    else -> throw IllegalArgumentException("Invalid message $lightstreamerServerMessage")
                }

        private fun onSubscriptionOk(): SubscriptionMessage.SubscriptionOk {
            return SubscriptionMessage.SubscriptionOk
        }

        private fun onClearSnapshot(): SubscriptionMessage.ClearSnapshot {
            values.fill(null)
            return SubscriptionMessage.ClearSnapshot
        }

        private fun onUpdate(message: ServerMessage.Update): SubscriptionMessage.Update {
            assert(subscription.itemNames.size == message.values.size)
            var i = 0
            for (s in message.values) {
                when {
                // value unchanged
                    s.isEmpty() -> Unit
                // value null
                    s == "#" -> values[i] = null
                // value empty
                    s == "\$" -> values[i] = ""
                // skip n values
                    s.startsWith('\'') -> i += s.substring(1).toInt() - 1
                // url encoded value
                    else -> values[i] = s.urlDecode()
                }
                i++
            }
            return SubscriptionMessage.Update(subscription, values.toList())
        }
    }

    internal companion object {
        const val defaultDataAdapterName = "DEFAULT"
        private val idGenerator = AtomicInteger()
        val itemNameComparator = compareBy(String::hashCode, { it })
    }
}
