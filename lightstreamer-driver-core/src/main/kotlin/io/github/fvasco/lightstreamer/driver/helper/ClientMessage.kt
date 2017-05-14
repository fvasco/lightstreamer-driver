package io.github.fvasco.lightstreamer.driver.helper

import io.github.fvasco.lightstreamer.driver.Subscription
import io.github.fvasco.lightstreamer.driver.UsernamePassword
import java.util.concurrent.atomic.AtomicInteger

/**
 * Message sent from connector to Lightstreamer server
 */
sealed class ClientMessage(val name: String) {

    val id = idGenerator.incrementAndGet()
    abstract val parameters: Map<String, String?>

    data class CreateSession(val adapterSetName: String, val userCredential: UsernamePassword? = null) : ClientMessage("create_session") {
        override val parameters by lazy {
            mapOf(
                    "LS_adapter_set" to adapterSetName,
                    "LS_cid" to "pcYgxptg4pkpW38AK1x-onG39Do",
                    "LS_report_info" to "true",
                    "LS_user" to userCredential?.username,
                    "LS_password" to userCredential?.password
            )
        }
    }

    data class Subscribe(val subscription: Subscription) : ClientMessage("control") {
        override val parameters by lazy {
            mapOf(
                    "LS_reqId" to id.toString(),
                    "LS_subId" to subscription.id.toString(),
                    "LS_op" to "add",
                    "LS_data_adapter" to subscription.dataAdapterName,
                    "LS_mode" to subscription.mode.name,
                    "LS_group" to subscription.name,
                    "LS_schema" to subscription.itemNames.joinToString(" "),
                    "LS_snapshot" to subscription.requestSnapshot.toString()
            )
        }
    }

    data class Unsubscribe(val subscription: Subscription) : ClientMessage("control") {
        override val parameters by lazy {
            mapOf(
                    "LS_reqId" to id.toString(),
                    "LS_subId" to subscription.id.toString(),
                    "LS_op" to "delete"
            )
        }
    }

    private companion object {
        private val idGenerator = AtomicInteger()
    }
}
