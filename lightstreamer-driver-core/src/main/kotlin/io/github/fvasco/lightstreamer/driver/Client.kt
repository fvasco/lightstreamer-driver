package io.github.fvasco.lightstreamer.driver;

import io.github.fvasco.lightstreamer.driver.helper.ClientMessage
import io.github.fvasco.lightstreamer.driver.helper.ServerMessage
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withMutex
import java.io.IOException
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

/**
 * Client for Lightstreamer server
 */
class Client(connectionOkMessage: ServerMessage.ConnectionOk,
             receiveChannel: ReceiveChannel<ServerMessage>,
             private val sendChannel: SendChannel<ClientMessage>,
             private val job: Job) : Job by job {

    val sessionId = connectionOkMessage.sessionId

    private val pendingRequestMap = HashMap<Int, Continuation<ServerMessage>>()
    private val requestMapMutex = Mutex()

    private val subscriptionMap = HashMap<Int, Subscription>()
    private val subscriptionMapMutex = Mutex()

    /**
     * Get a snapshot of all active subscription
     */
    val subscriptions: List<Subscription> get() = runBlocking(Unconfined) {
        subscriptionMapMutex.withMutex {
            subscriptionMap.values.toList()
        }
    }

    init {
        // manage message from LS server
        launch(Unconfined) {
            try {
                receiveChannel.consumeEach {
                    onServerMessage(it)
                }
                cancel()
            } catch (e: Exception) {
                cancel(e)
            }
        }
        job.invokeOnCompletion { throwable ->
            val error = IOException("Client closed", throwable)
            pendingRequestMap.values.forEach {
                launch(CommonPool) {
                    it.resumeWithException(error)
                }
            }
            if (!sendChannel.isClosedForSend) sendChannel.close(error)
        }
    }

    suspend fun subscribe(mode: SubscriptionMode,
                          name: String,
                          dataAdapterName: String = "DEFAULT",
                          itemNames: List<String>,
                          requestSnaphot: Boolean = false): Subscription {
        val subscription = Subscription(mode, name, dataAdapterName, itemNames, Job(job))

        subscription.invokeOnCompletion {
            launch(CommonPool) {
                val toUnsubscribe = subscriptionMapMutex.withMutex {
                    subscriptionMap.remove(subscription.id) != null
                }
                if (toUnsubscribe) {
                    sendChannel.send(ClientMessage.Unsubscribe(subscription))
                }
            }
        }

        val subscribe = ClientMessage.Subscribe(subscription, requestSnaphot)
        // send subscribe
        sendRequest(subscribe)

        // register subscription
        subscriptionMapMutex.withMutex {
            subscriptionMap[subscription.id] = subscription
        }
        return subscription
    }

    private suspend fun sendRequest(clientMessage: ClientMessage): ServerMessage =
            suspendCoroutine<ServerMessage> { cont ->
                launch(Unconfined) {
                    subscriptionMapMutex.withMutex {
                        pendingRequestMap[clientMessage.id] = cont
                    }
                    sendChannel.send(clientMessage)
                }
            }

    private suspend fun onServerMessage(message: ServerMessage) {
        val subscriptionId =
                when (message) {
                    is ServerMessage.SubscriptionOk -> message.subscriptionId
                    is ServerMessage.Update -> message.subscriptionId
                    is ServerMessage.ClearSnapshot -> message.subscriptionId
                    else -> null
                }

        if (subscriptionId == null) {
            when (message) {
                is ServerMessage.RequestOk -> onRequestOk(message)
                is ServerMessage.RequestError -> onRequestError(message)
                is ServerMessage.Error -> onError(message)
            }
        } else {
            subscriptionMapMutex.withMutex {
                subscriptionMap[subscriptionId]?.serverMessageChannel?.send(message)
            }
        }
    }

    private suspend fun onRequestOk(message: ServerMessage.RequestOk) {
        val continuation = requireNotNull(requestMapMutex.withMutex { pendingRequestMap.remove(message.requestId) }) { "Unexpected message: $message" }
        continuation.resume(message)
    }

    private suspend fun onRequestError(message: ServerMessage.RequestError) {
        val continuation = requireNotNull(requestMapMutex.withMutex { pendingRequestMap.remove(message.requestId) }) { "Unexpected message: $message" }
        val error = ServerException(message.code, message.message)
        continuation.resumeWithException(error)
    }

    private suspend fun onError(message: ServerMessage.Error) {
        val error = ServerException(message.code, message.message)
        if (!sendChannel.isClosedForSend) sendChannel.close(error)
        job.cancel(error)
    }
}
