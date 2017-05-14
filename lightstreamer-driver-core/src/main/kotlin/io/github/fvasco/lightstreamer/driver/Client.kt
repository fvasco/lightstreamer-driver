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
class Client(val sessionInfo: ServerMessage.ConnectionOk,
             serverMessages: ReceiveChannel<ServerMessage>,
             private val clientMessages: SendChannel<ClientMessage>,
             private val job: Job) : Job by job {

    private val pendingRequestMap = HashMap<Int, Continuation<ServerMessage>>()
    private val pendingRequestMapMutex = Mutex()

    private val subscriptionMap = HashMap<Int, Subscription>()
    private val subscriptionMapMutex = Mutex()

    /**
     * Get a snapshot of all active subscription
     */
    val subscriptions: List<Subscription> get() = runBlocking {
        subscriptionMapMutex.withMutex {
            subscriptionMap.values.toList()
        }
    }

    init {
        // manage message from LS server
        launch(Unconfined) {
            try {
                serverMessages.consumeEach {
                    onServerMessage(it)
                }
                this@Client.cancel()
            } catch (e: Throwable) {
                this@Client.cancel(e)
            }
        }
        job.invokeOnCompletion { throwable ->
            val error = IOException("Client closed", throwable)
            pendingRequestMap.values.forEach {
                launch(CommonPool) {
                    it.resumeWithException(error)
                }
            }
            if (!clientMessages.isClosedForSend) clientMessages.close(error)
        }
    }

    fun subscribe(mode: SubscriptionMode,
                  name: String,
                  dataAdapterName: String = Subscription.defaultDataAdapterName,
                  itemNames: List<String>,
                  requestSnapshot: Boolean = true): Deferred<Subscription> = async(Unconfined) {
        val subscription = Subscription(
                mode = mode,
                name = name,
                dataAdapterName = dataAdapterName,
                itemNames = itemNames,
                requestSnapshot = requestSnapshot,
                job = Job(job))

        subscription.invokeOnCompletion {
            launch(CommonPool) {
                val toUnsubscribe = subscriptionMapMutex.withMutex {
                    subscriptionMap.remove(subscription.id) != null
                }
                if (toUnsubscribe) {
                    clientMessages.send(ClientMessage.Unsubscribe(subscription))
                }
            }
        }

        val subscribe = ClientMessage.Subscribe(subscription)
        // send subscribe
        sendRequest(subscribe).await()

        // register subscription
        subscriptionMapMutex.withMutex {
            subscriptionMap[subscription.id] = subscription
        }
        return@async subscription
    }

    fun sendRequest(clientMessage: ClientMessage): Deferred<ServerMessage> = async(Unconfined) {
        suspendCoroutine<ServerMessage> { cont ->
            launch(Unconfined) {
                subscriptionMapMutex.withMutex {
                    pendingRequestMap[clientMessage.id] = cont
                }
                clientMessages.send(clientMessage)
            }
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
        val continuation = requireNotNull(pendingRequestMapMutex.withMutex { pendingRequestMap.remove(message.requestId) }) { "Unexpected message: $message" }
        continuation.resume(message)
    }

    private suspend fun onRequestError(message: ServerMessage.RequestError) {
        val continuation = requireNotNull(pendingRequestMapMutex.withMutex { pendingRequestMap.remove(message.requestId) }) { "Unexpected message: $message" }
        val error = ServerException(message.code, message.message)
        continuation.resumeWithException(error)
    }

    private suspend fun onError(message: ServerMessage.Error) {
        val error = ServerException(message.code, message.message)
        if (!clientMessages.isClosedForSend) clientMessages.close(error)
        job.cancel(error)
    }
}
