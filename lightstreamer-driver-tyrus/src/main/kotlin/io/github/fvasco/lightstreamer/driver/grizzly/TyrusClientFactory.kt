package io.github.fvasco.lightstreamer.driver.grizzly

import io.github.fvasco.lightstreamer.driver.Client
import io.github.fvasco.lightstreamer.driver.ClientFactory
import io.github.fvasco.lightstreamer.driver.ServerException
import io.github.fvasco.lightstreamer.driver.UsernamePassword
import io.github.fvasco.lightstreamer.driver.helper.ClientMessage
import io.github.fvasco.lightstreamer.driver.helper.ServerMessage
import io.github.fvasco.lightstreamer.driver.helper.ServerMessageBlcpParser
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.glassfish.tyrus.client.ClientManager
import java.io.IOException
import java.net.InetSocketAddress
import java.net.URI
import java.util.concurrent.CompletableFuture
import javax.websocket.ClientEndpointConfig
import javax.websocket.Endpoint
import javax.websocket.EndpointConfig
import javax.websocket.MessageHandler.Partial
import javax.websocket.Session


class TyrusClientFactory(override val serverAddress: InetSocketAddress,
                         override val userCredential: UsernamePassword? = null,
                         override val adapterSetName: String = ClientFactory.defaultAdapterName,
                         override val useSsl: Boolean = false,
                         private val job: Job = Job()) : ClientFactory, Job by job {

    override suspend fun connect(): Client {
        val serverMessageChannel = Channel<ServerMessage>()
        val clientMessageChannel = Channel<ClientMessage>()
        val parser = ServerMessageBlcpParser(serverMessageChannel)

        val protocol = if (useSsl) "wss" else "ws"
        val wsUrl = "$protocol://${serverAddress.hostString}:${serverAddress.port}/lightstreamer"

        val future = CompletableFuture<Session>()

        val cec = ClientEndpointConfig.Builder.create()
                .preferredSubprotocols(listOf("TLCP-2.0.0.lightstreamer.com"))
                .build();
        val client = ClientManager.createClient()
        client.connectToServer(object : Endpoint() {
            override fun onOpen(session: Session, config: EndpointConfig) {
                try {
                    future.complete(session)
                } catch (e: IOException) {
                    future.completeExceptionally(e)
                }

            }
        }, cec, URI(wsUrl))

        val session = future.await()
        session.addMessageHandler(Partial<String> { partialMessage, _ ->
            println("\t -< $partialMessage")
            parser.append(partialMessage)
        })
        val createSessionMessage = ClientMessage.CreateSession(adapterSetName, userCredential)
        println("createSessionMessage $createSessionMessage")
        session.basicRemote.sendText(createSessionMessage.toString())
        val createSessionResponseMessage = runBlocking { serverMessageChannel.receive() }
        if (createSessionResponseMessage is ServerMessage.ConnectionOk) {
            // send client message to server
            launch(Unconfined + job) {
                clientMessageChannel.consumeEach {
                    println("\t -> $it")
                    session.basicRemote.sendText(it.toString())
                }
            }
            return Client(createSessionResponseMessage, serverMessageChannel, clientMessageChannel, Job(job))
        } else if (createSessionResponseMessage is ServerMessage.ConnectionError)
            throw ServerException(createSessionResponseMessage.code, createSessionResponseMessage.message)
        else
            throw IOException("Bad create session response $createSessionResponseMessage")
    }
}
