package io.github.fvasco.lightstreamer.driver.grizzly

import io.github.fvasco.lightstreamer.driver.Client
import io.github.fvasco.lightstreamer.driver.ClientFactory
import io.github.fvasco.lightstreamer.driver.ServerException
import io.github.fvasco.lightstreamer.driver.UsernamePassword
import io.github.fvasco.lightstreamer.driver.helper.ClientMessage
import io.github.fvasco.lightstreamer.driver.helper.ServerMessage
import io.github.fvasco.lightstreamer.driver.helper.ServerMessageBlcpDecoder
import io.github.fvasco.lightstreamer.driver.helper.clientMessageBlcpEncoder
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.glassfish.tyrus.client.ClientManager
import java.io.IOException
import java.net.InetSocketAddress
import java.net.URI
import javax.websocket.ClientEndpointConfig
import javax.websocket.Endpoint
import javax.websocket.EndpointConfig
import javax.websocket.MessageHandler.Partial
import javax.websocket.Session
import kotlin.coroutines.experimental.suspendCoroutine


class TyrusClientFactory(override val serverAddress: InetSocketAddress,
                         override val userCredential: UsernamePassword? = null,
                         override val adapterSetName: String = ClientFactory.defaultAdapterName,
                         override val useSsl: Boolean = false,
                         private val job: Job = Job()) : ClientFactory, Job by job {

    override suspend fun connect(): Client {
        val serverMessageChannel = Channel<ServerMessage>()
        val clientMessageChannel = Channel<ClientMessage>()
        val blcpServerDecoder = ServerMessageBlcpDecoder(serverMessageChannel)
        val blcpClientEncoder = clientMessageBlcpEncoder(clientMessageChannel)

        val protocol = if (useSsl) "wss" else "ws"
        val wsUrl = "$protocol://${serverAddress.hostString}:${serverAddress.port}/lightstreamer"

        val cec = ClientEndpointConfig.Builder.create()
                .preferredSubprotocols(listOf("TLCP-2.0.0.lightstreamer.com"))
                .build();
        val client = ClientManager.createClient()
        val session = suspendCoroutine<Session> { cont ->
            try {
                client.asyncConnectToServer(object : Endpoint() {
                    override fun onOpen(session: Session, config: EndpointConfig) {
                        try {
                            cont.resume(session)
                        } catch (e: IOException) {
                            cont.resumeWithException(e)
                        }

                    }
                }, cec, URI(wsUrl))
            } catch(e: Exception) {
                cont.resumeWithException(e)
            }
        }

        session.addMessageHandler(Partial<String> { partialMessage, _ ->
            println("\t -< $partialMessage")
            blcpServerDecoder.append(partialMessage)
        })
        // send client message to server
        launch(Unconfined + job) {
            blcpClientEncoder.consumeEach {
                println("\t -> $it")
                session.basicRemote.sendText(it)
            }
        }
        val createSessionMessage = ClientMessage.CreateSession(adapterSetName, userCredential)
        println("createSessionMessage $createSessionMessage")
        clientMessageChannel.send(createSessionMessage)
        val createSessionResponseMessage = runBlocking { serverMessageChannel.receive() }
        if (createSessionResponseMessage is ServerMessage.ConnectionOk) {
            return Client(createSessionResponseMessage, serverMessageChannel, clientMessageChannel, Job(job))
        } else if (createSessionResponseMessage is ServerMessage.ConnectionError)
            throw ServerException(createSessionResponseMessage.code, createSessionResponseMessage.message)
        else
            throw IOException("Bad create session response $createSessionResponseMessage")
    }
}
