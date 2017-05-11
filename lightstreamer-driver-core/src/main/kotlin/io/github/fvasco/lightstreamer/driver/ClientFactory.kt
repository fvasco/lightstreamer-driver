package io.github.fvasco.lightstreamer.driver

import kotlinx.coroutines.experimental.Job
import java.net.InetSocketAddress

interface ClientFactory : Job {

    val serverAddress: InetSocketAddress
    val userCredential: UsernamePassword?
    val adapterSetName: String
    val useSsl: Boolean

    suspend fun connect(): Client

    companion object {
        const val defaultAdapterName = "DEFAULT"
    }
}
