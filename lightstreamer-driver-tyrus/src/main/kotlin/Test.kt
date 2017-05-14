import io.github.fvasco.lightstreamer.driver.SubscriptionMode
import io.github.fvasco.lightstreamer.driver.grizzly.TyrusClientFactory
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.runBlocking
import java.net.InetSocketAddress

fun main(vararg args: String) = runBlocking {
    try {
        val lcf = TyrusClientFactory(serverAddress = InetSocketAddress("push.lightstreamer.com", 443),
                adapterSetName = "ISSLIVE",
                useSsl = true)
        val client = lcf.connect()

        val subscription = client.subscribe(SubscriptionMode.MERGE, "TIME_000001", itemNames = listOf("TimeStamp"), requestSnapshot = true)
        println("Subscription ok")
        subscription.await().messageChannel.consumeEach {
            println(it) // TODO
        }
        println("DONE")
    } catch(t: Throwable) {
        t.printStackTrace()
    }
}