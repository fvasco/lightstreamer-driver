package io.github.fvasco.lightstreamer.driver.helper

/**
 * Define a generic server message
 */
sealed class ServerMessage {

    data class ClearSnapshot(val subscriptionId: Int, val itemId: Int) : ServerMessage()

    data class ClientIp(val clientIp: String) : ServerMessage()

    data class Configuration(val subscriptionId: Int, val maxFrequency: Int?, val filtered: Boolean) : ServerMessage()

    data class ConnectionOk(val sessionId: String, val requestLimit: Int, val keepAlive: Int, val controlLink: String) : ServerMessage()

    data class ConnectionError(val code: Int, val message: String) : ServerMessage()

    data class Contraint(val bandwidth: Int?) : ServerMessage()

    data class Error(val code: Int, val message: String) : ServerMessage()

    data class End(val code: Int, val message: String) : ServerMessage()

    data class EndOfSnapshot(val subscriptionId: Int, val itemId: Int) : ServerMessage()

    object NoOp : ServerMessage()

    data class Overflow(val subscriptionId: Int, val itemCount: Int, val fieldCount: Int) : ServerMessage()

    object Probe : ServerMessage()

    data class RequestOk(val requestId: Int) : ServerMessage()

    data class RequestError(val requestId: Int, val code: Int, val message: String) : ServerMessage()

    data class ServerName(val serverName: String) : ServerMessage()

    data class Synchronize(val secondsSinceInitialHeader: Int) : ServerMessage()

    data class UnsubscriptionOk(val subscriptionId: Int) : ServerMessage()

    data class Update(val subscriptionId: Int, val itemId: Int, val values: List<String>) : ServerMessage()

    data class SubscriptionOk(val subscriptionId: Int, val itemCount: Int, val fieldCount: Int) : ServerMessage()

    companion object {
        fun parse(line: List<String>): ServerMessage {
            return when (line[0]) {
                "CLIENTIP" -> ClientIp(line[1])
                "CONF" -> Configuration(line[1].toInt(), if (line[2] == "unlimited") null else line[2].toInt(), line[3] == "filtered")
                "CONS" -> Contraint(if (line[1] == "unlimited") null else line[1].toInt())
                "CONERR" -> ConnectionError(line[1].toInt(), line[2])
                "CONOK" -> ConnectionOk(line[1], line[2].toInt(), line[3].toInt(), line[4])
                "CS" -> ClearSnapshot(line[1].toInt(), line[2].toInt())
                "END" -> End(line[1].toInt(), line[2])
                "EOS" -> EndOfSnapshot(line[1].toInt(), line[2].toInt())
                "ERROR" -> Error(line[1].toInt(), line[2])
                "NOOP" -> NoOp
                "OV" -> Overflow(line[1].toInt(), line[2].toInt(), line[3].toInt())
                "PROBE" -> Probe
                "UNSUB" -> UnsubscriptionOk(line[1].toInt())
                "REQOK" -> RequestOk(line[1].toInt())
                "REQERR" -> RequestError(line[1].toInt(), line[2].toInt(), line[3])
                "SERVNAME" -> ServerName(line[1])
                "SYNC" -> Synchronize(line[1].toInt())
                "SUBOK" -> SubscriptionOk(line[1].toInt(), line[2].toInt(), line[3].toInt())
                "U" -> Update(line[1].toInt(), line[2].toInt(), line.slice(3 until line.size))
                else -> throw IllegalArgumentException("Invalid line: $line")
            }
        }
    }
}
