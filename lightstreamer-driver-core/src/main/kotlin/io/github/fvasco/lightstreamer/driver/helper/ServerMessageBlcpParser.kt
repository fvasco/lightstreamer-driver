package io.github.fvasco.lightstreamer.driver.helper

import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.runBlocking
import java.io.CharArrayWriter
import java.io.Writer

/**
 * Parse LS BLCP stream to serverMessageChannel
 */
class ServerMessageBlcpParser(private val serverMessageChannel: SendChannel<ServerMessage>) : Writer() {

    private val itemList = ArrayList<String>()
    private val itemWriter = CharArrayWriter()

    override fun write(cbuf: CharArray, off: Int, len: Int) {
        repeat(len) { i ->
            val c = cbuf[off + i]
            when (c) {
                ',', '|' -> onItemEnd()
                '\r' -> onLineEnd()
                '\n' -> check(itemList.size == 0 && itemWriter.size() == 0) // ignore
                else -> itemWriter.append(c)
            }
        }
    }

    override fun flush() = Unit
    override fun close() {
        serverMessageChannel.close()
    }

    private fun onItemEnd() {
        try {
            itemList +=
                    if (itemWriter.size() == 0)
                        ""
                    else
                        itemWriter.toString()
        } finally {
            itemWriter.reset()
        }
    }

    private fun onLineEnd() = runBlocking {
        onItemEnd()
        val serverMessage = ServerMessage.parse(itemList)
        itemList.clear()
        serverMessageChannel.send(serverMessage)
    }
}
