package com.shilapi.xcertplay.airplay

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.Closeable
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

/** Bidirectional encrypted DataStream socket carrying APTransport packages. */
internal class ApTransportDataStream(
    private val readKey: ByteArray,
    private val writeKey: ByteArray,
    bindAddress: InetAddress,
) : Closeable {
    interface Listener {
        fun onOpen(remoteAddress: String?) {}
        fun onPacket(packet: ApTransportPackage) {}
        fun onDebug(message: String) {}
        fun onClosed(cause: Throwable?) {}
    }

    private val closed = AtomicBoolean(false)
    private val connected = AtomicBoolean(false)
    private val bindAddress =
        if (bindAddress is Inet4Address) InetAddress.getByName("0.0.0.0") else bindAddress
    private val servers = CopyOnWriteArrayList<ServerSocket>()
    private val threads = CopyOnWriteArrayList<Thread>()
    private val writeLock = Any()
    @Volatile private var socket: Socket? = null
    @Volatile private var output: BufferedOutputStream? = null
    @Volatile private var cipher: ControlCipher? = null
    @Volatile private var listener: Listener = object : Listener {}

    fun listen(listener: Listener): Int {
        this.listener = listener
        val primary = bind(bindAddress, 0)
        servers += primary
        listener.onDebug("APTransport DataStream listener bound=${primary.localSocketAddress}")

        val secondaryAddress = when {
            bindAddress is java.net.Inet6Address && bindAddress.isAnyLocalAddress -> null
            bindAddress is java.net.Inet6Address -> InetAddress.getByName("0.0.0.0")
            else -> InetAddress.getByName("::")
        }
        secondaryAddress?.let { address ->
            runCatching { bind(address, primary.localPort) }
                .onSuccess { secondary ->
                    servers += secondary
                    listener.onDebug(
                        "APTransport DataStream secondary listener bound=${secondary.localSocketAddress}",
                    )
                }
                .onFailure { error ->
                    listener.onDebug(
                        "APTransport DataStream secondary bind failed address=$address " +
                            "port=${primary.localPort}: ${error.message}",
                    )
                }
        }
        servers.forEach { server ->
            Thread({ accept(server) }, "airplay-video-datastream").apply {
                isDaemon = true
                threads += this
                start()
            }
        }
        return primary.localPort
    }

    fun send(packet: ApTransportPackage): Boolean = synchronized(writeLock) {
        if (closed.get()) {
            listener.onDebug("APTransport DataStream tx rejected: transport is closed")
            return@synchronized false
        }
        val activeOutput = output
        val activeCipher = cipher
        if (activeOutput == null || activeCipher == null) {
            listener.onDebug(
                "APTransport DataStream tx rejected: peer is not connected " +
                    "package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                    "group=${java.lang.Long.toUnsignedString(packet.groupId)}",
            )
            return@synchronized false
        }
        val plaintext = ApTransportCodec.encode(packet)
        return@synchronized try {
            val ciphertext = activeCipher.encrypt(plaintext)
            activeOutput.write(ciphertext)
            activeOutput.flush()
            listener.onDebug(
                "APTransport DataStream tx package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                    "message=${messageTypeName(packet.messageType)} " +
                    "group=${java.lang.Long.toUnsignedString(packet.groupId)} " +
                    "replyToken=${java.lang.Long.toUnsignedString(packet.replyToken)} " +
                    "status=${packet.replyStatus} payload=${packet.payload.size} " +
                    "wire=${ciphertext.size}",
            )
            true
        } catch (error: Throwable) {
            listener.onDebug(
                "APTransport DataStream tx failed package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                    "error=${error.message ?: error.javaClass.simpleName}",
            )
            false
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        synchronized(writeLock) {
            output = null
            cipher = null
        }
        safeClose(socket)
        socket = null
        servers.forEach(::safeClose)
        servers.clear()
        threads.forEach(Thread::interrupt)
        threads.clear()
    }

    private fun bind(address: InetAddress, port: Int): ServerSocket {
        val server = ServerSocket()
        return try {
            server.reuseAddress = true
            server.bind(InetSocketAddress(address, port))
            server
        } catch (error: Throwable) {
            safeClose(server)
            throw error
        }
    }

    private fun accept(server: ServerSocket) {
        listener.onDebug("APTransport DataStream accepting local=${server.localSocketAddress}")
        val accepted = try {
            server.accept()
        } catch (error: Throwable) {
            if (!closed.get() && !connected.get()) listener.onClosed(error)
            return
        }
        if (closed.get() || !connected.compareAndSet(false, true)) {
            safeClose(accepted)
            return
        }
        servers.filter { it !== server }.forEach(::safeClose)
        accepted.setSoLinger(true, 0)
        val connectionCipher = ControlCipher(readKey, writeKey)
        val connectionOutput = BufferedOutputStream(accepted.getOutputStream())
        socket = accepted
        synchronized(writeLock) {
            cipher = connectionCipher
            output = connectionOutput
        }
        listener.onOpen(accepted.remoteSocketAddress?.toString())
        run(accepted, connectionCipher)
    }

    private fun run(accepted: Socket, connectionCipher: ControlCipher) {
        var encrypted = ByteArray(0)
        var plaintext = ByteArray(0)
        var failure: Throwable? = null
        try {
            val input = BufferedInputStream(accepted.getInputStream())
            val buffer = ByteArray(READ_CHUNK_BYTES)
            while (!closed.get()) {
                val count = input.read(buffer)
                if (count < 0) break
                encrypted += buffer.copyOf(count)
                val decrypted = connectionCipher.decrypt(encrypted)
                encrypted = decrypted.rest
                plaintext += decrypted.data
                while (plaintext.isNotEmpty()) {
                    val decoded = ApTransportCodec.decodeFirst(plaintext) ?: break
                    plaintext = decoded.rest
                    val packet = decoded.packet
                    listener.onDebug(
                        "APTransport DataStream rx package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                            "message=${messageTypeName(packet.messageType)} " +
                            "group=${java.lang.Long.toUnsignedString(packet.groupId)} " +
                            "replyToken=${java.lang.Long.toUnsignedString(packet.replyToken)} " +
                            "status=${packet.replyStatus} payload=${packet.payload.size}",
                    )
                    listener.onPacket(packet)
                }
            }
            if (!closed.get()) {
                listener.onDebug(
                    "APTransport DataStream peer EOF encryptedRemainder=${encrypted.size} " +
                        "plaintextRemainder=${plaintext.size}",
                )
            }
        } catch (error: Throwable) {
            failure = error
            listener.onDebug(
                "APTransport DataStream read/decode failed " +
                    "error=${error.javaClass.simpleName}: ${error.message ?: "no message"} " +
                    "encryptedRemainder=${encrypted.size} plaintextRemainder=${plaintext.size}",
            )
        } finally {
            synchronized(writeLock) {
                if (socket === accepted) {
                    output = null
                    cipher = null
                }
            }
            if (socket === accepted) socket = null
            safeClose(accepted)
            if (!closed.get()) listener.onClosed(failure)
        }
    }

    private fun messageTypeName(value: Int): String =
        if (value == 0) "0" else ApTransportCodec.fourCcString(value)

    private companion object {
        const val READ_CHUNK_BYTES = 16 * 1024
    }
}
