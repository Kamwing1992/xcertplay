package com.shilapi.xcertplay.airplay

/** APTransport package carried by a shared or dedicated RemoteControlSession connection. */
internal data class ApTransportPackage(
    val packageType: Int,
    val groupId: Long,
    val messageType: Int,
    val replyToken: Long,
    val replyStatus: Int,
    val payload: ByteArray,
)

internal object ApTransportCodec {
    const val HEADER_BYTES = 32
    const val MAX_PACKAGE_BYTES = 4 * 1024 * 1024

    val TYPE_ASYNC = fourCc("asyn")
    val TYPE_SYNC = fourCc("sync")
    val TYPE_REPLY = fourCc("rply")
    val MESSAGE_COMM = fourCc("comm")

    data class Decoded(val packet: ApTransportPackage, val rest: ByteArray)

    /** Shared APTransport packages begin with a bounded big-endian length, unlike ASCII RTSP. */
    fun isPotentialPrefix(bytes: ByteArray): Boolean = bytes.isNotEmpty() && bytes[0] == 0.toByte()

    fun decodeFirst(bytes: ByteArray): Decoded? {
        if (bytes.size < HEADER_BYTES) return null
        val totalLength = readU32(bytes, 0)
        require(totalLength in HEADER_BYTES..MAX_PACKAGE_BYTES) {
            "APTransport package length is invalid: $totalLength"
        }
        if (bytes.size < totalLength) return null
        val packageType = readI32(bytes, 4)
        require(packageType == TYPE_ASYNC || packageType == TYPE_SYNC || packageType == TYPE_REPLY) {
            "APTransport package type is invalid: ${fourCcString(packageType)}"
        }
        return Decoded(
            ApTransportPackage(
                packageType = packageType,
                groupId = readI64(bytes, 8),
                messageType = readI32(bytes, 16),
                replyToken = readI64(bytes, 20),
                replyStatus = readI32(bytes, 28),
                payload = bytes.copyOfRange(HEADER_BYTES, totalLength),
            ),
            bytes.copyOfRange(totalLength, bytes.size),
        )
    }

    fun encode(packet: ApTransportPackage): ByteArray {
        require(packet.payload.size <= MAX_PACKAGE_BYTES - HEADER_BYTES) {
            "APTransport payload exceeds the package limit"
        }
        val output = ByteArray(HEADER_BYTES + packet.payload.size)
        writeI32(output, 0, output.size)
        writeI32(output, 4, packet.packageType)
        writeI64(output, 8, packet.groupId)
        writeI32(output, 16, packet.messageType)
        writeI64(output, 20, packet.replyToken)
        writeI32(output, 28, packet.replyStatus)
        packet.payload.copyInto(output, HEADER_BYTES)
        return output
    }

    fun fourCc(value: String): Int {
        require(value.length == 4) { "FourCC must contain four ASCII characters" }
        return value.fold(0) { result, character ->
            require(character.code <= 0x7f) { "FourCC must be ASCII" }
            (result shl 8) or character.code
        }
    }

    fun fourCcString(value: Int): String = buildString(4) {
        append(((value ushr 24) and 0xff).toChar())
        append(((value ushr 16) and 0xff).toChar())
        append(((value ushr 8) and 0xff).toChar())
        append((value and 0xff).toChar())
    }

    private fun readU32(bytes: ByteArray, offset: Int): Int {
        val value = readI32(bytes, offset).toLong() and 0xffff_ffffL
        require(value <= Int.MAX_VALUE) { "APTransport package length exceeds the local limit" }
        return value.toInt()
    }

    private fun readI32(bytes: ByteArray, offset: Int): Int =
        ((bytes[offset].toInt() and 0xff) shl 24) or
            ((bytes[offset + 1].toInt() and 0xff) shl 16) or
            ((bytes[offset + 2].toInt() and 0xff) shl 8) or
            (bytes[offset + 3].toInt() and 0xff)

    private fun readI64(bytes: ByteArray, offset: Int): Long =
        ((readI32(bytes, offset).toLong() and 0xffff_ffffL) shl 32) or
            (readI32(bytes, offset + 4).toLong() and 0xffff_ffffL)

    private fun writeI32(bytes: ByteArray, offset: Int, value: Int) {
        bytes[offset] = (value ushr 24).toByte()
        bytes[offset + 1] = (value ushr 16).toByte()
        bytes[offset + 2] = (value ushr 8).toByte()
        bytes[offset + 3] = value.toByte()
    }

    private fun writeI64(bytes: ByteArray, offset: Int, value: Long) {
        writeI32(bytes, offset, (value ushr 32).toInt())
        writeI32(bytes, offset + 4, value.toInt())
    }
}
