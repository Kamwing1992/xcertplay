package com.shilapi.xcertplay.airplay

import java.io.ByteArrayOutputStream

/** Minimal OPACK codec for AirPlay DataStream dictionaries. */
internal object OpackCodec {
    fun decodeDictionary(bytes: ByteArray): Map<String, Any?> {
        val cursor = Cursor(bytes)
        val value = cursor.readValue()
        require(value is Map<*, *>) { "OPACK root is not a dictionary" }
        require(cursor.exhausted) { "Trailing bytes after OPACK dictionary" }
        return value.entries.associateTo(LinkedHashMap(value.size)) { (key, entry) ->
            require(key is String) { "OPACK dictionary key is not a string" }
            key to entry
        }
    }

    fun encodeDictionary(values: Map<String, Any?>): ByteArray {
        require(values.size <= 15) { "Only fixed OPACK dictionaries up to 15 entries are supported" }
        val output = ByteArrayOutputStream()
        output.write(0xE0 + values.size)
        values.forEach { (key, value) ->
            writeString(output, key)
            writeValue(output, value)
        }
        return output.toByteArray()
    }

    private fun writeValue(output: ByteArrayOutputStream, value: Any?) {
        when (value) {
            null -> output.write(0x04)
            true -> output.write(0x01)
            false -> output.write(0x02)
            is ByteArray -> writeData(output, value)
            is String -> writeString(output, value)
            is Byte -> writeInteger(output, value.toLong())
            is Short -> writeInteger(output, value.toLong())
            is Int -> writeInteger(output, value.toLong())
            is Long -> writeInteger(output, value)
            else -> throw IllegalArgumentException("Unsupported OPACK value ${value.javaClass.simpleName}")
        }
    }

    private fun writeInteger(output: ByteArrayOutputStream, value: Long) {
        require(value >= 0) { "Negative OPACK integers are not needed by the DataStream envelope" }
        when {
            value <= 39 -> output.write((0x08 + value).toInt())
            value <= 0xFF -> {
                output.write(0x30)
                output.write(value.toInt())
            }
            value <= 0xFFFF -> {
                output.write(0x31)
                writeLittleEndian(output, value, 2)
            }
            value <= 0xFFFF_FFFFL -> {
                output.write(0x32)
                writeLittleEndian(output, value, 4)
            }
            else -> {
                output.write(0x33)
                writeLittleEndian(output, value, 8)
            }
        }
    }

    private fun writeString(output: ByteArrayOutputStream, value: String) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        require(bytes.size <= 0x20) { "Only inline OPACK strings are needed by AirPlay DataStream" }
        output.write(0x40 + bytes.size)
        output.write(bytes)
    }

    private fun writeData(output: ByteArrayOutputStream, value: ByteArray) {
        when {
            value.size <= 0x20 -> output.write(0x70 + value.size)
            value.size <= 0xFF -> {
                output.write(0x91)
                output.write(value.size)
            }
            value.size <= 0xFFFF -> {
                output.write(0x92)
                writeLittleEndian(output, value.size.toLong(), 2)
            }
            else -> {
                output.write(0x93)
                writeLittleEndian(output, value.size.toLong(), 4)
            }
        }
        output.write(value)
    }

    private fun writeLittleEndian(output: ByteArrayOutputStream, value: Long, bytes: Int) {
        repeat(bytes) { offset -> output.write((value ushr (offset * 8)).toInt() and 0xFF) }
    }

    private class Cursor(private val bytes: ByteArray) {
        private var offset = 0
        val exhausted: Boolean get() = offset == bytes.size

        fun readValue(): Any? {
            val marker = readByte()
            return when (marker) {
                0x01 -> true
                0x02 -> false
                0x04 -> null
                in 0x08..0x2F -> (marker - 0x08).toLong()
                0x30 -> readLittleEndian(1)
                0x31 -> readLittleEndian(2)
                0x32 -> readLittleEndian(4)
                0x33 -> readLittleEndian(8)
                in 0x40..0x60 -> readString(marker - 0x40)
                // Strings longer than 32 bytes carry their length in the low nibble's byte count.
                // The VideoSettings property keys ("property_key_...") need this form.
                0x61 -> readString(readLittleEndian(1).toInt())
                0x62 -> readString(readLittleEndian(2).toInt())
                0x63 -> {
                    val length = readLittleEndian(4)
                    require(length <= Int.MAX_VALUE) { "OPACK string is too large" }
                    readString(length.toInt())
                }
                in 0x70..0x90 -> readBytes(marker - 0x70)
                0x91 -> readBytes(readLittleEndian(1).toInt())
                0x92 -> readBytes(readLittleEndian(2).toInt())
                0x93 -> {
                    val length = readLittleEndian(4)
                    require(length <= Int.MAX_VALUE) { "OPACK data is too large" }
                    readBytes(length.toInt())
                }
                in 0xE0..0xEF -> readDictionary(marker - 0xE0)
                else -> throw IllegalArgumentException("Unsupported OPACK marker 0x${marker.toString(16)}")
            }
        }

        private fun readDictionary(count: Int): Map<String, Any?> {
            val result = LinkedHashMap<String, Any?>(count)
            repeat(count) {
                val key = readValue()
                require(key is String) { "OPACK dictionary key is not a string" }
                result[key] = readValue()
            }
            return result
        }

        private fun readString(length: Int): String =
            String(readBytes(length), Charsets.UTF_8)

        private fun readLittleEndian(count: Int): Long {
            require(count in 1..8)
            var result = 0L
            repeat(count) { index -> result = result or (readByte().toLong() shl (index * 8)) }
            return result
        }

        private fun readByte(): Int {
            require(offset < bytes.size) { "Truncated OPACK value" }
            return bytes[offset++].toInt() and 0xFF
        }

        private fun readBytes(count: Int): ByteArray {
            require(count >= 0 && offset <= bytes.size - count) { "Truncated OPACK payload" }
            return bytes.copyOfRange(offset, offset + count).also { offset += count }
        }
    }
}
