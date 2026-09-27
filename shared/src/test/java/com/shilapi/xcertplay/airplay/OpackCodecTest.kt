package com.shilapi.xcertplay.airplay

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class OpackCodecTest {
    @Test
    fun decodesObservedVideoSettingsEnvelope() {
        val inner = "bplist00-video-settings".toByteArray()
        val encoded = OpackCodec.encodeDictionary(
            linkedMapOf(
                "data" to inner,
                "messageType" to 2L,
            ),
        )

        assertEquals(0xE2, encoded[0].toInt() and 0xFF)
        assertEquals(0x44, encoded[1].toInt() and 0xFF)
        assertEquals(0x0A, encoded.last().toInt() and 0xFF)

        val decoded = OpackCodec.decodeDictionary(encoded)
        assertTrue((decoded["data"] as ByteArray).contentEquals(inner))
        assertEquals(2L, decoded["messageType"])
    }

    @Test
    fun retainsLargeDataAcrossLengthForms() {
        listOf(32, 33, 255, 256, 65_536).forEach { size ->
            val data = ByteArray(size) { (it and 0xFF).toByte() }
            val encoded = OpackCodec.encodeDictionary(linkedMapOf("data" to data))
            val decoded = OpackCodec.decodeDictionary(encoded)
            assertTrue("size=$size", (decoded["data"] as ByteArray).contentEquals(data))
        }
    }

    @Test
    fun decodesPropertyKeysLongerThanTheInlineStringLimit() {
        // From the phone's VideoSettings bootstrap: both property keys exceed the 32-byte inline
        // OPACK string limit, so they use the 0x61 one-byte-length form.
        val firstKey = "property_key_ClosedCaptionsAndSDH_Enabled"
        val secondKey = "property_key_captionstyles_selectedstyle"
        assertEquals(41, firstKey.length)
        assertEquals(40, secondKey.length)

        val encoded = byteArrayOf(
            0xE1.toByte(),
            0x44, 'd'.code.toByte(), 'a'.code.toByte(), 't'.code.toByte(), 'a'.code.toByte(),
            0xE2.toByte(),
            0x61, firstKey.length.toByte(),
        ) + firstKey.toByteArray() + byteArrayOf(
            0x02,
            0x61, secondKey.length.toByte(),
        ) + secondKey.toByteArray() + byteArrayOf(0x01)

        val settings = OpackCodec.decodeDictionary(encoded)["data"] as Map<*, *>
        assertEquals(false, settings[firstKey])
        assertEquals(true, settings[secondKey])
    }

    @Test
    fun decodesNestedVideoSettingsDictionary() {
        val encoded = byteArrayOf(
            0xE2.toByte(),
            0x44, 'd'.code.toByte(), 'a'.code.toByte(), 't'.code.toByte(), 'a'.code.toByte(),
            0xE1.toByte(),
            0x43, 'f'.code.toByte(), 'o'.code.toByte(), 'o'.code.toByte(),
            0x01,
            0x4B, 'm'.code.toByte(), 'e'.code.toByte(), 's'.code.toByte(),
            's'.code.toByte(), 'a'.code.toByte(), 'g'.code.toByte(), 'e'.code.toByte(),
            'T'.code.toByte(), 'y'.code.toByte(), 'p'.code.toByte(), 'e'.code.toByte(),
            0x0A,
        )

        val decoded = OpackCodec.decodeDictionary(encoded)
        assertEquals(true, (decoded["data"] as Map<*, *>)["foo"])
        assertEquals(2L, decoded["messageType"])
    }
}
