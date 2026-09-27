package com.shilapi.xcertplay.airplay

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

class VideoPlaybackRcsSessionTest {
    @Test
    fun sharedRtspWrapperDispatchesInsertAndReturnsPlaybackInfo() {
        val controller = FakeController()
        val session = VideoPlaybackRcsSession(
            streamId = 2,
            streamConnectionId = 42,
            sendMessageAsIs = false,
            controller = controller,
            log = {},
        )
        val replies = ArrayList<ByteArray>()

        assertTrue(
            session.handleSharedCommand(
                wrap(
                    linkedMapOf(
                        "type" to "insertPlayQueueItem",
                        "item" to linkedMapOf(
                            "uuid" to "item-1",
                            "Content-Location" to "http://example.test/live.m3u8",
                            "mediaType" to "streaming",
                        ),
                    ),
                ),
                replies::add,
            ),
        )
        assertEquals("item-1", controller.inserted?.uuid)
        assertEquals("http://example.test/live.m3u8", controller.inserted?.contentLocation)
        assertTrue(replies.isEmpty())

        assertTrue(
            session.handleSharedCommand(
                wrap(
                    linkedMapOf(
                        "type" to "playbackInfo",
                        "kind" to "request",
                        "messageID" to 7,
                    ),
                ),
                replies::add,
            ),
        )
        val response = unwrap(replies.single())
        assertEquals("response", response["kind"])
        assertEquals(7L, (response["messageID"] as Number).toLong())
        val body = response["response"] as Map<*, *>
        assertNotNull(body["info"])
    }

    private fun wrap(inner: Map<String, Any?>): ByteArray = BplistCodec.encode(
        linkedMapOf(
            "params" to linkedMapOf(
                "data" to BplistCodec.encode(inner),
            ),
        ),
    )

    private fun unwrap(payload: ByteArray): Map<*, *> {
        val outer = BplistCodec.decode(payload) as Map<*, *>
        val params = outer["params"] as Map<*, *>
        return BplistCodec.decode(params["data"] as ByteArray) as Map<*, *>
    }

    private class FakeController : VideoPlaybackController {
        var inserted: VideoPlaybackItem? = null

        override val isVideoPlaybackActive: Boolean get() = inserted != null

        override fun insert(item: VideoPlaybackItem, afterUuid: String?): Int {
            inserted = item
            return 0
        }

        override fun remove(uuid: String?): Int = 0

        override fun setRate(rate: Double): Int = 0

        override fun seek(time: VideoPlaybackTime): Int = 0

        override fun stop(): Int {
            inserted = null
            return 0
        }

        override fun snapshot(): VideoPlaybackSnapshot = VideoPlaybackSnapshot(
            durationSeconds = 120.0,
            positionSeconds = 10.0,
            rate = 1.0,
            readyToPlay = true,
            loadedTimeRanges = emptyList(),
            seekableTimeRanges = emptyList(),
        )
    }
}
