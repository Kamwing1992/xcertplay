package com.shilapi.xcertplay.airplay

import android.util.Log
import java.io.Closeable
import java.net.URI

data class VideoPlaybackItem(
    val uuid: String?,
    val contentLocation: String,
    val startPositionSeconds: Double,
    val tlsEnabled: Boolean,
    val metadata: Map<String, Any?>,
)

data class VideoPlaybackTime(
    val value: Long,
    val timescale: Int,
    val flags: Long,
    val epoch: Long,
) {
    fun seconds(): Double? = if (timescale > 0) value.toDouble() / timescale else null
}

data class VideoPlaybackSnapshot(
    val durationSeconds: Double,
    val positionSeconds: Double,
    val rate: Double,
    val readyToPlay: Boolean,
    val loadedTimeRanges: List<Map<String, Any?>>,
    val seekableTimeRanges: List<Map<String, Any?>>,
) {
    fun toPlist(): Map<String, Any?> = linkedMapOf(
        "duration" to durationSeconds,
        "position" to positionSeconds,
        "rate" to rate,
        "readyToPlay" to readyToPlay,
        "loadedTimeRanges" to loadedTimeRanges,
        "seekableTimeRanges" to seekableTimeRanges,
    )
}

/** Android-independent command seam owned by the media backend. */
interface VideoPlaybackController {
    val isVideoPlaybackActive: Boolean
    fun insert(item: VideoPlaybackItem, afterUuid: String?): Int
    fun remove(uuid: String?): Int
    fun setRate(rate: Double): Int
    fun seek(time: VideoPlaybackTime): Int
    fun stop(): Int
    fun snapshot(): VideoPlaybackSnapshot
}

/** One CarPlayVideoSettings RemoteControlSession on a shared or dedicated APTransport connection. */
internal class VideoPlaybackRcsSession(
    val streamId: Long,
    val streamConnectionId: Long,
    private val sendMessageAsIs: Boolean,
    private val stopControllerOnClose: Boolean = true,
    private val controller: VideoPlaybackController,
    private val log: (String) -> Unit,
) : Closeable {
    @Volatile private var sender: ((ApTransportPackage) -> Boolean)? = null
    @Volatile private var sharedSender: ((ByteArray) -> Boolean)? = null
    @Volatile private var observedGroupId: Long? = null
    @Volatile private var closed = false
    private val properties = LinkedHashMap<String, Any?>()

    val active: Boolean get() = !closed && controller.isVideoPlaybackActive

    fun canHandle(packet: ApTransportPackage): Boolean {
        if (closed || packet.messageType != ApTransportCodec.MESSAGE_COMM || packet.payload.isEmpty()) {
            return false
        }
        if (packet.groupId == streamConnectionId || packet.groupId == observedGroupId) return true
        val inner = decodeInner(packet.payload)
        val matches = inner?.let {
            val type = inner["type"] as? String
            type in VIDEO_COMMANDS || inner["kind"] == "response" || inner["kind"] == "notification"
        } == true
        log(
            "CarPlayVideoSettings demux candidate group=${unsigned(packet.groupId)} " +
                "expected=${unsigned(streamConnectionId)} observed=${observedGroupId?.let(::unsigned) ?: "none"} " +
                "decoded=${inner != null} type=${inner?.get("type") ?: "missing"} accepted=$matches",
        )
        return matches
    }

    fun handle(
        packet: ApTransportPackage,
        reply: (ApTransportPackage) -> Boolean,
        acknowledgeOpaquePayload: Boolean = false,
    ): Boolean {
        if (!canHandle(packet)) return false
        sender = reply
        observedGroupId = packet.groupId
        val inner = decodeInner(packet.payload)
        if (inner == null) {
            log(
                "CarPlayVideoSettings payload decode failed sendMessageAsIs=$sendMessageAsIs " +
                    "package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                    "group=${unsigned(packet.groupId)} payload=${packet.payload.size} " +
                    "prefixHex=${packet.payload.prefixHex()}",
            )
            if (!acknowledgeOpaquePayload) return replyError(packet, OSSTATUS_PARAM, reply)
            if (packet.packageType != ApTransportCodec.TYPE_SYNC) {
                log("CarPlayVideoSettings accepted opaque async payload without transport reply")
                return true
            }
            val sent = reply(
                ApTransportPackage(
                    packageType = ApTransportCodec.TYPE_REPLY,
                    groupId = packet.groupId,
                    messageType = 0,
                    replyToken = packet.replyToken,
                    replyStatus = 0,
                    payload = ByteArray(0),
                ),
            )
            log(
                "CarPlayVideoSettings accepted opaque sync payload with success acknowledgement " +
                    "replyToken=${unsigned(packet.replyToken)} sent=$sent",
            )
            return sent
        }
        val type = inner["type"] as? String ?: ""
        log(
            "CarPlayVideoSettings rx package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                "group=${unsigned(packet.groupId)} expectedGroup=${unsigned(streamConnectionId)} " +
                "replyToken=${unsigned(packet.replyToken)} type=$type " +
                "kind=${inner["kind"] ?: "direct"} sendMessageAsIs=$sendMessageAsIs " +
                "keys=${inner.keys.sorted()}",
        )

        val outcome = try {
            dispatch(inner)
        } catch (error: Throwable) {
            Log.w(TAG, "video playback command failed type=$type", error)
            Outcome(OSSTATUS_INTERNAL)
        }
        log(
            "CarPlayVideoSettings handled type=$type status=${outcome.status} " +
                "innerResponse=${outcome.innerResponse != null} transportReply=" +
                "${packet.packageType == ApTransportCodec.TYPE_SYNC || outcome.innerResponse != null}",
        )
        if (packet.packageType != ApTransportCodec.TYPE_SYNC && outcome.innerResponse == null) {
            return true
        }
        val responsePayload = outcome.innerResponse?.let(::wrapInner) ?: ByteArray(0)
        val response = ApTransportPackage(
            packageType = if (packet.packageType == ApTransportCodec.TYPE_SYNC) {
                ApTransportCodec.TYPE_REPLY
            } else {
                ApTransportCodec.TYPE_ASYNC
            },
            groupId = packet.groupId,
            // Firmware marks MDC replies as zero. Online VideoSettings frames will verify this.
            messageType = if (packet.packageType == ApTransportCodec.TYPE_SYNC) {
                0
            } else {
                ApTransportCodec.MESSAGE_COMM
            },
            replyToken = packet.replyToken,
            replyStatus = outcome.status,
            payload = responsePayload,
        )
        val sent = reply(response)
        log(
            "CarPlayVideoSettings tx package=${ApTransportCodec.fourCcString(response.packageType)} " +
                "group=${unsigned(response.groupId)} replyToken=${unsigned(response.replyToken)} " +
                "status=${response.replyStatus} payload=${response.payload.size} sent=$sent",
        )
        return sent
    }

    /** Handles a controlType=1 RCS message delivered as RTSP /command + X-Apple-StreamID. */
    fun handleSharedCommand(
        payload: ByteArray,
        reply: (ByteArray) -> Boolean,
    ): Boolean {
        if (closed) {
            log("CarPlayVideoSettings shared command dropped: RCS is closed streamID=$streamId")
            return false
        }
        val inner = decodeInner(payload)
        if (inner == null) {
            log(
                "CarPlayVideoSettings shared command decode failed streamID=$streamId " +
                    "payload=${payload.size} prefixHex=${payload.prefixHex()}",
            )
            return false
        }
        sharedSender = reply
        val type = inner["type"] as? String ?: ""
        log(
            "CarPlayVideoSettings rx transport=rtsp streamID=$streamId type=$type " +
                "kind=${inner["kind"] ?: "direct"} messageID=${inner["messageID"] ?: "none"} " +
                "keys=${inner.keys.sorted()}",
        )
        val outcome = try {
            dispatch(inner)
        } catch (error: Throwable) {
            Log.w(TAG, "video playback shared command failed type=$type", error)
            requestError(inner["kind"] as? String, inner["messageID"] as? Number, OSSTATUS_INTERNAL)
        }
        val response = outcome.innerResponse
        if (response == null) {
            log(
                "CarPlayVideoSettings handled transport=rtsp streamID=$streamId type=$type " +
                    "status=${outcome.status} response=false",
            )
            return true
        }
        val encoded = wrapInner(response)
        val sent = reply(encoded)
        log(
            "CarPlayVideoSettings tx transport=rtsp streamID=$streamId type=response " +
                "messageID=${response["messageID"] ?: "none"} payload=${encoded.size} sent=$sent",
        )
        return sent
    }

    fun sendBackButtonEvent(): Boolean {
        if (closed) {
            log("CarPlayVideoSettings back event skipped: RCS is closed")
            return false
        }
        val inner = linkedMapOf<String, Any?>(
            "kind" to "notification",
            "type" to "videoPlayerBackButtonEvent",
        )
        val streamSend = sharedSender
        val sent = if (streamSend != null) {
            log("CarPlayVideoSettings tx back event transport=rtsp streamID=$streamId")
            streamSend(wrapInner(inner))
        } else {
            val transportSend = sender ?: run {
                log("CarPlayVideoSettings back event skipped: no RCS transport has been observed")
                return false
            }
            val group = observedGroupId ?: streamConnectionId
            log("CarPlayVideoSettings tx back event transport=APTransport group=${unsigned(group)}")
            transportSend(
                ApTransportPackage(
                    packageType = ApTransportCodec.TYPE_ASYNC,
                    groupId = group,
                    messageType = ApTransportCodec.MESSAGE_COMM,
                    replyToken = 0,
                    replyStatus = 0,
                    payload = wrapInner(inner),
                ),
            )
        }
        if (sent) {
            val stopStatus = controller.stop()
            log("CarPlayVideoSettings back event sent; local playback stop status=$stopStatus")
        } else {
            log("CarPlayVideoSettings back event write failed; playback remains active until teardown")
        }
        return sent
    }

    /** The phone returned the host UI to ScreenMain; keep audio policy on the phone and release video locally. */
    fun changeToAudioOnly() {
        if (closed) {
            log("CarPlayVideoSettings ScreenMain ignored: RCS already closed")
            return
        }
        val status = controller.stop()
        log("CarPlayVideoSettings ScreenMain -> audio-only local stop status=$status")
    }

    override fun close() {
        if (closed) return
        log(
            "CarPlayVideoSettings closing streamID=$streamId connectionID=${unsigned(streamConnectionId)} " +
                "observedGroup=${observedGroupId?.let(::unsigned) ?: "none"} active=${controller.isVideoPlaybackActive}",
        )
        closed = true
        sender = null
        sharedSender = null
        observedGroupId = null
        synchronized(properties) { properties.clear() }
        if (stopControllerOnClose) {
            val status = controller.stop()
            log("CarPlayVideoSettings closed; local playback stop status=$status")
        } else {
            log("CarPlayVideoSettings closed; playback controller retained for the command RCS")
        }
    }

    private fun dispatch(message: Map<String, Any?>): Outcome {
        val settings = map(message[OPACK_SETTINGS_KEY])
        if (settings != null) {
            synchronized(properties) { properties.putAll(settings) }
            log(
                "CarPlayVideoSettings OPACK settings messageType=${message[OPACK_MESSAGE_TYPE_KEY]} " +
                    "keys=${settings.keys.sorted()}",
            )
            return Outcome()
        }
        if (message.containsKey(OPACK_VALUE_KEY)) {
            log(
                "CarPlayVideoSettings OPACK control messageType=${message[OPACK_MESSAGE_TYPE_KEY]} " +
                    "value=${summarize(message[OPACK_VALUE_KEY])}",
            )
            return Outcome()
        }
        val kind = message["kind"] as? String
        val messageId = message["messageID"] as? Number
        if (kind == "response" || kind == "notification") {
            // The phone also sends its own replies and notifications on this channel. They carry
            // no request to answer, so acknowledge them at the transport layer rather than
            // reporting OSStatus -4 for what looks like an unknown command.
            log(
                "CarPlayVideoSettings peer $kind messageID=${messageId ?: "none"} " +
                    "keys=${message.keys.sorted()}",
            )
            return Outcome()
        }
        return when (message["type"] as? String) {
            "insertPlayQueueItem" -> {
                val item = parseItem(message)
                val afterUuid = itemUuid(message["itemAfter"])
                log(
                    "CarPlayVideoSettings insert uuid=${item.uuid ?: "missing"} " +
                        "after=${afterUuid ?: "head"} start=${item.startPositionSeconds}s " +
                        "tls=${item.tlsEnabled} url=${redactUrl(item.contentLocation)} " +
                        "metadataKeys=${item.metadata.keys.sorted()}",
                )
                Outcome(controller.insert(item, afterUuid))
            }
            "removePlayQueueItem" -> {
                val uuid = itemUuid(message["item"])
                log("CarPlayVideoSettings remove uuid=${uuid ?: "missing/all"}")
                Outcome(controller.remove(uuid))
            }
            "setRate" -> {
                val rate = (message["rate"] as? Number)?.toDouble() ?: 0.0
                log(
                    "CarPlayVideoSettings setRate rate=$rate " +
                        "networkClockTime=${message["networkClockTime"] ?: "missing"} " +
                        "wallClockTimeDiff=${message["wallClockTimeDiff"] ?: "missing"}",
                )
                Outcome(controller.setRate(rate))
            }
            "setProperty" -> {
                val property = message["property"] as? String
                    ?: return Outcome(OSSTATUS_PARAM)
                val value = message["value"]
                synchronized(properties) { properties[property] = value }
                log(
                    "CarPlayVideoSettings setProperty property=$property " +
                        "item=${itemUuid(message["item"]) ?: "session"} value=${summarize(value)}",
                )
                Outcome()
            }
            "stop" -> {
                log("CarPlayVideoSettings stop requested")
                Outcome(controller.stop())
            }
            "playbackInfo" -> {
                val snapshot = controller.snapshot()
                log(
                    "CarPlayVideoSettings playbackInfo id=${messageId ?: "missing"} " +
                        "duration=${snapshot.durationSeconds}s position=${snapshot.positionSeconds}s " +
                        "rate=${snapshot.rate} ready=${snapshot.readyToPlay} " +
                        "loadedRanges=${snapshot.loadedTimeRanges.size} " +
                        "seekableRanges=${snapshot.seekableTimeRanges.size}",
                )
                requestOutcome(
                    kind,
                    messageId,
                    linkedMapOf("info" to snapshot.toPlist()),
                )
            }
            "seek" -> {
                val time = parseTime(message["time"])
                    ?: return requestError(kind, messageId, OSSTATUS_PARAM)
                log(
                    "CarPlayVideoSettings seek id=${messageId ?: "missing"} value=${time.value} " +
                        "timescale=${time.timescale} flags=${time.flags} epoch=${time.epoch} " +
                        "seconds=${time.seconds() ?: "invalid"}",
                )
                val status = controller.seek(time)
                if (status == 0) requestOutcome(kind, messageId, emptyMap())
                else requestError(kind, messageId, status)
            }
            "property" -> {
                val property = message["property"] as? String
                    ?: return requestError(kind, messageId, OSSTATUS_PARAM)
                if (property == "selectedMediaArray") {
                    val value = synchronized(properties) { properties[property] } ?: emptyList<Any?>()
                    log(
                        "CarPlayVideoSettings property id=${messageId ?: "missing"} " +
                            "property=$property value=${summarize(value)}",
                    )
                    requestOutcome(kind, messageId, linkedMapOf("value" to value))
                } else {
                    log(
                        "CarPlayVideoSettings unsupported property id=${messageId ?: "missing"} " +
                            "property=$property",
                    )
                    requestError(kind, messageId, OSSTATUS_UNIMPLEMENTED)
                }
            }
            "authorizeItem", "streamingKey", "unhandledURL" -> {
                log(
                    "CarPlayVideoSettings unsupported type=${message["type"]} " +
                        "id=${messageId ?: "missing"} keys=${message.keys.sorted()}",
                )
                requestError(kind, messageId, OSSTATUS_UNIMPLEMENTED)
            }
            else -> {
                log(
                    "CarPlayVideoSettings unknown type=${message["type"] ?: "missing"} " +
                        "id=${messageId ?: "missing"} keys=${message.keys.sorted()}",
                )
                requestError(kind, messageId, OSSTATUS_UNIMPLEMENTED)
            }
        }
    }

    private fun parseItem(message: Map<String, Any?>): VideoPlaybackItem {
        val item = map(message["item"]) ?: throw IllegalArgumentException("Missing playback item")
        val url = (item["Content-Location"] as? String)
            ?: (item["Offline-HLS-Content-Location"] as? String)
            ?: throw IllegalArgumentException("Missing Content-Location")
        return VideoPlaybackItem(
            uuid = item["uuid"] as? String,
            contentLocation = url,
            startPositionSeconds = (item["Start-Position"] as? Number)?.toDouble() ?: 0.0,
            tlsEnabled = item["IsTLSEnabled"] as? Boolean ?: url.startsWith("https://"),
            metadata = map(message["metadata"]) ?: emptyMap(),
        )
    }

    private fun parseTime(value: Any?): VideoPlaybackTime? {
        val time = map(value) ?: return null
        return VideoPlaybackTime(
            value = (time["value"] as? Number)?.toLong() ?: return null,
            timescale = (time["timescale"] as? Number)?.toInt() ?: return null,
            flags = (time["flags"] as? Number)?.toLong() ?: 0,
            epoch = (time["epoch"] as? Number)?.toLong() ?: 0,
        )
    }

    private fun requestOutcome(
        kind: String?,
        messageId: Number?,
        response: Map<String, Any?>,
    ): Outcome {
        if (kind != "request" || messageId == null) {
            log("CarPlayVideoSettings request rejected: kind=${kind ?: "missing"} messageID=${messageId ?: "missing"}")
            return Outcome(OSSTATUS_PARAM)
        }
        return Outcome(
            innerResponse = linkedMapOf(
                "kind" to "response",
                "messageID" to messageId,
                "response" to response,
            ),
        )
    }

    private fun requestError(kind: String?, messageId: Number?, status: Int): Outcome {
        if (kind != "request" || messageId == null) {
            log(
                "CarPlayVideoSettings error has no inner reply: kind=${kind ?: "missing"} " +
                    "messageID=${messageId ?: "missing"} status=$status",
            )
            return Outcome(status)
        }
        return Outcome(
            innerResponse = linkedMapOf(
                "kind" to "response",
                "messageID" to messageId,
                "error" to status,
            ),
        )
    }

    private fun replyError(
        packet: ApTransportPackage,
        status: Int,
        reply: (ApTransportPackage) -> Boolean,
    ): Boolean {
        if (packet.packageType != ApTransportCodec.TYPE_SYNC) {
            log(
                "CarPlayVideoSettings cannot report decode error status=$status for async packet " +
                    "group=${unsigned(packet.groupId)}",
            )
            return false
        }
        log(
            "CarPlayVideoSettings tx transport error status=$status group=${unsigned(packet.groupId)} " +
                "replyToken=${unsigned(packet.replyToken)}",
        )
        return reply(
            ApTransportPackage(
                packageType = ApTransportCodec.TYPE_REPLY,
                groupId = packet.groupId,
                messageType = 0,
                replyToken = packet.replyToken,
                replyStatus = status,
                payload = ByteArray(0),
            ),
        )
    }

    private fun decodeInner(payload: ByteArray): Map<String, Any?>? {
        val opack = runCatching { OpackCodec.decodeDictionary(payload) }.getOrNull()
        val opackData = opack?.get("data")
        if (opackData is Map<*, *>) {
            return linkedMapOf(
                OPACK_SETTINGS_KEY to map(opackData),
                OPACK_MESSAGE_TYPE_KEY to opack["messageType"],
            )
        }
        if (opack != null && opack.containsKey("data") && opackData !is ByteArray) {
            return linkedMapOf(
                OPACK_VALUE_KEY to opackData,
                OPACK_MESSAGE_TYPE_KEY to opack["messageType"],
            )
        }
        val rootBytes = (opackData as? ByteArray) ?: payload
        return runCatching {
            val root = map(BplistCodec.decode(rootBytes)) ?: return null
            val wrapped = map(root["params"])
                ?.get("data")
                ?.let { it as? ByteArray }
                ?.let(BplistCodec::decode)
                ?.let(::map)
            // The capture flag is authoritative for encoding replies, but accepting either shape
            // here makes diagnostics resilient to peers that retain the generic RCS wrapper.
            wrapped ?: root
        }.getOrNull()
    }

    private fun wrapInner(inner: Map<String, Any?>): ByteArray = if (sendMessageAsIs) {
        OpackCodec.encodeDictionary(
            linkedMapOf(
                "data" to BplistCodec.encode(inner),
                "messageType" to DATASTREAM_MESSAGE_TYPE_RCS,
            ),
        )
    } else {
        BplistCodec.encode(
            linkedMapOf(
                "params" to linkedMapOf(
                    "data" to BplistCodec.encode(inner),
                ),
            ),
        )
    }

    private fun itemUuid(value: Any?): String? = when (value) {
        is String -> value
        else -> map(value)?.get("uuid") as? String
    }

    private fun unsigned(value: Long): String = java.lang.Long.toUnsignedString(value)

    private fun redactUrl(value: String): String = runCatching {
        val uri = URI(value)
        URI(uri.scheme, null, uri.host, uri.port, uri.path, null, null).toString()
    }.getOrDefault("<invalid-url>")

    private fun summarize(value: Any?): String = when (value) {
        null -> "null"
        is ByteArray -> "data(${value.size})"
        is Map<*, *> -> "map(keys=${value.keys.map { it.toString() }.sorted()})"
        is Collection<*> -> "list(size=${value.size})"
        else -> value.toString().take(160)
    }

    private fun ByteArray.prefixHex(limit: Int = 96): String =
        copyOf(minOf(size, limit)).joinToString("") { "%02x".format(it.toInt() and 0xff) } +
            if (size > limit) "..." else ""

    private fun map(value: Any?): Map<String, Any?>? {
        val source = value as? Map<*, *> ?: return null
        return source.entries.associateTo(LinkedHashMap(source.size)) { (key, entry) ->
            key.toString() to entry
        }
    }

    private data class Outcome(
        val status: Int = 0,
        val innerResponse: Map<String, Any?>? = null,
    )

    private companion object {
        const val TAG = "xcertplay-usb"
        const val OSSTATUS_PARAM = -50
        const val OSSTATUS_UNIMPLEMENTED = -4
        const val OSSTATUS_INTERNAL = -1
        const val DATASTREAM_MESSAGE_TYPE_RCS = 2L
        const val OPACK_SETTINGS_KEY = "__opackSettings"
        const val OPACK_VALUE_KEY = "__opackValue"
        const val OPACK_MESSAGE_TYPE_KEY = "__opackMessageType"
        val VIDEO_COMMANDS = setOf(
            "authorizeItem",
            "insertPlayQueueItem",
            "removePlayQueueItem",
            "stop",
            "setRate",
            "setProperty",
            "seek",
            "playbackInfo",
            "property",
            "streamingKey",
            "unhandledURL",
        )
    }
}
