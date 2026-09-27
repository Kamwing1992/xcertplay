package com.shilapi.xcertplay.airplay

import android.util.Log
import com.shilapi.xcertplay.mfi.MfiAuthenticator
import com.shilapi.xcertplay.transport.BlockingDuplexByteStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.Closeable
import java.math.BigInteger
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.security.SecureRandom
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

data class AirPlayDeviceInfo(
    val name: String,
    val deviceId: String,
    val wifiMac: String,
    val model: String,
)

/** Session lifecycle and command callbacks for the driver/UI layer. */
interface AirPlaySessionListener {
    fun onSessionActive(session: AirPlaySession) {}
    fun onSessionEnded(session: AirPlaySession) {}
    fun onTransportError(message: String) {}
    fun onDeviceInfo(session: AirPlaySession, info: AirPlayDeviceInfo) {}
    fun onHostUiRequested(session: AirPlaySession) {}
    fun onCommand(session: AirPlaySession, type: String, params: Map<String, Any?>) {}
    fun onDebugLog(message: String) {}
}

/** Stream transport seam; media decode/render is supplied by a later layer. */
interface AirPlayMediaHandler {
    fun onScreen(session: AirPlaySession, type: Int, stream: Map<String, Any?>): Int? = null
    fun onAudio(session: AirPlaySession, type: Int, stream: Map<String, Any?>): Map<String, Any?>? = null
    fun onDataStream(session: AirPlaySession, stream: Map<String, Any?>): Map<String, Any?>? = null
    fun onFeedback(session: AirPlaySession): Map<String, Any?>? = null
    fun onTeardown(session: AirPlaySession, type: Int) {}
    fun onSessionClosed(session: AirPlaySession) {}
    fun setIapTunnelHandler(handler: ((BlockingDuplexByteStream) -> Boolean)?) {}
    fun onSetupResponseSent(session: AirPlaySession) {}
    fun videoPlaybackController(): VideoPlaybackController? = null
}

/**
 * One CarPlay AirPlay control connection.
 *
 * It owns the RTSP framing, pairing/auth/info routing, the encrypted event channel used for HID
 * input, stream SETUP/TEARDOWN routing, the NTP timing exchange, and the keep-alive socket.
 */
class AirPlaySession(
    private val socket: Socket,
    private val config: AirPlayConfig,
    private val identity: AirPlayIdentity,
    private val pairings: PairingStore,
    private val mfi: MfiAuthenticator?,
    private val listener: AirPlaySessionListener,
    private val media: AirPlayMediaHandler,
) : Closeable {
    internal data class ActiveStream(
        val type: Int,
        val clientTypeUuid: String? = null,
        /**
         * The streamID this session returned in SETUP. The phone echoes it in TEARDOWN, and
         * several type-130 streams can share a type and a missing clientTypeUUID, so tearing a
         * stream down by type alone would close unrelated streams.
         */
        val streamId: Long? = null,
    )

    internal val pairSetup = PairSetup(identity, pairings)
    internal val pairVerify = PairVerify(identity, pairings)
    internal var cipher: ControlCipher? = null
    internal var encBuf = ByteArray(0)
    internal var deviceBtMac = ""
    internal val activeStreams = linkedSetOf<ActiveStream>()

    private val closed = AtomicBoolean(false)
    private val notified = AtomicBoolean(false)
    private var eventServer: ServerSocket? = null
    private var eventSocket: Socket? = null
    private var eventCipher: ControlCipher? = null
    private var eventOutput: BufferedOutputStream? = null
    private var controlOutput: BufferedOutputStream? = null
    private var eventCseq = 0
    private var pendingNightMode: Boolean? = null
    private val firstTouchSendLogged = AtomicBoolean(false)
    private val touchSendFailureLogged = AtomicBoolean(false)
    private val ntp = NtpClock()
    private var keepAliveSocket: DatagramSocket? = null
    private var keepAliveThread: Thread? = null
    private val eventWriteLock = Any()
    private val controlWriteLock = Any()
    private val eventThreads = CopyOnWriteArrayList<Thread>()
    private val streamIdCounter = AtomicLong(1)
    private val random = SecureRandom()
    /**
     * Runtime playback-command RCS sessions keyed by the streamID returned in SETUP. The phone
     * replaces this channel whenever it starts a new playback session, and it still addresses the
     * previous one by streamID while shutting it down, so both must stay individually reachable.
     */
    private val videoPlaybackRcs = ConcurrentHashMap<Long, VideoPlaybackRcsSession>()
    /** CarPlayVideoSettings RCS, opaque during the observed dedicated-channel bootstrap. */
    @Volatile private var videoSettingsRcs: VideoPlaybackRcsSession? = null
    @Volatile private var videoPlaybackDataStream: ApTransportDataStream? = null

    val host: String = socket.inetAddress?.hostAddress ?: ""
    val localAddress: InetAddress? = socket.localAddress
    internal val isWireless: Boolean = config.wirelessAudio
    private val peerAddress: InetAddress? = socket.inetAddress
    internal val remoteAddress: InetAddress?
        get() = (socket.remoteSocketAddress as? InetSocketAddress)?.address
    val controllerId: String? get() = pairVerify.verifiedControllerId
    val sharedSecret: ByteArray? get() = pairVerify.shared?.copyOf()

    fun syncedNtp(): BigInteger = ntp.syncedNtp()

    internal fun logDebug(message: String) = debugLog(message)

    internal fun logTrace(message: String) = trace(message)

    fun start() {
        Thread(::runControl, "airplay-control").apply {
            isDaemon = true
            start()
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        safeClose(socket)
        closeVideoPlayback("AirPlay session closing")
        try {
            media.onSessionClosed(this)
        } catch (error: Exception) {
            Log.w(TAG, "airplay media stream teardown failed", error)
        }
        teardown()
        if (notified.compareAndSet(false, true)) listener.onSessionEnded(this)
    }

    fun isVideoPlaybackActive(): Boolean =
        videoPlaybackRcs.values.any { it.active } || videoSettingsRcs?.active == true

    fun sendVideoPlaybackBackButtonEvent(): Boolean =
        (videoPlaybackRcs.values.firstOrNull { it.active } ?: videoSettingsRcs)
            ?.sendBackButtonEvent() == true

    fun sendCommand(command: Map<String, Any?>): Boolean = synchronized(eventWriteLock) {
        sendCommandLocked(command)
    }

    private fun sendCommandLocked(command: Map<String, Any?>): Boolean =
        sendCommandBodyLocked(
            body = BplistCodec.encode(command),
            streamId = null,
            description = "type=${command["type"] ?: "missing"}",
        )

    private fun sendVideoPlaybackStreamCommand(streamId: Long, body: ByteArray): Boolean =
        synchronized(eventWriteLock) {
            sendCommandBodyLocked(
                body = body,
                streamId = streamId,
                description = "videoPlayback streamID=$streamId",
            )
        }

    private fun sendCommandBodyLocked(
        body: ByteArray,
        streamId: Long?,
        description: String,
    ): Boolean {
        if (eventSocket == null) return false
        val cipher = eventCipher ?: return false
        val output = eventOutput ?: return false
        eventCseq++
        val head = "POST /command RTSP/1.0\r\n" +
            "Content-Type: $PLIST_CONTENT_TYPE\r\n" +
            (streamId?.let { "X-Apple-StreamID: $it\r\n" } ?: "") +
            "Content-Length: ${body.size}\r\n" +
            "CSeq: $eventCseq\r\n\r\n"
        trace("airplay event tx headers=$head bodyHex=${body.toHex()}")
        return try {
            val bytes = cipher.encrypt(head.toByteArray(Charsets.US_ASCII) + body)
            output.write(bytes)
            output.flush()
            true
        } catch (error: Exception) {
            Log.w(TAG, "airplay event command failed $description", error)
            close()
            false
        }
    }

    fun sendTouch(contacts: List<AirPlayContact>): Boolean {
        val scaled = contacts.map {
            it.copy(x = it.x * config.main.widthPixels, y = it.y * config.main.heightPixels)
        }
        val report = AirPlayHid.touchReport(scaled)
        val sent = sendHidReport(AirPlayHid.TOUCH_HID_UID, report)
        if (sent && firstTouchSendLogged.compareAndSet(false, true)) {
            val first = scaled.firstOrNull()
            Log.i(
                TAG,
                "airplay touch report sent contacts=${scaled.size} first=" +
                    "(${first?.x},${first?.y},down=${first?.down}) report=${report.toHexString()}",
            )
        } else if (!sent && touchSendFailureLogged.compareAndSet(false, true)) {
            Log.w(TAG, "airplay touch dropped: event channel is not ready")
        }
        return sent
    }

    fun sendKnob(state: AirPlayKnobState, momentary: Boolean = true) {
        sendHidReport(AirPlayHid.KNOB_HID_UID, AirPlayHid.knobReport(state))
        if (momentary) sendHidReport(AirPlayHid.KNOB_HID_UID, AirPlayHid.knobReport(AirPlayKnobState()))
    }

    fun sendKnobSelect(down: Boolean) =
        sendHidReport(AirPlayHid.KNOB_HID_UID, AirPlayHid.knobReport(AirPlayKnobState(select = down)))

    fun sendMedia(index: Int) {
        sendHidReport(AirPlayHid.MEDIA_HID_UID, AirPlayHid.mediaReport(index))
        sendHidReport(AirPlayHid.MEDIA_HID_UID, AirPlayHid.mediaReport(0))
    }

    fun sendTelephony(index: Int) {
        sendHidReport(AirPlayHid.TELEPHONY_HID_UID, AirPlayHid.telephonyReport(index))
        sendHidReport(AirPlayHid.TELEPHONY_HID_UID, AirPlayHid.telephonyReport(0))
    }

    fun invokeSiri() {
        sendCommand(linkedMapOf("type" to "requestSiri", "params" to linkedMapOf("siriAction" to 2)))
        sendCommand(linkedMapOf("type" to "requestSiri", "params" to linkedMapOf("siriAction" to 3)))
    }

    fun sendIapMessage(data: ByteArray, timeoutMillis: Long = 0L): Boolean {
        require(timeoutMillis >= 0) { "timeoutMillis must not be negative" }
        val command = linkedMapOf<String, Any?>(
            "type" to "iAPSendMessage",
            "params" to linkedMapOf("data" to data),
        )
        if (timeoutMillis == 0L) return sendCommand(command)

        val deadlineNanos = System.nanoTime() + timeoutMillis * NANOS_PER_MILLISECOND
        while (true) {
            val sent = synchronized(eventWriteLock) {
                if (eventSocket == null || eventCipher == null) {
                    null
                } else {
                    sendCommandLocked(command)
                }
            }
            if (sent != null) return sent
            if (closed.get()) return false

            val remainingNanos = deadlineNanos - System.nanoTime()
            if (remainingNanos <= 0) return false
            val sleepMillis = minOf(
                EVENT_READY_POLL_MILLIS,
                (remainingNanos + NANOS_PER_MILLISECOND - 1) / NANOS_PER_MILLISECOND,
            ).coerceAtLeast(1)
            try {
                Thread.sleep(sleepMillis)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }
    }

    fun setNightMode(night: Boolean): Boolean = synchronized(eventWriteLock) {
        pendingNightMode = night
        sendPendingNightModeLocked()
    }

    private fun sendPendingNightModeLocked(): Boolean {
        val night = pendingNightMode ?: return true
        val sent = sendCommandLocked(
            linkedMapOf("type" to "setNightMode", "params" to linkedMapOf("nightMode" to night)),
        )
        if (sent) pendingNightMode = null
        return sent
    }

    private fun sendHidReport(uid: Int, report: ByteArray): Boolean =
        sendCommand(
            linkedMapOf(
                "type" to "hidSendReport",
                "uuid" to uid.toString(16),
                "hidReport" to report,
            ),
        )

    private fun runControl() {
        val input = BufferedInputStream(socket.getInputStream())
        val output = BufferedOutputStream(socket.getOutputStream())
        controlOutput = output
        var accumulated = ByteArray(0)
        val buffer = ByteArray(READ_CHUNK_BYTES)
        var closeReason = "session closed"
        try {
            while (!closed.get()) {
                val count = input.read(buffer)
                if (count < 0) {
                    closeReason = "peer EOF"
                    break
                }
                var plaintext = buffer.copyOf(count)
                val activeCipher = cipher
                if (activeCipher != null) {
                    encBuf += plaintext
                    val decrypted = try {
                        activeCipher.decrypt(encBuf)
                    } catch (error: Exception) {
                        closeReason = "control decrypt failed: ${error.message ?: error.javaClass.simpleName}"
                        Log.e(TAG, "airplay $closeReason encrypted=${encBuf.size}", error)
                        break
                    }
                    encBuf = decrypted.rest
                    plaintext = decrypted.data
                }
                accumulated += plaintext
                while (accumulated.isNotEmpty()) {
                    if (ApTransportCodec.isPotentialPrefix(accumulated)) {
                        val decoded = decodeSharedPacket(accumulated, SharedChannel.CONTROL) ?: break
                        accumulated = decoded.rest
                        handleSharedTransport(decoded.packet, SharedChannel.CONTROL)
                        continue
                    }
                    val parsed = RtspMessage.parseFirst(accumulated)
                    val request = parsed.messages.firstOrNull() ?: break
                    accumulated = parsed.rest
                    handleControlRequest(request)
                }
                notifySetupResponseSent()
            }
        } catch (error: Exception) {
            closeReason = "control I/O failed: ${error.message ?: error.javaClass.simpleName}"
            if (!closed.get()) Log.e(TAG, "airplay $closeReason", error)
        } finally {
            controlOutput = null
            debugLog("airplay control closing reason=$closeReason activeStreams=$activeStreams")
            close()
        }
    }

    private fun handleControlRequest(request: RtspMessage.Request) {
        val cseq = request.headers["cseq"] ?: "-"
        val path = request.path.lowercase()
        val showInDebugOverlay =
            !path.endsWith("/feedback") &&
                !(request.method == "POST" && path.endsWith("/command"))
        debugLog(
            "airplay rx ${request.method} ${request.path} cseq=$cseq body=${request.body.size}",
            showInDebugOverlay,
        )
        trace("airplay control rx headers=${request.headers} bodyHex=${request.body.toHex()}")
        val response = try {
            handle(request)
        } catch (error: Exception) {
            Log.e(TAG, "airplay handler failed ${request.method} ${request.path} cseq=$cseq", error)
            RtspMessage.Response(status = 500)
        }
        debugLog(
            "airplay tx status=${response.status ?: 200} cseq=$cseq body=${response.body.size}",
            showInDebugOverlay,
        )
        val wire = RtspMessage.buildResponse(request, response)
        trace("airplay control tx wireHex=${wire.toHex()}")
        writeControl(wire)
        if (cipher == null && pairVerify.controlKeys != null) {
            val keys = pairVerify.controlKeys!!
            cipher = ControlCipher(keys.readKey, keys.writeKey)
            debugLog("airplay control encryption enabled")
        }
    }

    private fun writeControl(plaintext: ByteArray): Boolean = synchronized(controlWriteLock) {
        val output = controlOutput ?: return@synchronized false
        return@synchronized try {
            output.write(cipher?.encrypt(plaintext) ?: plaintext)
            output.flush()
            true
        } catch (error: Exception) {
            Log.w(TAG, "airplay shared control write failed", error)
            false
        }
    }

    private fun handle(request: RtspMessage.Request): RtspMessage.Response {
        when (request.method) {
            "SETUP" -> return handleSetup(request)
            "RECORD" -> {
                listener.onSessionActive(this)
                return RtspMessage.Response(status = 200)
            }
            "TEARDOWN" -> return handleTeardown(request)
        }

        val path = request.path.lowercase()
        return when {
            path.endsWith("/pair-setup") -> RtspMessage.Response(
                headers = mapOf("Content-Type" to PAIRING_CONTENT_TYPE),
                body = pairSetup.handle(request.body),
            )
            path.endsWith("/pair-verify") -> RtspMessage.Response(
                headers = mapOf("Content-Type" to PAIRING_CONTENT_TYPE),
                body = pairVerify.handle(request.body),
            )
            path.endsWith("/auth-setup") -> {
                val body = mfi?.let { MfiSapAuthSetup.handle(request.body, it) }
                if (body == null) RtspMessage.Response(status = 400)
                else RtspMessage.Response(headers = mapOf("Content-Type" to OCTET_CONTENT_TYPE), body = body)
            }
            path.endsWith("/info") -> {
                val info = AirPlayInfoPlist.build(config)
                if (request.body.isNotEmpty()) {
                    val requestInfo = try {
                        BplistCodec.decode(request.body).toString()
                    } catch (_: Exception) {
                        "<unparseable ${request.body.size} bytes>"
                    }
                    debugLog("airplay /info request=$requestInfo")
                }
                Log.i(
                    TAG,
                    "airplay /info features=${info["features"]} " +
                        "audioFormats=${(info["audioFormats"] as? List<*>)?.size ?: 0} " +
                        "audioLatencies=${(info["audioLatencies"] as? List<*>)?.size ?: 0}",
                )
                debugLog(
                    "airplay /info audio=${if (config.wirelessAudio) "wireless PCM+Opus" else "wired PCM"} " +
                        "microphone=${config.microphone}",
                )
                debugLog(
                    "airplay /info videoPlayback enabled=${config.videoPlayback.enabled} " +
                        "playbackCapabilities=${info["playbackCapabilities"] ?: "omitted"} " +
                        "videoPlaybackInfo=${info["videoPlaybackInfo"] ?: "omitted"}",
                )
                debugLog("airplay /info displays=${info["displays"]}")
                RtspMessage.Response(
                    headers = mapOf("Content-Type" to PLIST_CONTENT_TYPE),
                    body = BplistCodec.encode(info),
                )
            }
            request.method == "POST" && path.endsWith("/command") -> handleCommand(request)
            request.method == "POST" && path.endsWith("/feedback") -> {
                val body = media.onFeedback(this)
                if (body == null) {
                    RtspMessage.Response(status = 200)
                } else {
                    RtspMessage.Response(
                        headers = mapOf("Content-Type" to PLIST_CONTENT_TYPE),
                        body = BplistCodec.encode(body),
                    )
                }
            }
            else -> RtspMessage.Response(status = 200)
        }
    }

    private fun notifySetupResponseSent() {
        try {
            media.onSetupResponseSent(this)
        } catch (error: Exception) {
            Log.w(TAG, "airplay SETUP response callback failed", error)
        }
    }

    private fun debugLog(message: String, uiVisible: Boolean = true) {
        Log.i(TAG, message)
        if (!uiVisible) return
        try {
            listener.onDebugLog(message)
        } catch (error: Exception) {
            Log.w(TAG, "debug log callback failed", error)
        }
    }

    private fun trace(message: String) {
        try {
            listener.onDebugLog("TRACE $message")
        } catch (error: Exception) {
            Log.w(TAG, "trace log callback failed", error)
        }
    }

    private fun handleSetup(request: RtspMessage.Request): RtspMessage.Response {
        val dict = try {
            asMap(BplistCodec.decode(request.body)) ?: return RtspMessage.Response(status = 400)
        } catch (error: Exception) {
            Log.e(TAG, "airplay SETUP plist decode failed body=${request.body.size}", error)
            return RtspMessage.Response(status = 400)
        }
        debugLog("airplay SETUP keys=${dict.keys.sorted()}")
        val streams = dict["streams"] as? List<*>
        if (streams != null) {
            val responseStreams = handleStreams(streams)
            debugLog("airplay SETUP response streams=$responseStreams")
            val body = BplistCodec.encode(linkedMapOf("streams" to responseStreams))
            trace("airplay SETUP response bplistHex=${body.toHex()}")
            return RtspMessage.Response(headers = mapOf("Content-Type" to PLIST_CONTENT_TYPE), body = body)
        }

        val name = string(dict["name"])
        val deviceId = string(dict["deviceID"])
        val wifiMac = string(dict["macAddress"]).lowercase()
        val model = string(dict["model"])
        if (deviceId.isNotEmpty()) deviceBtMac = deviceId
        if (name.isNotEmpty() || deviceId.isNotEmpty() || wifiMac.isNotEmpty()) {
            listener.onDeviceInfo(this, AirPlayDeviceInfo(name, deviceId, wifiMac, model))
        }

        val peerTimingPort = long(dict["timingPort"])?.toInt() ?: 0
        val response = linkedMapOf<String, Any?>(
            "timingPort" to openTiming(peerTimingPort),
            "eventPort" to openEvent(),
        )
        if (dict["keepAliveLowPower"] == true || dict["keepAliveLowPower"] == 1L) {
            response["keepAlivePort"] = openKeepAlive()
        }
        val features = mutableListOf<String>()
        if (config.hevc) features.add("hevc")
        features.add("iAPChannel")
        features.add("viewAreas")
        if (config.cluster != null) features.add("altScreen")
        if (config.videoPlayback.enabled) {
            // Experimental force switch: intentionally advertise the formal feature even when a
            // peer omits it, while logging that deviation from normal intersection semantics.
            val offered = (dict["features"] as? List<*>)?.any { it == "videoPlayback" } == true
            features.add("videoPlayback")
            val capabilities = config.videoPlayback.capabilities
            debugLog(
                "airplay videoPlayback force-enabled offeredByPhone=$offered " +
                    "allowed=${config.videoPlayback.allowed} featuresEx=${config.videoPlayback.featuresEx} " +
                    "offlineHLS=${capabilities.supportsOfflineHls} " +
                    "v2Artwork=${capabilities.supportsV2ArtworkMetadata} " +
                    "fpsSecureStop=${capabilities.supportsFpsSecureStop} " +
                    "audioOnlyUI=${capabilities.supportsUiForAudioOnlyContent}",
            )
        }
        response["enabledFeatures"] = features
        return RtspMessage.Response(
            headers = mapOf("Content-Type" to PLIST_CONTENT_TYPE),
            body = BplistCodec.encode(response),
        )
    }

    private fun handleStreams(streams: List<*>): List<Any?> {
        val result = arrayListOf<Any?>()
        for (entry in streams) {
            val stream = asMap(entry) ?: continue
            val type = long(stream["type"])?.toInt() ?: continue
            debugLog("airplay SETUP stream type=$type payload=$stream")
            when (type) {
                STREAM_TYPE_MAIN_SCREEN, STREAM_TYPE_ALT_SCREEN -> {
                    val port = media.onScreen(this, type, stream)
                    debugLog("airplay screen stream type=$type dataPort=${port ?: "rejected"}")
                    if (port != null) {
                        activeStreams.add(ActiveStream(type))
                        result.add(linkedMapOf("type" to type, "dataPort" to port))
                    }
                }
                STREAM_TYPE_MAIN_AUDIO, STREAM_TYPE_ALT_AUDIO, STREAM_TYPE_MAIN_HIGH_AUDIO -> {
                    val streamResponse = media.onAudio(this, type, stream)
                    debugLog(
                        "airplay audio stream type=$type accepted=${streamResponse != null} " +
                            "dataPort=${streamResponse?.get("dataPort") ?: "none"} " +
                            "controlPort=${streamResponse?.get("controlPort") ?: "none"}",
                    )
                    if (streamResponse != null) {
                        activeStreams.add(ActiveStream(type))
                        result.add(streamResponse)
                    }
                }
                STREAM_TYPE_DATA -> {
                    val uuid = string(stream["clientTypeUUID"]).uppercase()
                    val streamResponse = when {
                        uuid == VIDEO_PLAYBACK_UUID && config.videoPlayback.enabled ->
                            setupVideoPlaybackStream(stream)
                        uuid == VIDEO_PLAYBACK_CONTROL_UUID && config.videoPlayback.enabled ->
                            setupVideoPlaybackControlStream(stream)
                        uuid == VIDEO_PLAYBACK_UUID -> {
                            debugLog(
                                "AirPlay CarPlayVideoSettings SETUP rejected: experimental setting disabled",
                            )
                            null
                        }
                        uuid == VIDEO_PLAYBACK_CONTROL_UUID -> {
                            debugLog(
                                "AirPlay video playback control SETUP rejected: " +
                                    "experimental setting disabled",
                            )
                            null
                        }
                        else -> media.onDataStream(this, stream)
                    }
                    debugLog(
                        "airplay data stream type=$type uuid=${uuid.ifEmpty { "missing" }} " +
                            "accepted=${streamResponse != null} " +
                            "dataPort=${streamResponse?.get("dataPort") ?: "none"}",
                    )
                    if (streamResponse != null) {
                        // Only the video playback channels are addressed by a streamID this
                        // session allocates. The media engine reports a fixed streamID for its
                        // own streams, which would collide with that range.
                        val allocatedStreamId = if (
                            uuid == VIDEO_PLAYBACK_UUID || uuid == VIDEO_PLAYBACK_CONTROL_UUID
                        ) {
                            long(streamResponse["streamID"])
                        } else {
                            null
                        }
                        activeStreams.add(
                            ActiveStream(type, uuid.ifEmpty { null }, allocatedStreamId),
                        )
                        result.add(streamResponse)
                    }
                }
                else -> debugLog("airplay unsupported stream type=$type")
            }
        }
        return result
    }

    private fun setupVideoPlaybackStream(stream: Map<String, Any?>): Map<String, Any?>? {
        val controlType = long(stream["controlType"])
        val wantsDedicatedSocket = plistBoolean(stream["wantsDedicatedSocket"])
        val sendMessageAsIs = plistBoolean(stream["sendMessageAsIs"])
        val seed = unsignedPlistDecimal(stream["seed"])
        debugLog(
            "AirPlay CarPlayVideoSettings SETUP request keys=${stream.keys.sorted()} " +
                "clientTypeUUID=${stream["clientTypeUUID"] ?: "missing"} " +
                "clientUUID=${stream["clientUUID"] ?: "missing"} " +
                "channelID=${stream["channelID"] ?: "missing"} " +
                "controlType=${controlType ?: "missing"} " +
                "seed=${seed ?: "missing"} " +
                "dedicated=$wantsDedicatedSocket sendAsIs=$sendMessageAsIs",
        )
        debugLog(
            "AirPlay CarPlayVideoSettings SETUP transport selection " +
                "controlType=${controlType ?: "missing"} mode=" +
                "${if (wantsDedicatedSocket) "dedicated" else "shared"} " +
                "seedPresent=${seed != null} sendMessageAsIs=$sendMessageAsIs",
        )
        val controller = media.videoPlaybackController() ?: run {
            debugLog("AirPlay CarPlayVideoSettings SETUP rejected: media backend has no VideoPlaybackController")
            return null
        }
        closeVideoSettings("replacing CarPlayVideoSettings stream")
        val streamId = streamIdCounter.getAndIncrement()
        return if (wantsDedicatedSocket) {
            setupDedicatedVideoPlaybackStream(
                stream = stream,
                streamId = streamId,
                seed = seed,
                sendMessageAsIs = sendMessageAsIs,
                controller = controller,
            )
        } else {
            setupSharedVideoPlaybackStream(
                stream = stream,
                streamId = streamId,
                sendMessageAsIs = sendMessageAsIs,
                controller = controller,
            )
        }
    }

    private fun setupDedicatedVideoPlaybackStream(
        stream: Map<String, Any?>,
        streamId: Long,
        seed: String?,
        sendMessageAsIs: Boolean,
        controller: VideoPlaybackController,
    ): Map<String, Any?>? {
        if (seed == null) {
            debugLog(
                "AirPlay CarPlayVideoSettings dedicated SETUP rejected: " +
                    "wantsDedicatedSocket=true but seed is missing or invalid",
            )
            return null
        }
        val shared = sharedSecret ?: run {
            debugLog(
                "AirPlay CarPlayVideoSettings dedicated SETUP rejected: " +
                    "pair-verify shared secret unavailable",
            )
            return null
        }
        val salt = "DataStream-Salt$seed".toByteArray(Charsets.US_ASCII)
        val readKey = AirPlayCrypto.hkdfSha512(
            shared,
            salt,
            DATASTREAM_OUTPUT_KEY.toByteArray(Charsets.US_ASCII),
            DATASTREAM_KEY_BYTES,
        )
        val writeKey = AirPlayCrypto.hkdfSha512(
            shared,
            salt,
            DATASTREAM_INPUT_KEY.toByteArray(Charsets.US_ASCII),
            DATASTREAM_KEY_BYTES,
        )
        val rcs = VideoPlaybackRcsSession(
            streamId = streamId,
            streamConnectionId = 0L,
            sendMessageAsIs = sendMessageAsIs,
            stopControllerOnClose = false,
            controller = controller,
            log = ::debugLog,
        )
        val transport = ApTransportDataStream(
            readKey = readKey,
            writeKey = writeKey,
            bindAddress = videoPlaybackBindAddress(),
        )
        videoSettingsRcs = rcs
        videoPlaybackDataStream = transport
        val port = try {
            transport.listen(
                object : ApTransportDataStream.Listener {
                    override fun onOpen(remoteAddress: String?) {
                        debugLog(
                            "AirPlay CarPlayVideoSettings dedicated DataStream connected " +
                                "remote=${remoteAddress ?: "unknown"} streamID=$streamId seed=$seed",
                        )
                    }

                    override fun onPacket(packet: ApTransportPackage) {
                        handleDedicatedVideoPlaybackTransport(packet, transport, rcs)
                    }

                    override fun onDebug(message: String) {
                        debugLog("AirPlay CarPlayVideoSettings $message")
                    }

                    override fun onClosed(cause: Throwable?) {
                        debugLog(
                            "AirPlay CarPlayVideoSettings dedicated DataStream closed " +
                                "streamID=$streamId reason=" +
                                "${cause?.message ?: "peer EOF"} active=${rcs.active}",
                        )
                        if (videoPlaybackDataStream === transport) {
                            videoPlaybackDataStream = null
                            if (videoSettingsRcs === rcs) videoSettingsRcs = null
                            transport.close()
                            rcs.close()
                        }
                    }
                },
            )
        } catch (error: Throwable) {
            debugLog(
                "AirPlay CarPlayVideoSettings dedicated listener failed streamID=$streamId " +
                    "error=${error.message ?: error.javaClass.simpleName}",
            )
            if (videoPlaybackDataStream === transport) videoPlaybackDataStream = null
            if (videoSettingsRcs === rcs) videoSettingsRcs = null
            transport.close()
            rcs.close()
            return null
        }
        debugLog(
            "AirPlay CarPlayVideoSettings dedicated SETUP channelID=${stream["channelID"] ?: "missing"} " +
                "controlType=${stream["controlType"] ?: "missing"} streamID=$streamId " +
                "dataPort=$port seed=$seed sendMessageAsIs=$sendMessageAsIs",
        )
        return linkedMapOf(
            "type" to STREAM_TYPE_DATA,
            "streamID" to streamId,
            "dataPort" to port,
        )
    }

    private fun setupSharedVideoPlaybackStream(
        stream: Map<String, Any?>,
        streamId: Long,
        sendMessageAsIs: Boolean,
        controller: VideoPlaybackController,
        assign: (VideoPlaybackRcsSession) -> Unit = { videoSettingsRcs = it },
        label: String = "CarPlayVideoSettings",
        stopControllerOnClose: Boolean = false,
    ): Map<String, Any?> {
        var connectionId = random.nextLong()
        if (connectionId == 0L) connectionId = 1L
        val rcs = VideoPlaybackRcsSession(
            streamId = streamId,
            streamConnectionId = connectionId,
            sendMessageAsIs = sendMessageAsIs,
            stopControllerOnClose = stopControllerOnClose,
            controller = controller,
            log = ::debugLog,
        )
        assign(rcs)
        debugLog(
            "AirPlay $label shared SETUP channelID=${stream["channelID"] ?: "missing"} " +
                "controlType=${stream["controlType"] ?: "missing"} streamID=$streamId " +
                "streamConnectionID=${java.lang.Long.toUnsignedString(connectionId)} " +
                "sendMessageAsIs=$sendMessageAsIs",
        )
        return linkedMapOf(
            "type" to STREAM_TYPE_DATA,
            "streamID" to streamId,
            "streamConnectionID" to unsignedPlistInteger(connectionId),
        )
    }

    /**
     * iOS 27 opens this controlType=1 RCS when a video-capable app starts playback. It is
     * distinct from the dedicated CarPlayVideoSettings bootstrap channel and uses shared
     * APTransport framing.
     */
    private fun setupVideoPlaybackControlStream(stream: Map<String, Any?>): Map<String, Any?>? {
        val controller = media.videoPlaybackController() ?: run {
            debugLog("AirPlay video playback control SETUP rejected: media backend unavailable")
            return null
        }
        val controlType = long(stream["controlType"])
        if (controlType != 1L) {
            debugLog(
                "AirPlay video playback control SETUP rejected: expected controlType=1 " +
                    "actual=${controlType ?: "missing"} payload=$stream",
            )
            return null
        }
        closeVideoPlaybackControl("replacing playback command RCS")
        val streamId = streamIdCounter.getAndIncrement()
        val response = setupSharedVideoPlaybackStream(
            stream = stream,
            streamId = streamId,
            sendMessageAsIs = false,
            controller = controller,
            assign = { videoPlaybackRcs[streamId] = it },
            label = "video-playback-control",
            stopControllerOnClose = true,
        )
        debugLog(
            "AirPlay video playback control SETUP accepted uuid=$VIDEO_PLAYBACK_CONTROL_UUID " +
                "clientUUID=${stream["clientUUID"] ?: "missing"} " +
                "channelID=${stream["channelID"] ?: "missing"} response=$response",
        )
        return response
    }

    private fun videoPlaybackBindAddress(): InetAddress = if (isWireless) {
        InetAddress.getByName("::")
    } else {
        localAddress ?: when (remoteAddress) {
            is Inet6Address -> InetAddress.getByName("::")
            is Inet4Address -> InetAddress.getByName("0.0.0.0")
            else -> InetAddress.getByName("0.0.0.0")
        }
    }

    private fun closeVideoPlayback(reason: String) {
        closeVideoPlaybackControl(reason)
        closeVideoSettings(reason)
    }

    private fun closeVideoPlaybackControl(reason: String) = closeVideoPlaybackControl(null, reason)

    /** Closes one playback-command RCS, or every one when [streamId] is null. */
    private fun closeVideoPlaybackControl(streamId: Long?, reason: String) {
        val closing = if (streamId == null) {
            val all = videoPlaybackRcs.values.toList()
            videoPlaybackRcs.clear()
            all
        } else {
            listOfNotNull(videoPlaybackRcs.remove(streamId))
        }
        closing.forEach { rcs ->
            debugLog(
                "AirPlay video playback control closing reason=$reason " +
                    "streamID=${rcs.streamId} active=${rcs.active}",
            )
            rcs.close()
        }
    }

    private fun closeVideoSettings(reason: String) {
        val transport = videoPlaybackDataStream
        val rcs = videoSettingsRcs
        videoPlaybackDataStream = null
        videoSettingsRcs = null
        if (transport != null || rcs != null) {
            debugLog(
                "AirPlay CarPlayVideoSettings closing reason=$reason " +
                    "transport=${if (transport == null) "shared/none" else "dedicated"} " +
                    "rcsPresent=${rcs != null} active=${rcs?.active == true}",
            )
        }
        transport?.close()
        rcs?.close()
    }

    private fun handleCommand(request: RtspMessage.Request): RtspMessage.Response {
        val streamIdHeader = request.headers["x-apple-streamid"]
        if (streamIdHeader != null) {
            val streamId = streamIdHeader.toLongOrNull()
            if (streamId == null) {
                debugLog(
                    "airplay RCS /command rejected: invalid X-Apple-StreamID=$streamIdHeader " +
                        "body=${request.body.size}",
                )
                return RtspMessage.Response(status = 400)
            }
            val rcs = videoPlaybackRcs[streamId]
                ?: videoSettingsRcs?.takeIf { it.streamId == streamId }
            if (rcs == null) {
                debugLog(
                    "airplay RCS /command has no session streamID=$streamId " +
                        "playbackStreams=${videoPlaybackRcs.keys.sorted()} " +
                        "settingsStream=${videoSettingsRcs?.streamId ?: "none"} " +
                        "body=${request.body.size}",
                )
                return RtspMessage.Response(status = 200)
            }
            val handled = rcs.handleSharedCommand(request.body) { responseBody ->
                sendVideoPlaybackStreamCommand(streamId, responseBody)
            }
            debugLog(
                "airplay RCS /command streamID=$streamId handled=$handled " +
                    "body=${request.body.size} active=${rcs.active}",
            )
            return RtspMessage.Response(status = 200)
        }
        val body = try {
            asMap(BplistCodec.decode(request.body)) ?: emptyMap()
        } catch (_: Exception) {
            emptyMap<String, Any?>()
        }
        val type = string(body["type"])
        val params = asMap(body["params"]) ?: emptyMap()
        debugLog("airplay command type=$type keys=${params.keys.sorted()}")
        if (type == "modesChanged") {
            val resources = (params["resources"] as? List<*>)
                ?.mapNotNull(::asMap)
                ?.sortedBy { long(it["resourceID"]) ?: Long.MAX_VALUE }
                ?.joinToString { resource ->
                    "id=${long(resource["resourceID"])} owner=${long(resource["entity"])} " +
                        "permanent=${long(resource["permanentEntity"])}"
                }
            debugLog("airplay modesChanged resources=${resources ?: "missing"}")
        }
        if (type == "requestUI") listener.onHostUiRequested(this)
        if (type == "ScreenMain") {
            debugLog(
                "airplay command ScreenMain: applying documented audio-only fallback " +
                    "playbackRcs=${videoPlaybackRcs.size} settingsRcs=${videoSettingsRcs != null} " +
                    "active=${isVideoPlaybackActive()}",
            )
            (videoPlaybackRcs.values.firstOrNull() ?: videoSettingsRcs)?.changeToAudioOnly()
        }
        listener.onCommand(this, type, params)
        return RtspMessage.Response(status = 200)
    }

    private fun handleTeardown(request: RtspMessage.Request): RtspMessage.Response {
        var decodedBody: Any? = null
        val requestedStreams = try {
            val decoded = BplistCodec.decode(request.body)
            decodedBody = decoded
            val dict = asMap(decoded)
            (dict?.get("streams") as? List<*>)
                ?.mapNotNull { entry ->
                    val stream = asMap(entry) ?: return@mapNotNull null
                    val type = long(stream["type"])?.toInt() ?: return@mapNotNull null
                    ActiveStream(
                        type = type,
                        clientTypeUuid = string(stream["clientTypeUUID"]).uppercase().ifEmpty { null },
                        streamId = long(stream["streamID"]),
                    )
                }
                ?: emptyList()
        } catch (_: Exception) {
            emptyList()
        }

        debugLog(
            "airplay TEARDOWN streams=$requestedStreams activeBefore=$activeStreams " +
                "body=${request.body.size} bytes payload=$decodedBody",
        )
        trace("airplay TEARDOWN raw${request.body.size}Hex=${request.body.toHex()}")

        if (requestedStreams.isEmpty()) {
            activeStreams.toList().forEach(::teardownStream)
            activeStreams.clear()
        } else {
            requestedStreams.forEach { requested ->
                // Match on the narrowest identity the phone supplied. A TEARDOWN that names a
                // streamID must never widen to every stream of that type: the iAP tunnel, the
                // VideoSettings bootstrap and the playback RCS all share type 130.
                val matches = activeStreams.filter { active ->
                    when {
                        requested.streamId != null ->
                            active.streamId == requested.streamId && active.type == requested.type
                        requested.clientTypeUuid != null ->
                            active.type == requested.type &&
                                active.clientTypeUuid == requested.clientTypeUuid
                        else -> active.type == requested.type
                    }
                }
                matches.forEach { active ->
                    if (activeStreams.remove(active)) teardownStream(active)
                }
            }
        }
        return RtspMessage.Response(status = 200)
    }

    private fun teardownStream(stream: ActiveStream) {
        if (stream.type == STREAM_TYPE_DATA && stream.clientTypeUuid == VIDEO_PLAYBACK_UUID) {
            debugLog(
                "airplay teardown CarPlayVideoSettings stream=$stream " +
                    "transport=${if (videoPlaybackDataStream == null) "shared/none" else "dedicated"} " +
                    "rcsPresent=${videoSettingsRcs != null}",
            )
            closeVideoSettings("RTSP TEARDOWN")
        } else if (
            stream.type == STREAM_TYPE_DATA &&
            stream.clientTypeUuid == VIDEO_PLAYBACK_CONTROL_UUID
        ) {
            debugLog("airplay teardown video playback control stream=$stream")
            closeVideoPlaybackControl(stream.streamId, "RTSP TEARDOWN")
        } else {
            debugLog("airplay teardown delegated stream=$stream")
            media.onTeardown(this, stream.type)
        }
    }

    private fun openTiming(peerPort: Int): Int {
        val port = ntp.listen()
        if (peerPort > 0) peerAddress?.let { ntp.start(it, peerPort) }
        return port
    }

    private fun openKeepAlive(): Int {
        val socket = DatagramSocket(null)
        socket.reuseAddress = true
        socket.bind(InetSocketAddress(InetAddress.getByName("::"), 0))
        keepAliveSocket = socket
        keepAliveThread = Thread({ runKeepAlive(socket) }, "airplay-keepalive").apply {
            isDaemon = true
            start()
        }
        return socket.localPort
    }

    private fun runKeepAlive(socket: DatagramSocket) {
        val buffer = ByteArray(512)
        while (!closed.get()) {
            try {
                socket.receive(DatagramPacket(buffer, buffer.size))
            } catch (_: Exception) {
                if (closed.get()) return
            }
        }
    }

    private fun openEvent(): Int {
        val server = ServerSocket(0, 50, InetAddress.getByName("::"))
        eventServer = server
        spawnEvent("airplay-event-accept") { acceptEvent(server) }
        return server.localPort
    }

    private fun teardown() {
        ntp.close()
        safeClose(keepAliveSocket)
        keepAliveSocket = null
        keepAliveThread?.interrupt()
        keepAliveThread = null
        safeClose(eventServer)
        eventServer = null
        safeClose(eventSocket)
        eventSocket = null
        eventOutput = null
        eventCipher = null
        eventThreads.forEach { it.interrupt() }
        eventThreads.clear()
    }

    private fun acceptEvent(server: ServerSocket) {
        try {
            val socket = server.accept()
            socket.setSoLinger(true, 0)
            debugLog("airplay event connection accepted from ${socket.remoteSocketAddress}")
            eventSocket = socket
            val shared = pairVerify.shared
            if (shared == null) {
                Log.e(TAG, "airplay event rejected: pair-verify shared secret unavailable")
                safeClose(socket)
                close()
                return
            }
            val writeKey = AirPlayCrypto.hkdfSha512(
                shared,
                "Events-Salt".asciiBytes(),
                "Events-Write-Encryption-Key".asciiBytes(),
                32,
            )
            val readKey = AirPlayCrypto.hkdfSha512(
                shared,
                "Events-Salt".asciiBytes(),
                "Events-Read-Encryption-Key".asciiBytes(),
                32,
            )
            eventCipher = ControlCipher(readKey, writeKey)
            val output = BufferedOutputStream(socket.getOutputStream())
            eventOutput = output
            synchronized(eventWriteLock) {
                sendPendingNightModeLocked()
            }
            runEventRead(socket, output)
        } catch (error: Exception) {
            if (!closed.get()) {
                Log.e(TAG, "airplay event accept failed", error)
                close()
            }
        }
    }

    private fun runEventRead(socket: Socket, output: BufferedOutputStream) {
        try {
            val input = BufferedInputStream(socket.getInputStream())
            var encrypted = ByteArray(0)
            var plaintext = ByteArray(0)
            val buffer = ByteArray(READ_CHUNK_BYTES)
            while (!closed.get()) {
                val count = input.read(buffer)
                if (count < 0) break
                val cipher = eventCipher ?: break
                encrypted += buffer.copyOf(count)
                val decrypted = try {
                    cipher.decrypt(encrypted)
                } catch (error: Exception) {
                    Log.e(TAG, "airplay event decrypt failed encrypted=${encrypted.size}", error)
                    break
                }
                encrypted = decrypted.rest
                plaintext += decrypted.data
                while (plaintext.isNotEmpty()) {
                    if (ApTransportCodec.isPotentialPrefix(plaintext)) {
                        val decoded = decodeSharedPacket(plaintext, SharedChannel.EVENT) ?: break
                        plaintext = decoded.rest
                        handleSharedTransport(decoded.packet, SharedChannel.EVENT)
                        continue
                    }
                    val parsed = RtspMessage.parseFirst(plaintext)
                    val message = parsed.messages.firstOrNull() ?: break
                    plaintext = parsed.rest
                    if (message.method.startsWith("RTSP/") || message.method.startsWith("HTTP/")) continue
                    debugLog(
                        "airplay event rx ${message.method} ${message.path} cseq=${message.headers["cseq"] ?: "-"} body=${message.body.size}",
                    )
                    val response = RtspMessage.buildResponse(message, RtspMessage.Response(status = 200))
                    trace(
                        "airplay event rx headers=${message.headers} " +
                            "bodyHex=${message.body.toHex()}",
                    )
                    trace("airplay event tx wireHex=${response.toHex()}")
                    writeEvent(response)
                }
            }
        } catch (error: Exception) {
            if (!closed.get()) Log.e(TAG, "airplay event read failed", error)
        } finally {
            debugLog("airplay event connection closed")
            if (eventSocket === socket) eventSocket = null
            eventOutput = null
            eventCipher = null
            safeClose(socket)
            if (!closed.get()) close()
        }
    }

    private fun writeEvent(plaintext: ByteArray): Boolean = synchronized(eventWriteLock) {
        val cipher = eventCipher ?: return@synchronized false
        val output = eventOutput ?: return@synchronized false
        return@synchronized try {
            output.write(cipher.encrypt(plaintext))
            output.flush()
            true
        } catch (error: Exception) {
            Log.w(TAG, "airplay shared event write failed", error)
            false
        }
    }

    private fun handleSharedTransport(packet: ApTransportPackage, channel: SharedChannel) {
        debugLog(
            "airplay APTransport channel=${channel.name.lowercase()} " +
                "package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                "message=${if (packet.messageType == 0) "0" else ApTransportCodec.fourCcString(packet.messageType)} " +
                "group=${java.lang.Long.toUnsignedString(packet.groupId)} payload=${packet.payload.size}",
        )
        val dedicatedActive = videoPlaybackDataStream != null
        val candidates = buildList {
            addAll(videoPlaybackRcs.values)
            if (!dedicatedActive) videoSettingsRcs?.let(::add)
        }
        var owner: VideoPlaybackRcsSession? = null
        val handled = candidates.any { candidate ->
            val accepted = candidate.handle(
                packet = packet,
                reply = { response ->
                    val bytes = ApTransportCodec.encode(response)
                    when (channel) {
                        SharedChannel.CONTROL -> writeControl(bytes)
                        SharedChannel.EVENT -> writeEvent(bytes)
                    }
                },
            )
            if (accepted) owner = candidate
            accepted
        }
        debugLog(
                "airplay APTransport demux result channel=${channel.name.lowercase()} " +
                "rcsCandidates=${candidates.size} owner=${owner?.streamId ?: "none"} " +
                "dedicatedActive=$dedicatedActive " +
                "handled=$handled group=${java.lang.Long.toUnsignedString(packet.groupId)}",
        )
        if (!handled && candidates.isNotEmpty() && packet.packageType == ApTransportCodec.TYPE_SYNC) {
            debugLog(
                "airplay APTransport replying unimplemented channel=${channel.name.lowercase()} " +
                    "group=${java.lang.Long.toUnsignedString(packet.groupId)} " +
                    "replyToken=${java.lang.Long.toUnsignedString(packet.replyToken)}",
            )
            val response = ApTransportPackage(
                packageType = ApTransportCodec.TYPE_REPLY,
                groupId = packet.groupId,
                messageType = 0,
                replyToken = packet.replyToken,
                replyStatus = OSSTATUS_UNIMPLEMENTED,
                payload = ByteArray(0),
            )
            when (channel) {
                SharedChannel.CONTROL -> writeControl(ApTransportCodec.encode(response))
                SharedChannel.EVENT -> writeEvent(ApTransportCodec.encode(response))
            }
        }
    }

    private fun handleDedicatedVideoPlaybackTransport(
        packet: ApTransportPackage,
        transport: ApTransportDataStream,
        rcs: VideoPlaybackRcsSession,
    ) {
        if (videoPlaybackDataStream !== transport || videoSettingsRcs !== rcs) {
            debugLog(
                "AirPlay CarPlayVideoSettings dropping packet for stale dedicated stream " +
                    "group=${java.lang.Long.toUnsignedString(packet.groupId)}",
            )
            return
        }
        val handled = rcs.handle(
            packet = packet,
            reply = transport::send,
            acknowledgeOpaquePayload = true,
        )
        debugLog(
            "AirPlay CarPlayVideoSettings dedicated demux result handled=$handled " +
                "package=${ApTransportCodec.fourCcString(packet.packageType)} " +
                "group=${java.lang.Long.toUnsignedString(packet.groupId)} active=${rcs.active}",
        )
        if (!handled && packet.packageType == ApTransportCodec.TYPE_SYNC) {
            debugLog(
                "AirPlay CarPlayVideoSettings dedicated replying unimplemented " +
                    "group=${java.lang.Long.toUnsignedString(packet.groupId)} " +
                    "replyToken=${java.lang.Long.toUnsignedString(packet.replyToken)}",
            )
            transport.send(
                ApTransportPackage(
                    packageType = ApTransportCodec.TYPE_REPLY,
                    groupId = packet.groupId,
                    messageType = 0,
                    replyToken = packet.replyToken,
                    replyStatus = OSSTATUS_UNIMPLEMENTED,
                    payload = ByteArray(0),
                ),
            )
        }
    }

    private fun decodeSharedPacket(
        bytes: ByteArray,
        channel: SharedChannel,
    ): ApTransportCodec.Decoded? = try {
        val decoded = ApTransportCodec.decodeFirst(bytes)
        if (decoded == null) {
            val declaredLength = if (bytes.size >= 4) {
                ((bytes[0].toInt() and 0xff) shl 24) or
                    ((bytes[1].toInt() and 0xff) shl 16) or
                    ((bytes[2].toInt() and 0xff) shl 8) or
                    (bytes[3].toInt() and 0xff)
            } else {
                null
            }
            debugLog(
                "airplay APTransport partial channel=${channel.name.lowercase()} " +
                    "buffered=${bytes.size} declaredLength=${declaredLength ?: "unknown"}",
            )
        }
        decoded
    } catch (error: IllegalArgumentException) {
        debugLog(
            "airplay APTransport invalid channel=${channel.name.lowercase()} buffered=${bytes.size} " +
                "prefixHex=${bytes.copyOf(minOf(bytes.size, 64)).toHex()} " +
                "error=${error.message ?: error.javaClass.simpleName}",
        )
        throw error
    }

    private fun spawnEvent(name: String, body: () -> Unit) {
        val thread = Thread(body, name).apply { isDaemon = true }
        eventThreads.add(thread)
        thread.start()
    }

    private enum class SharedChannel { CONTROL, EVENT }

    private companion object {
        const val TAG = "xcertplay-usb"
        const val PLIST_CONTENT_TYPE = "application/x-apple-binary-plist"
        const val PAIRING_CONTENT_TYPE = "application/pairing+tlv8"
        const val OCTET_CONTENT_TYPE = "application/octet-stream"

        const val STREAM_TYPE_MAIN_SCREEN = 110
        const val STREAM_TYPE_ALT_SCREEN = 111
        const val STREAM_TYPE_MAIN_AUDIO = 100
        const val STREAM_TYPE_ALT_AUDIO = 101
        const val STREAM_TYPE_MAIN_HIGH_AUDIO = 102
        const val STREAM_TYPE_DATA = 130
        const val VIDEO_PLAYBACK_UUID = "BB493F61-A6B8-4769-8D74-80C23A9F71C4"
        const val VIDEO_PLAYBACK_CONTROL_UUID = "A6B27562-B43A-4F2D-B75F-82391E250194"
        const val OSSTATUS_UNIMPLEMENTED = -4
        const val DATASTREAM_OUTPUT_KEY = "DataStream-Output-Encryption-Key"
        const val DATASTREAM_INPUT_KEY = "DataStream-Input-Encryption-Key"
        const val DATASTREAM_KEY_BYTES = 32

        const val READ_CHUNK_BYTES = 16 * 1024
        const val EVENT_READY_POLL_MILLIS = 25L
        const val NANOS_PER_MILLISECOND = 1_000_000L
    }
}

internal fun safeClose(closeable: Closeable?) {
    try {
        closeable?.close()
    } catch (_: Exception) {
        // Best-effort close.
    }
}

private fun ByteArray.toHex(): String =
    joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }

private fun asMap(value: Any?): Map<String, Any?>? {
    val map = value as? Map<*, *> ?: return null
    val result = LinkedHashMap<String, Any?>(map.size)
    for ((key, entry) in map) result[key.toString()] = entry
    return result
}

private fun string(value: Any?): String = value as? String ?: ""

private fun long(value: Any?): Long? = (value as? Number)?.toLong()

private fun plistBoolean(value: Any?): Boolean = when (value) {
    is Boolean -> value
    is Number -> value.toInt() != 0
    else -> false
}
