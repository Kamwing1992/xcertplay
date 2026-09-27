package com.shilapi.xcertplay.airplay

/** Display insets in pixels, used for CarPlay viewArea and safeArea declarations. */
data class AirPlayInsets(
    val top: Int = 0,
    val bottom: Int = 0,
    val left: Int = 0,
    val right: Int = 0,
)

/** One display advertised to the phone in /info. */
data class AirPlayDisplayConfig(
    val widthPixels: Int,
    val heightPixels: Int,
    val widthPhysicalMm: Int? = null,
    val heightPhysicalMm: Int? = null,
    val fps: Int = 60,
    val primaryInputDevice: Int = 1,
    val viewArea: AirPlayInsets? = null,
    val safeArea: AirPlayInsets? = null,
    val safeAreaDrawOutside: Boolean? = null,
    val initialUrl: String? = null,
)

/** One OEM homescreen icon. */
data class AirPlayIcon(
    val widthPixels: Int,
    val heightPixels: Int,
    val data: ByteArray,
)

/** Player features reported through /info.playbackCapabilities. */
data class AirPlayPlaybackCapabilities(
    val supportsOfflineHls: Boolean = false,
    val supportsV2ArtworkMetadata: Boolean = false,
    val supportsFpsSecureStop: Boolean = false,
    val supportsUiForAudioOnlyContent: Boolean = true,
)

/** Experimental URL-fling video player advertised to CarPlay. */
data class AirPlayVideoPlaybackConfig(
    val enabled: Boolean = false,
    val allowed: Boolean = true,
    val featuresEx: String = FEATURES_EX_VIDEO_PLAYBACK,
    val capabilities: AirPlayPlaybackCapabilities = AirPlayPlaybackCapabilities(),
) {
    init {
        require(!enabled || featuresEx.isNotBlank()) {
            "featuresEx is required when video playback is enabled"
        }
    }

    companion object {
        /** APFeature bit 70: byte 8, bit 6, standard base64 without padding. */
        const val FEATURES_EX_VIDEO_PLAYBACK = "AAAAAAAAAABA"
    }
}

/** Immutable accessory configuration consumed by the AirPlay session server. */
data class AirPlayConfig(
    val deviceName: String,
    val deviceId: String,
    val btMac: String,
    val sourceVersion: String,
    val main: AirPlayDisplayConfig,
    val cluster: AirPlayDisplayConfig? = null,
    val rightHandDrive: Boolean = false,
    val port: Int = 7000,
    val entertainmentSampleRate: Int = 48000,
    val hevc: Boolean = false,
    val disableAudioOutput: Boolean = false,
    val microphone: Boolean = false,
    val wirelessAudio: Boolean = false,
    val manufacturer: String = "xcertplay",
    val model: String = "xcertplay",
    val oemLabel: String = "xcertplay",
    val icons: List<AirPlayIcon> = emptyList(),
    val videoPlayback: AirPlayVideoPlaybackConfig = AirPlayVideoPlaybackConfig(),
)
