/*
 This file is part of TrollVNC
 Copyright (c) 2025 82Flex <82flex@gmail.com> and contributors

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2
 as published by the Free Software Foundation.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

/*
 BartStream — H.264 push video alongside TrollVNC (added 2026-11-12).

 Why this exists: VNC/RFB is request-response, so frame rate is RTT-gated
 (~0.25 s fixed cost per frame over the Tailscale proxy -> ~5 fps best).
 This module pushes H.264 video instead: the server encodes frames as they
 arrive and sends them over one persistent TCP connection. Latency stays
 ~0.3 s, but frame rate is no longer RTT-gated (15-30 fps expected at
 500 kbps-1 Mbps on a 0.6 MB/s link). VNC on port 5901 is kept for
 touch/keyboard input only; video comes from here.

 Wire format (port 5902 by default, plain TCP, one viewer at a time):
   - Each frame: 4-byte big-endian length N, followed by N bytes of
     Annex-B H.264 NAL units (0x00 0x00 0x00 0x01 start codes).
   - SPS/PPS are sent on connect and prepended to every keyframe, so a
     client can start decoding mid-stream with no out-of-band setup.
   - No container, no RTP. ffmpeg reads it directly:
         ffplay -f h264 -i tcp://100.69.57.81:5902
   - No auth in Phase 1-2 (same posture as VNC today: Tailscale-only
     by binding; a password decision is Garrett's, not this module's).

 Backpressure: frames are DROPPED, never queued. If the socket would
 block, or more than 2 frames are waiting on the encoder, the newest
 frame is dropped. A stream that drops to 15 fps under load is correct;
 a stream that queues seconds of frames is not.

 Capture source: the CMSampleBufferRef produced by ScreenCapturer
 (IOSurface -> zero-copy CVPixelBuffer, 32BGRA) is exactly what
 VTCompressionSessionEncodeFrame takes. No pixel copy between capture
 and encode. The encoder session is created at the output size, so
 VideoToolbox does the downscale on the hardware scaler.

 Threading & lifetime:
 - startListener/stop may be called from any thread.
 - encodeSampleBuffer: MUST be called on the main thread (the
   IOSurface backing the pixel buffer is reused by the next capture
   tick, so the frame is handed to the encoder synchronously while
   the surface is current). It never blocks: if the encoder is backed
   up, the frame is dropped.
 - The output callback runs on a VideoToolbox queue and hops to the
   streamer's serial queue for NAL parsing and socket writes.
 - ARC only.
 */

#ifndef H264Streamer_h
#define H264Streamer_h

#import <CoreMedia/CoreMedia.h>
#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/** Default H.264 push-stream TCP port. 0 disables the stream. */
static const int kH264StreamDefaultPort = 5902;
/** Default average bitrate: 500 kbps (10% of the 0.6 MB/s link). */
static const int kH264StreamDefaultBitrateBps = 500000;
/** Default output scale relative to the capture source (0.25 -> ~512x384 landscape). */
static const double kH264StreamDefaultScale = 0.25;
/** Default keyframe interval in frames (60 @ 30 fps = 2 s). */
static const int kH264StreamDefaultKeyFrameInterval = 60;
/** Default expected frame rate fed to the encoder. */
static const int kH264StreamDefaultFrameRate = 30;

@interface H264Streamer : NSObject

/** Returns the shared singleton instance. */
+ (instancetype)sharedStreamer;

+ (instancetype)new NS_UNAVAILABLE;
- (instancetype)init NS_UNAVAILABLE;

/**
 Configure the streamer. Must be called before -startListener.
 - port: TCP listen port (0 = disabled, default 5902).
 - bitrateBps: average bitrate, e.g. 500000 or 1000000.
 - outputScale: scale factor applied to the capture source size when no
   explicit width/height is given (0 = use default 0.25).
 - explicitWidth/explicitHeight: if both > 0, the encoder session is
   created at exactly this size (caller picks landscape/portrait to
   match the current orientation); otherwise outputScale is used.
   Odd dimensions are rounded DOWN to even (the encoder silently
   adjusts 405 -> 404; the configured size must be the real size).
 - bindHost: IPv4 literal to bind (e.g. the Tailscale IP), or nil to
   bind all interfaces (same posture as VNC when -b is not given).
 Returns NO if the arguments are invalid (port out of range, scale
 out of (0, 1], negative bitrate); the previous configuration is kept.
 */
- (BOOL)configureWithPort:(int)port
              bitrateBps:(int)bitrateBps
             outputScale:(double)outputScale
           explicitWidth:(int)explicitWidth
          explicitHeight:(int)explicitHeight
                bindHost:(nullable NSString *)bindHost;

/**
 Parse a spec string of the form "0.25@500" (scale@kbps) or
 "810x1080@1000" (WxH@kbps) into the configure arguments. Missing
 parts fall back to the defaults (scale 0.25, 500 kbps). Returns NO
 if the string is non-empty and unparseable.
 */
- (BOOL)configureWithSpec:(nullable NSString *)spec port:(int)port bindHost:(nullable NSString *)bindHost;

/** Start listening for one H.264 viewer. Returns NO if the socket
    cannot be bound (the error is logged; VNC keeps working). */
- (BOOL)startListener;

/** Stop the listener, drop the viewer, and tear down the encoder. */
- (void)stop;

/**
 Feed one captured frame to the encoder. MUST be called on the main
 thread. Safe to call when no viewer is connected (returns immediately
 without encoding — no CPU is spent when nobody is watching).
 The sample buffer is the one from ScreenCapturer's frame handler.
 */
- (void)encodeSampleBuffer:(CMSampleBufferRef)sampleBuffer;

/** YES while a viewer is connected to the push stream. */
@property(nonatomic, readonly) BOOL hasClients;

/**
 Invoked (on the streamer's serial queue — hop to main before touching
 UI or ScreenCapturer) whenever viewer presence changes 0 <-> 1.
 The server uses this to keep screen capture running while an H.264
 viewer is connected even with no VNC client, and to stop it when
 nobody is watching at all.
 */
@property(nonatomic, copy, nullable) void (^demandChangedHandler)(BOOL hasClients);

/** One-line status for the server log (port, size, bitrate, counters). */
@property(nonatomic, strong, readonly) NSString *statusDescription;

@end

NS_ASSUME_NONNULL_END

#endif /* H264Streamer_h */
