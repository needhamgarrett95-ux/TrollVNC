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
 BartStream implementation notes (read before changing):
 - The encoder session is created LAZILY, on the first captured frame
   that arrives while a viewer is connected. Creating it earlier would
   require guessing the source size; the first frame tells us the truth.
   Creation costs ~0.04 s warm / ~0.44 s cold (measured by the
   standalone smoke test) on the main thread, once per session — a
   one-time hitch when the first viewer connects, not per frame.
   It is kept synchronous deliberately: the alternative (async
   creation) would drop the first ~half-second of frames instead, for
   the same total delay, with a new race to verify.
 - encodeSampleBuffer: runs on the main thread (ScreenCapturer's
   CADisplayLink). It never blocks: if the encoder already has 2 frames
   in flight, the frame is dropped and counted.
 - The IOSurface behind the pixel buffer is REUSED by the next capture
   tick. VTCompressionSessionEncodeFrame is therefore called
   synchronously here, while the surface is current. Do NOT dispatch
   the EncodeFrame call to another queue: by the time it ran, the
   surface could already hold the next frame. (Phase 2 watches for
   tearing; if it appears, the fix is a dedicated NV12 copy surface,
   not an async encode.)
 - The output callback only parses NALs and writes to the socket, on
   the streamer's serial queue. It never touches the IOSurface.
 - Socket rule that keeps the stream resyncable: a frame is sent with
   ONE send() call. If send() would block (EAGAIN) the whole frame is
   dropped — the stream stays intact. If send() sends only PART of a
   frame, the client is closed: a half-sent length-prefixed frame
   would desync the client, and a reconnect (fresh SPS/PPS + forced
   keyframe) is the clean recovery. Partial sends are rare (256 KB
   SO_SNDBUF); dropping the client is correct, not a bug.
 - If VTCompressionSessionCreate fails (e.g. no hardware encoder in
   the daemon context), the streamer disables itself and logs loudly.
   VNC is unaffected. That failure IS the Phase-2 gate result.
 */

#if !__has_feature(objc_arc)
#warning This file must be compiled with ARC. Use -fobjc-arc flag.
#endif

#import "H264Streamer.h"

#import <VideoToolbox/VideoToolbox.h>

#import <arpa/inet.h>
#import <atomic>
#import <errno.h>
#import <netinet/in.h>
#import <netinet/tcp.h>
#import <string.h>
#import <sys/socket.h>
#import <unistd.h>

#import "Logging.h"

static const int kH264MaxInFlightFrames = 2;       // drop new frames beyond this
static const int kH264MaxConsecutiveEncodeErrors = 10; // then tear the session down
static const NSTimeInterval kH264SessionRetryDelaySec = 2.0; // backoff after a failed create
static const NSTimeInterval kH264StatsLogIntervalSec = 30.0;
static const int kH264ClientSendBufferBytes = 256 * 1024;
static const uint8_t kH264AnnexBStartCode[4] = {0x00, 0x00, 0x00, 0x01};

// Queue-specific key: lets stop/statusDescription detect "already on the
// streamer queue" so they never dispatch_sync onto themselves (deadlock).
static const void *kH264QueueSpecificKey = &kH264QueueSpecificKey;

static void H264CompressionOutputCallback(void *outputCallbackRefCon, void *sourceFrameRefCon, OSStatus status,
                                          VTEncodeInfoFlags infoFlags, CMSampleBufferRef sampleBuffer) {
    (void)sourceFrameRefCon;
    (void)infoFlags;
    H264Streamer *self = (__bridge H264Streamer *)outputCallbackRefCon;
    if (!self)
        return;
    // The sample buffer is only valid for the duration of this callback,
    // so retain it before hopping to the serial queue.
    if (sampleBuffer)
        CFRetain(sampleBuffer);
    dispatch_async([self queue], ^{
        [self handleEncodedSampleBuffer:sampleBuffer encodeStatus:status];
        if (sampleBuffer)
            CFRelease(sampleBuffer);
    });
}

@interface H264Streamer ()

// Serial queue: all socket state, NAL parsing, sends, stats.
@property(nonatomic, strong, readonly) dispatch_queue_t queue;

// Config (set before startListener; read on main + queue, immutable after start).
@property(nonatomic) int port;
@property(nonatomic) int bitrateBps;
@property(nonatomic) double outputScale;
@property(nonatomic) int explicitWidth;
@property(nonatomic) int explicitHeight;
@property(nonatomic, strong, nullable) NSString *bindHost;

- (BOOL)isOnQueue;
- (void)acceptLoop;
- (void)adoptClientFd:(int)clientFd peer:(NSString *)peer;
- (void)closeClientWithReason:(NSString *)reason;
- (nullable VTCompressionSessionRef)ensureSessionForPixelBuffer:(CVPixelBufferRef)pixelBuffer;
- (nullable VTCompressionSessionRef)createSessionWithWidth:(int)width height:(int)height;
- (void)setSessionProperty:(VTCompressionSessionRef)session key:(CFStringRef)key value:(CFTypeRef)value;
- (void)teardownSession;
- (void)handleEncodedSampleBuffer:(nullable CMSampleBufferRef)sampleBuffer encodeStatus:(OSStatus)status;
- (BOOL)sampleBufferIsKeyframe:(CMSampleBufferRef)sampleBuffer;
- (nullable NSMutableData *)annexBDataFromAVCCData:(const char *)data length:(size_t)length;
- (nullable NSData *)spsPpsAnnexBFromSampleBuffer:(CMSampleBufferRef)sampleBuffer;
- (BOOL)sendFramedData:(NSData *)payload closeClientOnFailure:(BOOL)closeClientOnFailure;

@end

@implementation H264Streamer {
    NSLock *_sessionLock;              // guards _session, _encWidth/_encHeight, _sessionFailed state
    VTCompressionSessionRef _session;  // NULL until lazily created on first frame with a client
    int _encWidth;                     // encoder session dimensions (0 = not created)
    int _encHeight;
    BOOL _sessionFailed;               // create failed; retry only after _nextSessionRetryTime
    CFAbsoluteTime _nextSessionRetryTime;
    BOOL _loggedFirstFrame;            // first encoded frame logged once per session

    std::atomic<int> _inFlight;        // frames submitted to VT, callback not yet run
    std::atomic<int> _consecutiveEncodeErrors;
    std::atomic<bool> _forceKeyFrame;  // set on new client connect, consumed by next encode

    std::atomic<int> _listenFd;        // -1 when not listening
    std::atomic<int> _clientFd;        // -1 when no viewer
    std::atomic<bool> _running;
    NSString *_clientPeer;             // guarded by _queue (written on queue, read on queue)

    NSData *_cachedSpsPpsAnnexB;       // guarded by _queue; sent on connect + prepended to keyframes

    // Counters (queue-only except where noted; read for statusDescription on _queue).
    uint64_t _framesEncoded;
    uint64_t _framesSent;
    uint64_t _framesDroppedNoClient;   // encode skipped: nobody watching (informational)
    uint64_t _framesDroppedBacklog;    // dropped: encoder already had 2 frames in flight
    uint64_t _framesDroppedSend;       // dropped: socket would block
    uint64_t _clientsDroppedPartial;   // clients closed after a partial send (resync)
    uint64_t _bytesSent;
    uint64_t _keyframesSent;
    CFAbsoluteTime _lastStatsLogTime;
}

+ (instancetype)sharedStreamer {
    static H264Streamer *_inst = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        _inst = [[self alloc] init];
    });
    return _inst;
}

- (instancetype)init {
    self = [super init];
    if (!self)
        return nil;
    _queue = dispatch_queue_create("com.82flex.trollvnc.h264", DISPATCH_QUEUE_SERIAL);
    dispatch_queue_set_specific(_queue, kH264QueueSpecificKey, (__bridge void *)self, NULL);
    _sessionLock = [[NSLock alloc] init];
    _port = kH264StreamDefaultPort;
    _bitrateBps = kH264StreamDefaultBitrateBps;
    _outputScale = kH264StreamDefaultScale;
    _explicitWidth = 0;
    _explicitHeight = 0;
    _bindHost = nil;
    _session = NULL;
    _encWidth = 0;
    _encHeight = 0;
    _sessionFailed = NO;
    _nextSessionRetryTime = 0;
    _loggedFirstFrame = NO;
    _inFlight.store(0);
    _consecutiveEncodeErrors.store(0);
    _forceKeyFrame.store(false);
    _listenFd.store(-1);
    _clientFd.store(-1);
    _running.store(false);
    _lastStatsLogTime = 0;
    return self;
}

- (void)dealloc {
    [self stop];
}

#pragma mark - Configuration

- (BOOL)configureWithPort:(int)port
              bitrateBps:(int)bitrateBps
             outputScale:(double)outputScale
           explicitWidth:(int)explicitWidth
          explicitHeight:(int)explicitHeight
                bindHost:(nullable NSString *)bindHost {
    if (port < 0 || port > 65535) {
        TVLog(@"H264: invalid port %d (expected 0..65535); configuration unchanged", port);
        return NO;
    }
    if (bitrateBps < 0) {
        TVLog(@"H264: invalid bitrate %d; configuration unchanged", bitrateBps);
        return NO;
    }
    if (outputScale <= 0.0 || outputScale > 1.0) {
        TVLog(@"H264: invalid outputScale %.3f (expected 0 < scale <= 1); configuration unchanged", outputScale);
        return NO;
    }
    if ((explicitWidth > 0) != (explicitHeight > 0)) {
        TVLog(@"H264: explicitWidth/explicitHeight must be given together; configuration unchanged");
        return NO;
    }
    if (explicitWidth > 0 && (explicitWidth < 16 || explicitHeight < 16)) {
        TVLog(@"H264: explicit size %dx%d too small (min 16x16); configuration unchanged", explicitWidth,
              explicitHeight);
        return NO;
    }
    // H.264 needs even dimensions. The encoder silently adjusts odd
    // ones (Track 3 measured 540x405 -> 540x404), which would leave
    // our recorded size disagreeing with the real output. Round down
    // here, once, so the configured size is always the real size.
    if (explicitWidth > 0 && (explicitWidth & 1)) {
        TVLog(@"H264: explicit width %d is odd; using %d", explicitWidth, explicitWidth & ~1);
        explicitWidth &= ~1;
    }
    if (explicitHeight > 0 && (explicitHeight & 1)) {
        TVLog(@"H264: explicit height %d is odd; using %d", explicitHeight, explicitHeight & ~1);
        explicitHeight &= ~1;
    }
    _port = port;
    _bitrateBps = (bitrateBps > 0) ? bitrateBps : kH264StreamDefaultBitrateBps;
    _outputScale = outputScale;
    _explicitWidth = explicitWidth;
    _explicitHeight = explicitHeight;
    _bindHost = bindHost;
    return YES;
}

- (BOOL)configureWithSpec:(nullable NSString *)spec port:(int)port bindHost:(nullable NSString *)bindHost {
    double scale = kH264StreamDefaultScale;
    int bitrateBps = kH264StreamDefaultBitrateBps;
    int width = 0, height = 0;

    if (spec && spec.length > 0) {
        const char *cstr = [spec UTF8String];
        // Form 1: "810x1080@1000" (explicit WxH, kbps). Form 2: "0.25@500" (scale, kbps).
        int w = 0, h = 0, kbps = 0;
        double s = 0.0;
        if (sscanf(cstr, "%dx%d@%d", &w, &h, &kbps) == 3) {
            width = w;
            height = h;
        } else if (sscanf(cstr, "%lf@%d", &s, &kbps) == 2) {
            scale = s;
        } else if (sscanf(cstr, "%dx%d", &w, &h) == 2) {
            width = w;
            height = h;
        } else if (sscanf(cstr, "%lf", &s) == 1) {
            scale = s;
        } else {
            TVLog(@"H264: unparseable spec '%@' (expected 0.25@500 or 810x1080@1000); configuration unchanged",
                  spec);
            return NO;
        }
        if (kbps > 0)
            bitrateBps = kbps * 1000;
    }

    return [self configureWithPort:port
                        bitrateBps:bitrateBps
                       outputScale:scale
                     explicitWidth:width
                    explicitHeight:height
                          bindHost:bindHost];
}

#pragma mark - Listener / sockets

- (BOOL)startListener {
    if (_port == 0) {
        TVLog(@"H264: stream disabled (port 0); VNC-only mode");
        return YES;
    }
    if (_running.load()) {
        TVLog(@"H264: listener already running");
        return YES;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        TVLog(@"H264: socket() failed: %s", strerror(errno));
        return NO;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)_port);
    if (_bindHost && _bindHost.length > 0) {
        struct in_addr v4;
        if (inet_pton(AF_INET, [_bindHost UTF8String], &v4) == 1) {
            addr.sin_addr = v4;
        } else {
            TVLog(@"H264: bind host '%@' is not a valid IPv4 literal; binding all interfaces", _bindHost);
            addr.sin_addr.s_addr = htonl(INADDR_ANY);
        }
    } else {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        TVLog(@"H264: bind(%@:%d) failed: %s — H.264 stream OFF, VNC keeps working",
              _bindHost ?: @"*", _port, strerror(errno));
        close(fd);
        return NO;
    }
    if (listen(fd, 1) != 0) {
        TVLog(@"H264: listen() failed: %s — H.264 stream OFF, VNC keeps working", strerror(errno));
        close(fd);
        return NO;
    }

    _listenFd.store(fd);
    _running.store(true);
    TVLog(@"H264: listening on %@:%d (Annex-B H.264, one viewer, newest wins)",
          _bindHost ?: @"0.0.0.0", _port);

    __weak H264Streamer *weakSelf = self;
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        [weakSelf acceptLoop];
    });
    return YES;
}

- (void)acceptLoop {
    while (_running.load()) {
        int listenFd = _listenFd.load();
        if (listenFd < 0)
            break;
        struct sockaddr_in peer;
        socklen_t peerLen = sizeof(peer);
        int clientFd = accept(listenFd, (struct sockaddr *)&peer, &peerLen);
        if (clientFd < 0) {
            if (!_running.load())
                break; // listener stopped
            if (errno == EINTR)
                continue;
            TVLog(@"H264: accept() failed: %s", strerror(errno));
            // Do not hot-spin if accept keeps failing (e.g. EMFILE):
            // back off briefly before retrying.
            usleep(100 * 1000);
            continue;
        }

        char peerBuf[INET_ADDRSTRLEN] = {0};
        inet_ntop(AF_INET, &peer.sin_addr, peerBuf, sizeof(peerBuf));
        NSString *peerStr = [NSString stringWithFormat:@"%s:%d", peerBuf, ntohs(peer.sin_port)];

        int one = 1;
        setsockopt(clientFd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        setsockopt(clientFd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
        int sndBuf = kH264ClientSendBufferBytes;
        setsockopt(clientFd, SOL_SOCKET, SO_SNDBUF, &sndBuf, sizeof(sndBuf));

        // Hand the new client to the serial queue: newest viewer wins.
        dispatch_async(_queue, ^{
            [self adoptClientFd:clientFd peer:peerStr];
        });
    }
}

// Queue-only: replace the current viewer (if any) with a new one.
- (void)adoptClientFd:(int)clientFd peer:(NSString *)peer {
    BOOL hadClient = (_clientFd.load() >= 0);
    int oldFd = _clientFd.exchange(clientFd);
    if (oldFd >= 0) {
        close(oldFd);
        TVLog(@"H264: viewer %@ replaced the previous viewer", peer);
    }
    _clientPeer = peer;
    _forceKeyFrame.store(true);
    TVLog(@"H264: viewer connected from %@ (bitrate %d bps)", peer, _bitrateBps);

    // Send cached SPS/PPS immediately so the client can configure its
    // decoder before the first slice arrives. Best effort only: a
    // failure here must NEVER close the just-connected client — the
    // next keyframe carries SPS/PPS anyway.
    if (_cachedSpsPpsAnnexB && _cachedSpsPpsAnnexB.length > 0) {
        [self sendFramedData:_cachedSpsPpsAnnexB closeClientOnFailure:NO];
    }

    if (!hadClient && _demandChangedHandler) {
        _demandChangedHandler(YES);
    }
}

// Queue-only: close the current viewer. Notifies demand change on 1 -> 0.
- (void)closeClientWithReason:(NSString *)reason {
    int fd = _clientFd.exchange(-1);
    if (fd < 0)
        return;
    close(fd);
    TVLog(@"H264: viewer %@ disconnected (%@)", _clientPeer ?: @"?", reason);
    _clientPeer = nil;
    if (_demandChangedHandler) {
        _demandChangedHandler(NO);
    }
}

- (BOOL)isOnQueue {
    return dispatch_get_specific(kH264QueueSpecificKey) != NULL;
}

- (void)stop {
    _running.store(false);
    int listenFd = _listenFd.exchange(-1);
    if (listenFd >= 0) {
        shutdown(listenFd, SHUT_RDWR);
        close(listenFd);
    }
    // Never dispatch_sync onto our own queue (deadlock if stop is ever
    // called from it, e.g. from dealloc on the queue).
    if ([self isOnQueue]) {
        [self closeClientWithReason:@"streamer stopping"];
    } else {
        dispatch_sync(_queue, ^{
            [self closeClientWithReason:@"streamer stopping"];
        });
    }
    [self teardownSession];
}

#pragma mark - Encoding

- (BOOL)hasClients {
    return _clientFd.load() >= 0;
}

- (dispatch_queue_t)queue {
    return _queue;
}

- (void)encodeSampleBuffer:(CMSampleBufferRef)sampleBuffer {
    if (!sampleBuffer)
        return;
    if (![NSThread isMainThread]) {
        // The IOSurface behind this buffer is only current on the main
        // thread's capture tick. Encoding from another thread risks
        // feeding the encoder a surface that already holds the next
        // frame. Drop rather than guess.
        static BOOL sLoggedOnce = NO;
        if (!sLoggedOnce) {
            sLoggedOnce = YES;
            TVLog(@"H264: encodeSampleBuffer called off the main thread — dropping (bug in caller)");
        }
        return;
    }
    if (_port == 0)
        return; // stream disabled
    if (!_running.load())
        return; // listener not started (or failed to bind); VNC-only
    if (![self hasClients]) {
        // Nobody watching: spend no encoder CPU at all. The counters
        // below are informational for the status line.
        dispatch_async(_queue, ^{
            _framesDroppedNoClient++;
        });
        return;
    }
    if (_inFlight.load() >= kH264MaxInFlightFrames) {
        // Encoder is backed up. Drop this frame; the next dirty frame
        // will follow within a tick. Never queue.
        dispatch_async(_queue, ^{
            _framesDroppedBacklog++;
        });
        return;
    }

    CVPixelBufferRef pixelBuffer = CMSampleBufferGetImageBuffer(sampleBuffer);
    if (!pixelBuffer)
        return;

    VTCompressionSessionRef session = [self ensureSessionForPixelBuffer:pixelBuffer];
    if (!session)
        return; // creation failed (logged inside); VNC unaffected

    BOOL forceKeyFrame = _forceKeyFrame.exchange(false);
    CFDictionaryRef frameProps = NULL;
    NSDictionary *forceDict = nil;
    if (forceKeyFrame) {
        forceDict = @{(id)kVTEncodeFrameOptionKey_ForceKeyFrame : @YES};
        frameProps = (__bridge CFDictionaryRef)forceDict;
    }

    CMTime pts = CMSampleBufferGetPresentationTimeStamp(sampleBuffer);
    CMTime duration = CMSampleBufferGetDuration(sampleBuffer);
    if (CMTIME_IS_INVALID(duration))
        duration = kCMTimeInvalid;

    _inFlight.fetch_add(1);
    OSStatus status = VTCompressionSessionEncodeFrame(session, pixelBuffer, pts, duration, frameProps, NULL, NULL);
    CFRelease(session); // local retain from ensureSessionForPixelBuffer
    if (status != noErr) {
        _inFlight.fetch_sub(1);
        dispatch_async(_queue, ^{
            _framesDroppedBacklog++;
        });
        TVLogVerbose(@"H264: EncodeFrame failed: %d", (int)status);
        return;
    }
}

// Main-thread entry. Creates (or recreates) the encoder session for the
// source size. Returns a RETAINED session ref, or NULL on failure.
// The caller must CFRelease it.
- (nullable VTCompressionSessionRef)ensureSessionForPixelBuffer:(CVPixelBufferRef)pixelBuffer {
    size_t srcWidth = CVPixelBufferGetWidth(pixelBuffer);
    size_t srcHeight = CVPixelBufferGetHeight(pixelBuffer);
    if (srcWidth == 0 || srcHeight == 0)
        return NULL;

    int outWidth, outHeight;
    if (_explicitWidth > 0 && _explicitHeight > 0) {
        outWidth = _explicitWidth;
        outHeight = _explicitHeight;
    } else {
        outWidth = (int)(srcWidth * _outputScale);
        outHeight = (int)(srcHeight * _outputScale);
        // H.264 needs even dimensions; round down (never up — up would
        // exceed the requested scale).
        outWidth = MAX(2, outWidth & ~1);
        outHeight = MAX(2, outHeight & ~1);
    }

    [_sessionLock lock];
    if (_session) {
        if (_encWidth == outWidth && _encHeight == outHeight) {
            VTCompressionSessionRef session = _session;
            CFRetain(session);
            [_sessionLock unlock];
            return session;
        }
        // Source size changed (orientation fix changed under us — only
        // expected across restarts). Tear down and recreate below.
        TVLog(@"H264: output size changed %dx%d -> %dx%d; recreating encoder", _encWidth, _encHeight, outWidth,
              outHeight);
        VTCompressionSessionRef old = _session;
        _session = NULL;
        _encWidth = 0;
        _encHeight = 0;
        [_sessionLock unlock];
        // NOTE: no VTCompressionSessionCompleteFrames here, deliberately.
        // With a numeric timestamp it would complete only frames with
        // PTS <= that value (ours are large and positive, so: none);
        // with kCMTimeInvalid it would flush everything but BLOCK until
        // the encoder drains — a stuck encoder would hang the main
        // thread in this path. Teardown drops pending frames by design;
        // Invalidate disposes of them and their callbacks fire with an
        // error (or not at all — the _inFlight clamp covers that).
        VTCompressionSessionInvalidate(old);
        CFRelease(old);
        [_sessionLock lock];
    }
    if (_sessionFailed) {
        CFAbsoluteTime now = CFAbsoluteTimeGetCurrent();
        if (now < _nextSessionRetryTime) {
            [_sessionLock unlock];
            return NULL;
        }
        _sessionFailed = NO; // retry now
    }
    [_sessionLock unlock];

    VTCompressionSessionRef session = [self createSessionWithWidth:outWidth height:outHeight];
    if (!session) {
        [_sessionLock lock];
        _sessionFailed = YES;
        _nextSessionRetryTime = CFAbsoluteTimeGetCurrent() + kH264SessionRetryDelaySec;
        [_sessionLock unlock];
        return NULL;
    }

    [_sessionLock lock];
    _session = session; // stored ref (owned); caller gets its own retain below
    _encWidth = outWidth;
    _encHeight = outHeight;
    _loggedFirstFrame = NO;
    CFRetain(session);
    [_sessionLock unlock];
    return session;
}

// Creates + configures a session. Logs loudly on failure (the Phase-2
// gate). Returns a retained session, or NULL. Runs on the main thread.
- (nullable VTCompressionSessionRef)createSessionWithWidth:(int)width height:(int)height {
    // Low-latency mode is an ENCODER SPECIFICATION key (WWDC 2021
    // Session 10158), not a session property. It also implies a
    // hardware encoder. RequireHardware makes creation FAIL (instead
    // of silently falling back to software) if no hardware encoder is
    // available in this daemon context — that failure is the signal.
    NSDictionary *encoderSpec = @{
        (id)kVTVideoEncoderSpecification_EnableLowLatencyRateControl : @YES,
        (id)kVTVideoEncoderSpecification_RequireHardwareAcceleratedVideoEncoder : @YES,
    };
    NSDictionary *sourceAttrs = @{(id)kCVPixelBufferPixelFormatTypeKey : @(kCVPixelFormatType_32BGRA)};

    VTCompressionSessionRef session = NULL;
    OSStatus status = VTCompressionSessionCreate(kCFAllocatorDefault, width, height, kCMVideoCodecType_H264,
                                                 (__bridge CFDictionaryRef)encoderSpec,
                                                 (__bridge CFDictionaryRef)sourceAttrs, NULL,
                                                 H264CompressionOutputCallback, (__bridge void *)self, &session);
    if (status != noErr || !session) {
        TVLog(@"H264: VTCompressionSessionCreate(%dx%d) FAILED: %d — hardware H.264 encode is NOT available "
              @"in this daemon context. H.264 stream OFF; VNC keeps working. (Phase-2 gate: fall back to MJPEG push.)",
              width, height, (int)status);
        return NULL;
    }

    // Properties: each checked; a rejected property is a warning, not
    // fatal (the session still encodes with the remaining settings).
    [self setSessionProperty:session key:kVTCompressionPropertyKey_RealTime value:(__bridge CFTypeRef) @YES];
    [self setSessionProperty:session
                         key:kVTCompressionPropertyKey_ProfileLevel
                       value:kVTProfileLevel_H264_ConstrainedBaseline_AutoLevel];
    [self setSessionProperty:session
                         key:kVTCompressionPropertyKey_AverageBitRate
                       value:(__bridge CFTypeRef) @(_bitrateBps)];
    [self setSessionProperty:session
                         key:kVTCompressionPropertyKey_ExpectedFrameRate
                       value:(__bridge CFTypeRef) @(kH264StreamDefaultFrameRate)];
    [self setSessionProperty:session
                         key:kVTCompressionPropertyKey_MaxKeyFrameInterval
                       value:(__bridge CFTypeRef) @(kH264StreamDefaultKeyFrameInterval)];
    [self setSessionProperty:session
                         key:kVTCompressionPropertyKey_AllowFrameReordering
                       value:(__bridge CFTypeRef) @NO];

    status = VTCompressionSessionPrepareToEncodeFrames(session);
    if (status != noErr) {
        TVLog(@"H264: PrepareToEncodeFrames returned %d (continuing anyway)", (int)status);
    }

    // Report whether a hardware encoder was actually selected. NOTE:
    // for low-latency encoders this property is documented as NOT
    // being set (Chromium: neither UsingHardwareAcceleratedVideoEncoder
    // nor IsHardwareAccelerated is set for the rtvc encoder), so NO
    // here means "unreported", not "software". The authoritative
    // signal is that Create succeeded with RequireHardware set.
    CFBooleanRef usingHW = NULL;
    if (VTSessionCopyProperty(session, kVTCompressionPropertyKey_UsingHardwareAcceleratedVideoEncoder,
                              kCFAllocatorDefault, &usingHW) == noErr &&
        usingHW) {
        BOOL hw = CFBooleanGetValue(usingHW);
        TVLog(@"H264: encoder session %dx%d @ %d bps created (hardware-reported=%@)", width, height, _bitrateBps,
              hw ? @"YES" : @"unreported");
        CFRelease(usingHW);
    } else {
        TVLog(@"H264: encoder session %dx%d @ %d bps created (hardware flag unreported — expected for "
              @"low-latency encoders; creation with RequireHardware is the gate)",
              width, height, _bitrateBps);
    }
    return session;
}

- (void)setSessionProperty:(VTCompressionSessionRef)session key:(CFStringRef)key value:(CFTypeRef)value {
    OSStatus status = VTSessionSetProperty(session, key, value);
    if (status != noErr) {
        TVLog(@"H264: VTSessionSetProperty(%@) rejected: %d (continuing with remaining settings)",
              (__bridge NSString *)key, (int)status);
    }
}

- (void)teardownSession {
    [_sessionLock lock];
    VTCompressionSessionRef old = _session;
    _session = NULL;
    _encWidth = 0;
    _encHeight = 0;
    _sessionFailed = NO;
    [_sessionLock unlock];
    // Frames in flight at teardown may never get a callback; reset the
    // counter so the next session is not permanently "backed up".
    // (A late callback after this just clamps the counter at 0; see
    // handleEncodedSampleBuffer.)
    _inFlight.store(0);
    if (old) {
        // See the note in ensureSessionForPixelBuffer: no
        // CompleteFrames here — teardown drops pending frames and must
        // never block on the encoder.
        VTCompressionSessionInvalidate(old);
        CFRelease(old);
        TVLog(@"H264: encoder session torn down");
    }
}

#pragma mark - Encoded output (queue-only)

- (void)handleEncodedSampleBuffer:(nullable CMSampleBufferRef)sampleBuffer encodeStatus:(OSStatus)status {
    // Clamp at 0: a callback lost to a concurrent teardown must not
    // leave the counter stuck >= 2 (which would drop every future
    // frame as "backlog" forever). teardownSession also resets it.
    if (_inFlight.fetch_sub(1) <= 1) {
        _inFlight.store(0);
    }

    if (status != noErr || !sampleBuffer) {
        int errors = _consecutiveEncodeErrors.fetch_add(1) + 1;
        TVLog(@"H264: encode callback error %d (consecutive=%d)", (int)status, errors);
        if (errors >= kH264MaxConsecutiveEncodeErrors) {
            TVLog(@"H264: %d consecutive encode errors — tearing the session down (it will be recreated on the "
                  @"next frame with a viewer)",
                  errors);
            _consecutiveEncodeErrors.store(0);
            [self teardownSession];
        }
        return;
    }
    _consecutiveEncodeErrors.store(0);

    if (_clientFd.load() < 0) {
        return; // viewer left between encode and callback; frame is useless
    }

    CMBlockBufferRef blockBuffer = CMSampleBufferGetDataBuffer(sampleBuffer);
    if (!blockBuffer)
        return;

    size_t totalLength = 0;
    char *dataPointer = NULL;
    OSStatus blockStatus = CMBlockBufferGetDataPointer(blockBuffer, 0, NULL, &totalLength, &dataPointer);
    if (blockStatus != noErr || !dataPointer || totalLength < 4)
        return;

    BOOL isKeyframe = [self sampleBufferIsKeyframe:sampleBuffer];
    NSMutableData *annexB = [self annexBDataFromAVCCData:dataPointer length:totalLength];
    if (!annexB || annexB.length == 0)
        return; // malformed AVCC (logged inside, once); drop the frame

    if (isKeyframe) {
        // Prepend SPS/PPS so a client that just connected (or that lost
        // the previous keyframe) can decode this frame. Rebuilt from
        // the format description every keyframe: cheap, and it can
        // never go stale.
        NSData *spsPps = [self spsPpsAnnexBFromSampleBuffer:sampleBuffer];
        if (spsPps && spsPps.length > 0) {
            _cachedSpsPpsAnnexB = spsPps; // sent on the next connect
            NSMutableData *withParams = [NSMutableData dataWithData:spsPps];
            [withParams appendData:annexB];
            annexB = withParams;
        }
    }

    _framesEncoded++;
    if (isKeyframe)
        _keyframesSent++;

    if (![self sendFramedData:annexB closeClientOnFailure:YES]) {
        // sendFramedData already closed the client on partial/error.
        return;
    }
    _framesSent++;
    _bytesSent += (uint64_t)annexB.length + 4; // payload + length prefix

    [_sessionLock lock];
    BOOL logFirst = !_loggedFirstFrame;
    if (logFirst)
        _loggedFirstFrame = YES;
    [_sessionLock unlock];
    if (logFirst) {
        TVLog(@"H264: first encoded frame sent: %zu bytes, keyframe=%@ (stream is live)",
              (size_t)annexB.length, isKeyframe ? @"YES" : @"NO");
    }

    CFAbsoluteTime now = CFAbsoluteTimeGetCurrent();
    if (_lastStatsLogTime == 0) {
        _lastStatsLogTime = now;
    } else if (now - _lastStatsLogTime >= kH264StatsLogIntervalSec) {
        _lastStatsLogTime = now;
        TVLog(@"H264: stats enc=%llu sent=%llu key=%llu dropped(no-client=%llu backlog=%llu send=%llu) "
              @"bytes=%llu clients-dropped-partial=%llu",
              _framesEncoded, _framesSent, _keyframesSent, _framesDroppedNoClient, _framesDroppedBacklog,
              _framesDroppedSend, _bytesSent, _clientsDroppedPartial);
    }
}

// Queue-only: YES when the sample has no NotSync attachment (i.e. it is
// a keyframe / sync sample). Attachments array is Get-rule: not owned.
- (BOOL)sampleBufferIsKeyframe:(CMSampleBufferRef)sampleBuffer {
    CFArrayRef attachments = CMSampleBufferGetSampleAttachmentsArray(sampleBuffer, false);
    if (!attachments || CFArrayGetCount(attachments) == 0)
        return YES; // no attachments: treat as sync (safe: forces SPS/PPS prepend)
    CFDictionaryRef attachment = (CFDictionaryRef)CFArrayGetValueAtIndex(attachments, 0);
    if (!attachment)
        return YES;
    CFBooleanRef notSync = (CFBooleanRef)CFDictionaryGetValue(attachment, kCMSampleAttachmentKey_NotSync);
    return (notSync == NULL) || !CFBooleanGetValue(notSync);
}

// Queue-only: AVCC (4-byte big-endian length-prefixed NALs) -> Annex-B
// (0x00000001 start codes). Returns nil on malformed input.
- (nullable NSMutableData *)annexBDataFromAVCCData:(const char *)data length:(size_t)length {
    NSMutableData *out = [NSMutableData dataWithCapacity:length + 32];
    size_t offset = 0;
    while (offset + 4 <= length) {
        // memcpy, not a direct uint32_t deref: NAL boundaries are not
        // guaranteed 4-byte aligned, and an unaligned deref is UB.
        uint32_t nalLengthBE = 0;
        memcpy(&nalLengthBE, data + offset, 4);
        uint32_t nalLength = CFSwapInt32BigToHost(nalLengthBE);
        offset += 4;
        if (nalLength == 0 || offset + nalLength > length) {
            static BOOL sLoggedOnce = NO;
            if (!sLoggedOnce) {
                sLoggedOnce = YES;
                TVLog(@"H264: malformed AVCC data (nalLength=%u at offset=%zu of %zu) — dropping frame",
                      nalLength, offset, length);
            }
            return nil;
        }
        [out appendBytes:kH264AnnexBStartCode length:4];
        [out appendBytes:(data + offset) length:nalLength];
        offset += nalLength;
    }
    if (offset != length) {
        // Trailing 1-3 bytes that cannot form a length prefix: malformed.
        TVLogVerbose(@"H264: AVCC trailing %zu bytes ignored", length - offset);
    }
    return out;
}

// Queue-only: SPS (index 0) + PPS (index 1) from the format description,
// as Annex-B. Parameter-set pointers are owned by the format description.
- (nullable NSData *)spsPpsAnnexBFromSampleBuffer:(CMSampleBufferRef)sampleBuffer {
    CMFormatDescriptionRef formatDesc = CMSampleBufferGetFormatDescription(sampleBuffer);
    if (!formatDesc)
        return nil;
    NSMutableData *out = [NSMutableData data];
    for (size_t index = 0; index < 2; index++) {
        const uint8_t *paramSet = NULL;
        size_t paramSetSize = 0;
        size_t paramSetCount = 0;
        int nalHeaderLength = 0;
        OSStatus status = CMVideoFormatDescriptionGetH264ParameterSetAtIndex(formatDesc, index, &paramSet,
                                                                             &paramSetSize, &paramSetCount,
                                                                             &nalHeaderLength);
        if (status != noErr || !paramSet || paramSetSize == 0)
            return nil; // SPS without PPS (or vice versa) is useless; skip both
        [out appendBytes:kH264AnnexBStartCode length:4];
        [out appendBytes:paramSet length:paramSetSize];
    }
    return out;
}

// Queue-only: send one length-prefixed frame (4-byte big-endian length +
// payload) with a SINGLE send() call. Returns YES on full send.
// - EAGAIN (nothing sent): the whole frame is dropped, stream intact.
// - Partial send or any other error: the client is CLOSED (a half-sent
//   length-prefixed frame would desync it; reconnect resyncs cleanly),
//   unless closeClientOnFailure is NO (SPS/PPS on connect: the next
//   keyframe carries SPS/PPS anyway, so a failure there must never
//   close a fresh client). Returns NO on any failure.
- (BOOL)sendFramedData:(NSData *)payload closeClientOnFailure:(BOOL)closeClientOnFailure {
    int fd = _clientFd.load();
    if (fd < 0)
        return NO;

    uint32_t netLength = CFSwapInt32HostToBig((uint32_t)payload.length);
    // Single contiguous buffer: header + payload, one send() call.
    NSMutableData *framed = [NSMutableData dataWithCapacity:payload.length + 4];
    [framed appendBytes:&netLength length:4];
    [framed appendData:payload];

    ssize_t sent = send(fd, framed.bytes, framed.length, MSG_DONTWAIT);
    if (sent < 0 && errno == EINTR) {
        // Extremely unlikely with MSG_DONTWAIT; retry once.
        sent = send(fd, framed.bytes, framed.length, MSG_DONTWAIT);
    }
    if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            _framesDroppedSend++;
            TVLogVerbose(@"H264: socket would block — dropping %zu-byte frame (client slow)",
                         (size_t)payload.length);
            return NO;
        }
        TVLog(@"H264: send() failed: %s", strerror(errno));
        if (closeClientOnFailure) {
            _clientsDroppedPartial++;
            [self closeClientWithReason:@"send error"];
        }
        return NO;
    }
    if (sent != (ssize_t)framed.length) {
        // Partial send: the client would desync. Close it; reconnect
        // resyncs with fresh SPS/PPS + forced keyframe.
        TVLog(@"H264: partial send (%zd of %zu bytes)", sent, (size_t)framed.length);
        if (closeClientOnFailure) {
            _clientsDroppedPartial++;
            [self closeClientWithReason:@"partial send"];
        }
        return NO;
    }
    return YES;
}

#pragma mark - Status

- (NSString *)statusDescription {
    // Counters are queue-owned; read them on the queue unless already there.
    __block NSString *desc = nil;
    void (^buildDesc)(void) = ^{
        [_sessionLock lock];
        int w = _encWidth, h = _encHeight;
        [_sessionLock unlock];
        desc = [NSString stringWithFormat:@"H264 port=%d %@ %dx%d @%d bps enc=%llu sent=%llu dropped=%llu",
                                          _port, (_clientFd.load() >= 0) ? @"client-connected" : @"no-client",
                                          w, h, _bitrateBps, _framesEncoded, _framesSent,
                                          _framesDroppedBacklog + _framesDroppedSend];
    };
    if ([self isOnQueue]) {
        buildDesc();
    } else {
        dispatch_sync(_queue, buildDesc);
    }
    return desc ?: @"H264 (status unavailable)";
}

@end
