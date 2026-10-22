// Copyright (c) 2025 Perfect Aduh. MIT License. See LICENSE for details.

import AGUICore
import Foundation

/// Controls whether malformed stream data is skipped or reported to the consumer.
public enum EventStreamDecodingPolicy: Sendable {
    /// Skip malformed events and continue with the next complete event (the default).
    case tolerant
    /// Throw decoder errors and explicit SSE framing errors.
    case strict
}

/// Errors reported by strict streaming validation.
public enum EventStreamError: Error, Sendable, Equatable {
    /// A completed event or trailing fragment contains invalid UTF-8.
    case invalidUTF8
    /// A single event exceeds the configured byte limit.
    case eventTooLarge(maximumByteCount: Int)
    /// The stream ended before the final event's blank-line terminator.
    case incompleteEvent
}

/// AsyncSequence of AG-UI events from a streaming HTTP response.
///
/// `EventStream` integrates the complete event streaming pipeline:
/// 1. Reads bytes from HTTP response (any AsyncSequence<UInt8>)
/// 2. Parses Server-Sent Events (SSE) using SseParser
/// 3. Decodes AG-UI events using AGUIEventDecoder
/// 4. Handles errors gracefully
///
public struct EventStream<Bytes: AsyncSequence>: AsyncSequence where Bytes.Element == UInt8 {
    public typealias Element = any AGUIEvent

    // MARK: - Shared last-event-id box

    /// Reference box that lets the iterator write the last SSE id back to the stream.
    ///
    /// Using a class (reference semantics) means every iterator created from the same
    /// stream updates the same location, so callers can read `lastEventId` on the
    /// stream value after an iterator throws.
    ///
    /// Declared as an `actor` so the compiler synthesises `Sendable` automatically
    /// and serialises all reads and writes without manual locking.
    actor LastEventIdBox {
        var value: String?

        func set(_ newValue: String?) {
            value = newValue
        }
    }

    // MARK: - Stored properties

    /// The source byte stream from HTTP response.
    private let bytes: Bytes

    /// The AG-UI event decoder.
    private let decoder: AGUIEventDecoder
    private let decodingPolicy: EventStreamDecodingPolicy
    private let maximumEventByteCount: Int

    /// Shared box updated by the iterator whenever a non-nil SSE `id:` field is seen.
    private let lastEventIdBox = LastEventIdBox()

    // MARK: - Public surface

    /// The most recent SSE event `id:` field received, or `nil` if none has arrived yet.
    ///
    /// Read this after a mid-stream failure to obtain the resume cursor for
    /// `Last-Event-ID` on reconnect.
    public var lastEventId: String? { get async { await lastEventIdBox.value } }

    // MARK: - Initialization

    /// Creates a new event stream.
    ///
    /// - Parameters:
    ///   - bytes: Async sequence of bytes from HTTP streaming response
    ///   - decoder: AG-UI event decoder
    ///   - decodingPolicy: Whether malformed events are skipped or thrown (default: tolerant).
    ///   - maximumEventByteCount: Positive limit for a single buffered SSE event.
    public init(bytes: Bytes, decoder: AGUIEventDecoder,
                decodingPolicy: EventStreamDecodingPolicy = .tolerant,
                maximumEventByteCount: Int = SseParser.maxBufferByteCount)
    {
        self.bytes = bytes
        self.decoder = decoder
        self.decodingPolicy = decodingPolicy
        self.maximumEventByteCount = maximumEventByteCount
    }

    /// Creates an async iterator for streaming events.
    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(bytes: bytes.makeAsyncIterator(), decoder: decoder, lastEventIdBox: lastEventIdBox,
                      decodingPolicy: decodingPolicy, maximumEventByteCount: maximumEventByteCount)
    }

    /// Iterator that processes streaming bytes into AG-UI events.
    ///
    /// The iterator maintains internal state for:
    /// - Bounded SSE byte parsing
    /// - Event decoding policy
    /// - Last-Event-ID tracking (via shared `LastEventIdBox`)
    public struct AsyncIterator: AsyncIteratorProtocol {
        /// Source byte iterator.
        private var bytesIterator: Bytes.AsyncIterator

        /// AG-UI event decoder.
        private let decoder: AGUIEventDecoder

        /// SSE parser for extracting events from stream.
        private var sseParser: SseParser
        private let decodingPolicy: EventStreamDecodingPolicy

        /// Shared reference that is updated with the most recent SSE `id:` value.
        private let lastEventIdBox: LastEventIdBox

        /// Creates a new iterator.
        ///
        /// - Parameters:
        ///   - bytes: Source byte iterator
        ///   - decoder: AG-UI event decoder
        ///   - lastEventIdBox: Shared box written to whenever a non-nil SSE id is seen
        init(bytes: Bytes.AsyncIterator, decoder: AGUIEventDecoder, lastEventIdBox: LastEventIdBox,
             decodingPolicy: EventStreamDecodingPolicy, maximumEventByteCount: Int)
        {
            bytesIterator = bytes
            self.decoder = decoder
            self.lastEventIdBox = lastEventIdBox
            self.decodingPolicy = decodingPolicy
            sseParser = SseParser(maximumEventByteCount: maximumEventByteCount)
        }

        /// Returns the next AG-UI event from the stream.
        ///
        /// Reads through the next complete event, skipping malformed events in tolerant mode.
        /// Strict mode reports decoder errors, invalid UTF-8, oversized events and incomplete EOF.
        ///
        /// - Returns: Next event, or nil when stream ends
        /// - Throws: Network errors or critical decoding failures
        public mutating func next() async throws -> (any AGUIEvent)? {
            try Task.checkCancellation()
            while let byte = try await bytesIterator.next() {
                try Task.checkCancellation()
                guard let sseEvent = try sseParser.consume(byte, policy: decodingPolicy) else { continue }
                if let id = sseEvent.id {
                    await lastEventIdBox.set(id)
                }
                do {
                    let event = try decoder.decode(Data(sseEvent.data.utf8))
                    await Task.yield()
                    return event
                } catch {
                    if decodingPolicy == .strict { throw error }
                }
            }
            // Async streams may return nil when their consumer is cancelled.
            // Cancellation is not a malformed end-of-input condition.
            try Task.checkCancellation()
            try sseParser.finish(policy: decodingPolicy)
            return nil
        }
    }
}
