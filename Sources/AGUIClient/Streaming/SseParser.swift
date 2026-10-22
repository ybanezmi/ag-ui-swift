// Copyright (c) 2025 Perfect Aduh. MIT License. See LICENSE for details.

import Foundation

/// Incremental parser for Server-Sent Events (SSE) streams.
///
/// `SseParser` is a stateful parser that handles SSE data arriving in
/// arbitrary chunks. It maintains an internal buffer for incomplete events
/// and returns complete events as they become available.
///
public struct SseParser {
    /// Default maximum UTF-8 bytes retained for a single event (10 MiB).
    public static let maxBufferByteCount = 10 * 1_048_576

    private let maximumEventByteCount: Int
    private var buffer: [UInt8] = []
    private var lineIsEmpty = true
    private var previousWasCR = false
    private var discardingEvent = false

    /// Creates a parser with a positive per-event memory limit.
    ///
    /// Oversized events are discarded through the next blank line, so their
    /// remaining fields cannot be interpreted as a new event.
    /// - Parameter maximumEventByteCount: Maximum buffered bytes, including line endings.
    public init(maximumEventByteCount: Int = Self.maxBufferByteCount) {
        precondition(maximumEventByteCount > 0, "The event size limit must be positive")
        self.maximumEventByteCount = maximumEventByteCount
    }

    /// Parses text incrementally, returning complete events and ignoring malformed events.
    /// CRLF pairs are recognized even when split across calls. The memory limit
    /// applies to individual events rather than the size of the supplied chunk.
    public mutating func parse(_ chunk: String) -> [SseEvent] {
        var events: [SseEvent] = []
        for byte in chunk.utf8 {
            if let event = try? consume(byte, policy: .tolerant) {
                events.append(event)
            }
        }
        return events
    }

    /// Feeds a byte without accumulating an unbounded line or UTF-8 buffer.
    mutating func consume(_ byte: UInt8, policy: EventStreamDecodingPolicy) throws -> SseEvent? {
        if previousWasCR {
            previousWasCR = false
            if byte == 10 { return nil }
        }
        if byte == 13 { previousWasCR = true }
        let isNewline = byte == 10 || byte == 13
        if isNewline, lineIsEmpty {
            defer {
                buffer.removeAll(keepingCapacity: true)
                discardingEvent = false
            }
            guard !discardingEvent, !buffer.isEmpty else { return nil }
            guard let text = String(bytes: buffer, encoding: .utf8) else {
                if policy == .strict { throw EventStreamError.invalidUTF8 }
                return nil
            }
            return parseEvent(text)
        }
        lineIsEmpty = isNewline
        guard !discardingEvent else { return nil }
        guard buffer.count < maximumEventByteCount else {
            buffer.removeAll(keepingCapacity: true)
            discardingEvent = true
            if policy == .strict {
                throw EventStreamError.eventTooLarge(maximumByteCount: maximumEventByteCount)
            }
            return nil
        }
        buffer.append(isNewline ? 10 : byte)
        return nil
    }

    /// Validates unfinished data when the input ends. SSE requires a blank-line terminator.
    mutating func finish(policy: EventStreamDecodingPolicy) throws {
        defer { reset() }
        guard policy == .strict, !buffer.isEmpty || discardingEvent else { return }
        if String(bytes: buffer, encoding: .utf8) == nil { throw EventStreamError.invalidUTF8 }
        throw EventStreamError.incompleteEvent
    }

    /// Parses a single complete event from its text representation.
    ///
    /// - Parameter text: The event text (between double newlines)
    /// - Returns: Parsed event, or nil if no data field present
    private func parseEvent(_ text: String) -> SseEvent? {
        var dataLines: [String] = []
        var id: String?
        var eventType: String?

        // Process each line in the event
        for line in text.components(separatedBy: "\n") {
            // Skip empty lines
            guard !line.isEmpty else { continue }

            // Comments start with ':'
            if line.hasPrefix(":") {
                continue
            }

            // Find the colon separator
            guard let colonIndex = line.firstIndex(of: ":") else {
                // Lines without colon are ignored per spec
                continue
            }

            let field = String(line[..<colonIndex])
            var value = String(line[line.index(after: colonIndex)...])

            // Remove single leading space after colon (per spec)
            if value.hasPrefix(" ") {
                value = String(value.dropFirst())
            }

            // Process field
            switch field {
            case "data":
                dataLines.append(value)
            case "id":
                id = value
            case "event":
                eventType = value
            default:
                // Unknown fields are ignored per spec
                break
            }
        }

        // Events without data field are ignored
        guard !dataLines.isEmpty else {
            return nil
        }

        // Concatenate multiple data lines with newlines
        let data = dataLines.joined(separator: "\n")

        return SseEvent(
            data: data,
            id: id,
            event: eventType ?? "message"
        )
    }

    /// Resets the parser, clearing any buffered incomplete events.
    ///
    /// Use this when starting a new stream or when an error requires
    /// discarding incomplete data.
    ///
    /// ## Example
    ///
    /// ```swift
    /// var parser = SseParser()
    /// _ = parser.parse("data: incomplete")
    ///
    /// // Discard buffered data
    /// parser.reset()
    ///
    /// // Start fresh
    /// let events = parser.parse("data: new\n\n")
    /// ```
    public mutating func reset() {
        buffer.removeAll(keepingCapacity: true)
        lineIsEmpty = true
        previousWasCR = false
        discardingEvent = false
    }
}
