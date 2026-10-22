// Copyright (c) 2025 Perfect Aduh. MIT License. See LICENSE for details.

@testable import AGUIClient
@testable import AGUICore
import XCTest

final class StrictEventStreamTests: XCTestCase {
    private let valid = "data: {\"type\":\"RUN_STARTED\",\"threadId\":\"t\",\"runId\":\"r\"}\n\n"

    private func count(_ data: Data, policy: EventStreamDecodingPolicy = .tolerant,
                       limit: Int = SseParser.maxBufferByteCount) async throws -> Int
    {
        let bytes = AsyncStream<UInt8> { continuation in
            for byte in data {
                continuation.yield(byte)
            }
            continuation.finish()
        }
        let stream = EventStream(bytes: bytes, decoder: AGUIEventDecoder(), decodingPolicy: policy,
                                 maximumEventByteCount: limit)
        var count = 0
        for try await _ in stream {
            count += 1
        }
        return count
    }

    func testTolerantSkipsMalformedJSONAndKnownEvent() async throws {
        let result = try await count(Data(("data: invalid\n\ndata: {\"type\":\"RUN_STARTED\"}\n\n" + valid).utf8))
        XCTAssertEqual(result, 1)
    }

    func testStrictRejectsMalformedJSONAndKnownEvent() async {
        for payload in ["invalid", "{\"type\":\"RUN_STARTED\"}"] {
            do {
                _ = try await count(Data("data: \(payload)\n\n".utf8), policy: .strict)
                XCTFail("Expected decoding failure")
            } catch {
                XCTAssertTrue(error is EventDecodingError)
            }
        }
    }

    func testInvalidUTF8StrictAndTolerant() async throws {
        let data = Data("data: ".utf8) + Data([0xFF]) + Data(("\n\n" + valid).utf8)
        let result = try await count(data)
        XCTAssertEqual(result, 1)
        do {
            _ = try await count(data, policy: .strict)
            XCTFail("Expected invalid UTF-8")
        } catch {
            XCTAssertEqual(error as? EventStreamError, .invalidUTF8)
        }
    }

    func testOversizedStrictAndTolerantRecovery() async throws {
        let data = Data((String(repeating: "x", count: 100) + "data: suffix\n\n" + valid).utf8)
        let result = try await count(data, limit: 80)
        XCTAssertEqual(result, 1)
        do {
            _ = try await count(data, policy: .strict, limit: 80)
            XCTFail("Expected event size error")
        } catch {
            XCTAssertEqual(error as? EventStreamError, .eventTooLarge(maximumByteCount: 80))
        }
    }

    func testIncompleteEOFStrictAndTolerant() async throws {
        let result = try await count(Data("data: unfinished".utf8))
        XCTAssertEqual(result, 0)
        do {
            _ = try await count(Data("data: unfinished".utf8), policy: .strict)
            XCTFail("Expected incomplete event")
        } catch {
            XCTAssertEqual(error as? EventStreamError, .incompleteEvent)
        }
    }

    func testInvalidUTF8AtEOF() async {
        do {
            _ = try await count(Data("data: ".utf8) + Data([0xF0, 0x9F]), policy: .strict)
            XCTFail("Expected truncated UTF-8 failure")
        } catch {
            XCTAssertEqual(error as? EventStreamError, .invalidUTF8)
        }
    }

    func testConfigurationPropagatesToBothRunAPIs() async throws {
        let configuration = HttpAgentConfiguration(baseURL: URL(string: "https://agent.example.com")!,
                                                   decodingPolicy: .strict)
        let input = try RunAgentInput.builder().threadId("t").runId("r").build()
        for useTransport in [false, true] {
            let client = MockHTTPClient()
            try await client.setResponse(HTTPResponse.mock(data: Data("data: invalid\n\n".utf8),
                                                           headers: ["Content-Type": "text/event-stream"]))
            do {
                if useTransport {
                    let transport = HttpAgentTransport(configuration: configuration, httpClient: client)
                    for try await _ in transport.run(input: input) {}
                } else {
                    let agent = HttpAgent(configuration: configuration, httpClient: client)
                    for try await _ in try await agent.run(input) {}
                }
                XCTFail("Expected strict decoder failure")
            } catch {
                XCTAssertTrue(error is EventDecodingError)
            }
        }
    }

    func testCancelledPartialStreamReportsCancellation() async {
        let waiting = expectation(description: "Consumed partial input and waiting for more")
        let bytes = PartialBytes(prefix: Array("data: partial".utf8), waiting: waiting)
        let stream = EventStream(bytes: bytes, decoder: AGUIEventDecoder(), decodingPolicy: .strict)
        let consumer = Task {
            var iterator = stream.makeAsyncIterator()
            return try await iterator.next()
        }
        defer { consumer.cancel() }
        await fulfillment(of: [waiting], timeout: 2)
        consumer.cancel()
        do {
            _ = try await consumer.value
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError, "Unexpected error: \(error)")
        }
    }

    func testConfiguredSizeLimitPropagatesToBothRunAPIs() async throws {
        let configuration = HttpAgentConfiguration(baseURL: URL(string: "https://agent.example.com")!,
                                                   decodingPolicy: .strict, maximumEventByteCount: 8)
        let input = try RunAgentInput.builder().threadId("t").runId("r").build()
        for useTransport in [false, true] {
            let client = MockHTTPClient()
            try await client.setResponse(HTTPResponse.mock(data: Data(valid.utf8),
                                                           headers: ["Content-Type": "text/event-stream"]))
            do {
                if useTransport {
                    let transport = HttpAgentTransport(configuration: configuration, httpClient: client)
                    for try await _ in transport.run(input: input) {}
                } else {
                    let agent = HttpAgent(configuration: configuration, httpClient: client)
                    for try await _ in try await agent.run(input) {}
                }
                XCTFail("Expected configured size limit")
            } catch {
                XCTAssertEqual(error as? EventStreamError, .eventTooLarge(maximumByteCount: 8))
            }
        }
    }

    func testSplitUnicodeAndCRLF() async throws {
        let input = "data: {\"type\":\"TEXT_MESSAGE_CONTENT\",\"messageId\":\"m\",\"delta\":\"🌍\"}\r\n\r\n"
        let result = try await count(Data(input.utf8), policy: .strict)
        XCTAssertEqual(result, 1)
    }
}

private struct PartialBytes: AsyncSequence {
    typealias Element = UInt8
    let prefix: [UInt8]
    let waiting: XCTestExpectation

    func makeAsyncIterator() -> Iterator {
        Iterator(prefix: prefix, waiting: waiting)
    }

    struct Iterator: AsyncIteratorProtocol {
        let prefix: [UInt8]
        let waiting: XCTestExpectation
        var index = 0

        mutating func next() async throws -> UInt8? {
            if index < prefix.count {
                defer { index += 1 }
                return prefix[index]
            }
            waiting.fulfill()
            let heldOpen = AsyncThrowingStream<UInt8, Error> { _ in }
            var iterator = heldOpen.makeAsyncIterator()
            return try await iterator.next()
        }
    }
}
