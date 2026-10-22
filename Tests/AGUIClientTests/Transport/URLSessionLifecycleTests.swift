// Copyright (c) 2025 Perfect Aduh. MIT License. See LICENSE for details.

@testable import AGUIClient
import Foundation
import XCTest

final class URLSessionLifecycleTests: XCTestCase {
    func testCancellingConsumerStopsUnderlyingRequest() async throws {
        let scenario = StreamScenario()
        let session = makeSession()
        defer { session.invalidateAndCancel() }
        let response = try await URLSessionHTTPClient(session: session).execute(scenario.request)
        let consumer = Task {
            for try await _ in response.bytes {}
        }
        consumer.cancel()
        _ = try? await consumer.value
        await fulfillment(of: [scenario.stopped], timeout: 2)
    }

    func testCancellingBeforeHeadersStopsUnderlyingRequest() async {
        let scenario = StreamScenario(sendsHeaders: false)
        let session = makeSession()
        defer { session.invalidateAndCancel() }
        let client = URLSessionHTTPClient(session: session)
        let requestTask = Task { try await client.execute(scenario.request) }
        await fulfillment(of: [scenario.started], timeout: 2)
        requestTask.cancel()
        do {
            _ = try await requestTask.value
            XCTFail("Expected cancellation")
        } catch let error as ClientError {
            guard case .cancelled = error else { return XCTFail("Unexpected error: \(error)") }
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        await fulfillment(of: [scenario.stopped], timeout: 2)
    }

    func testOwnedSessionIsInvalidatedWhenClientIsReleased() async throws {
        let scenario = StreamScenario()
        let configuration = makeConfiguration()
        var client: URLSessionHTTPClient? = .create(configuration: configuration)
        let response = try await client!.execute(scenario.request)
        weak var releasedClient = client
        client = nil
        XCTAssertNil(releasedClient)
        await fulfillment(of: [scenario.stopped], timeout: 2)
        withExtendedLifetime(response) {}
    }

    func testBorrowedSessionSurvivesClientRelease() async throws {
        let session = makeSession()
        defer { session.invalidateAndCancel() }
        var client: URLSessionHTTPClient? = URLSessionHTTPClient(session: session)
        weak var releasedClient = client
        client = nil
        XCTAssertNil(releasedClient)
        let scenario = StreamScenario(finishes: true)
        let response = try await URLSessionHTTPClient(session: session).execute(scenario.request)
        var received: [UInt8] = []
        for try await byte in response.bytes {
            received.append(byte)
        }
        XCTAssertEqual(received, [42])
    }

    func testExplicitCancellationAfterEarlyBreakStopsRequest() async throws {
        let scenario = StreamScenario()
        let session = makeSession()
        defer { session.invalidateAndCancel() }
        let response = try await URLSessionHTTPClient(session: session).execute(scenario.request)
        for try await byte in response.bytes {
            XCTAssertEqual(byte, 42)
            break
        }
        response.cancel()
        response.cancel()
        await fulfillment(of: [scenario.stopped], timeout: 2)
        var iterator = response.bytes.makeAsyncIterator()
        do {
            _ = try await iterator.next()
            XCTFail("Expected cancelled stream")
        } catch {}
    }

    func testCancellingOneResponseLeavesSharedSessionUsable() async throws {
        let session = makeSession()
        defer { session.invalidateAndCancel() }
        let client = URLSessionHTTPClient(session: session)
        let first = StreamScenario()
        let response = try await client.execute(first.request)
        response.cancel()
        await fulfillment(of: [first.stopped], timeout: 2)

        let second = StreamScenario(finishes: true)
        let nextResponse = try await client.execute(second.request)
        var received: [UInt8] = []
        for try await byte in nextResponse.bytes {
            received.append(byte)
        }
        XCTAssertEqual(received, [42])
    }

    func testReleasingResponseStopsRequest() async throws {
        let scenario = StreamScenario()
        let session = makeSession()
        defer { session.invalidateAndCancel() }
        var response: HTTPResponse? = try await URLSessionHTTPClient(session: session).execute(scenario.request)
        XCTAssertNotNil(response)
        response = nil
        await fulfillment(of: [scenario.stopped], timeout: 2)
    }

    private func makeConfiguration() -> URLSessionConfiguration {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LifecycleURLProtocol.self]
        return configuration
    }

    private func makeSession() -> URLSession {
        URLSession(configuration: makeConfiguration())
    }
}

private final class StreamScenario: NSObject {
    let started = XCTestExpectation(description: "request started")
    let stopped = XCTestExpectation(description: "request stopped")
    let sendsHeaders: Bool
    let finishes: Bool

    init(sendsHeaders: Bool = true, finishes: Bool = false) {
        self.sendsHeaders = sendsHeaders
        self.finishes = finishes
    }

    var request: URLRequest {
        let request = NSMutableURLRequest(url: URL(string: "https://stream.example/\(UUID().uuidString)")!)
        URLProtocol.setProperty(self, forKey: "scenario", in: request)
        return request as URLRequest
    }
}

private final class LifecycleURLProtocol: URLProtocol {
    override class func canInit(with _: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let scenario = URLProtocol.property(forKey: "scenario", in: request) as? StreamScenario else {
            client?.urlProtocol(self, didFailWithError: URLError(.badURL))
            return
        }
        scenario.started.fulfill()
        if scenario.sendsHeaders {
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil,
                                           headerFields: ["Content-Type": "text/event-stream"])!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: Data([42]))
            if scenario.finishes { client?.urlProtocolDidFinishLoading(self) }
        }
    }

    override func stopLoading() {
        let scenario = URLProtocol.property(forKey: "scenario", in: request) as? StreamScenario
        scenario?.stopped.fulfill()
    }
}
