// Copyright (c) 2025 Perfect Aduh. MIT License. See LICENSE for details.

@testable import AGUIClient
import AGUICore
import XCTest

final class HttpSSELifecycleTests: XCTestCase {
    func testInvalidSSEResponsesCancelTheirRequests() async throws {
        for (status, headers) in [
            (200, ["Content-Type": "application/json"]),
            (200, [:]),
            (204, ["Content-Type": "text/event-stream"]),
        ] {
            try await assertRejectedResponseIsCancelled(status: status, headers: headers, error: .invalidResponse)
        }
    }

    func testHTTPFailureCancelsItsRequest() async throws {
        try await assertRejectedResponseIsCancelled(status: 503, headers: [:], error: .httpError(statusCode: 503))
    }

    private func assertRejectedResponseIsCancelled(
        status: Int, headers: [String: String], error: ClientError
    ) async throws {
        let cancelled = expectation(description: "rejected response cancelled")
        let url = try XCTUnwrap(URL(string: "https://agent.example.com"))
        let metadata = try XCTUnwrap(HTTPURLResponse(url: url, statusCode: status, httpVersion: nil, headerFields: headers))
        let response = HTTPResponse(
            bytes: AsyncThrowingStream { $0.finish() },
            httpResponse: metadata,
            cancel: { cancelled.fulfill() }
        )
        let client = MockHTTPClient()
        await client.setResponse(response)
        let transport = HttpTransport(configuration: HttpAgentConfiguration(baseURL: url), httpClient: client)
        do {
            _ = try await transport.execute(endpoint: "/run", input: RunAgentInput(threadId: "thread", runId: "run"))
            XCTFail("Expected rejected response")
        } catch let actual as ClientError {
            XCTAssertEqual(actual, error)
        }
        await fulfillment(of: [cancelled], timeout: 1)
    }
}
