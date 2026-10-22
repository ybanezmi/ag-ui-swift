// Copyright (c) 2025 Perfect Aduh. MIT License. See LICENSE for details.

import Foundation

/// URLSession-based HTTP client implementation.
///
/// `URLSessionHTTPClient` is the default HTTP client that uses Apple's
/// URLSession for networking. It supports full URLSession configuration
/// and can be injected with a custom session for testing.
///
public actor URLSessionHTTPClient: HTTPClient {
    private let session: URLSession
    private let ownsSession: Bool

    /// Creates a new URLSession HTTP client with the specified session.
    ///
    /// This is the primary initializer that accepts a URLSession instance,
    /// enabling full control over session configuration and injection of
    /// mock sessions for testing.
    ///
    /// The caller retains ownership; releasing this client never invalidates the session.
    ///
    /// - Parameter session: The URLSession to use for requests
    ///
    /// ## Example
    ///
    /// ```swift
    /// let config = URLSessionConfiguration.default
    /// config.timeoutIntervalForRequest = 60
    /// let session = URLSession(configuration: config)
    /// let client = URLSessionHTTPClient(session: session)
    /// ```
    public init(session: URLSession) {
        self.session = session
        ownsSession = false
    }

    private init(ownedSession: URLSession) {
        session = ownedSession
        ownsSession = true
    }

    deinit {
        if ownsSession { session.invalidateAndCancel() }
    }

    /// Creates a new URLSession HTTP client with the specified configuration.
    ///
    /// This factory method provides a convenient way to create a client
    /// with custom URLSession configuration. The client owns the created session
    /// and invalidates it when released.
    ///
    /// - Parameter configuration: URLSession configuration (default: .default)
    /// - Returns: A new URLSession HTTP client
    ///
    /// ## Example
    ///
    /// ```swift
    /// let config = URLSessionConfiguration.ephemeral
    /// config.timeoutIntervalForRequest = 30
    /// let client = URLSessionHTTPClient.create(configuration: config)
    /// ```
    public static func create(
        configuration: URLSessionConfiguration = .default
    ) -> URLSessionHTTPClient {
        let session = URLSession(configuration: configuration)
        return URLSessionHTTPClient(ownedSession: session)
    }

    /// Executes an HTTP request using URLSession.
    ///
    /// - Parameter request: The URL request to execute
    /// - Returns: An HTTP response containing streaming bytes and metadata
    /// - Throws: `ClientError` if the request fails
    ///
    /// ## Error Mapping
    ///
    /// URLErrors are mapped to ClientError:
    /// - `.timedOut` → `.timeout`
    /// - `.cancelled` → `.cancelled`
    /// - Other errors → `.networkError`
    public func execute(_ request: URLRequest) async throws -> HTTPResponse {
        let (bytes, response): (URLSession.AsyncBytes, URLResponse)

        do {
            (bytes, response) = try await session.bytes(for: request)
        } catch let error as URLError {
            throw mapURLError(error)
        } catch {
            throw ClientError.networkError(error)
        }

        guard let httpResponse = response as? HTTPURLResponse else {
            bytes.task.cancel()
            throw ClientError.invalidResponse
        }

        // Bridge URLSession.AsyncBytes → AsyncThrowingStream<UInt8, Error> so that
        // HTTPResponse is decoupled from URLSession and can be mocked in tests.
        let (stream, continuation) = AsyncThrowingStream<UInt8, Error>.makeStream()
        // This producer bridges a callback-style stream to an async sequence. Its
        // handle is retained by termination/cancellation and cancelled with the request.
        let producer = Task {
            do {
                for try await byte in bytes {
                    try Task.checkCancellation()
                    guard case .enqueued = continuation.yield(byte) else { break }
                }
                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }
        continuation.onTermination = { _ in
            producer.cancel()
            bytes.task.cancel()
        }

        return HTTPResponse(bytes: stream, httpResponse: httpResponse, cancel: {
            producer.cancel()
            bytes.task.cancel()
            continuation.finish(throwing: CancellationError())
        })
    }

    /// Maps URLError to ClientError.
    private func mapURLError(_ error: URLError) -> ClientError {
        switch error.code {
        case .timedOut:
            .timeout
        case .cancelled:
            .cancelled
        default:
            .networkError(error)
        }
    }
}
