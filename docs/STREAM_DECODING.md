# SSE stream decoding

`HttpAgentConfiguration` defaults to tolerant decoding. Malformed events are
skipped so later complete events can still be consumed. Set `decodingPolicy` to
`.strict` to report malformed JSON, invalid known event payloads, and unknown
events rejected by the configured decoder:

```swift
let configuration = HttpAgentConfiguration(
    baseURL: URL(string: "https://agent.example.com")!,
    decodingPolicy: .strict,
    maximumEventByteCount: 1_048_576
)
let agent = HttpAgent(configuration: configuration)
```

The same options are available when constructing `EventStream` directly. Decoder
failures retain their existing `EventDecodingError` type. Strict framing failures
use `EventStreamError.invalidUTF8`, `.eventTooLarge(maximumByteCount:)`, or
`.incompleteEvent` when input ends before a blank-line event terminator.

The parser defaults to a 10 MiB limit per event, including normalized line
endings. The limit must be positive. It bounds buffered event data, rather than
aggregate network chunk size. In tolerant mode, an oversized event is discarded
through its next blank-line terminator; remaining fields cannot become a new
event. Invalid UTF-8 events and unfinished data at EOF are also discarded. Valid
UTF-8 and CRLF line endings may cross arbitrary chunk boundaries.
