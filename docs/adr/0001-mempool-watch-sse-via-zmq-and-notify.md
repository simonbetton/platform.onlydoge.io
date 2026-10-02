# Mempool watch via ZMQ, Postgres NOTIFY, and one-shot SSE

## Status

Accepted.

## Context

Engineers need a low-latency ping when funds appear to one address in the mempool. OnlyDoge already runs a combined API/indexer runtime in local development and can split API and indexer roles in production. The platform must avoid adding new infrastructure dependencies while still supporting low-latency detection and authenticated delivery to a single client session.

## Decision

Deliver mempool appear notifications as an authenticated one-shot SSE session:

- `GET /v1/explorer/mempool/watch?address=...&minValueBase=...`
- close on first qualifying appear or after five minutes
- detect with Dogecoin ZMQ `rawtx` through an out-of-process Node bridge because native zeromq crashes Bun in-process
- supplement ZMQ with RPC polling while watches are active in the indexer
- filter through an active-watch registry
- fan out matches with Postgres `NOTIFY` when API and indexer share metadata through Postgres

## Alternatives considered

- Webhooks: rejected because they add extra RTT and require customer-hosted endpoints.
- WebSockets: rejected because duplex transport is unnecessary for a one-shot notification.
- Blind fan-out of every mempool output: rejected because it is noisy and expensive.
- Redis pub/sub: rejected because it adds a new operational dependency for a narrow use case.

## Consequences

- Split HTTP/indexer production topology requires Postgres so `NOTIFY` can bridge roles.
- The indexer owns detection; the API owns authenticated SSE session lifecycle.
- Watch sessions are bounded per API key and degrade when the mempool cache is over capacity.

## Failure modes

- ZMQ bridge unavailable: RPC polling remains active while watches exist.
- RPC backlog or oversized mempool: new watch sessions may return `425` until the detector recovers.
- Proxy idle timeouts: SSE heartbeats are emitted while waiting.
- Split-role misconfiguration without Postgres: startup fails fast.

## Security and operations

- Route requires `x-api-token`.
- Watch output values are normalized metadata only; raw transaction hex is not streamed to clients.
- ZMQ and RPC credentials remain server-side environment configuration.
- Session timeout is fixed at five minutes to limit resource retention.

## Test strategy

- Unit tests cover watch registry matching, cache bounds, and SSE event serialization.
- Integration tests cover Postgres `NOTIFY` fan-out in split-role topology.
- Adapter smoke tests exercise real ClickHouse and Docker service wiring where enabled.
