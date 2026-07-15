# OnlyDoge Context

OnlyDoge is an authenticated, single-chain Dogecoin block explorer and analytics API. The domain model is Dogecoin itself: blocks, transactions, inputs, outputs, spends, UTXOs, addresses, balances, mempool samples, and analytics facts.

## Language

**API key**:
An authenticated client's identity for protected OnlyDoge API routes. One API key owns one request budget.
_Avoid_: User, account, token

**Admin API key**:
An API key with authority over platform lifecycle resources such as API keys and audit events. Admin API keys can inspect and manage API keys according to role rules.
_Avoid_: Admin token, root token, superuser

**API token**:
The secret credential a client presents to prove it controls an API key. API tokens are returned once and OnlyDoge stores only token hashes.
_Avoid_: API key, user

**Rate-limit budget**:
The request allowance assigned to one API key for a time window.
_Avoid_: User limit, token limit

**Analytics rate-limit budget**:
The separate request and concurrency allowance assigned to one API key for guarded analytics SQL queries. It protects heavier ClickHouse reads without changing the API key's ordinary route budget.
_Avoid_: SQL user limit, chat user budget, token analytics limit

**Audit event**:
An activity record for a protected request after it has resolved to an active API key. An audit event identifies the acting API key without storing the API token or request body.
_Avoid_: Access log, user event, token log

**Dogecoin config**:
The singleton runtime configuration for Dogecoin Core RPC, ZMQ, RPS, reprocess depth, and mempool sampling.
_Avoid_: Network catalog entry, chain registry row

**Canonical output**:
A Dogecoin transaction output stored with its txid/vout, script, address when extractable, value, creating block, and current spend status.
_Avoid_: Transfer, token balance row

**UTXO**:
A canonical output that is spendable and not currently spent. Address balances are derived from current UTXOs.
_Avoid_: Cached balance source, transaction history row

**Address activity**:
Confirmed credit/debit facts derived only from canonical outputs and resolved spends. Address received, sent, and transaction counts come from address activity.
_Avoid_: Heuristic transfer, source link, label

**Transaction fact**:
A confirmed Dogecoin transaction summary stored for analytics, including block position, input/output counts, resolved input value, gross output value, and fee when resolvable.
_Avoid_: Raw transaction, transfer, UTXO

**Gross output value**:
The sum of all outputs in a transaction, including change outputs. Gross output value is used for "biggest transaction" analytics and is not a change-adjusted economic transfer amount.
_Avoid_: Transfer value, sent amount, economic value

**AI analytics query**:
A guarded read-only ClickHouse query generated for the AI chat application against OnlyDoge's curated analytics schema. An AI analytics query is not free-form access to internal warehouse tables.
_Avoid_: Raw SQL endpoint, warehouse mutation, admin SQL

**Mempool watch session**:
A short-lived, API-key-authenticated wait for unconfirmed funds to one Dogecoin address. One API key may hold at most five concurrent mempool watch sessions; each session ends on the first qualifying mempool appear or after five minutes.
_Avoid_: Subscription, webhook, persistent watchlist, address alert

**Mempool appear**:
The event that a watched address is present as a receiving output in a mempool transaction, optionally meeting a minimum output value. A mempool appear is not confirmation and does not cover spends from the address.
_Avoid_: Payment confirmed, deposit settled, address activity

## Example Dialogue

Developer: "Should we rate limit the user?"
Domain expert: "OnlyDoge does not have users yet. Rate limit the API key resolved from the API token."

Developer: "If two clients use different API tokens, do they share budget?"
Domain expert: "No. Each API token resolves to its own API key, and each API key has its own budget."

Developer: "Can we compute address sent amount from transaction outputs?"
Domain expert: "No. Sent amount comes from resolved spends of previous outputs controlled by that address."

Developer: "Can a balance row be trusted by itself?"
Domain expert: "Only if it agrees with the current UTXO set for the same address."

Developer: "How do I get notified when payment hits an address in the mempool?"
Domain expert: "Open one mempool watch session for that address. You get a mempool appear when receiving outputs show up, then the session ends."
