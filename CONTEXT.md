# OnlyDoge Context

OnlyDoge is an authenticated Dogecoin investigation and explorer API. This language keeps client identity and credential terms distinct.

## Language

**API key**:
An authenticated client's identity for calling protected OnlyDoge API routes. One API key owns one request budget.
_Avoid_: User, account, token

**Admin API key**:
An API key with authority over platform-owned lifecycle resources such as API keys, networks, and currency tokens. Admin API keys can inspect all owner-scoped metadata but do not own another API key's metadata.
_Avoid_: Admin token, root token, superuser

**Owner API key**:
The API key that owns an investigation metadata graph. One owner API key owns its entities, risk tags, and address labels.
_Avoid_: User, account owner, creator user

**API token**:
The secret credential a client presents to prove it controls an API key. An API token is not itself the client identity.
_Avoid_: API key, token, user

**Token**:
A currency tracked on a network, such as DOGE. A token is not an authentication credential.
_Avoid_: API token, API key

**Rate-limit budget**:
The request allowance assigned to one API key for a time window.
_Avoid_: User limit, token limit

**Audit event**:
An activity record for a protected request after it has resolved to an active API key. An audit event identifies the acting API key without storing the API token or request body.
_Avoid_: Access log, user event, token log

## Example Dialogue

Developer: "Should we rate limit the user?"
Domain expert: "OnlyDoge does not have users yet. Rate limit the API key resolved from the API token."

Developer: "If two clients use different API tokens, do they share budget?"
Domain expert: "No. Each API token resolves to its own API key, and each API key has its own budget."

Developer: "Can the admin API token edit a member's entity?"
Domain expert: "The credential is an API token, but the actor is the admin API key. Admin API keys can inspect another owner API key's metadata, not edit it."

Developer: "Should failed owner checks be recorded?"
Domain expert: "Yes. Once an active API key is resolved, the request produces an audit event even if authorization or validation fails."
