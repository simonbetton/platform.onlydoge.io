# API key roles, metadata ownership, and audit events

OnlyDoge treats the resolved API key as the authenticated actor, not the API token credential. We store explicit API key roles, make investigation metadata owned by the creating API key, and record protected active-key requests as audit events because authorization, ownership, and accountability all need to be explainable from persisted API key identity rather than from recoverable credentials.

## Considered Options

- Keep a separate configured admin token: rejected because it would make the operational secret a hidden identity concept outside the API key model.
- Use globally shared investigation metadata: rejected because owner-only CRUD and owner-filtered overlays require the metadata graph to have a clear owner.
- Store raw request bodies in audit events: rejected because auditability does not require duplicating sensitive request content.
