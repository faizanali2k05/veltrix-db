# Security Policy

VeltrixDB is an educational database and is not production-ready. It has no
authentication, authorization, TLS, command ACLs, tenant isolation, or network
encryption. Bind to loopback or a trusted private network only; never expose it
directly to the public internet.

The RESP2 decoder bounds frame bytes, bulk strings, arrays, and nesting depth.
The server also limits concurrent connections. These controls reduce accidental
and hostile resource use but are not a security boundary. Snapshot files contain
keys and values in cleartext JSON and must be protected with filesystem access
controls.

Please report vulnerabilities privately to the repository maintainers rather
than opening a public issue. Include affected revision, reproduction steps,
impact, and any suggested mitigation. No formal response-time SLA is offered.

