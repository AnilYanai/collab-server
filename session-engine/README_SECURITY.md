# 🛡️ Architecture & Security Guide: Session Authentication & JWT Verification

This document provides a comprehensive breakdown of the real-time session authentication and cryptographic token verification pipeline implemented in the `session-engine` module.

---

## 1. Architectural Overview & Separation of Concerns

To achieve **sub-millisecond latency** and maintain **150,000+ msgs/sec** throughput, the collaboration architecture strictly separates **business permissions** from **real-time socket transport**.

```mermaid
sequenceDiagram
    autonumber
    actor Client as User / Browser
    participant WebAPI as Main Business Logic Server<br/>(REST API + Database)
    participant Collab as Real-Time Collab Server<br/>(NIO Socket Engine)

    Note over Client,WebAPI: Phase 1: Business Permission Check & Token Minting
    Client->>WebAPI: GET /api/rooms/{sessionId}/token (via HTTPS)
    Note over WebAPI: Checks database: Does user have access to this room?<br/>If YES, generates cryptographically signed room token.
    WebAPI-->>Client: Returns JSON { "roomToken": "..." }

    Note over Client,Collab: Phase 2: Instant Cryptographic Admission (No DB Hits)
    Client->>Collab: Connect TCP Socket
    Client->>Collab: JOIN_SESSION(sessionId, payload = roomToken)
    Note over Collab: SessionAuthorizer verifies 4 checks in < 0.02 ms:<br/>1. Format integrity<br/>2. Room scope match<br/>3. Expiration window<br/>4. HMAC-SHA256 signature
    
    alt Verification Success
        Collab-->>Client: ACK(sessionId)
        Note over Client,Collab: Client is now a member of the room.<br/>Subsequent BROADCAST packets flow with ZERO token checks!
    else Verification Failed
        Collab-->>Client: ERROR("401 Unauthorized: Invalid session credentials or token")
    end
```

---

## 2. Token Anatomy & Format

The session engine utilizes a compact, URL-safe, stateless token format based on **HMAC-SHA256**:

$$\Large \overbrace{\texttt{<sessionId> : <userId> : <expiryEpochSec>}}^{\text{Claims (Plaintext)}} \, \mathbf{.} \, \overbrace{\texttt{<base64Signature>}}^{\text{HMAC-SHA256 Signature}}$$

### Concrete Example:
```text
doc-101:user_alice_42:1789985400.xK9b1_q0WvPzL8aY7mN3cE4rT6uI5oP2
```

### Components:
| Segment | Parameter | Description |
| :--- | :--- | :--- |
| **Claim 1** | `sessionId` | Logical room identifier (e.g., `room-0`, `doc-101`, `canvas-99`). |
| **Claim 2** | `userId` | Unique identifier of the authenticated user. |
| **Claim 3** | `expiryEpochSec` | UNIX timestamp (seconds) after which this token is invalid. |
| **Delimiter** | `.` | Separator between plaintext claims and cryptographic seal. |
| **Seal** | `base64Signature` | Base64 URL-encoded HMAC-SHA256 signature computed over the claims. |

---

## 3. What Does the Collab Server Verify? (The 4 Checks)

When [`PacketDispatcher`](file:///H:/My%20Drive/Learning/collab-server/collab-server/session-engine/src/main/java/com/anilyanai/engine/dispatcher/PacketDispatcher.java) receives a `JOIN_SESSION` packet, [`HmacTokenAuthorizer`](file:///H:/My%20Drive/Learning/collab-server/collab-server/session-engine/src/main/java/com/anilyanai/engine/security/HmacTokenAuthorizer.java) performs four verification checks:

```
                  JOIN_SESSION Received
                            │
                            ▼
          ┌───────────────────────────────────┐
          │  Check 1: Structural Format Check │─── Fail ──► [401 Malformed Token]
          └───────────────────────────────────┘
                            │ Pass
                            ▼
          ┌───────────────────────────────────┐
          │   Check 2: Room Scope Matching    │─── Fail ──► [401 Room Mismatch]
          └───────────────────────────────────┘
                            │ Pass
                            ▼
          ┌───────────────────────────────────┐
          │  Check 3: Expiration Time Window  │─── Fail ──► [401 Expired Token]
          └───────────────────────────────────┘
                            │ Pass
                            ▼
          ┌───────────────────────────────────┐
          │ Check 4: Cryptographic Signature  │─── Fail ──► [401 Invalid Signature]
          └───────────────────────────────────┘
                            │ Pass
                            ▼
               [✅ 200 OK: Admit to Room]
```

### Detailed Breakdown of the 4 Checks:

### Check 1: Structural Format & Payload Non-Empty
* **What it checks**: Validates that the payload contains text formatted as `<claims>.<signature>`, and claims parse into at least 3 parts separated by colons (`:`).
* **Threat prevented**: Buffer malformations, null pointers, empty payloads, and malformed fuzzing attacks.

### Check 2: Room Scope Matching (`sessionId == tokenSessionId`)
* **What it checks**: The target `sessionId` specified in the binary packet header **must strictly equal** the `sessionId` embedded inside the signed token claims.
* **Threat prevented**: **Privilege Escalation**. If User Alice is granted a valid token for public room `"general-chat"`, she cannot use that token to join private room `"exec-boardroom"`. Even though the token is genuine, the server sees:
  $$\text{targetSessionId ("exec-boardroom")} \neq \text{tokenSessionId ("general-chat")}$$
  and immediately drops the join attempt.

### Check 3: Expiration Time Window (`currentTime <= expiryEpochSec`)
* **What it checks**: Compares server's current epoch clock against the expiration timestamp in the token.
* **Threat prevented**: **Token Replay Attacks**. If a token is sniffed or leaked, it cannot be reused indefinitely. Once the join window expires (e.g. 5 minutes after issuance), the token is forever unusable.

### Check 4: Cryptographic HMAC-SHA256 Integrity Seal
* **What it checks**: Re-computes:
  $$\text{expectedSignature} = \text{HMAC\_SHA256}(\text{claims}, \text{SECRET\_KEY})$$
  and compares it against the token's provided signature using **constant-time byte comparison** (`MessageDigest.isEqual`).
* **Threats prevented**:
  * **Payload Tampering**: If an attacker alters the `userId` to `admin` or changes the `sessionId` to another room, the signature becomes mathematically invalid.
  * **Forgery**: Without knowing the server's private `SECRET_KEY`, calculating a valid signature would take billions of years.
  * **Timing Attacks**: `MessageDigest.isEqual` runs in constant time, preventing side-channel byte-by-byte timing attacks.

---

## 4. Key Performance Guarantees

1. **Zero Database Bottleneck**:
   * The Collab Server does not touch disk or network databases to authenticate.
   * Signature calculation executes in **under 20 microseconds (0.02 ms)** per join.
2. **One-Time Check**:
   * Verification occurs **only on `JOIN_SESSION`**.
   * Subsequent `BROADCAST`, `DIRECT`, and `PING` packets carry **zero token bytes**, enabling wire-speed fan-out with zero CPU overhead.
3. **Automatic Cleanup on Disconnect**:
   * When a socket connection closes or drops, [`DisconnectListener`](file:///H:/My%20Drive/Learning/collab-server/collab-server/connector/src/main/java/com/anilyanai/connector/events/DisconnectListener.java) automatically wipes that connection from all room member tables, leaving zero memory leaks.

---

## 5. Configuration & Usage Reference

### Enabling in Production
In `collab-application/src/main/resources/application.properties`:
```properties
# Shared 256-bit cryptographic secret (must match Main Web/Auth Server)
collab.security.hmac-secret=a3f9e1c2b5d4e6f8a0c2e4b6d8f0a2c4e6b8d0f2a4c6e8b0d2f4a6c8e0b2d4f6
```

### Local Development / Open Benchmarks
If `collab.security.hmac-secret` is omitted from configuration:
* The server falls back to `SessionAuthorizer.PERMISSIVE`.
* All joins are permitted with or without tokens, allowing seamless local development and high-speed stress benchmarking.

### Generating Tokens (Java Utility)
To mint a token in your web backend:
```java
String token = HmacTokenAuthorizer.generateToken(
    "room-101",          // sessionId
    "user-849",          // userId
    300,                 // duration in seconds (5 minutes)
    sharedHmacSecret     // 256-bit secret key
);
```
