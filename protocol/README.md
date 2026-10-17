# Protocol Module (`protocol`)

The **`protocol`** module defines the application-agnostic wire protocol and binary codecs for real-time collaborative communication.

---

## 💡 Design Philosophy

The server is **domain-agnostic**:
* It does not parse application-specific data models (drawing shapes, CRDT operations, game player states, or chat objects).
* Application payloads are treated as **opaque byte arrays (`byte[]`)**.
* The protocol envelope contains strictly what the server requires to route messages efficiently: `opcode`, `flags`, `sessionId`, optional `targetId`, and chunking metadata.

---

## 📦 Wire Packet Format

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0xCA)  |  Version (1)  |  Opcode (1B)  |  Flags (1B)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Message ID (4 bytes)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      Chunk Index (2 bytes)    |      Total Chunks (2 bytes)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| SessionLen(1B)| Session ID String (UTF-8) ...                 |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| [If Direct] TargetLen(1B) | Target ID String (UTF-8) ...      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Payload Length (4 bytes)                  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Opaque Payload Data (N bytes)             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

---

## ⚡ Infrastructure Opcodes

| Opcode | Hex | Description |
|---|---|---|
| `JOIN_SESSION` | `0x01` | Registers connection with a collaborative session/room |
| `LEAVE_SESSION` | `0x02` | Deregisters connection from a session |
| `BROADCAST` | `0x03` | Relays opaque payload to all peers in the session (excluding sender) |
| `BROADCAST_ALL`| `0x04` | Relays opaque payload to all session members (including sender) |
| `DIRECT` | `0x05` | Point-to-point delivery to a specific peer in the session |
| `PING` | `0x06` | Client/server heartbeat check |
| `PONG` | `0x07` | Heartbeat reply |
| `ACK` | `0x08` | Operation success confirmation |
| `ERROR` | `0x09` | Error indicator |

---

## 🧩 Large Payload & Media Chunking

For large files, video/audio frames, or initial canvas state snapshots:
* Clients slice data into chunks tagged with `messageId`, `chunkIndex`, and `totalChunks`.
* The server relays each chunk immediately upon receipt with **zero server reassembly latency and near-zero memory footprint**.
* Peer clients reassemble chunks on the receiving end.

---

## 🚀 Usage

```java
PacketCodec codec = new BinaryPacketCodec();

// Encode a broadcast message
byte[] drawingData = "{ \"x\": 100, \"y\": 200 }".getBytes(StandardCharsets.UTF_8);
Packet packet = Packet.ofBroadcast("whiteboard-room-1", drawingData);
byte[] wireBytes = codec.encode(packet);

// Decode on reception
Packet received = codec.decode(wireBytes);
System.out.println("Opcode: " + received.opcode() + " for session: " + received.sessionId());
```
