# 🚀 High-Performance Networking Notes: UDP Outbound Throughput & Off-Heap Direct Buffers

This document explains the architectural principles behind network I/O performance in Java NIO, contrasting TCP gathering batch writes against UDP datagram transmission, detailing the mechanics of **Off-Heap Direct Buffers (`ByteBuffer.allocateDirect()`)**, and documenting a critical OS-level difference between **Linux and Windows Winsock** regarding socket reuse.

---

## 1. Why Was TCP Outbound (~320k msg/sec) Higher than UDP (~40k msg/sec)?

When benchmarking 100 concurrent clients across 5 rooms (380,000 broadcasts), the initial benchmarks revealed:
- **TCP Outbound:** ~322,580 msg/sec (1.18 seconds drain time)
- **UDP Outbound:** ~39,538 msg/sec (9.61 seconds drain time)

The difference is rooted in three fundamental OS kernel and Java NIO architectural differences:

```
       TCP Architecture (Batching & Partitioned Sockets)
       =================================================
  Worker Threads       Outbound Selectors (2 Threads)          Kernel Syscalls
  [Worker Pool]  --->  [Per-Client Queues]             --->   WSASend(up to 128 msgs)
                       (Partitioned across 100 Sockets)         (~2,500 syscalls/sec)
                                                                 ⚡ 322,000 msg/sec

       UDP Architecture (Single Socket, Unbatched Syscalls)
       ====================================================
  Worker Threads       Single Shared DatagramChannel           Kernel Syscalls
  [Worker Pool]  --->  synchronized(writeLock)         --->   sendto(1 datagram)
                       (All 100 clients on 1 socket)            (40,000 syscalls/sec)
                                                                 🛑 40,000 msg/sec
```

### A. Gathering Batch Writes (`writev` / `WSASend`) vs Individual `sendto`
- **TCP:** TCP connections maintain a FIFO queue of buffers. When the `OP_WRITE` selector triggers, [`TCPConnection.safeWrite()`](../connector/src/main/java/com/anilyanai/connector/connection/TCPConnection.java) executes a **gathering write** (`channel.write(ByteBuffer[])`) which flushes up to **128 queued messages in a single kernel syscall**.
  - For 380,000 messages: TCP needs only $\approx 2,968$ system calls.
- **UDP:** Java NIO `DatagramChannel.send(ByteBuffer, SocketAddress)` sends **strictly 1 datagram to 1 recipient per call**.
  - For 380,000 messages: UDP must invoke **380,000 individual `sendto` / `WSASendTo` system calls** (128× more kernel boundary context switches).

### B. Socket Descriptor Locking (`writeLock`)
- **TCP:** Each client has its own independent `SocketChannel` and file descriptor. In addition, the channels are partitioned across independent selector threads (`outbound-selector-0` and `outbound-selector-1`) with **zero lock contention** between clients.
- **UDP:** The server uses a single `DatagramChannel` bound to port `8080`. In OpenJDK (`sun.nio.ch.DatagramChannelImpl`), `send()` acquires an internal `synchronized (writeLock)`. When 20 inbound worker threads simultaneously fan out messages to 19 peers, they all queue up sequentially on that single lock.

### C. Connectionless Route Resolution
- **TCP:** TCP sockets are in an `ESTABLISHED` state. Destination IP, MAC address, MTU, and routing paths are cached in the OS Transmission Control Block (TCB). The OS simply appends bytes to the send buffer.
- **UDP:** Because UDP is connectionless, for every single packet the kernel must parse the destination `InetSocketAddress`, consult routing tables, build a full IP/UDP header, and calculate checksums.

---

## 2. What is an Off-Heap Direct Buffer?

In Java, memory is divided into two primary zones: **JVM Heap Memory** and **Off-Heap (Native) Memory**.

```
+-----------------------------------------------------------------------------------+
|                                  PHYSICAL RAM                                     |
|                                                                                   |
|   +------------------------------------+   +----------------------------------+   |
|   |         JVM HEAP MEMORY            |   |          OFF-HEAP MEMORY         |   |
|   |   (ByteBuffer.allocate(size))      |   |   (ByteBuffer.allocateDirect())  |   |
|   |                                    |   |                                  |   |
|   |  • Managed by Garbage Collector    |   |  • Native OS C-memory (malloc)   |   |
|   |  • Moves around during GC compact  |   |  • Fixed physical memory address |   |
|   |  • Cannot be read directly by OS   |   |  • Accessible directly by OS     |   |
|   +------------------------------------+   +----------------------------------+   |
|                      |                                       |                    |
|                      | (Extra copy needed!)                  | (Zero-copy!)       |
|                      v                                       v                    |
|   +---------------------------------------------------------------------------+   |
|   |                     OPERATING SYSTEM KERNEL (DMA / NIC)                   |   |
|   |              Network Interface Card transmits bytes to wire               |   |
|   +---------------------------------------------------------------------------+   |
+-----------------------------------------------------------------------------------+
```

### A. JVM Heap Buffer (`ByteBuffer.allocate(size)` / `ByteBuffer.wrap(byte[])`)
* Allocated inside the JVM garbage-collected heap.
* **The Problem:** The Garbage Collector (GC) runs concurrently and moves objects around in physical RAM to defragment memory. Because a byte array's physical address can change at any moment, the operating system kernel network driver (which requires a stable C pointer for DMA - Direct Memory Access) **cannot safely read directly from the Java heap**.
* **The Hidden Cost:** Whenever you pass a heap buffer to a socket (`channel.write()` or `channel.send()`), the JVM must internally:
  1. Allocate a temporary off-heap direct buffer.
  2. Copy the bytes from Heap memory to Off-Heap memory (`memcpy`).
  3. Pass the off-heap pointer to the OS socket system call.
  4. Discard or recycle the temporary direct buffer.

### B. Off-Heap Direct Buffer (`ByteBuffer.allocateDirect(size)`)
* Allocated outside the JVM heap using native OS memory allocation (equivalent to `malloc()` in C/C++).
* **The Benefit:** Its physical memory address is **pinned and invariant**. The Garbage Collector never relocates it.
* **Zero-Copy Network I/O:** The JVM passes the physical native memory address directly to the OS kernel (`WSASendTo` on Windows, `sendto` on Linux). The Network Interface Card (NIC) streams the data directly from RAM using Direct Memory Access (DMA) with **zero intermediate memory copies** and **zero GC allocation pressure**.

---

## 3. Comparison Matrix

| Property | Heap Buffer (`ByteBuffer.allocate`) | Direct Off-Heap Buffer (`ByteBuffer.allocateDirect`) |
| :--- | :--- | :--- |
| **Location** | Inside JVM Managed Heap | Outside JVM Heap (Native C-Memory) |
| **GC Relocation** | Relocated / compacted by GC | Fixed in RAM (never moved by GC) |
| **Network Transfer** | Requires copy: `Heap -> Off-Heap -> Kernel` | **Zero-Copy**: `Off-Heap -> Kernel (DMA)` |
| **Allocation Cost** | Very fast | Slower (best when pre-allocated and reused) |
| **GC Pressure** | Generates GC churn if created per packet | **Zero GC pressure** |
| **Ideal Use Case** | In-memory business logic & parsing | Network socket I/O (reads & writes) |

---

## 4. The OS-Level Discovery: Linux vs. Windows Winsock (`SO_REUSEPORT` Trap)

When attempting to build a multi-socket UDP pool, a critical kernel behavior difference between **Linux** and **Windows** must be handled.

### A. The Linux Model (`SO_REUSEPORT`)
On Linux (kernel 3.9+), multiple UDP sockets can set the `SO_REUSEPORT` socket option and bind to the exact same port (e.g. `8080`).
- **Kernel Load Balancing:** Linux hashes incoming datagrams across all bound sockets automatically.
- **Symmetric Transmit:** Any of the sockets can send datagrams out, and they all appear to come from port `8080`.

### B. The Windows Winsock Model (`SO_REUSEADDR`)
Windows does **not** implement Linux's `SO_REUSEPORT`. Instead, Windows provides `SO_REUSEADDR`.
In Winsock, `SO_REUSEADDR` has very different semantics:
1. Winsock allows a second socket to bind to an already-bound port.
2. **However**, when the second socket attempts to call `sendto()` / `WSASendTo()`, Windows rejects the transmission and throws:
   ```
   java.net.BindException: Cannot assign requested address: sendto
       at java.base/sun.nio.ch.DatagramChannelImpl.send0(Native Method)
   ```
3. Furthermore, on Windows, if two sockets bind to port `8080`, incoming packets are non-deterministically stolen by the newest socket, starving the primary inbound reader.

### C. The Solution: Asymmetric Inbound / Egress Architecture

To make the architecture 100% portable, robust, and lightning-fast on both Windows and Linux, we implemented **Asymmetric Channel Specialization**:

```
                                  [ INBOUND TRAFFIC ]
                                  (Clients send to 8080)
                                            │
                                            ▼
                               +─────────────────────────+
                               │  Inbound DatagramChannel│  <-- Bound to Port 8080
                               │  (Dedicated Receiver)   │  <-- Inbound Selector Loop
                               +─────────────────────────+
                                            │
                                            ▼
                              [ 16 Worker Threads Pool ]
                                            │
                      ┌─────────────────────┼─────────────────────┐
                      │                     │                     │
                      ▼                     ▼                     ▼
              [Egress Channel 0]    [Egress Channel 1]    [Egress Channel 2..N]
              (Port: Ephemeral 1)   (Port: Ephemeral 2)   (Port: Ephemeral N)
              (Direct Buffer 64K)   (Direct Buffer 64K)   (Direct Buffer 64K)
                      │                     │                     │
                      └─────────────────────┼─────────────────────┘
                                            │
                                            ▼
                                  [ OUTBOUND BROADCAST ]
                                (Zero Lock Contention)
```

1. **Inbound Channel (Port 8080):** 
   - Bound explicitly to port `8080`.
   - Dedicated **100% exclusively to receiving** incoming datagrams.
   - It is never locked or blocked by outbound sends!
2. **Outbound Egress Pool (`UDPEgressPool`):**
   - Bound using `channel.bind(null)`.
   - This instructs the OS kernel to assign an independent ephemeral port and dedicated kernel socket send buffer (`SO_SNDBUF = 4 MB`) to each channel.
   - **Result:** No Winsock `BindException`, no port collision, and zero lock contention between threads!

---

## 5. Measured Micro-Benchmark Results

To verify the design on the host CPU before deploying to production, we ran a multi-threaded stress test (8 concurrent threads transmitting 160,000 datagrams):

| Pipeline Architecture | Execution Time | Outbound Throughput |
| :--- | :--- | :--- |
| **Old:** Single Shared Channel + Heap `wrap()` | 2,951 ms | **54,219 msg/sec** |
| **New:** 4-Channel Pool + Direct Off-Heap Buffers | 791 ms | **202,276 msg/sec** |

**Result:** A **~3.7x throughput increase** while completely eliminating JVM garbage collection churn.
