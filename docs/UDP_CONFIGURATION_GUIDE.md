# 🚀 UDP Configuration & Performance Tuning Guide

A practical guide for tuning UDP configuration properties in `application.properties` for ultra-low-latency game networking, Battle Royale simulations, voice/video streaming, and high-frequency broadcasts.

---

## 📋 Table of Contents
1. [Core UDP Configuration Properties](#1-core-udp-configuration-properties)
2. [Batch Size Tuning Guide: Small vs. Large Batches](#2-batch-size-tuning-guide-small-vs-large-batches)
3. [Thread Budget & CPU Core Affinity](#3-thread-budget--cpu-core-affinity)
4. [Multi-Socket Egress Pool Tuning](#4-multi-socket-egress-pool-tuning)
5. [Recommended Profiles by Use Case](#5-recommended-profiles-by-use-case)
6. [OS Kernel & Socket Buffer Configuration](#6-os-kernel--socket-buffer-configuration)

---

## 1. Core UDP Configuration Properties

All UDP configuration options are managed in `src/main/resources/application.properties`:

```properties
# =========================================================================
# UDP Transport Configuration
# =========================================================================
collab.connector.udp-enabled=true
collab.connector.udp-port=8080

# UDP Inbound: Dedicated NIO Reactor & Isolated Platform Worker Pool
collab.connector.udp-inbound-worker-threads=7
collab.connector.udp-inbound-batch-size=8

# UDP Outbound: Parallel Non-Blocking Egress Sockets (0% writeLock contention)
collab.connector.udp-outbound-egress-channels=64
```

### Property Reference

| Property | Default | Range | Description |
| :--- | :---: | :---: | :--- |
| `collab.connector.udp-enabled` | `true` | `true/false` | Toggles UDP listener and egress pool on/off. |
| `collab.connector.udp-port` | `8080` | `1024–65535` | UDP server listening port for all inbound client packets. |
| `collab.connector.udp-inbound-worker-threads` | `7` | `1–64` | Number of dedicated platform threads in the FIFO inbound worker pool. |
| `collab.connector.udp-inbound-batch-size` | `8` | `1–256` | Maximum datagrams batched from the NIO reader before submitting to worker pool. |
| `collab.connector.udp-outbound-egress-channels` | `64` | `1–256` | Number of dedicated parallel outbound datagram channels. |

---

## 2. Batch Size Tuning Guide: Small vs. Large Batches

The batch size parameter (`collab.connector.udp-inbound-batch-size`) dictates how many inbound datagrams the NIO reactor thread gathers before passing them to a worker thread.

$$\text{Work performed by Worker Thread} = \text{Batch Size} \times (N_{\text{room members}} - 1) \text{ socket } \texttt{send()} \text{ calls}$$

```
Large Batch Size (e.g., 64) -> Head-of-Line Blocking & Worker Idling:
  [ Inbound Selector ] ===(Batch of 64)===> [ Worker 1 (3,200 socket sends!) ]
                                            [ Worker 2 (IDLE) ]
                                            [ Worker 3 (IDLE) ]
                                            [ Worker 4 (IDLE) ]

Small Batch Size (e.g., 1–8) -> Continuous Work Distribution & Full Core Saturation:
  [ Inbound Selector ] --(1-8)--> [ Worker 1 (50-400 sends) -> Channel #1 ]
                       --(1-8)--> [ Worker 2 (50-400 sends) -> Channel #2 ]
                       --(1-8)--> [ Worker 3 (50-400 sends) -> Channel #3 ]
                       --(1-8)--> [ Worker 4 (50-400 sends) -> Channel #4 ]
```

### Why Smaller Batch Size Boosts UDP Broadcast Performance

1. **Eliminates Head-of-Line (HoL) Blocking:**
   - In a room with 50 participants, a batch of 64 packets forces a single worker to perform $64 \times 50 = 3,200$ socket transmissions in a row.
   - The 64th packet suffers extreme queueing delay while waiting for the first 63 packets to finish transmitting.
   - Reducing batch size to `1 to 8` cuts per-task duration by **8–64×**, transmitting packets with sub-millisecond latency.

2. **Balances Multi-Core CPU Load:**
   - Small batches distribute tasks evenly across all available worker threads in the FIFO queue.
   - No single worker thread gets overwhelmed while others sit idle.

3. **Maximizes Parallel UDP Egress Channels:**
   - Each worker thread transmits through its own dedicated channel in `UDPEgressPool`.
   - Distributing batches evenly across all worker threads ensures that all parallel egress sockets are utilized concurrently with **0% socket lock contention**.

### When to Use Small vs. Large Batch Sizes

| Use Case | Recommended Batch Size | Reason |
| :--- | :---: | :--- |
| **Battle Royale / Large Gaming Rooms ($N \ge 10$)** | **`1 to 8`** | High fan-out per packet; prevents single-thread saturation. |
| **1-on-1 Video / Audio Calls ($N = 2$)** | **`8 to 16`** | Low fan-out per packet; balances throughput and latency. |
| **Direct 1-to-1 Point-to-Point Messaging** | **`32 to 64`** | Single target destination; larger batch reduces queue enqueue overhead. |
| **High-Throughput Telemetry Ingestion (1-way)** | **`64 to 128`** | Minimal processing; optimizes throughput over per-packet latency. |

---

## 3. Thread Budget & CPU Core Affinity

To eliminate thread contention and context-switching overhead, tune worker threads to match your machine's logical CPU cores:

$$\text{Total Active Server Threads} = \text{TCP Selector} + \text{TCP Outbound} + \text{TCP Workers} + \text{UDP Selector} + \text{UDP Workers}$$

### Example: 8-Core CPU Budget Allocation

For a dedicated 8-core / 16-thread host machine:

| Component | Thread Name | Thread Count | Role |
| :--- | :--- | :---: | :--- |
| **UDP Inbound Selector** | `udp-inbound-selector` | `1` | Reads UDP datagrams off port `8080`. |
| **UDP Worker Pool** | `inbound-worker-N` | `7` | Executes state persistence & UDP broadcast fan-out. |
| **Total UDP Dedicated Threads** | | **8** | Matches 8 physical cores perfectly. |

---

## 4. Multi-Socket Egress Pool Tuning

Java NIO's `DatagramChannelImpl.send()` acquires an internal `synchronized (writeLock)`. If multiple threads transmit through the same channel, they serialize on this lock.

The `UDPEgressPool` solves this by assigning dedicated outbound sockets per thread using thread affinity:

```properties
collab.connector.udp-outbound-egress-channels=64
```

* **Recommended Setting:** Set `udp-outbound-egress-channels` $\ge$ `udp-inbound-worker-threads` (typically `32` to `64`).
* **Memory Overhead:** Each channel requires zero additional heap memory and only ~4 MB of OS kernel send buffer (`SO_SNDBUF`).

---

## 5. Recommended Profiles by Use Case

### Profile A: Battle Royale & Low-Latency Gaming (50–100 Players/Room)
* **Goal:** Absolute lowest broadcast latency, zero jitter, instant frame fan-out.

```properties
collab.connector.udp-enabled=true
collab.connector.udp-port=8080
collab.connector.udp-inbound-worker-threads=7
collab.connector.udp-inbound-batch-size=8
collab.connector.udp-outbound-egress-channels=64
```

### Profile B: 1-on-1 & Small Group Video Meeting (2–8 Participants)
* **Goal:** High video packet throughput with low CPU overhead.

```properties
collab.connector.udp-enabled=true
collab.connector.udp-port=8080
collab.connector.udp-inbound-worker-threads=4
collab.connector.udp-inbound-batch-size=16
collab.connector.udp-outbound-egress-channels=32
```

### Profile C: Strict Sequential Ingestion (Single-Threaded Deterministic)
* **Goal:** 100% strict sequential ordering without concurrency locks.

```properties
collab.connector.udp-enabled=true
collab.connector.udp-port=8080
collab.connector.udp-inbound-worker-threads=1
collab.connector.udp-inbound-batch-size=1
collab.connector.udp-outbound-egress-channels=1
```

---

## 6. OS Kernel & Socket Buffer Configuration

For maximum packet burst handling without kernel packet drops:

### Linux Socket Buffer Tuning (`sysctl`)
```bash
# Increase maximum and default socket receive/send buffers
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
sudo sysctl -w net.core.rmem_default=4194304
sudo sysctl -w net.core.wmem_default=4194304
```

### Windows Winsock Settings
On Windows, `UDPEgressPool` automatically uses independent ephemeral egress channels with `4 MB SO_SNDBUF` per channel, preventing Winsock port collisions and `BindException` errors.
