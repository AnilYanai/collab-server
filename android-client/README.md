# 📱 Collab Mobile Client (Android Native Kotlin)

A real-time, low-latency Android application connecting directly to `collab-server` over **UDP Datagrams** with cryptographic HMAC-SHA256 session token authentication.

---

## 🏗️ Architecture

```
  📱 Android Phone (Wi-Fi)                   💻 Collab-Server (PC)
  ┌────────────────────────┐                ┌────────────────────────┐
  │  MainActivity.kt       │                │  Port: 8080 (UDP)      │
  │  CollabUdpClient.kt    │ ─── UDP ────>  │  UDPInboundTraffic     │
  │  BinaryPacketCodec.kt  │ <─── ACKs/Msgs │  SessionDispatcher     │
  │  HmacTokenAuthorizer   │                │  UDPEgressPool         │
  └────────────────────────┘                └────────────────────────┘
              ▲                                         │
              │                                         │
              │           Session: "mobile-room-1"      ▼
              └──────────────────────────────  DesktopPeer.java (Console)
```

---

## 🚀 How to Run and Test

### Step 1: Start the Collab Server
Make sure `CollabServerApplication` is running in IntelliJ IDEA.
* UDP Port: `8080`
* Your PC's Wi-Fi IP: `192.168.1.2` (pre-filled in the app)

---

### Step 2: Open and Run the Mobile App in Android Studio
1. Open **Android Studio**.
2. Click **File -> Open...** and select the folder:
   ```
   C:\Users\donfa\Desktop\collab-server\collab-server\android-client
   ```
3. Let Gradle sync.
4. Connect your Android phone via USB (with USB Debugging enabled) OR start an Android Emulator.
5. Click **Run 'app'** (`Shift + F10`).

---

### Step 3: Start the Desktop Companion Peer
In IntelliJ IDEA (or terminal), run:
```
com.anilyanai.peer.DesktopPeer
```
* The desktop peer connects to `localhost:8080` and joins the session: `mobile-room-1`.

---

### Step 4: Join from Your Phone & Chat in Real Time!
1. On your phone app:
   - **Host:** `192.168.1.2` (Make sure your phone is connected to the same Wi-Fi network!)
   - **Port:** `8080`
   - **Session:** `mobile-room-1`
   - **Username:** `AndroidUser`
2. Tap **"Join Session (UDP)"**.
   - The badge turns green: `Joined: mobile-room-1`.
3. Type a message on your phone and tap **Send**:
   - The Desktop console instantly prints:
     ```
     📱 [AndroidUser] (4 ms): Hello from my phone!
     ```
4. Type a message in the Desktop console and press **Enter**:
   - Your Android phone screen instantly updates with:
     ```
     [DesktopPC] (3ms): Hello from PC!
     ```
