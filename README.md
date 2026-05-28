# DistRes — Distributed Resource Access and Synchronisation Engine

**6CM604 Course Work 2** — extends the ConRes engine (CW1) into a fully
distributed Client–Server system that lets multiple networked nodes safely
access a shared resource over TCP/IP, with a publish–subscribe mechanism for
change notifications.

---

## High-level architecture

![UML Component Diagram](diagrams/component_diagram.png)

The system is a classic Client–Server distributed design with two layers
inside the server:

- **Application / Logic Layer** — `DistResServer` (connection management,
  command dispatch) and `PubSubManager` (NOTIFY broadcasts).
- **Data Layer** — `DataLayer` (file + DB facade) and `ReadWriteLock`
  (concurrency primitive carried over from CW1).

Clients run a Tkinter GUI (`DistResClientGUI`) over a `ServerConnection`
that handles the TCP socket plus a background listener thread.

---

## Runtime deployment

![UML Deployment Diagram](diagrams/deployment_diagram.png)

Each client machine runs `client.py` and connects to a single server machine
running `server.py`. The communication path is a TCP/IP socket on port 5050
carrying line-delimited JSON. NOTIFY events are pushed back over the same
socket — no separate broadcast channel is needed.

---

## Message flow at a glance

![UML Sequence Diagram](diagrams/sequence_diagram.png)

A complete login → read → write → logout cycle, showing where the
`ReadWriteLock` is held and which events trigger a pub-sub broadcast.

---

## Features

- **Client–Server distribution** over TCP sockets (line-delimited JSON).
- **Concurrent reader / exclusive writer** access via `ReadWriteLock`.
- **Publish–Subscribe** broadcast — every connected client receives
  `CLIENT_JOINED`, `CLIENT_LEFT`, `CLIENT_READ`, and `FILE_UPDATED` events.
- **Layered architecture** — clean separation of logic and data layers.
- **Fault tolerance** — connection retry, dead-subscriber reaping,
  guaranteed cleanup on disconnect.
- **Tk GUI client** with a live activity log showing all NOTIFY events.

---

## Repository structure

```
distres/
├── server.py                 # TCP server + PubSubManager + DataLayer
├── client.py                 # Tk GUI client + ServerConnection
├── db_setup.py               # Seeds the SQLite user database
├── ProductSpecification.txt  # Shared distributed resource
├── distres_users.db          # SQLite credential store (created by db_setup)
├── diagrams/                 # UML diagrams used in this README
└── screenshots/              # GUI screenshots referenced by the report
```

---

## Running the system

Python 3.10+ required (uses only the standard library).

```bash
# 1. One-time database setup
python db_setup.py

# 2. Start the server (in one terminal)
python server.py
#   [Server] DistRes listening on 0.0.0.0:5050

# 3. Start one or more clients (each in a separate terminal)
python client.py
python client.py
```

Default seeded accounts:

| Username | Password   | User ID |
|----------|------------|---------|
| alice    | alice123   | U001    |
| bob      | bob123     | U002    |
| carol    | carol123   | U003    |
| dave     | dave123    | U004    |
| eve      | eve123     | U005    |

---

## Wire protocol

Every message is one JSON object terminated by `\n`.

| Client → Server | Server → Client                                  |
|-----------------|--------------------------------------------------|
| `LOGIN`         | `LOGIN_RESULT`                                   |
| `READ`          | `READ_RESULT`                                    |
| `WRITE`         | `WRITE_RESULT` (+ `NOTIFY FILE_UPDATED` to all)  |
| `LIST_CLIENTS`  | `CLIENT_LIST`                                    |
| `LOGOUT`        | `LOGOUT_RESULT` (+ `NOTIFY CLIENT_LEFT` to all)  |
| `PING`          | `PONG`                                           |

Pub-sub events broadcast to *every* subscribed client:

- `CLIENT_JOINED`
- `CLIENT_LEFT`
- `CLIENT_READ`
- `FILE_UPDATED`

---

## Mapping of scenario requirements to design

| CW2 scenario requirement | Implementation in DistRes |
|---|---|
| Client-server distributed communication | TCP sockets + line-delimited JSON |
| Server hosts user credentials database | SQLite `distres_users.db` via `DataLayer.authenticate()` |
| Server hosts shared distributed file | `ProductSpecification.txt` via `DataLayer.read_file/write_file` |
| Layered software architecture | Logic layer (`DistResServer`, `PubSubManager`) over Data layer (`DataLayer`, `ReadWriteLock`) |
| Concurrent readers / exclusive writers | `ReadWriteLock` (first-reader / last-reader pattern) |
| Pub-sub notification on writes | `PubSubManager` broadcasts `FILE_UPDATED` to all subscribers |
| Graceful node-failure handling with retries | `ServerConnection.connect()` retries × 3; dead-socket reaping in `PubSubManager.publish` |

---

*University of Derby — 6CM604 Design & Implementation of Concurrent Systems*
