# DistRes — Distributed Resource Access and Synchronisation Engine

**6CM604 Course Work 2** — extends the ConRes engine (CW1) into a fully distributed
Client–Server system that lets multiple networked nodes safely access a shared
resource over TCP/IP, with a publish–subscribe mechanism for change notifications.

---

## Features

- **Client–Server distribution** over TCP sockets (line-delimited JSON wire format).
- **Concurrent reader / exclusive writer** access to the shared resource via a
  `ReadWriteLock` (carried over from CW1).
- **Publish–Subscribe** broadcast: every connected client receives `NOTIFY`
  events when another client joins, leaves, reads, or updates the resource.
- **Layered architecture**: an application/logic layer (connection management,
  request dispatch) sits cleanly on top of a data layer (SQLite credential
  store and lock-guarded file access).
- **Fault tolerance**: clients retry connections up to three times with a short
  delay; the server reaps dead subscriber sockets during every broadcast.
- **Tk-based GUI client** with live activity log and notification feed.

---

## Repository structure

```
distres/
├── server.py                 # DistRes server (TCP listener + pub-sub + data layer)
├── client.py                 # Tk GUI client (login / read / write / notifications)
├── db_setup.py               # Creates and seeds the SQLite user database
├── ProductSpecification.txt  # Shared distributed resource
├── distres_users.db          # SQLite credential store (created by db_setup.py)
└── README.md
```

---

## Running the system

You need Python 3.10+. Everything else is in the Python standard library.

```bash
# 1. One-time database setup
python db_setup.py

# 2. Start the server (in one terminal)
python server.py
#   [Server] DistRes listening on 0.0.0.0:5050

# 3. Start one or more clients (in separate terminals)
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

In each client GUI:

1. Fill in the server address / port (defaults to `127.0.0.1:5050`).
2. Enter your username and password and press **Login**.
3. Click **Read file** to fetch the shared resource.
4. Edit the text and click **Write file** — every other connected client
   receives a `FILE_UPDATED` notification in its activity log.
5. **List clients** asks the server for the current connected-client list.
6. **Logout** cleanly drops the session.

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

## Architectural overview

**Layered architecture inside the server:**

```
Application / Logic Layer
  DistResServer  -- accept, dispatch, lifecycle
  PubSubManager  -- subscriber registry + NOTIFY broadcast

Data Layer
  DataLayer      -- authenticate(), read_file(), write_file()
  ReadWriteLock  -- concurrent readers, exclusive writer
  SQLite store + ProductSpecification.txt
```

**Concurrency safety:**

- One Python thread per connected client (daemon thread, so the server can
  exit cleanly).
- `ReadWriteLock` allows N concurrent readers but only one exclusive writer.
- `PubSubManager` copies its subscriber list under a `threading.Lock`
  before broadcasting so the broadcast itself doesn't hold the lock.
- Each socket reader buffers bytes until it finds a `\n` so partial reads
  cannot corrupt the JSON stream.

**Fault tolerance:**

- Client `connect()` retries up to 3 times with a 1 s back-off before
  surfacing the error.
- `PubSubManager.publish()` catches `OSError` per subscriber and unsubscribes
  dead sockets without affecting the others.
- Server wraps each client handler in a `try ... finally` to guarantee that
  subscriptions are removed and sockets closed on any exit path.

---

## Course-work mapping

| Deliverable                              | File / artefact          |
|------------------------------------------|--------------------------|
| Distributed communication mechanism      | TCP sockets in `server.py` / `client.py` |
| Layered software architecture            | `DistResServer` (logic) over `DataLayer` (data) |
| Publish-subscribe mechanism              | `PubSubManager` in `server.py`  |
| Read-write coordination                  | `ReadWriteLock` in `server.py`  |
| User-credential database                 | `distres_users.db` (SQLite)    |
| Shared distributed file                  | `ProductSpecification.txt`     |
| Fault tolerance / retries                | `ServerConnection.connect()` in `client.py` |

---

*University of Derby — 6CM604 Design & Implementation of Concurrent Systems*
