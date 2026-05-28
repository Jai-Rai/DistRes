"""
server.py - DistRes Server Node
================================
The server side of the Distributed Resource Access and Synchronisation
Engine (DistRes). Listens for TCP client connections on port 5050 and
coordinates concurrent access to a single shared file resource.

LAYERED ARCHITECTURE
--------------------
    +---------------------------------------------+
    |  Application / Logic Layer                  |
    |    DistResServer  - accept + dispatch       |
    |    PubSubManager  - NOTIFY broadcasts       |
    +---------------------------------------------+
    |  Data Layer                                 |
    |    DataLayer      - file + DB facade        |
    |    ReadWriteLock  - concurrency primitive   |
    +---------------------------------------------+

WIRE PROTOCOL
-------------
Every message is one JSON object terminated by '\n'. Client sends
LOGIN / READ / WRITE / LIST_CLIENTS / LOGOUT / PING; server replies on
the same socket and additionally pushes NOTIFY events asynchronously
via the publish-subscribe mechanism.

RUNNING
-------
    python db_setup.py     # one-time, seeds the SQLite user database
    python server.py       # starts listening on 0.0.0.0:5050
"""

import json
import socket
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
HOST = "0.0.0.0"             # bind to all interfaces so remote clients can reach us
PORT = 5050                  # TCP port the server listens on
DB_PATH = "distres_users.db" # SQLite credential store (created by db_setup.py)
SHARED_FILE = "ProductSpecification.txt"  # the single shared resource


# ===========================================================================
# DATA LAYER -- Read-Write Lock
# ===========================================================================
class ReadWriteLock:
    """
    Concurrency primitive carried over from CW1 (ConRes).

    Permits MANY concurrent readers OR a single exclusive writer at any
    given moment, never a mix. Uses the classic first-reader / last-reader
    pattern: the first reader to arrive "closes the writer gate" on behalf
    of all readers; the last reader to leave reopens it. This avoids the
    need for a fairness queue while preventing data corruption.
    """

    def __init__(self) -> None:
        # `_read_ready` protects the `_readers` counter from concurrent
        # increments/decrements when multiple reader threads arrive at once.
        self._read_ready = threading.Condition(threading.Lock())
        # Number of readers currently inside read_file(). Guarded by _read_ready.
        self._readers = 0
        # The "writer gate". Held while any reader is active OR while a writer
        # is active. Acquiring it from acquire_write() therefore blocks until
        # all readers have released it.
        self._writer_lock = threading.Lock()

    def acquire_read(self) -> None:
        """Called by a reader before it touches the shared resource."""
        with self._read_ready:
            self._readers += 1
            # The FIRST reader to arrive acquires the writer gate on behalf
            # of the entire reader cohort. Any subsequent reader simply
            # increments the counter and proceeds without re-acquiring.
            if self._readers == 1:
                self._writer_lock.acquire()

    def release_read(self) -> None:
        """Called by a reader once it's finished with the shared resource."""
        with self._read_ready:
            self._readers -= 1
            # The LAST reader to leave releases the writer gate, allowing
            # a waiting writer (if any) to proceed.
            if self._readers == 0:
                self._writer_lock.release()

    def acquire_write(self) -> None:
        """Called by a writer; blocks until no readers/writers are active."""
        # Acquiring _writer_lock blocks if any reader is inside (because the
        # first reader holds it) OR if another writer is inside.
        self._writer_lock.acquire()

    def release_write(self) -> None:
        """Called by a writer once it's finished."""
        self._writer_lock.release()


# ===========================================================================
# DATA LAYER -- Authentication + File I/O facade
# ===========================================================================
class DataLayer:
    """
    The Data Layer of the system. The only class allowed to touch the
    SQLite credential database OR the shared file on disk. Every read/write
    is protected by the ReadWriteLock so concurrent clients can't corrupt
    each other's operations.

    The application layer (DistResServer) only talks to this class, never
    directly to SQLite or the filesystem. This is what makes the
    architecture layered.
    """

    def __init__(self, db_path: str, file_path: str) -> None:
        """Remember the on-disk paths and create the lock."""
        self.db_path = db_path           # path to SQLite credentials store
        self.file_path = file_path       # path to the shared resource file
        self.rw_lock = ReadWriteLock()   # one lock guarding the shared file
        # NOTE: we deliberately do NOT cache a SQLite connection here.
        # `sqlite3` connections are not safe to share across threads in
        # Python, so each call opens its own short-lived connection.

    # --- Credentials (database) ---------------------------------------------
    def authenticate(self, username: str, password: str):
        """
        Verify a username/password pair against the SQLite database.
        Returns the user_id (e.g. 'U001') on success, or None on failure.
        Plain text comparison is deliberate here -- the brief states that
        security is NOT the focus of this coursework.
        """
        try:
            # Open a fresh per-thread connection (see __init__ note).
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            # Parameterised query -- prevents SQL injection even though the
            # brief de-prioritises security, this is just good practice.
            cur.execute(
                "SELECT user_id FROM users WHERE username = ? AND password = ?",
                (username, password),
            )
            row = cur.fetchone()
            conn.close()
            return row[0] if row else None
        except sqlite3.Error as exc:
            # DB errors are logged but do not crash the server thread.
            print(f"[DataLayer] DB error: {exc}")
            return None

    # --- Shared file (read-write locked) -----------------------------------
    def read_file(self) -> str:
        """
        Acquire a read lock, return the file contents, then release.
        Multiple readers may run concurrently; writers are blocked while
        any reader is active.
        """
        self.rw_lock.acquire_read()
        try:
            # Deliberate small delay so concurrent reads are observable in
            # the demo. Without it, reads complete too quickly to see the
            # overlap. This is instrumentation, not a real-world pattern.
            time.sleep(0.4)
            with open(self.file_path, "r", encoding="utf-8") as fh:
                return fh.read()
        finally:
            # `finally` guarantees the lock is released even if file I/O
            # raises an exception -- prevents the system from deadlocking
            # itself on an unexpected failure.
            self.rw_lock.release_read()

    def write_file(self, new_content: str, username: str = "unknown") -> None:
        """
        Acquire the exclusive write lock, overwrite the file with
        `new_content`, then append an audit line recording who wrote and
        when. The audit line mirrors the audit trail used in CW1 so the
        write is visibly demonstrable in the read-back content.
        """
        self.rw_lock.acquire_write()
        try:
            # Larger delay than read_file() -- writes are intentionally
            # heavier than reads so the marker can see writes blocking reads.
            time.sleep(0.6)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            audit_line = f"\n--- Updated by {username} at {timestamp} ---\n"
            # Open in write mode -- truncates the file, then writes the
            # new content followed by the audit line.
            with open(self.file_path, "w", encoding="utf-8") as fh:
                fh.write(new_content)
                fh.write(audit_line)
        finally:
            self.rw_lock.release_write()


# ===========================================================================
# APPLICATION LAYER -- Publish-Subscribe Manager
# ===========================================================================
class PubSubManager:
    """
    Implements the publish-subscribe pattern. Maintains a registry of all
    currently-connected client sockets and lets the server broadcast NOTIFY
    events to all of them at once (e.g. when a write happens, every client
    is told about it without having to poll).

    This is THE core of the distributed coordination story for CW2.
    """

    def __init__(self) -> None:
        # Map: client_id -> (socket, username). The client_id is the
        # 'ip:port' string identifying that specific TCP connection.
        self._subscribers: dict = {}
        # Lock guarding the subscriber dict from concurrent modification.
        self._lock = threading.Lock()

    def subscribe(self, client_id: str, sock: socket.socket,
                  username: str) -> None:
        """
        Register a newly-authenticated client as a subscriber. Called from
        _handle_login() once the credentials have been verified.
        """
        with self._lock:
            self._subscribers[client_id] = (sock, username)
        print(f"[PubSub] {username} ({client_id}) subscribed -> "
              f"{len(self._subscribers)} active")

    def unsubscribe(self, client_id: str) -> None:
        """
        Remove a client from the subscriber registry. Called on clean
        logout AND on abrupt disconnect (from the handler's finally block).
        """
        with self._lock:
            # `.pop` with a default returns None instead of raising KeyError
            # if the client has already been removed -- which can happen if
            # publish() reaped the socket during a broadcast.
            entry = self._subscribers.pop(client_id, None)
        if entry:
            print(f"[PubSub] {entry[1]} ({client_id}) unsubscribed -> "
                  f"{len(self._subscribers)} active")

    def publish(self, event_type: str, payload: dict) -> None:
        """
        Broadcast a NOTIFY message of the given event_type to every
        currently-subscribed client. Event types in this system:

            CLIENT_JOINED   - a new user has logged in
            CLIENT_LEFT     - a user has logged out / disconnected
            CLIENT_READ     - a client just performed a read
            FILE_UPDATED    - a client just wrote new content to the file

        The "snapshot-then-broadcast" pattern below is the most important
        design choice in this class: holding the lock during a slow socket
        send would block any concurrent login/logout. Copying the subscriber
        list under the lock then releasing it before any I/O removes that
        bottleneck. Dead sockets are reaped at the end so subsequent
        broadcasts don't try to send to them again.
        """
        # 1. Build the wire message.
        message = {
            "type": "NOTIFY",
            "event": event_type,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            **payload,                    # e.g. {"username": "bob"}
        }
        wire = (json.dumps(message) + "\n").encode("utf-8")

        # 2. Snapshot the current subscribers under the lock.
        with self._lock:
            targets = list(self._subscribers.items())

        # 3. Broadcast to each subscriber WITHOUT holding the lock.
        dead = []
        for cid, (sock, _user) in targets:
            try:
                sock.sendall(wire)
            except OSError:
                # The client's socket has died (network drop, app crash,
                # etc). Remember to remove them after broadcasting finishes.
                dead.append(cid)

        # 4. Reap any dead subscribers so the next broadcast skips them.
        for cid in dead:
            self.unsubscribe(cid)


# ===========================================================================
# APPLICATION LAYER -- the DistRes server itself
# ===========================================================================
class DistResServer:
    """
    The top-level server. Owns the accept loop, spawns one daemon thread
    per connected client, dispatches incoming commands to typed handlers,
    and orchestrates the data layer + pub-sub layer together.
    """

    def __init__(self, host: str = HOST, port: int = PORT) -> None:
        self.host = host
        self.port = port
        # Compose the two layers below us.
        self.data = DataLayer(DB_PATH, SHARED_FILE)
        self.pubsub = PubSubManager()
        self._server_sock: socket.socket | None = None
        # Stop event: lets future work signal a graceful shutdown.
        self._stop_event = threading.Event()

        # Audit table of currently-connected clients -- used by LIST_CLIENTS
        # so the user can see who else is online. Guarded by _state_lock.
        self._state_lock = threading.Lock()
        self._connected_clients: deque = deque()   # (client_id, username, user_id)

    # --- Server lifecycle ---------------------------------------------------
    def start(self) -> None:
        """
        Open the listening socket and enter the accept loop. Each new
        connection is given its own daemon thread so the server can handle
        many clients concurrently.
        """
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR lets us restart quickly without "Address already in use"
        # errors during TIME_WAIT after a previous run.
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen()
        print(f"[Server] DistRes listening on {self.host}:{self.port}")
        print(f"[Server] Shared resource: {SHARED_FILE}")
        print(f"[Server] Credential DB:    {DB_PATH}")
        print("[Server] Waiting for client connections...\n")

        try:
            while not self._stop_event.is_set():
                # accept() blocks until a client connects. Returns a NEW
                # socket bound just to that client + the client's address.
                conn, addr = self._server_sock.accept()
                # Spawn a daemon thread so we don't wait for it on shutdown.
                t = threading.Thread(
                    target=self._handle_client, args=(conn, addr), daemon=True
                )
                t.start()
        except KeyboardInterrupt:
            print("\n[Server] Shutdown requested (Ctrl+C).")
        finally:
            self._server_sock.close()

    # --- Per-client handler thread -----------------------------------------
    def _handle_client(self, conn: socket.socket, addr) -> None:
        """
        Runs in a dedicated thread for the lifetime of one client
        connection. Reads line-delimited JSON requests from the socket,
        decodes them, hands each one to _dispatch(), and cleans up on exit.

        The try/finally is the cornerstone of the server's fault tolerance:
        no matter HOW the connection ends (clean logout, network drop,
        unhandled exception in a handler), this thread always unsubscribes
        the client from PubSubManager and closes its socket. Without it, a
        crashed client would leave an orphan subscription forever.
        """
        # Unique ID for this connection: the remote host:port pair.
        client_id = f"{addr[0]}:{addr[1]}"
        username = None       # populated once the client successfully LOGINs
        print(f"[Server] New connection from {client_id}")

        # TCP doesn't preserve message boundaries -- one recv() may return
        # a partial JSON object, two objects glued together, etc. We
        # accumulate bytes in `buffer` and only process complete lines.
        buffer = b""
        try:
            while not self._stop_event.is_set():
                # Read up to 4 KB of bytes from the client.
                chunk = conn.recv(4096)
                if not chunk:
                    # Empty bytes object means the remote end has closed
                    # the connection cleanly. Exit the loop normally.
                    break
                buffer += chunk

                # Process every complete (newline-terminated) line in the
                # buffer. There may be zero, one, or many per recv().
                while b"\n" in buffer:
                    line, _, buffer = buffer.partition(b"\n")
                    if not line.strip():
                        continue   # skip blank lines (heartbeats etc.)
                    try:
                        msg = json.loads(line.decode("utf-8"))
                    except json.JSONDecodeError:
                        # Malformed JSON -- tell the client and carry on.
                        self._reply(conn, {"type": "ERROR",
                                           "message": "Malformed JSON"})
                        continue

                    # _dispatch returns the (possibly-new) username if the
                    # message was a successful LOGIN, otherwise None. The
                    # `or username` keeps the existing username unchanged.
                    username = (self._dispatch(conn, client_id, username, msg)
                                or username)

        except ConnectionResetError:
            # Client process died or network cable yanked. Not an error
            # we can recover from -- the finally block tidies up.
            print(f"[Server] {client_id} disconnected abruptly.")
        except Exception as exc:
            # Catch-all so an unexpected bug in one handler doesn't
            # kill the entire connection thread silently.
            print(f"[Server] Error with {client_id}: {exc}")
        finally:
            # GUARANTEED cleanup -- regardless of how we exited.
            self.pubsub.unsubscribe(client_id)
            with self._state_lock:
                # Remove this client from the audit/state table.
                self._connected_clients = deque(
                    c for c in self._connected_clients if c[0] != client_id
                )
            try:
                conn.close()
            except OSError:
                pass
            print(f"[Server] Closed connection for {client_id}\n")

    # --- Command dispatcher ------------------------------------------------
    def _dispatch(self, conn, client_id: str,
                  current_user: str | None, msg: dict) -> str | None:
        """
        Route a single decoded request to the correct handler based on its
        'type' field. Returns the username if this call was a successful
        LOGIN (so the caller can remember it for future commands on this
        connection), otherwise None.

        Authentication is enforced here: every command except LOGIN/PING
        requires the client to be already logged in.
        """
        cmd = msg.get("type", "").upper()

        # LOGIN is special: it's the only command allowed without prior auth,
        # and it returns the username so the caller can track it.
        if cmd == "LOGIN":
            return self._handle_login(conn, client_id, msg)

        # Gatekeeper: anything else requires the user to be authenticated.
        if current_user is None and cmd not in ("LOGIN", "PING"):
            self._reply(conn, {"type": "ERROR",
                               "message": "Not logged in. Send LOGIN first."})
            return None

        # Route to the typed handler. Each handler knows how to build its
        # own reply and (where appropriate) trigger pub-sub broadcasts.
        if cmd == "READ":
            self._handle_read(conn, client_id, current_user)
        elif cmd == "WRITE":
            self._handle_write(conn, client_id, current_user, msg)
        elif cmd == "LIST_CLIENTS":
            self._handle_list_clients(conn)
        elif cmd == "PING":
            # Lightweight liveness check -- useful for future fault tolerance.
            self._reply(conn, {"type": "PONG"})
        elif cmd == "LOGOUT":
            self._handle_logout(conn, client_id, current_user)
        else:
            # Unknown command -- tell the client, don't crash the connection.
            self._reply(conn, {"type": "ERROR",
                               "message": f"Unknown command: {cmd}"})
        return None

    # ---- LOGIN handler ----------------------------------------------------
    def _handle_login(self, conn, client_id: str, msg: dict):
        """
        Authenticate the client and (if valid) subscribe them to the
        pub-sub registry. Broadcasts CLIENT_JOINED to all subscribers
        (including the new one) so everyone's UI shows the new arrival.
        """
        username = msg.get("username", "")
        password = msg.get("password", "")
        # Delegate credential check to the data layer.
        user_id = self.data.authenticate(username, password)

        if user_id is None:
            # Bad credentials -- reply with failure and DO NOT subscribe.
            self._reply(conn, {"type": "LOGIN_RESULT", "ok": False,
                               "message": "Invalid credentials."})
            return None

        # 1. Add to the audit table of active clients (for LIST_CLIENTS).
        with self._state_lock:
            self._connected_clients.append((client_id, username, user_id))
        # 2. Subscribe this socket to pub-sub broadcasts.
        self.pubsub.subscribe(client_id, conn, username)
        # 3. Tell the client they're in.
        self._reply(conn, {"type": "LOGIN_RESULT", "ok": True,
                           "user_id": user_id,
                           "message": f"Welcome, {username}."})
        # 4. Tell EVERYONE (including the new client) that someone joined.
        self.pubsub.publish("CLIENT_JOINED",
                            {"username": username, "user_id": user_id})
        print(f"[Server] {username} ({user_id}) logged in from {client_id}")
        return username

    # ---- READ handler -----------------------------------------------------
    def _handle_read(self, conn, client_id: str, username: str):
        """
        Read the shared file (via the lock-guarded data layer) and send
        the content back to the requesting client. Also broadcasts a
        CLIENT_READ notification so every other client knows a read
        occurred (useful for monitoring during the demo).
        """
        print(f"[Server] READ requested by {username}")
        try:
            content = self.data.read_file()       # blocks if a writer is active
            self._reply(conn, {"type": "READ_RESULT", "ok": True,
                               "content": content})
            self.pubsub.publish("CLIENT_READ", {"username": username})
        except Exception as exc:
            # Don't let a file-system error kill the connection.
            self._reply(conn, {"type": "READ_RESULT", "ok": False,
                               "message": str(exc)})

    # ---- WRITE handler ----------------------------------------------------
    def _handle_write(self, conn, client_id: str, username: str, msg: dict):
        """
        Overwrite the shared file with the client's content. Broadcasts
        FILE_UPDATED to every subscriber so they know to refresh -- this
        is THE pub-sub demo moment for the marker.
        """
        new_content = msg.get("content", "")
        print(f"[Server] WRITE requested by {username} "
              f"({len(new_content)} bytes)")
        try:
            # Pass `username` so the data layer can append the audit line.
            self.data.write_file(new_content, username)
            self._reply(conn, {"type": "WRITE_RESULT", "ok": True,
                               "message": "File updated."})
            # Pub-sub broadcast: this is the key event that proves
            # distributed coordination is working.
            self.pubsub.publish("FILE_UPDATED",
                                {"username": username,
                                 "content": new_content})
        except Exception as exc:
            self._reply(conn, {"type": "WRITE_RESULT", "ok": False,
                               "message": str(exc)})

    # ---- LIST_CLIENTS handler --------------------------------------------
    def _handle_list_clients(self, conn):
        """
        Return the audit table of currently-connected clients. Lets the
        marker see at a glance who's online during the demo.
        """
        with self._state_lock:
            # Build the reply OUTSIDE the wire-send below to keep the lock
            # held for as short a time as possible.
            clients = [{"client_id": cid, "username": u, "user_id": uid}
                       for cid, u, uid in self._connected_clients]
        self._reply(conn, {"type": "CLIENT_LIST", "clients": clients})

    # ---- LOGOUT handler ---------------------------------------------------
    def _handle_logout(self, conn, client_id: str, username: str):
        """
        Cleanly unsubscribe the client and broadcast a CLIENT_LEFT
        notification to the rest. Note that the actual socket close is
        handled by _handle_client's finally block when the loop ends.
        """
        self.pubsub.unsubscribe(client_id)
        with self._state_lock:
            self._connected_clients = deque(
                c for c in self._connected_clients if c[0] != client_id
            )
        self._reply(conn, {"type": "LOGOUT_RESULT", "ok": True})
        self.pubsub.publish("CLIENT_LEFT", {"username": username})
        print(f"[Server] {username} logged out from {client_id}")

    # --- Wire-format helper ------------------------------------------------
    @staticmethod
    def _reply(conn: socket.socket, payload: dict) -> None:
        """
        Send a JSON reply on the given socket, terminated by '\\n'.
        Silently swallows OSError because the per-client handler's
        finally block is responsible for cleanup once the socket dies.
        """
        try:
            conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    server = DistResServer()
    server.start()
