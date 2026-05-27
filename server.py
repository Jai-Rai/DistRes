"""
server.py - DistRes Server Node
================================
The server in the Distributed Resource Access and Synchronisation Engine.

Responsibilities (layered architecture):

    +---------------------------------------------+
    |  Application/Logic Layer                    |
    |    - Connection management                  |
    |    - Command dispatch (LOGIN/READ/WRITE...) |
    |    - Publish-Subscribe broadcast            |
    +---------------------------------------------+
    |  Data Layer                                 |
    |    - SQLite credential database             |
    |    - ReadWriteLock-guarded file access      |
    +---------------------------------------------+

Wire protocol: simple newline-terminated JSON messages over TCP.
Each client message is one JSON object; each server reply / notification
is also one JSON object terminated by '\\n'.

Run:
    python db_setup.py     # once, to seed the user database
    python server.py       # starts listening on 0.0.0.0:5050
"""

import json
import socket
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime

HOST = "0.0.0.0"
PORT = 5050
DB_PATH = "distres_users.db"
SHARED_FILE = "ProductSpecification.txt"


# ---------------------------------------------------------------------------
# Read-Write Lock (carried over from CW1 / ConRes - first-reader preference)
# ---------------------------------------------------------------------------
class ReadWriteLock:
    """Multiple concurrent readers OR a single exclusive writer."""

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer_lock = threading.Lock()

    def acquire_read(self) -> None:
        with self._read_ready:
            self._readers += 1
            if self._readers == 1:
                # First reader closes the writer gate.
                self._writer_lock.acquire()

    def release_read(self) -> None:
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                # Last reader reopens the writer gate.
                self._writer_lock.release()

    def acquire_write(self) -> None:
        self._writer_lock.acquire()

    def release_write(self) -> None:
        self._writer_lock.release()


# ---------------------------------------------------------------------------
# Data Layer
# ---------------------------------------------------------------------------
class DataLayer:
    """Encapsulates the database and shared-file access primitives."""

    def __init__(self, db_path: str, file_path: str) -> None:
        self.db_path = db_path
        self.file_path = file_path
        self.rw_lock = ReadWriteLock()
        # SQLite needs a per-thread connection, so we open one on demand.

    # --- Credentials (database) -----------------------------------------
    def authenticate(self, username: str, password: str):
        """Return user_id if credentials are valid, otherwise None."""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT user_id FROM users WHERE username = ? AND password = ?",
                (username, password),
            )
            row = cur.fetchone()
            conn.close()
            return row[0] if row else None
        except sqlite3.Error as exc:
            print(f"[DataLayer] DB error: {exc}")
            return None

    # --- Shared file (read-write locked) --------------------------------
    def read_file(self) -> str:
        self.rw_lock.acquire_read()
        try:
            # Simulate non-trivial work so concurrency is observable.
            time.sleep(0.4)
            with open(self.file_path, "r", encoding="utf-8") as fh:
                return fh.read()
        finally:
            self.rw_lock.release_read()

    def write_file(self, new_content: str) -> None:
        self.rw_lock.acquire_write()
        try:
            time.sleep(0.6)
            with open(self.file_path, "w", encoding="utf-8") as fh:
                fh.write(new_content)
        finally:
            self.rw_lock.release_write()


# ---------------------------------------------------------------------------
# Publish-Subscribe Manager
# ---------------------------------------------------------------------------
class PubSubManager:
    """Maintains the set of subscriber sockets and broadcasts events."""

    def __init__(self) -> None:
        self._subscribers = {}       # client_id -> (socket, username)
        self._lock = threading.Lock()

    def subscribe(self, client_id: str, sock: socket.socket, username: str) -> None:
        with self._lock:
            self._subscribers[client_id] = (sock, username)
        print(f"[PubSub] {username} ({client_id}) subscribed -> "
              f"{len(self._subscribers)} active")

    def unsubscribe(self, client_id: str) -> None:
        with self._lock:
            entry = self._subscribers.pop(client_id, None)
        if entry:
            print(f"[PubSub] {entry[1]} ({client_id}) unsubscribed -> "
                  f"{len(self._subscribers)} active")

    def publish(self, event_type: str, payload: dict) -> None:
        """Broadcast a NOTIFY message to every subscribed client."""
        message = {
            "type": "NOTIFY",
            "event": event_type,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            **payload,
        }
        wire = (json.dumps(message) + "\n").encode("utf-8")

        # Copy under lock so broadcasting itself doesn't hold it.
        with self._lock:
            targets = list(self._subscribers.items())

        dead = []
        for cid, (sock, _user) in targets:
            try:
                sock.sendall(wire)
            except OSError:
                dead.append(cid)

        # Reap any sockets that failed during broadcast.
        for cid in dead:
            self.unsubscribe(cid)


# ---------------------------------------------------------------------------
# Application / Logic Layer  ---  the actual server
# ---------------------------------------------------------------------------
class DistResServer:
    def __init__(self, host: str = HOST, port: int = PORT) -> None:
        self.host = host
        self.port = port
        self.data = DataLayer(DB_PATH, SHARED_FILE)
        self.pubsub = PubSubManager()
        self._server_sock: socket.socket | None = None
        self._stop_event = threading.Event()

        # Audit state for the optional admin/status command.
        self._state_lock = threading.Lock()
        self._connected_clients = deque()    # (client_id, username, addr)

    # --- Socket lifecycle ------------------------------------------------
    def start(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen()
        print(f"[Server] DistRes listening on {self.host}:{self.port}")
        print(f"[Server] Shared resource: {SHARED_FILE}")
        print(f"[Server] Credential DB:    {DB_PATH}")
        print("[Server] Waiting for client connections...\n")

        try:
            while not self._stop_event.is_set():
                conn, addr = self._server_sock.accept()
                t = threading.Thread(
                    target=self._handle_client, args=(conn, addr), daemon=True
                )
                t.start()
        except KeyboardInterrupt:
            print("\n[Server] Shutdown requested.")
        finally:
            self._server_sock.close()

    # --- Per-client handler ---------------------------------------------
    def _handle_client(self, conn: socket.socket, addr) -> None:
        client_id = f"{addr[0]}:{addr[1]}"
        username = None
        print(f"[Server] New connection from {client_id}")

        # Each socket is line-delimited JSON; buffer up partial reads.
        buffer = b""
        try:
            while not self._stop_event.is_set():
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer += chunk

                while b"\n" in buffer:
                    line, _, buffer = buffer.partition(b"\n")
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line.decode("utf-8"))
                    except json.JSONDecodeError:
                        self._reply(conn, {"type": "ERROR",
                                           "message": "Malformed JSON"})
                        continue

                    username = self._dispatch(conn, client_id, username, msg) \
                               or username

        except ConnectionResetError:
            print(f"[Server] {client_id} disconnected abruptly.")
        except Exception as exc:
            print(f"[Server] Error with {client_id}: {exc}")
        finally:
            # Always tidy up: unsubscribe & close the socket.
            self.pubsub.unsubscribe(client_id)
            with self._state_lock:
                self._connected_clients = deque(
                    c for c in self._connected_clients if c[0] != client_id
                )
            try:
                conn.close()
            except OSError:
                pass
            print(f"[Server] Closed connection for {client_id}\n")

    # --- Command dispatch ------------------------------------------------
    def _dispatch(self, conn, client_id: str,
                  current_user: str | None, msg: dict) -> str | None:
        """Route a single client request. Returns the (new) username if logged in."""
        cmd = msg.get("type", "").upper()

        if cmd == "LOGIN":
            return self._handle_login(conn, client_id, msg)

        if current_user is None and cmd not in ("LOGIN", "PING"):
            self._reply(conn, {"type": "ERROR",
                               "message": "Not logged in. Send LOGIN first."})
            return None

        if cmd == "READ":
            self._handle_read(conn, client_id, current_user)
        elif cmd == "WRITE":
            self._handle_write(conn, client_id, current_user, msg)
        elif cmd == "LIST_CLIENTS":
            self._handle_list_clients(conn)
        elif cmd == "PING":
            self._reply(conn, {"type": "PONG"})
        elif cmd == "LOGOUT":
            self._handle_logout(conn, client_id, current_user)
        else:
            self._reply(conn, {"type": "ERROR",
                               "message": f"Unknown command: {cmd}"})
        return None

    # ---- LOGIN ---------------------------------------------------------
    def _handle_login(self, conn, client_id: str, msg: dict):
        username = msg.get("username", "")
        password = msg.get("password", "")
        user_id = self.data.authenticate(username, password)

        if user_id is None:
            self._reply(conn, {"type": "LOGIN_RESULT", "ok": False,
                               "message": "Invalid credentials."})
            return None

        # Track and subscribe.
        with self._state_lock:
            self._connected_clients.append((client_id, username, user_id))
        self.pubsub.subscribe(client_id, conn, username)

        self._reply(conn, {"type": "LOGIN_RESULT", "ok": True,
                           "user_id": user_id,
                           "message": f"Welcome, {username}."})
        # Notify all subscribers (including new one) that a new client joined.
        self.pubsub.publish("CLIENT_JOINED",
                            {"username": username, "user_id": user_id})
        print(f"[Server] {username} ({user_id}) logged in from {client_id}")
        return username

    # ---- READ ----------------------------------------------------------
    def _handle_read(self, conn, client_id: str, username: str):
        print(f"[Server] READ requested by {username}")
        try:
            content = self.data.read_file()
            self._reply(conn, {"type": "READ_RESULT", "ok": True,
                               "content": content})
            self.pubsub.publish("CLIENT_READ", {"username": username})
        except Exception as exc:
            self._reply(conn, {"type": "READ_RESULT", "ok": False,
                               "message": str(exc)})

    # ---- WRITE ---------------------------------------------------------
    def _handle_write(self, conn, client_id: str, username: str, msg: dict):
        new_content = msg.get("content", "")
        print(f"[Server] WRITE requested by {username} "
              f"({len(new_content)} bytes)")
        try:
            self.data.write_file(new_content)
            self._reply(conn, {"type": "WRITE_RESULT", "ok": True,
                               "message": "File updated."})
            # Publish-subscribe: notify *all* clients about the update.
            self.pubsub.publish("FILE_UPDATED",
                                {"username": username,
                                 "content": new_content})
        except Exception as exc:
            self._reply(conn, {"type": "WRITE_RESULT", "ok": False,
                               "message": str(exc)})

    # ---- LIST_CLIENTS --------------------------------------------------
    def _handle_list_clients(self, conn):
        with self._state_lock:
            clients = [{"client_id": cid, "username": u, "user_id": uid}
                       for cid, u, uid in self._connected_clients]
        self._reply(conn, {"type": "CLIENT_LIST", "clients": clients})

    # ---- LOGOUT --------------------------------------------------------
    def _handle_logout(self, conn, client_id: str, username: str):
        self.pubsub.unsubscribe(client_id)
        with self._state_lock:
            self._connected_clients = deque(
                c for c in self._connected_clients if c[0] != client_id
            )
        self._reply(conn, {"type": "LOGOUT_RESULT", "ok": True})
        self.pubsub.publish("CLIENT_LEFT", {"username": username})
        print(f"[Server] {username} logged out from {client_id}")

    # --- Wire helpers ----------------------------------------------------
    @staticmethod
    def _reply(conn: socket.socket, payload: dict) -> None:
        try:
            conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))
        except OSError:
            # The socket has been closed; the handler loop will tidy up.
            pass


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    server = DistResServer()
    server.start()
