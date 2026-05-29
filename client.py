"""
client.py - DistRes Client Node
================================
Tkinter GUI client for the DistRes distributed system. Connects to the
server over a TCP socket, sends JSON commands, and renders incoming
replies + pub-sub NOTIFY events into a desktop window.

ARCHITECTURE
------------
Two cooperating classes:

    ServerConnection   : the network layer. Owns the TCP socket, has a
                           background listener thread that pushes every
                           parsed message onto a shared Queue.

    DistResClientGUI   : the presentation layer. Polls the Queue from
                           the Tk main loop and updates widgets. Tk is
                           not thread-safe so all widget updates must run
                           on the main thread: the Queue is the bridge.

FAULT TOLERANCE
---------------
ServerConnection.connect() retries up to MAX_RETRIES times with a
RETRY_DELAY_S pause between attempts, so a client started before the
server is ready will wait rather than crash.

RUNNING
-------
    python client.py
"""

import json
import queue
import socket
import threading
import time
import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
SERVER_HOST = "127.0.0.1"   # default server address (localhost)
SERVER_PORT = 5050          # default server port
MAX_RETRIES = 3             # how many connect() attempts before giving up
RETRY_DELAY_S = 1.0         # seconds to wait between failed attempts


# ===========================================================================
# NETWORK LAYER. TCP socket wrapper with background listener
# ===========================================================================
class ServerConnection:
    """
    Thin wrapper around a TCP socket that speaks line-delimited JSON.
    A daemon listener thread runs while the socket is open and pushes
    every parsed server message onto the shared `inbox` queue so the
    GUI thread can drain it at its own pace.
    """

    def __init__(self, host: str, port: int, inbox: queue.Queue) -> None:
        """Remember connection params; defer actual socket creation."""
        self.host = host
        self.port = port
        # The GUI passes in a Queue that it polls from the Tk main loop.
        # All server messages flow through here. This is the thread-safe
        # bridge between the listener thread and Tkinter widgets.
        self.inbox = inbox
        self.sock: socket.socket | None = None
        self._listener: threading.Thread | None = None
        self._running = False        # listener loop's exit flag

    # ---- Connect / disconnect lifecycle -----------------------------------
    def connect(self) -> bool:
        """
        Try to connect to the server. Returns True on success, False if
        all retry attempts have been exhausted. Implements the FAULT
        TOLERANCE requirement from the brief: up to MAX_RETRIES attempts
        with RETRY_DELAY_S pause between them.
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Build a fresh socket each attempt: once a connect()
                # has failed the socket is no longer usable.
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, self.port))
                self._running = True
                # Spawn the daemon listener so the GUI starts seeing
                # incoming messages as soon as they arrive.
                self._listener = threading.Thread(
                    target=self._listen_loop, daemon=True
                )
                self._listener.start()
                # Pipe an informational message into the GUI activity log.
                self.inbox.put({"type": "LOCAL_INFO",
                                "message": f"Connected to "
                                           f"{self.host}:{self.port}"})
                return True
            except OSError as exc:
                # Log the failed attempt and wait before retrying.
                self.inbox.put({"type": "LOCAL_INFO",
                                "message": f"Connection attempt "
                                           f"{attempt}/{MAX_RETRIES} failed: "
                                           f"{exc}"})
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_S)
        # All attempts exhausted: surface a hard error to the GUI.
        self.inbox.put({"type": "LOCAL_ERROR",
                        "message": "Could not reach the server. "
                                   "Is it running?"})
        return False

    def close(self) -> None:
        """Tear down the socket and signal the listener loop to exit."""
        self._running = False
        if self.sock:
            try:
                # shutdown() before close() ensures any blocked recv()
                # on the listener thread wakes up immediately.
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                # Already closed: safe to ignore.
                pass
            self.sock.close()
            self.sock = None

    # ---- Outgoing: send one JSON message ---------------------------------
    def send(self, message: dict) -> bool:
        """Encode `message` as JSON + newline and write to the socket."""
        if not self.sock:
            self.inbox.put({"type": "LOCAL_ERROR",
                            "message": "Not connected."})
            return False
        try:
            # The '\n' is essential: the server uses it as a frame
            # delimiter to know where one message ends and the next begins.
            self.sock.sendall((json.dumps(message) + "\n").encode("utf-8"))
            return True
        except OSError as exc:
            # Send failed (socket dead, network drop, etc).
            self.inbox.put({"type": "LOCAL_ERROR",
                            "message": f"Send failed: {exc}"})
            return False

    # ---- Incoming: listener loop running in its own thread ---------------
    def _listen_loop(self) -> None:
        """
        Runs in a daemon thread for the lifetime of the connection.
        Reads bytes from the socket, splits them on '\\n' into complete
        JSON messages, and pushes each parsed message onto the inbox
        queue. Never touches Tk widgets directly: that would crash Tk
        because Tk is not thread-safe.
        """
        # Same buffering pattern as the server: TCP doesn't preserve
        # message boundaries, so bytes are accumulated until a newline appears.
        buffer = b""
        try:
            while self._running and self.sock:
                chunk = self.sock.recv(4096)
                if not chunk:
                    # Empty bytes = server closed the connection cleanly.
                    break
                buffer += chunk
                # Drain every complete line out of the buffer.
                while b"\n" in buffer:
                    line, _, buffer = buffer.partition(b"\n")
                    if not line.strip():
                        continue
                    try:
                        self.inbox.put(json.loads(line.decode("utf-8")))
                    except json.JSONDecodeError:
                        # Drop malformed lines silently; server bug, not ours.
                        pass
        except OSError:
            # Socket died: exit the loop and let the finally tidy up.
            pass
        finally:
            # Tell the GUI the connection was lost (only sent once).
            self.inbox.put({"type": "LOCAL_INFO",
                            "message": "Disconnected from server."})
            self._running = False


# ===========================================================================
# PRESENTATION LAYER. Tkinter GUI
# ===========================================================================
class DistResClientGUI:
    """
    The main Tk window. Builds the widgets in three labelled frames
    (connection, file, log) and drives them via a single inbox queue.
    All UI updates happen on the Tk main thread: the listener thread
    only enqueues messages, it never touches widgets.
    """

    # How often (ms) the Tk main loop drains the inbox queue. 80 ms = 12 Hz,
    # smooth enough for a responsive UI without burning CPU.
    POLL_MS = 80

    def __init__(self, root: tk.Tk) -> None:
        """Build the window and start the periodic queue drainer."""
        self.root = root
        self.root.title("DistRes Client")
        self.root.geometry("780x620")

        # The thread-safe message queue connecting network <-> GUI.
        self.inbox: queue.Queue = queue.Queue()
        # The current connection (None until the user clicks Login).
        self.conn: ServerConnection | None = None
        # Tracked state about the logged-in user (None when logged out).
        self.username: str | None = None
        self.user_id: str | None = None
        self.logged_in = False

        # Build widgets and put the UI into the logged-out state.
        self._build_widgets()
        self._set_logged_out_state()
        # Schedule the first drain: this re-arms itself every POLL_MS.
        self.root.after(self.POLL_MS, self._drain_inbox)
        # Hook the window-close button to send a clean LOGOUT first.
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---- Widget construction ---------------------------------------------
    def _build_widgets(self) -> None:
        """Lay out the three labelled frames and all their controls."""
        # ===== Top frame: connection details + login/logout buttons =====
        top = ttk.LabelFrame(self.root, text="Connection / Login")
        top.pack(fill="x", padx=8, pady=6)

        ttk.Label(top, text="Server:").grid(row=0, column=0, padx=4, pady=4)
        self.host_var = tk.StringVar(value=SERVER_HOST)
        ttk.Entry(top, textvariable=self.host_var, width=14).grid(
            row=0, column=1, padx=4)

        ttk.Label(top, text="Port:").grid(row=0, column=2, padx=4)
        self.port_var = tk.StringVar(value=str(SERVER_PORT))
        ttk.Entry(top, textvariable=self.port_var, width=6).grid(
            row=0, column=3, padx=4)

        ttk.Label(top, text="Username:").grid(row=1, column=0, padx=4, pady=4)
        self.user_var = tk.StringVar(value="alice")
        ttk.Entry(top, textvariable=self.user_var, width=14).grid(
            row=1, column=1, padx=4)

        ttk.Label(top, text="Password:").grid(row=1, column=2, padx=4)
        self.pass_var = tk.StringVar(value="alice123")
        # `show="*"` masks the password as the user types it.
        ttk.Entry(top, textvariable=self.pass_var, width=14, show="*").grid(
            row=1, column=3, padx=4)

        self.login_btn = ttk.Button(top, text="Login", command=self._on_login)
        self.login_btn.grid(row=0, column=4, padx=8, rowspan=2, sticky="ns")

        self.logout_btn = ttk.Button(top, text="Logout",
                                     command=self._on_logout)
        self.logout_btn.grid(row=0, column=5, padx=4, rowspan=2, sticky="ns")

        # Live status label: shows "Disconnected" / "Logged in as alice"
        self.status_var = tk.StringVar(value="Disconnected")
        ttk.Label(top, textvariable=self.status_var,
                  foreground="#444").grid(row=0, column=6, rowspan=2,
                                          padx=10, sticky="w")

        # ===== Middle frame: file contents + Read/Write/List buttons ====
        mid = ttk.LabelFrame(self.root, text="Shared Resource "
                                            "(ProductSpecification.txt)")
        mid.pack(fill="both", expand=True, padx=8, pady=4)

        # Scrollable text widget showing the file content. The user can
        # edit it directly here, then click Write file to send the changes.
        self.file_text = scrolledtext.ScrolledText(mid, height=14, wrap="word")
        self.file_text.pack(fill="both", expand=True, padx=4, pady=4)

        btn_row = ttk.Frame(mid)
        btn_row.pack(fill="x", padx=4, pady=4)
        self.read_btn = ttk.Button(btn_row, text="Read file",
                                   command=self._on_read)
        self.read_btn.pack(side="left", padx=4)
        self.write_btn = ttk.Button(btn_row, text="Write file",
                                    command=self._on_write)
        self.write_btn.pack(side="left", padx=4)
        self.list_btn = ttk.Button(btn_row, text="List clients",
                                   command=self._on_list_clients)
        self.list_btn.pack(side="left", padx=4)

        # ===== Bottom frame: pub-sub notifications + activity log =======
        bot = ttk.LabelFrame(self.root, text="Pub-Sub Notifications "
                                            "& Activity Log")
        bot.pack(fill="both", expand=True, padx=8, pady=6)
        # Read-only log: state="disabled" prevents the user from typing
        # into it. It is flipped to "normal" briefly inside _append_log().
        self.log = scrolledtext.ScrolledText(bot, height=9, wrap="word",
                                             state="disabled")
        self.log.pack(fill="both", expand=True, padx=4, pady=4)

    # ---- Button click handlers -------------------------------------------
    def _on_login(self) -> None:
        """User clicked Login: open the connection and send LOGIN."""
        if self.logged_in:
            return    # already logged in; ignore double-clicks
        host = self.host_var.get().strip() or SERVER_HOST
        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            messagebox.showerror("DistRes", "Port must be a number.")
            return

        # Build the network connection and attempt to open it.
        self.conn = ServerConnection(host, port, self.inbox)
        self._append_log(f"Connecting to {host}:{port} ...")
        if not self.conn.connect():
            return     # all retries exhausted; error already logged
        # Connection up: send the LOGIN command. The server's reply
        # arrives via the inbox queue and is handled in _handle_message.
        self.conn.send({"type": "LOGIN",
                        "username": self.user_var.get().strip(),
                        "password": self.pass_var.get().strip()})

    def _on_logout(self) -> None:
        """User clicked Logout: send LOGOUT and let the server reply."""
        if not self.logged_in or not self.conn:
            return
        self.conn.send({"type": "LOGOUT"})

    def _on_read(self) -> None:
        """User clicked Read file: send the READ command."""
        if self._guard_logged_in():
            self.conn.send({"type": "READ"})
            self._append_log("[REQ] READ sent")

    def _on_write(self) -> None:
        """User clicked Write file: send the current text area content."""
        if not self._guard_logged_in():
            return
        # `get("1.0", "end-1c")` reads everything except the trailing
        # newline Tkinter implicitly appends. "end-1c" = "end minus 1 char".
        new_content = self.file_text.get("1.0", "end-1c")
        if not new_content.strip():
            messagebox.showwarning("DistRes",
                                   "Type something into the file area first.")
            return
        self.conn.send({"type": "WRITE", "content": new_content})
        self._append_log(f"[REQ] WRITE sent ({len(new_content)} bytes)")

    def _on_list_clients(self) -> None:
        """User clicked List clients: ask the server who's online."""
        if self._guard_logged_in():
            self.conn.send({"type": "LIST_CLIENTS"})
            self._append_log("[REQ] LIST_CLIENTS sent")

    # ---- Login-state helpers --------------------------------------------
    def _guard_logged_in(self) -> bool:
        """Return True if logged in; otherwise pop a warning and return False."""
        if self.logged_in and self.conn:
            return True
        messagebox.showwarning("DistRes", "Please log in first.")
        return False

    def _set_logged_in_state(self) -> None:
        """Enable post-login buttons, disable Login, show status."""
        self.logged_in = True
        self.status_var.set(f"Logged in as {self.username} ({self.user_id})")
        self.login_btn.state(["disabled"])
        self.logout_btn.state(["!disabled"])      # the '!' clears 'disabled'
        for b in (self.read_btn, self.write_btn, self.list_btn):
            b.state(["!disabled"])

    def _set_logged_out_state(self) -> None:
        """Initial state: Login enabled, everything else greyed out."""
        self.logged_in = False
        self.username = None
        self.user_id = None
        self.status_var.set("Disconnected")
        self.login_btn.state(["!disabled"])
        self.logout_btn.state(["disabled"])
        for b in (self.read_btn, self.write_btn, self.list_btn):
            b.state(["disabled"])

    # ---- The Queue drainer: bridge from network -> UI -------------------
    def _drain_inbox(self) -> None:
        """
        Drain every pending message from the inbox queue onto the GUI.
        Called every POLL_MS milliseconds from the Tk main loop. This
        is the only place network messages cross into Tk territory.
        """
        try:
            while True:
                msg = self.inbox.get_nowait()
                self._handle_message(msg)
        except queue.Empty:
            pass
        # Re-arm so draining continues for as long as Tk runs.
        self.root.after(self.POLL_MS, self._drain_inbox)

    def _handle_message(self, msg: dict) -> None:
        """
        Dispatch one incoming server message to the right UI update.
        Mirrors the server's _dispatch but for replies instead of requests.
        """
        mtype = msg.get("type", "")

        # ---- Local-only messages (generated by ServerConnection) -----
        if mtype == "LOCAL_INFO":
            self._append_log(f"[INFO] {msg.get('message', '')}")
        elif mtype == "LOCAL_ERROR":
            self._append_log(f"[ERROR] {msg.get('message', '')}")
            messagebox.showerror("DistRes", msg.get("message", "Unknown error"))

        # ---- Server replies -------------------------------------------
        elif mtype == "LOGIN_RESULT":
            if msg.get("ok"):
                # Successful login: remember the username/ID and unlock buttons.
                self.username = self.user_var.get().strip()
                self.user_id = msg.get("user_id")
                self._set_logged_in_state()
                self._append_log(f"[LOGIN] {msg.get('message', 'OK')}")
            else:
                # Bad credentials: log it, alert the user, drop the socket.
                self._append_log(f"[LOGIN FAILED] {msg.get('message', '')}")
                messagebox.showerror("DistRes",
                                     msg.get("message", "Login failed"))
                if self.conn:
                    self.conn.close()

        elif mtype == "READ_RESULT":
            if msg.get("ok"):
                # Replace the text area with the freshly-read content.
                self.file_text.delete("1.0", "end")
                self.file_text.insert("1.0", msg.get("content", ""))
                self._append_log("[READ] File loaded.")
            else:
                self._append_log(f"[READ FAILED] {msg.get('message', '')}")

        elif mtype == "WRITE_RESULT":
            if msg.get("ok"):
                self._append_log("[WRITE] Server confirmed update.")
            else:
                self._append_log(f"[WRITE FAILED] {msg.get('message', '')}")

        elif mtype == "CLIENT_LIST":
            # Render the connected-clients list into the activity log.
            self._append_log("[CLIENTS] Currently connected:")
            for c in msg.get("clients", []):
                self._append_log(f"   - {c['username']} ({c['user_id']}) "
                                 f"@ {c['client_id']}")

        elif mtype == "LOGOUT_RESULT":
            self._append_log("[LOGOUT] Goodbye.")
            self._set_logged_out_state()
            if self.conn:
                self.conn.close()
                self.conn = None

        # ---- Pub-sub NOTIFY events ------------------------------------
        elif mtype == "NOTIFY":
            # All four event types just show a line in the activity log.
            # In a richer client it would update other widgets too (e.g.
            # auto-refresh on FILE_UPDATED), but the brief asks for the
            # notification to be visible, not auto-applied.
            event = msg.get("event", "?")
            ts = msg.get("timestamp", "")
            if event == "FILE_UPDATED":
                self._append_log(
                    f"[NOTIFY {ts}] {msg.get('username')} updated the file. "
                    f"Press 'Read file' to refresh.")
            elif event == "CLIENT_JOINED":
                self._append_log(
                    f"[NOTIFY {ts}] {msg.get('username')} joined "
                    f"({msg.get('user_id')}).")
            elif event == "CLIENT_LEFT":
                self._append_log(
                    f"[NOTIFY {ts}] {msg.get('username')} left.")
            elif event == "CLIENT_READ":
                self._append_log(
                    f"[NOTIFY {ts}] {msg.get('username')} performed a read.")
            else:
                self._append_log(f"[NOTIFY {ts}] {event} -> {msg}")

        elif mtype == "ERROR":
            self._append_log(f"[SERVER ERROR] {msg.get('message', '')}")

    # ---- Misc -------------------------------------------------------------
    def _append_log(self, line: str) -> None:
        """
        Append one line to the read-only activity log, auto-scrolling
        to keep the newest line visible. Briefly flips the widget to
        'normal' state to allow the insert, then flips it back.
        """
        self.log.configure(state="normal")
        self.log.insert("end", line + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _on_close(self) -> None:
        """
        Window 'X' button handler. Send a LOGOUT if still logged in
        so the server can broadcast CLIENT_LEFT before closing.
        """
        if self.conn:
            try:
                self.conn.send({"type": "LOGOUT"})
            except Exception:
                pass
            self.conn.close()
        self.root.destroy()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    DistResClientGUI(root)
    root.mainloop()
