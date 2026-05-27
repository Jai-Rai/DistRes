"""
client.py - DistRes Client Node
================================
Tkinter-based GUI client for the DistRes distributed system.

Responsibilities:
    * Open a TCP socket to the DistRes server.
    * Send LOGIN / READ / WRITE / LOGOUT JSON commands.
    * Run a background listener thread that receives any line-delimited
      JSON message from the server (replies *and* pub-sub NOTIFY events).
    * Fault tolerance: connection attempts retry up to MAX_RETRIES times
      with a short delay before reporting failure.
    * GUI updates are routed through a Queue polled by the Tk main loop
      so worker threads never touch widgets directly (same pattern as CW1).

Run:
    python client.py
"""

import json
import queue
import socket
import threading
import time
import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5050
MAX_RETRIES = 3
RETRY_DELAY_S = 1.0


# ---------------------------------------------------------------------------
# Network worker
# ---------------------------------------------------------------------------
class ServerConnection:
    """A thin wrapper around a TCP socket speaking line-delimited JSON."""

    def __init__(self, host: str, port: int, inbox: queue.Queue) -> None:
        self.host = host
        self.port = port
        self.inbox = inbox            # all server messages land here
        self.sock: socket.socket | None = None
        self._listener: threading.Thread | None = None
        self._running = False

    # ---- Connection lifecycle ------------------------------------------
    def connect(self) -> bool:
        """Try to connect, retrying up to MAX_RETRIES times. True on success."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, self.port))
                self._running = True
                self._listener = threading.Thread(
                    target=self._listen_loop, daemon=True
                )
                self._listener.start()
                self.inbox.put({"type": "LOCAL_INFO",
                                "message": f"Connected to "
                                           f"{self.host}:{self.port}"})
                return True
            except OSError as exc:
                self.inbox.put({"type": "LOCAL_INFO",
                                "message": f"Connection attempt "
                                           f"{attempt}/{MAX_RETRIES} failed: "
                                           f"{exc}"})
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_S)
        self.inbox.put({"type": "LOCAL_ERROR",
                        "message": "Could not reach the server. "
                                   "Is it running?"})
        return False

    def close(self) -> None:
        self._running = False
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
            self.sock = None

    # ---- Send -----------------------------------------------------------
    def send(self, message: dict) -> bool:
        if not self.sock:
            self.inbox.put({"type": "LOCAL_ERROR",
                            "message": "Not connected."})
            return False
        try:
            self.sock.sendall((json.dumps(message) + "\n").encode("utf-8"))
            return True
        except OSError as exc:
            self.inbox.put({"type": "LOCAL_ERROR",
                            "message": f"Send failed: {exc}"})
            return False

    # ---- Receive loop ---------------------------------------------------
    def _listen_loop(self) -> None:
        """Read until socket closes; push parsed JSON messages to the inbox."""
        buffer = b""
        try:
            while self._running and self.sock:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, _, buffer = buffer.partition(b"\n")
                    if not line.strip():
                        continue
                    try:
                        self.inbox.put(json.loads(line.decode("utf-8")))
                    except json.JSONDecodeError:
                        pass
        except OSError:
            pass
        finally:
            self.inbox.put({"type": "LOCAL_INFO",
                            "message": "Disconnected from server."})
            self._running = False


# ---------------------------------------------------------------------------
# Tkinter GUI
# ---------------------------------------------------------------------------
class DistResClientGUI:
    POLL_MS = 80   # how often the GUI drains the network inbox

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("DistRes Client")
        self.root.geometry("780x620")

        self.inbox: queue.Queue = queue.Queue()
        self.conn: ServerConnection | None = None
        self.username: str | None = None
        self.user_id: str | None = None
        self.logged_in = False

        self._build_widgets()
        self._set_logged_out_state()
        self.root.after(self.POLL_MS, self._drain_inbox)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---- Layout ---------------------------------------------------------
    def _build_widgets(self) -> None:
        # ---- Connection / login frame ----------------------------------
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
        ttk.Entry(top, textvariable=self.pass_var, width=14, show="*").grid(
            row=1, column=3, padx=4)

        self.login_btn = ttk.Button(top, text="Login", command=self._on_login)
        self.login_btn.grid(row=0, column=4, padx=8, rowspan=2, sticky="ns")

        self.logout_btn = ttk.Button(top, text="Logout",
                                     command=self._on_logout)
        self.logout_btn.grid(row=0, column=5, padx=4, rowspan=2, sticky="ns")

        self.status_var = tk.StringVar(value="Disconnected")
        ttk.Label(top, textvariable=self.status_var,
                  foreground="#444").grid(row=0, column=6, rowspan=2,
                                          padx=10, sticky="w")

        # ---- File contents frame ---------------------------------------
        mid = ttk.LabelFrame(self.root, text="Shared Resource "
                                            "(ProductSpecification.txt)")
        mid.pack(fill="both", expand=True, padx=8, pady=4)

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

        # ---- Notifications / log frame ---------------------------------
        bot = ttk.LabelFrame(self.root, text="Pub-Sub Notifications "
                                            "& Activity Log")
        bot.pack(fill="both", expand=True, padx=8, pady=6)
        self.log = scrolledtext.ScrolledText(bot, height=9, wrap="word",
                                             state="disabled")
        self.log.pack(fill="both", expand=True, padx=4, pady=4)

    # ---- Button handlers -----------------------------------------------
    def _on_login(self) -> None:
        if self.logged_in:
            return
        host = self.host_var.get().strip() or SERVER_HOST
        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            messagebox.showerror("DistRes", "Port must be a number.")
            return

        self.conn = ServerConnection(host, port, self.inbox)
        self._append_log(f"Connecting to {host}:{port} ...")
        if not self.conn.connect():
            return
        self.conn.send({"type": "LOGIN",
                        "username": self.user_var.get().strip(),
                        "password": self.pass_var.get().strip()})

    def _on_logout(self) -> None:
        if not self.logged_in or not self.conn:
            return
        self.conn.send({"type": "LOGOUT"})

    def _on_read(self) -> None:
        if self._guard_logged_in():
            self.conn.send({"type": "READ"})
            self._append_log("[REQ] READ sent")

    def _on_write(self) -> None:
        if not self._guard_logged_in():
            return
        new_content = self.file_text.get("1.0", "end-1c")
        if not new_content.strip():
            messagebox.showwarning("DistRes",
                                   "Type something into the file area first.")
            return
        self.conn.send({"type": "WRITE", "content": new_content})
        self._append_log(f"[REQ] WRITE sent ({len(new_content)} bytes)")

    def _on_list_clients(self) -> None:
        if self._guard_logged_in():
            self.conn.send({"type": "LIST_CLIENTS"})
            self._append_log("[REQ] LIST_CLIENTS sent")

    # ---- State helpers --------------------------------------------------
    def _guard_logged_in(self) -> bool:
        if self.logged_in and self.conn:
            return True
        messagebox.showwarning("DistRes", "Please log in first.")
        return False

    def _set_logged_in_state(self) -> None:
        self.logged_in = True
        self.status_var.set(f"Logged in as {self.username} ({self.user_id})")
        self.login_btn.state(["disabled"])
        self.logout_btn.state(["!disabled"])
        for b in (self.read_btn, self.write_btn, self.list_btn):
            b.state(["!disabled"])

    def _set_logged_out_state(self) -> None:
        self.logged_in = False
        self.username = None
        self.user_id = None
        self.status_var.set("Disconnected")
        self.login_btn.state(["!disabled"])
        self.logout_btn.state(["disabled"])
        for b in (self.read_btn, self.write_btn, self.list_btn):
            b.state(["disabled"])

    # ---- Inbox draining (server messages -> GUI) ------------------------
    def _drain_inbox(self) -> None:
        try:
            while True:
                msg = self.inbox.get_nowait()
                self._handle_message(msg)
        except queue.Empty:
            pass
        self.root.after(self.POLL_MS, self._drain_inbox)

    def _handle_message(self, msg: dict) -> None:
        mtype = msg.get("type", "")

        if mtype == "LOCAL_INFO":
            self._append_log(f"[INFO] {msg.get('message', '')}")
        elif mtype == "LOCAL_ERROR":
            self._append_log(f"[ERROR] {msg.get('message', '')}")
            messagebox.showerror("DistRes", msg.get("message", "Unknown error"))

        elif mtype == "LOGIN_RESULT":
            if msg.get("ok"):
                self.username = self.user_var.get().strip()
                self.user_id = msg.get("user_id")
                self._set_logged_in_state()
                self._append_log(f"[LOGIN] {msg.get('message', 'OK')}")
            else:
                self._append_log(f"[LOGIN FAILED] {msg.get('message', '')}")
                messagebox.showerror("DistRes",
                                     msg.get("message", "Login failed"))
                if self.conn:
                    self.conn.close()

        elif mtype == "READ_RESULT":
            if msg.get("ok"):
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

        elif mtype == "NOTIFY":
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

    # ---- Misc -----------------------------------------------------------
    def _append_log(self, line: str) -> None:
        self.log.configure(state="normal")
        self.log.insert("end", line + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _on_close(self) -> None:
        if self.conn:
            try:
                self.conn.send({"type": "LOGOUT"})
            except Exception:
                pass
            self.conn.close()
        self.root.destroy()


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    DistResClientGUI(root)
    root.mainloop()
