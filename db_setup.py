"""
db_setup.py — Initialises the SQLite user-credential database used by the
DistRes server. Creates a `users` table and inserts a few default accounts
so that clients have something to log in with out of the box.

Run once before the first server start, or whenever you want to reset
the database:

    python db_setup.py
"""

import sqlite3
import os

DB_PATH = "distres_users.db"

DEFAULT_USERS = [
    ("alice", "alice123", "U001"),
    ("bob",   "bob123",   "U002"),
    ("carol", "carol123", "U003"),
    ("dave",  "dave123",  "U004"),
    ("eve",   "eve123",   "U005"),
]


def init_db(db_path: str = DB_PATH) -> None:
    """Create the users table and seed it with default accounts."""
    # Start clean every time the script runs so re-running gives a known state.
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"[db_setup] Removed existing database: {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE users (
            user_id  TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT         NOT NULL
        )
        """
    )

    cur.executemany(
        "INSERT INTO users (username, password, user_id) VALUES (?, ?, ?)",
        DEFAULT_USERS,
    )

    conn.commit()
    conn.close()

    print(f"[db_setup] Created {db_path} with {len(DEFAULT_USERS)} default users:")
    for u, p, i in DEFAULT_USERS:
        print(f"           - {u:<6} / {p:<10} (id={i})")


if __name__ == "__main__":
    init_db()
