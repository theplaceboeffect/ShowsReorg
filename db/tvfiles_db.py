#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///

"""
tvfiles_db.py

Scan directory trees and store file metadata in a SQLite database.
Print each directory as it is scanned and the number of files found.
"""

import argparse
import os
import sqlite3
from datetime import datetime, UTC
from pathlib import Path

DB_NAME = "tvfiles.sqlite3"


def connect_db():
    return sqlite3.connect(DB_NAME)


def init_db(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_date TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scan_dirs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dirname TEXT UNIQUE NOT NULL,
        first_added TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        filepath TEXT NOT NULL,
        creation_date TEXT,
        added_date TEXT NOT NULL,
        removed_date TEXT,
        UNIQUE(filename, filepath)
    )
    """)

    conn.commit()


def utc_now():
    return datetime.now(UTC).isoformat()


def record_scan(conn):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scans (scan_date) VALUES (?)",
        (utc_now(),)
    )
    conn.commit()
    return cur.lastrowid


def record_dir(conn, dirname):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO scan_dirs (dirname, first_added)
        VALUES (?, ?)
    """, (dirname, utc_now()))
    conn.commit()


def scan_directory(conn, dirname):
    cur = conn.cursor()
    now = utc_now()

    total_inserted = 0

    for root, _, files in os.walk(dirname):
        root_path = str(Path(root).resolve())
        file_count = 0

        for fname in files:
            full_path = Path(root) / fname
            try:
                stat = full_path.stat()
                creation_date = datetime.fromtimestamp(
                    stat.st_ctime, tz=UTC
                ).isoformat()
            except OSError:
                creation_date = None

            cur.execute("""
                INSERT OR IGNORE INTO files
                (filename, filepath, creation_date, added_date, removed_date)
                VALUES (?, ?, ?, ?, NULL)
            """, (
                fname,
                root_path,
                creation_date,
                now
            ))

            if cur.rowcount > 0:
                total_inserted += 1
            file_count += 1

        if file_count > 0:
            print(f"Scanned: {root_path}  | files: {file_count}")

    conn.commit()
    print(f"Total files inserted this scan: {total_inserted}")


def update_directories(conn):
    cur = conn.cursor()
    now = utc_now()

    cur.execute("SELECT dirname FROM scan_dirs")
    dirs = [row[0] for row in cur.fetchall()]

    existing = set()
    for d in dirs:
        for root, _, files in os.walk(d):
            root_path = str(Path(root).resolve())
            for fname in files:
                existing.add((fname, root_path))

    cur.execute("""
        SELECT id, filename, filepath
        FROM files
        WHERE removed_date IS NULL
    """)

    removed = 0
    for file_id, fname, parent in cur.fetchall():
        if (fname, parent) not in existing:
            cur.execute("""
                UPDATE files
                SET removed_date = ?
                WHERE id = ?
            """, (now, file_id))
            removed += 1

    conn.commit()
    print(f"Files marked as removed: {removed}")


def main():
    parser = argparse.ArgumentParser(
        description="TV Files SQLite indexer"
    )
    parser.add_argument(
        "--dirname",
        help="Directory to scan"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update previously scanned directories"
    )

    args = parser.parse_args()

    conn = connect_db()
    init_db(conn)
    record_scan(conn)

    if args.dirname:
        dirname = str(Path(args.dirname).resolve())
        record_dir(conn, dirname)
        scan_directory(conn, dirname)

    if args.update:
        update_directories(conn)

    conn.close()


if __name__ == "__main__":
    main()
