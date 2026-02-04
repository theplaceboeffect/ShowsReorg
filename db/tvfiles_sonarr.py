#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///

"""
tvfiles_sonarr.py

Sync file metadata from a tvfiles SQLite database with Sonarr metadata.
Stores normalized Sonarr-derived information with tables prefixed `sonarr_`.

Requirements:
- Sonarr v3 API
"""

import argparse
import sqlite3
from datetime import datetime, UTC
from pathlib import Path
import requests

# -------------------------
# helpers
# -------------------------

def utc_now():
    return datetime.now(UTC).isoformat()

def connect_db(db_path):
    return sqlite3.connect(db_path)

# -------------------------
# schema
# -------------------------

def init_schema(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sonarr_series (
        id INTEGER PRIMARY KEY,
        sonarr_id INTEGER UNIQUE NOT NULL,
        title TEXT NOT NULL,
        path TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sonarr_episodes (
        id INTEGER PRIMARY KEY,
        sonarr_id INTEGER UNIQUE NOT NULL,
        series_id INTEGER NOT NULL,
        season_number INTEGER NOT NULL,
        episode_number INTEGER NOT NULL,
        FOREIGN KEY(series_id) REFERENCES sonarr_series(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sonarr_files (
        id INTEGER PRIMARY KEY,
        file_path TEXT UNIQUE NOT NULL,
        series_id INTEGER NOT NULL,
        episode_id INTEGER NOT NULL,
        added_date TEXT NOT NULL,
        removed_date TEXT,
        FOREIGN KEY(series_id) REFERENCES sonarr_series(id),
        FOREIGN KEY(episode_id) REFERENCES sonarr_episodes(id)
    )
    """)

    conn.commit()

# -------------------------
# sonarr api
# -------------------------

def sonarr_get(base_url, api_key, endpoint):
    r = requests.get(
        f"{base_url}/api/v3/{endpoint}",
        headers={"X-Api-Key": api_key},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

# -------------------------
# sync logic
# -------------------------

def load_tvfiles(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT parent_path || '/' || filename AS full_path
        FROM files
        WHERE removed_date IS NULL
    """)
    return {Path(row[0]).resolve() for row in cur.fetchall()}

def sync_sonarr(conn, base_url, api_key):
    cur = conn.cursor()
    now = utc_now()

    series_list = sonarr_get(base_url, api_key, "series")
    episode_files = sonarr_get(base_url, api_key, "episodefile")
    episodes = sonarr_get(base_url, api_key, "episode")

    episode_by_id = {e["id"]: e for e in episodes}
    series_by_id = {s["id"]: s for s in series_list}

    seen_paths = set()

    for ef in episode_files:
        series = series_by_id.get(ef["seriesId"])
        episode = episode_by_id.get(ef["episodeId"])
        if not series or not episode:
            continue

        file_path = Path(ef["path"]).resolve()
        seen_paths.add(str(file_path))

        # series
        cur.execute("""
            INSERT OR IGNORE INTO sonarr_series
            (sonarr_id, title, path)
            VALUES (?, ?, ?)
        """, (series["id"], series["title"], series["path"]))

        cur.execute(
            "SELECT id FROM sonarr_series WHERE sonarr_id = ?",
            (series["id"],),
        )
        series_id = cur.fetchone()[0]

        # episode
        cur.execute("""
            INSERT OR IGNORE INTO sonarr_episodes
            (sonarr_id, series_id, season_number, episode_number)
            VALUES (?, ?, ?, ?)
        """, (
            episode["id"],
            series_id,
            episode["seasonNumber"],
            episode["episodeNumber"],
        ))

        cur.execute(
            "SELECT id FROM sonarr_episodes WHERE sonarr_id = ?",
            (episode["id"],),
        )
        episode_id = cur.fetchone()[0]

        # file
        cur.execute("""
            INSERT OR IGNORE INTO sonarr_files
            (file_path, series_id, episode_id, added_date, removed_date)
            VALUES (?, ?, ?, ?, NULL)
        """, (str(file_path), series_id, episode_id, now))

        # reappeared file
        cur.execute("""
            UPDATE sonarr_files
            SET removed_date = NULL
            WHERE file_path = ?
        """, (str(file_path),))

    # mark removed
    cur.execute("""
        SELECT file_path FROM sonarr_files
        WHERE removed_date IS NULL
    """)
    for (path,) in cur.fetchall():
        if path not in seen_paths:
            cur.execute("""
                UPDATE sonarr_files
                SET removed_date = ?
                WHERE file_path = ?
            """, (now, path))

    conn.commit()

# -------------------------
# main
# -------------------------

def main():
    p = argparse.ArgumentParser(description="Sync Sonarr metadata into SQLite")
    p.add_argument("--db", required=True, help="tvfiles sqlite database")
    p.add_argument("--sonarr-url", required=True, help="Sonarr base URL")
    p.add_argument("--sonarr-api-key", required=True, help="Sonarr API key")

    args = p.parse_args()

    conn = connect_db(args.db)
    init_schema(conn)
    sync_sonarr(conn, args.sonarr_url.rstrip("/"), args.sonarr_api_key)
    conn.close()

if __name__ == "__main__":
    main()
