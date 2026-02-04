#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///

"""
tvfiles_sonarr.py

Robust Sonarr v3 → SQLite normalizer.

Correctly handles:
- episodeIds missing or empty
- season packs / specials
- multi-episode files
- added / removed lifecycle
"""

import argparse
import sqlite3
from datetime import datetime, UTC
from pathlib import Path
import requests


def utc_now():
    return datetime.now(UTC).isoformat()


def sonarr_get(base_url, api_key, path):
    r = requests.get(
        f"{base_url}/api/v3/{path}",
        headers={"X-Api-Key": api_key},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


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
        season_number INTEGER,
        episode_number INTEGER,
        FOREIGN KEY(series_id) REFERENCES sonarr_series(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sonarr_files (
        id INTEGER PRIMARY KEY,
        file_path TEXT UNIQUE NOT NULL,
        series_id INTEGER NOT NULL,
        episode_id INTEGER,
        added_date TEXT NOT NULL,
        removed_date TEXT,
        FOREIGN KEY(series_id) REFERENCES sonarr_series(id),
        FOREIGN KEY(episode_id) REFERENCES sonarr_episodes(id)
    )
    """)

    conn.commit()


def sync_sonarr(conn, base_url, api_key):
    cur = conn.cursor()
    now = utc_now()

    series_list = sonarr_get(base_url, api_key, "series")
    print(f"Series found: {len(series_list)}")

    seen_files = set()
    total_files = 0
    resolved_episodes = 0
    unresolved_files = 0

    for series in series_list:
        series_api_id = series["id"]
        title = series["title"]

        print(f"\n▶ {title}")

        cur.execute("""
            INSERT OR IGNORE INTO sonarr_series
            (sonarr_id, title, path)
            VALUES (?, ?, ?)
        """, (series_api_id, title, series["path"]))

        cur.execute(
            "SELECT id FROM sonarr_series WHERE sonarr_id = ?",
            (series_api_id,),
        )
        series_db_id = cur.fetchone()[0]

        episodes = sonarr_get(
            base_url, api_key, f"episode?seriesId={series_api_id}"
        )
        episode_by_id = {e["id"]: e for e in episodes}

        episode_files = sonarr_get(
            base_url, api_key, f"episodefile?seriesId={series_api_id}"
        )

        print(f"  episode files: {len(episode_files)}")

        for ef in episode_files:
            file_path = str(Path(ef["path"]).resolve())
            seen_files.add(file_path)
            total_files += 1

            episode_id_db = None
            episode_ids = ef.get("episodeIds") or []

            if episode_ids:
                ep = episode_by_id.get(episode_ids[0])
                if ep:
                    cur.execute("""
                        INSERT OR IGNORE INTO sonarr_episodes
                        (sonarr_id, series_id, season_number, episode_number)
                        VALUES (?, ?, ?, ?)
                    """, (
                        ep["id"],
                        series_db_id,
                        ep.get("seasonNumber"),
                        ep.get("episodeNumber"),
                    ))

                    cur.execute(
                        "SELECT id FROM sonarr_episodes WHERE sonarr_id = ?",
                        (ep["id"],),
                    )
                    episode_id_db = cur.fetchone()[0]
                    resolved_episodes += 1
                else:
                    unresolved_files += 1
            else:
                unresolved_files += 1

            cur.execute("""
                INSERT OR IGNORE INTO sonarr_files
                (file_path, series_id, episode_id, added_date, removed_date)
                VALUES (?, ?, ?, ?, NULL)
            """, (
                file_path,
                series_db_id,
                episode_id_db,
                now,
            ))

            cur.execute("""
                UPDATE sonarr_files
                SET removed_date = NULL
                WHERE file_path = ?
            """, (file_path,))

    # removed files
    cur.execute("""
        SELECT file_path FROM sonarr_files
        WHERE removed_date IS NULL
    """)
    for (path,) in cur.fetchall():
        if path not in seen_files:
            cur.execute("""
                UPDATE sonarr_files
                SET removed_date = ?
                WHERE file_path = ?
            """, (now, path))

    conn.commit()

    print("\nSummary:")
    print(f"  files seen:      {total_files}")
    print(f"  episodes linked: {resolved_episodes}")
    print(f"  unresolved:     {unresolved_files}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--sonarr-url", required=True)
    p.add_argument("--sonarr-api-key", required=True)

    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    init_schema(conn)
    sync_sonarr(conn, args.sonarr_url.rstrip("/"), args.sonarr_api_key)
    conn.close()


if __name__ == "__main__":
    main()
