#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///

"""
tvfiles_plex.py

Scan a Plex TV library and normalize episode file metadata into SQLite.

- Tables prefixed with plex_
- file paths split into filename + filepath
- Idempotent and repeatable
- Tracks added_date / removed_date
"""

import argparse
import sqlite3
from datetime import datetime, UTC
from pathlib import Path
import requests
import xml.etree.ElementTree as ET


# -------------------------
# helpers
# -------------------------

def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def plex_get(base_url: str, token: str, path: str) -> str:
    r = requests.get(
        f"{base_url}{path}",
        headers={"X-Plex-Token": token},
        timeout=60,
    )
    r.raise_for_status()
    return r.text


# -------------------------
# schema
# -------------------------

def init_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS plex_series (
        id INTEGER PRIMARY KEY,
        plex_key TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS plex_episodes (
        id INTEGER PRIMARY KEY,
        plex_key TEXT UNIQUE NOT NULL,
        series_id INTEGER NOT NULL,
        season_number INTEGER,
        episode_number INTEGER,
        FOREIGN KEY(series_id) REFERENCES plex_series(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS plex_files (
        id INTEGER PRIMARY KEY,
        filename TEXT NOT NULL,
        filepath TEXT NOT NULL,
        series_id INTEGER NOT NULL,
        episode_id INTEGER,
        added_date TEXT NOT NULL,
        removed_date TEXT,
        UNIQUE(filename, filepath),
        FOREIGN KEY(series_id) REFERENCES plex_series(id),
        FOREIGN KEY(episode_id) REFERENCES plex_episodes(id)
    )
    """)

    conn.commit()


# -------------------------
# plex discovery
# -------------------------

def get_tv_library_sections(base_url: str, token: str):
    xml = plex_get(base_url, token, "/library/sections")
    root = ET.fromstring(xml)

    sections = []
    for d in root.findall(".//Directory"):
        if d.get("type") == "show":
            sections.append({
                "key": d.get("key"),
                "title": d.get("title"),
            })
    return sections


# -------------------------
# sync logic
# -------------------------

def sync_plex(conn: sqlite3.Connection, base_url: str, token: str):
    cur = conn.cursor()
    now = utc_now()

    sections = get_tv_library_sections(base_url, token)
    print(f"TV libraries found: {len(sections)}")

    seen_files: set[tuple[str, str]] = set()
    total_files = 0

    for section in sections:
        print(f"\n▶ Library: {section['title']}")

        xml = plex_get(base_url, token, f"/library/sections/{section['key']}/all")
        root = ET.fromstring(xml)

        for show in root.findall(".//Directory"):
            show_key = show.get("key")
            show_title = show.get("title")

            print(f"  Series: {show_title}")

            # --- series ---
            cur.execute("""
                INSERT OR IGNORE INTO plex_series
                (plex_key, title)
                VALUES (?, ?)
            """, (show_key, show_title))

            cur.execute(
                "SELECT id FROM plex_series WHERE plex_key = ?",
                (show_key,),
            )
            series_db_id = cur.fetchone()[0]

            # --- seasons ---
            seasons_xml = plex_get(base_url, token, show_key)
            seasons_root = ET.fromstring(seasons_xml)

            for season in seasons_root.findall(".//Directory"):
                season_key = season.get("key")
                season_number = season.get("index")

                episodes_xml = plex_get(base_url, token, season_key)
                episodes_root = ET.fromstring(episodes_xml)

                for ep in episodes_root.findall(".//Video"):
                    ep_key = ep.get("key")
                    episode_number = ep.get("index")

                    # --- episode ---
                    cur.execute("""
                        INSERT OR IGNORE INTO plex_episodes
                        (plex_key, series_id, season_number, episode_number)
                        VALUES (?, ?, ?, ?)
                    """, (
                        ep_key,
                        series_db_id,
                        season_number,
                        episode_number,
                    ))

                    cur.execute(
                        "SELECT id FROM plex_episodes WHERE plex_key = ?",
                        (ep_key,),
                    )
                    episode_db_id = cur.fetchone()[0]

                    # --- files ---
                    for part in ep.findall(".//Part"):
                        full_path = Path(part.get("file")).resolve()
                        filepath = str(full_path.parent)
                        filename = full_path.name

                        seen_files.add((filename, filepath))
                        total_files += 1

                        cur.execute("""
                            INSERT OR IGNORE INTO plex_files
                            (filename, filepath, series_id, episode_id, added_date, removed_date)
                            VALUES (?, ?, ?, ?, ?, NULL)
                        """, (
                            filename,
                            filepath,
                            series_db_id,
                            episode_db_id,
                            now,
                        ))

                        cur.execute("""
                            UPDATE plex_files
                            SET removed_date = NULL
                            WHERE filename = ? AND filepath = ?
                        """, (filename, filepath))

    # --- removed files ---
    cur.execute("""
        SELECT filename, filepath
        FROM plex_files
        WHERE removed_date IS NULL
    """)
    for filename, filepath in cur.fetchall():
        if (filename, filepath) not in seen_files:
            cur.execute("""
                UPDATE plex_files
                SET removed_date = ?
                WHERE filename = ? AND filepath = ?
            """, (now, filename, filepath))

    conn.commit()

    print("\nSummary:")
    print(f"  files seen: {total_files}")


# -------------------------
# main
# -------------------------

def main():
    p = argparse.ArgumentParser(description="Normalize Plex TV metadata into SQLite")
    p.add_argument("--db", required=True, help="SQLite database filename")
    p.add_argument("--plex-url", required=True, help="Plex base URL (e.g. http://host:32400)")
    p.add_argument("--plex-token", required=True, help="Plex X-Plex-Token")

    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    init_schema(conn)
    sync_plex(conn, args.plex_url.rstrip("/"), args.plex_token)
    conn.close()


if __name__ == "__main__":
    main()
