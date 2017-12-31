#!/usr/bin/env python3

import gzip
import sqlite3


POSTS_TABLE = """
CREATE TABLE IF NOT EXISTS posts (
    id text PRIMARY KEY
);
"""

USER_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    name text PRIMARY KEY,
    seen integer NOT NULL
);
"""

with sqlite3.connect("snoo_watch_db.sqlite3") as con:
    con.execute(POSTS_TABLE)
    con.execute(USER_TABLE)

