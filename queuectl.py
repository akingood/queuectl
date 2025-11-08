from __future__ import annotations

import os
import json
import time
import uuid
import signal
import sqlite3
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import multiprocessing as mp
import platform

BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = BASE_DIR / "queuectl.db"
LOG_DIR = BASE_DIR / "job_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CONFIG = {
    "backoff_base": 2,
    "default_max_retries": 3,
    "worker_poll_interval": 1.0,
    "job_timeout": 60
}

# ------------------------ Database Utilities ------------------------
def make_conn():
    conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
      attempts INTEGER NOT NULL DEFAULT 0,
      max_retries INTEGER NOT NULL DEFAULT 3,
      next_run_at TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_error TEXT,
      job_meta TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dlq (
      id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      attempts INTEGER,
      last_error TEXT,
      moved_at TEXT NOT NULL,
      job_meta TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS workers (
      pid INTEGER PRIMARY KEY,
      started_at TEXT NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
    """)
    for k, v in DEFAULT_CONFIG.items():
        cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (k, json.dumps(v)))
    conn.commit()
    conn.close()

def get_config(key: str, default=None):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    if row:
        try:
            return json.loads(row["value"])
        except Exception:
            return row["value"]
    return default

def set_config(key: str, value):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, json.dumps(value)))
    conn.commit()
    conn.close()

# ------------------------ Job Management ------------------------
def enqueue_job_from_text(job_text: str) -> str:
    init_db()
    try:
        obj = json.loads(job_text)
        if isinstance(obj, dict) and "command" in obj:
            command = obj["command"]
            jid = obj.get("id") or str(uuid.uuid4())
            max_attempts = int(obj.get("max_retries", 3))
        else:
            command = str(obj)
            jid = str(uuid.uuid4())
            max_attempts = 3
    except json.JSONDecodeError:
        command = job_text
        jid = str(uuid.uuid4())
        max_attempts = 3

    now_iso = datetime.utcnow().isoformat() + "Z"
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO jobs (id, command, state, attempts, max_retries, next_run_at, created_at, updated_at, last_error)
      VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?)
    """, (jid, command, max_attempts, now_iso, now_iso, now_iso, None))
    conn.commit()
    conn.close()
    print(f"✅ Enqueued job {jid}: {command}")
    return jid

# ------------------------ Worker & Execution ------------------------
def _atomic_fetch_and_lock_job(now_iso: str) -> Optional[Dict[str, Any]]:
    conn = make_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("""
          SELECT * FROM jobs
          WHERE state = 'pending' AND next_run_at <= ?
          ORDER BY created_at ASC
          LIMIT 1
        """, (now_iso,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        job = dict(row)
        cur.execute("UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ?", (now_iso, job["id"]))
        conn.commit()
        return job
    except sqlite3.OperationalError:
        try: conn.rollback()
        except: pass
        return None
    finally:
        conn.close()

def complete_job(job_id: str):
    now_iso = datetime.utcnow().isoformat() + "Z"
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (now_iso, job_id))
    conn.commit()
    conn.close()

def schedule_retry(job_id: str, attempts: int, max_attempts: int, last_error: str):
    backoff_base = get_config("backoff_base", DEFAULT_CONFIG["backoff_base"])
    delay = float(backoff_base) ** attempts
    next_run_iso = (datetime.utcnow() + timedelta(seconds=delay)).isoformat() + "Z"
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("""
      UPDATE jobs
      SET attempts = ?, state = 'pending', next_run_at = ?, updated_at = ?, last_error = ?
      WHERE id = ?
    """, (attempts, next_run_iso, datetime.utcnow().isoformat()+"Z", last_error, job_id))
    conn.commit()
    conn.close()
    return delay, next_run_iso

def move_job_to_dlq(job_id: str, last_error: str):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    if not row: conn.close(); return
    cur.execute("INSERT OR REPLACE INTO dlq (id, command, attempts, last_error, moved_at, job_meta) VALUES (?, ?, ?, ?, ?, ?)",
                (row["id"], row["command"], row["attempts"], last_error, datetime.utcnow().isoformat()+"Z", row["job_meta"]))
    cur.execute("DELETE FROM jobs WHERE id=?", (job_id,))
    conn.commit()
    conn.close()

def dlq_retry(job_id: str):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT * FROM dlq")
        for r in cur.fetchall():
            try:
                meta = json.loads(r["job_meta"] or "{}")
                if meta.get("id") == job_id:
                    row = r
                    break
            except: pass
    if not row:
        print(f"No DLQ job {job_id}")
        conn.close()
        return
    now_iso = datetime.utcnow().isoformat() + "Z"
    cur.execute("""
      INSERT OR REPLACE INTO jobs (id, command, state, attempts, max_retries, next_run_at, created_at, updated_at, last_error, job_meta)
      VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, NULL, ?)
    """, (row["id"], row["command"], get_config("default_max_retries", DEFAULT_CONFIG["default_max_retries"]), now_iso, now_iso, now_iso, row["job_meta"]))
    cur.execute("DELETE FROM dlq WHERE id=?", (row["id"],))
    conn.commit()
    conn.close()
    print(f"🔁 Retried DLQ job {row['id']}")