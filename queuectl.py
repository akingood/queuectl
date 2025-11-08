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
        try:
            conn.rollback()
        except:
            pass
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
    try:
        delay = float(backoff_base) ** attempts
    except Exception:
        delay = DEFAULT_CONFIG["backoff_base"] ** attempts
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
    if not row:
        conn.close()
        return
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
        all_rows = cur.fetchall()
        for r in all_rows:
            if r["job_meta"]:
                meta = json.loads(r["job_meta"])
                if meta.get("id") == job_id:
                    row = r
                    break
    if not row:
        print(f"No DLQ job {job_id}")
        conn.close()
        return
    command = row["command"]
    job_meta = row["job_meta"]
    now_iso = datetime.utcnow().isoformat() + "Z"
    cur.execute("""
      INSERT OR REPLACE INTO jobs (id, command, state, attempts, max_retries, next_run_at, created_at, updated_at, last_error, job_meta)
      VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, NULL, ?)
    """, (row["id"], command, get_config("default_max_retries", DEFAULT_CONFIG["default_max_retries"]), now_iso, now_iso, now_iso, job_meta))
    cur.execute("DELETE FROM dlq WHERE id=?", (row["id"],))
    conn.commit()
    conn.close()
    print(f"🔁 Retried DLQ job {row['id']}")

# ------------------------ Worker & Command Execution ------------------------
def _run_command_capture(job_id: str, command: str, timeout_s: int):
    start = datetime.utcnow().isoformat() + "Z"
    log_path = LOG_DIR / f"{job_id}.log"
    try:
        completed = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout_s)
        out = (completed.stdout or "") + (completed.stderr or "")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n--- run at {start} ---\n")
            f.write(out)
            f.write(f"\n--- exit {completed.returncode} ---\n")
        return (completed.returncode == 0), completed.returncode, out
    except subprocess.TimeoutExpired as te:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n--- run at {start} TIMEOUT ({timeout_s}s) ---\n")
        return False, -1, f"Timeout after {timeout_s}s"
    except Exception as e:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n--- run at {start} ERROR ---\n{repr(e)}\n")
        return False, -1, str(e)

def register_worker_in_db(pid: int):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO workers (pid, started_at) VALUES (?, ?)", (pid, datetime.utcnow().isoformat()+"Z"))
    conn.commit()
    conn.close()

def deregister_worker_in_db(pid: int):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM workers WHERE pid = ?", (pid,))
    conn.commit()
    conn.close()

def worker_process(stop_event: mp.Event, worker_name: str):
    pid = os.getpid()
    register_worker_in_db(pid)
    print(f"[{worker_name}] started pid={pid}")
    poll = float(get_config("worker_poll_interval", DEFAULT_CONFIG["worker_poll_interval"]))
    timeout_s = int(get_config("job_timeout", DEFAULT_CONFIG["job_timeout"]))
    try:
        while not stop_event.is_set():
            now_iso = datetime.utcnow().isoformat() + "Z"
            job = _atomic_fetch_and_lock_job(now_iso)
            if not job:
                time.sleep(poll)
                continue
            jid = job["id"]
            command = job["command"]
            print(f"[{worker_name}] picked job {jid}: {command}")
            success, exit_code, out = _run_command_capture(jid, command, timeout_s)
            if success:
                complete_job(jid)
                print(f"[{worker_name}] job {jid} completed (exit {exit_code})")
            else:
                attempts = job["attempts"] + 1
                max_attempts = int(job["max_retries"])
                if attempts >= max_attempts:
                    move_job_to_dlq(jid, out[:2000])
                    print(f"[{worker_name}] job {jid} moved to DLQ after {attempts} attempts")
                else:
                    delay, next_run = schedule_retry(jid, attempts, job["max_retries"], out[:2000])
                    print(f"[{worker_name}] job {jid} failed (attempt {attempts}), retry in {delay:.1f}s")
            time.sleep(0.01)
    except KeyboardInterrupt:
        print(f"[{worker_name}] received KeyboardInterrupt, exiting")
    finally:
        deregister_worker_in_db(pid)
        print(f"[{worker_name}] stopped pid={pid}")

# ------------------------ CLI Commands ------------------------
def list_jobs(state: str):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, command, attempts, max_retries, next_run_at, created_at FROM jobs WHERE state = ? ORDER BY created_at", (state,))
    rows = cur.fetchall()
    conn.close()
    if not rows:
        print(f"No {state} jobs")
        return
    for r in rows:
        print(f"{r['id']} | attempts: {r['attempts']}/{r['max_retries']} | next_run: {r['next_run_at']} | created: {r['created_at']}\n  cmd: {r['command']}")

def status():
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
    rows = cur.fetchall()
    summary = {r["state"]: r["cnt"] for r in rows}
    cur.execute("SELECT COUNT(*) as cnt FROM dlq")
    dlq_count = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(*) as cnt FROM workers")
    worker_count = cur.fetchone()["cnt"]
    conn.close()
    print(f"Workers (registered): {worker_count}")
    print(f"pending: {summary.get('pending', 0)} | processing: {summary.get('processing', 0)} | completed: {summary.get('completed', 0)} | failed: {summary.get('failed', 0)} | dlq: {dlq_count}")

def dlq_list():
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, command, attempts, last_error, moved_at FROM dlq")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        print("DLQ is empty")
        return
    for r in rows:
        err = (r["last_error"][:200] + "...") if r["last_error"] and len(r["last_error"]) > 200 else r["last_error"]
        print(f"{r['id']} | attempts: {r['attempts']} | moved_at: {r['moved_at']}\n  cmd: {r['command']}\n  err: {err}")

def worker_stop_by_db():
    if platform.system() == "Windows":
        print("Worker stop on Windows: please stop using Ctrl+C in terminal or taskkill /PID <pid> /F")
        return
    conn = make_conn()
    cur = conn.cursor()
    cur.execute("SELECT pid FROM workers")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        print("No workers registered to stop.")
        return
    stopped = 0
    for r in rows:
        pid = r["pid"]
        try:
            os.kill(pid, signal.SIGINT)
            stopped += 1
        except ProcessLookupError:
            deregister_worker_in_db(pid)
        except PermissionError:
            print(f"Permission error when signaling {pid}")
        except Exception as e:
            print(f"Error signaling {pid}: {e}")
    print(f"Signaled {stopped} worker(s).")

# ------------------------ Main CLI ------------------------
def main():
    init_db()
    parser = argparse.ArgumentParser(description="queuectl - CLI Job Queue (single file)")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("init", help="Initialize DB and default config")

    p = sub.add_parser("enqueue", help='Enqueue job: provide JSON or plain command string')
    p.add_argument("job", help='Either JSON like \'{"command":"echo hi","max_retries":3}\' or a plain command string')

    wp = sub.add_parser("worker", help="Worker management")
    wsp = wp.add_subparsers(dest="wcmd")
    start = wsp.add_parser("start", help="Start workers")
    start.add_argument("--count", type=int, default=1)
    wsp.add_parser("stop", help="Stop registered workers (sends SIGINT to PIDs)")

    sub.add_parser("status", help="Show overall status")

    lp = sub.add_parser("list", help="List jobs by state")
    lp.add_argument("--state", default="pending", choices=["pending", "processing", "completed", "failed", "dead"])

    dlq = sub.add_parser("dlq", help="DLQ commands")
    dlqs = dlq.add_subparsers(dest="dcmd")
    dlqs.add_parser("list", help="List DLQ")
    retry = dlqs.add_parser("retry", help="Retry DLQ job")
    retry.add_argument("job_id")

    cp = sub.add_parser("config", help="Config get/set")
    cps = cp.add_subparsers(dest="ccmd")
    setc = cps.add_parser("set", help="Set config key")
    setc.add_argument("key")
    setc.add_argument("value")
    getc = cps.add_parser("get", help="Get config key")
    getc.add_argument("key", nargs="?")

    args = parser.parse_args()

    if not args.cmd:
        parser.print_help()
        return

    if args.cmd == "init":
        init_db()
        print("DB initialized")
        return

    if args.cmd == "enqueue":
        enqueue_job_from_text(args.job)
        return

    if args.cmd == "worker":
        if args.wcmd == "start":
            count = args.count
            stop_event = mp.Event()
            processes = []

            def shutdown(sig, frame):
                print("\nShutting down workers...")
                stop_event.set()
                for p in processes:
                    try:
                        if platform.system() != "Windows":
                            os.kill(p.pid, signal.SIGINT)
                    except Exception:
                        pass

            signal.signal(signal.SIGINT, shutdown)
            signal.signal(signal.SIGTERM, shutdown)

            for i in range(count):
                wid = f"worker-{os.getpid()}-{i}"
                p = mp.Process(target=worker_process, args=(stop_event, wid), daemon=True)
                p.start()
                processes.append(p)

            try:
                while any(p.is_alive() for p in processes):
                    time.sleep(0.5)
            except KeyboardInterrupt:
                shutdown(None, None)
            return
        elif args.wcmd == "stop":
            worker_stop_by_db()
            return

    if args.cmd == "status":
        status()
        return

    if args.cmd == "list":
        list_jobs(args.state)
        return

    if args.cmd == "dlq":
        if args.dcmd == "list":
            dlq_list()
            return
        elif args.dcmd == "retry":
            dlq_retry(args.job_id)
            return

    if args.cmd == "config":
        if args.ccmd == "set":
            set_config(args.key, args.value)
        elif args.ccmd == "get":
            val = get_config(args.key)
            print(val)
        return

if __name__ == "__main__":
    main()
