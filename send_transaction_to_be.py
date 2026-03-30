import asyncio
import httpx
import json
import os
from datetime import datetime, timedelta
from typing import Dict
from models import Transaction  # Peewee model
from decouple import config

# File paths
LIVE_FILE = "live_queue.jsonl"
FAILED_FILE = "failed.jsonl"
FATAL_FILE = "fatal_queue.jsonl"

# Constants
RETRY_BASE_INTERVAL = 60  # seconds
MAX_RETRIES = 10
MAIN_CMS_BACKEND_URL = config(
    "MAIN_CMS_COMPLETED_TXN_URL",
    default="https://be.cms.ocpp.transev.site/users/deductcalculate",
)
SINGLE_SESSION_BACKEND_URL = config(
    "SINGLE_SESSION_COMPLETED_TXN_URL",
    default=None,
)
apiauthkey = config("APIAUTHKEY")

# Worker state
_worker_task = None
_shutdown_event = asyncio.Event()


# ==== FILE I/O ====
def append_jsonl(path: str, obj: Dict):
    with open(path, "a") as f:
        f.write(json.dumps(obj) + "\n")


def read_jsonl(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_jsonl(path: str, objects: list):
    with open(path, "w") as f:
        for obj in objects:
            f.write(json.dumps(obj) + "\n")


# ==== TASK INTERFACE ====
def add_to_queue(uuiddb: str):
    task = {
        "uuiddb": uuiddb,
        "retries": 0,
        "next_retry": datetime.now().isoformat()
    }
    current = read_jsonl(LIVE_FILE)
    if any(t["uuiddb"] == uuiddb for t in current):
        return
    append_jsonl(LIVE_FILE, task)
    print(f"[QUEUE 📥] Queued {uuiddb}")


def log_fatal(task: Dict, reason: str):
    task["fatal_reason"] = reason
    task["logged_at"] = datetime.now().isoformat()
    append_jsonl(FATAL_FILE, task)
    print(f"[☠️] Logged fatal task {task.get('uuiddb', 'unknown')} – {reason}")


def serialize_transaction(tx: Transaction) -> Dict:
    return {
        "sessionid": str(tx.transaction_id),
        "chargerid": tx.charger_id,
        "starttime": tx.start_time.isoformat(),
        "stoptime": tx.stop_time.isoformat() if tx.stop_time else None,
        "userid": tx.id_tag,
        "meterstart": str(tx.meter_start),
        "meterstop": str(tx.meter_stop),
        "consumedkwh": str(tx.total_consumption)
    }


# ==== RETRY HANDLER ====
async def try_post(uuiddb: str):
    try:
        tx = Transaction.get(Transaction.uuiddb == uuiddb)
        target_url = (
            SINGLE_SESSION_BACKEND_URL
            if tx.is_single_session
            else MAIN_CMS_BACKEND_URL
        )

        if not target_url:
            log_fatal({"uuiddb": uuiddb}, "Missing target completed-transaction URL")
            return "fatal"

        # Fatal: bad/missing data
        if tx.stop_time is None:
            log_fatal({"uuiddb": uuiddb}, "Missing stop_time")
            return "fatal"
        if tx.total_consumption is None or tx.total_consumption <= 0:
            log_fatal({"uuiddb": uuiddb}, f"Invalid kWh: {tx.total_consumption}")
            return "fatal"
        if tx.meter_start is None or tx.meter_stop is None:
            log_fatal({"uuiddb": uuiddb}, "Missing meter readings")
            return "fatal"

        try:
            data = serialize_transaction(tx)
        except Exception as e:
            log_fatal({"uuiddb": uuiddb}, f"Serialization error: {e}")
            return "fatal"

        headers = {"apiauthkey": apiauthkey}

        async with httpx.AsyncClient() as client:
            resp = await client.post(target_url, json=data, headers=headers, timeout=10)

        if resp.status_code >= 500:
            raise Exception(f"[Send Completed Transaction Callback Hook]:\n Server error: {resp.status_code}\n{resp.text}")
        elif resp.status_code >= 400:
            log_fatal({"uuiddb": uuiddb}, f"HTTP {resp.status_code}: {resp.text}")
            return "fatal"

        print(f"[✅] Sent {uuiddb}")
        return True

    except Exception as e:
        print(f"[❌] Retryable failure for {uuiddb}: {e}")
        return False


# ==== QUEUE LOOP ====
async def process_live_queue():
    while not _shutdown_event.is_set():
        live = read_jsonl(LIVE_FILE)
        now = datetime.now()
        next_queue = []

        for entry in live:
            retry_time = datetime.fromisoformat(entry["next_retry"])
            if now >= retry_time:
                result = await try_post(entry["uuiddb"])
                if result == True:
                    continue
                elif result == "fatal":
                    continue
                else:
                    entry["retries"] += 1
                    if entry["retries"] >= MAX_RETRIES:
                        append_jsonl(FAILED_FILE, entry)
                        print(f"[💀] Max retries exceeded for {entry['uuiddb']}")
                        continue
                    backoff = RETRY_BASE_INTERVAL * (2 ** (entry["retries"] - 1))
                    entry["next_retry"] = (now + timedelta(seconds=backoff)).isoformat()
            next_queue.append(entry)

        write_jsonl(LIVE_FILE, next_queue)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass


async def retry_failed_queue():
    while not _shutdown_event.is_set():
        failed = read_jsonl(FAILED_FILE)
        still_failed = []

        for entry in failed:
            result = await try_post(entry["uuiddb"])
            if result != True:
                still_failed.append(entry)

        write_jsonl(FAILED_FILE, still_failed)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass


# ==== LIFECYCLE ====
async def _worker_loop():
    print("[QUEUE 🔁] Background worker running...")
    await asyncio.gather(
        process_live_queue(),
        retry_failed_queue()
    )


def start_worker():
    global _worker_task
    if _worker_task is None:
        _worker_task = asyncio.create_task(_worker_loop())
        print("[QUEUE 🚀] Worker launched.")


async def shutdown_worker():
    print("[QUEUE 🛑] Shutting down...")
    _shutdown_event.set()
    if _worker_task:
        await _worker_task
    print("[QUEUE ✅] Clean shutdown complete.")
