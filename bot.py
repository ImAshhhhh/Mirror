# -*- coding: utf-8 -*-

import os
import json
import time
import shutil
import asyncio
import mimetypes

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# =========================================================
# CONFIG — everything from environment variables (GitHub secrets)
# =========================================================
# REQUIRED:
#   API_ID, API_HASH, SESSION_STRING
#   SOURCE_GROUP  -> the group/channel WITH the videos (-100xxxx or @username)
#   TARGET_GROUP  -> YOUR group where videos go      (-100xxxx or @username)
# OPTIONAL:
#   START_ID         default 1   -> start from the very first message
#   END_ID           default off -> go all the way to the newest message
#   SOURCE_TOPIC_ID  only copy from this forum topic
#   TARGET_TOPIC_ID  upload into this forum topic in the target
#   RATE_LIMIT       seconds between uploads (default 2)
#   MAX_DOWNLOAD_WORKERS parallel downloads (default 3)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
STRING_SESSION = os.environ["SESSION_STRING"]
SOURCE_GROUP = os.environ["SOURCE_GROUP"].strip()
TARGET_GROUP = os.environ["TARGET_GROUP"].strip()

START_ID = int(os.environ.get("START_ID", "1"))
END_ID = int(os.environ["END_ID"]) if os.environ.get("END_ID") else None
SOURCE_TOPIC_ID = int(os.environ["SOURCE_TOPIC_ID"]) if os.environ.get("SOURCE_TOPIC_ID") else None
TARGET_TOPIC_ID = int(os.environ["TARGET_TOPIC_ID"]) if os.environ.get("TARGET_TOPIC_ID") else None

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
RATE_LIMIT = float(os.environ.get("RATE_LIMIT", "2"))
MAX_DOWNLOAD_WORKERS = int(os.environ.get("MAX_DOWNLOAD_WORKERS", "3"))

TASK_FILE = "task.json"   # resume state — committed back to the repo by the workflow

# =========================================================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

upload_queue = asyncio.Queue()
current_task = None
stats = {"uploaded": 0, "queued": 0}
scan_cursor = {"id": 0}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def free_gb():
    return shutil.disk_usage(".").free / 1e9


def load_task():
    try:
        with open(TASK_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_task(task):
    tmp = TASK_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(task, f, indent=2)
    os.replace(tmp, TASK_FILE)   # atomic — safe even if the job is killed mid-write


def clear_task_file():
    try:
        os.remove(TASK_FILE)
    except FileNotFoundError:
        pass


# =========================================================
# RESOLVE CHATS — works on a fresh runner (accepts id or @username)
# =========================================================

async def resolve_entity(ref, label):
    ref = str(ref).strip()
    try:
        ref_int = int(ref)
    except ValueError:
        ref_int = None

    try:
        entity = await client.get_entity(ref)
        name = getattr(entity, "title", None) or getattr(entity, "username", None) or entity.id
        log(f"{label} resolved -> '{name}' (id: {entity.id})")
        return entity
    except Exception:
        pass

    if ref_int is not None:
        async for dialog in client.iter_dialogs():
            ent_id = getattr(dialog.entity, "id", None)
            try:
                marked = int(f"-100{ent_id}") if ent_id is not None else None
            except (TypeError, ValueError):
                marked = None
            if dialog.id == ref_int or ent_id == ref_int or marked == ref_int:
                log(f"{label} resolved via dialog scan -> '{dialog.name}'")
                return dialog.entity

    raise RuntimeError(
        f"{label} '{ref}' NOT FOUND. Is this account a member of that chat? "
        f"Run list_chats.py locally and copy the exact id."
    )


async def resolve_any(candidates, label):
    last_err = None
    for c in candidates:
        if c in (None, ""):
            continue
        try:
            return await resolve_entity(c, label)
        except Exception as e:
            last_err = e
    raise last_err or RuntimeError(f"{label} could not be resolved")


# =========================================================
# DOWNLOAD WORKER (same logic as your bot: parallel + queue)
# =========================================================

async def download_worker(message, destination, topic_id):
    try:
        # back off if disk is filling — finished uploads free space as they go
        while free_gb() < 1.5 and not upload_queue.empty():
            await asyncio.sleep(10)

        log(f"[DOWNLOAD] msg {message.id}")
        file_path = await message.download_media(file=DOWNLOAD_DIR)
        if not file_path:
            log(f"[SKIP] msg {message.id} — no downloadable media")
            return

        caption = (message.message or "")[:1024]
        stats["queued"] += 1
        await upload_queue.put({
            "file_path": file_path,
            "caption": caption,
            "destination": destination,
            "topic_id": topic_id,
            "msg_id": message.id,
        })
        log(f"[QUEUED] msg {message.id} -> {os.path.basename(file_path)}")
    except Exception as e:
        log(f"[DOWNLOAD ERROR] msg {message.id}: {e}")


# =========================================================
# UPLOAD WORKER (one at a time + rate limit + flood retry)
# =========================================================

async def upload_worker():
    while True:
        data = await upload_queue.get()
        try:
            file_path = data["file_path"]
            log(f"[UPLOAD] msg {data['msg_id']}")

            mime_type, _ = mimetypes.guess_type(file_path)
            supports_streaming = bool(mime_type and mime_type.startswith("video"))

            async def send_it():
                kwargs = {
                    "file": file_path,
                    "caption": data["caption"],
                    "supports_streaming": supports_streaming,
                    "force_document": False,
                    "allow_cache": False,
                    "part_size_kb": 512,
                }
                if data.get("topic_id"):
                    kwargs["reply_to"] = data["topic_id"]
                return await client.send_file(data["destination"], **kwargs)

            try:
                await send_it()
            except FloodWaitError as e:
                wait_time = int(e.seconds)
                log(f"[FLOOD WAIT] {wait_time}s — sleeping")
                await asyncio.sleep(wait_time)
                await send_it()

            stats["uploaded"] += 1
            log(f"[UPLOADED] msg {data['msg_id']} (total this run: {stats['uploaded']})")

            # RESUME POINT — this message is fully done
            if current_task is not None:
                recent = current_task.setdefault("done_recent", [])
                if data["msg_id"] not in recent:
                    recent.append(data["msg_id"])
                del recent[:-100]   # keep last 100 — small file, safe resume window
                current_task["last_done_id"] = max(
                    current_task.get("last_done_id", 0), data["msg_id"])
                save_task(current_task)

            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass

            await asyncio.sleep(RATE_LIMIT)

        except Exception as e:
            log(f"[UPLOAD ERROR] msg {data['msg_id']}: {e}")
        finally:
            upload_queue.task_done()


# =========================================================
# TASK EXECUTION
# =========================================================

def in_source_topic(message, topic_id):
    if not topic_id:
        return True
    reply = getattr(message, "reply_to", None)
    if not reply:
        return False
    top = getattr(reply, "reply_to_top_id", None) or getattr(reply, "reply_to_msg_id", None)
    return top == topic_id


async def execute_task(task):
    global current_task
    current_task = task
    save_task(task)

    source = await resolve_any([task.get("source_id"), task.get("source_ref")], "SOURCE")
    destination = await resolve_any(
        [task.get("destination_id"), task.get("destination_ref")], "TARGET")

    # remember resolved ids/names so the next run can resume by id
    task["source_id"] = source.id
    task["destination_id"] = destination.id
    task["destination_name"] = getattr(destination, "title", None) or str(destination.id)
    save_task(task)

    if source.id == destination.id:
        log("!!! WARNING: SOURCE and TARGET are the same chat — check your secrets !!!")

    done_ids = set(task.get("done_recent") or [])
    min_id = task.get("last_done_id") or (task.get("start_id", 1) - 1)
    if done_ids:
        min_id = min(min_id, min(done_ids) - 1)   # re-scan window in case a kill left stragglers

    log(f"Transfer: '{getattr(source, 'title', source.id)}' -> '{task['destination_name']}'")
    log(f"Continuing after msg id {min_id} | known-done ids: {len(done_ids)}")

    kwargs = {"min_id": min_id, "reverse": True}   # reverse=True = oldest -> newest
    if task.get("end_id"):
        kwargs["max_id"] = task["end_id"] + 1
    iterator = client.iter_messages(source, **kwargs)

    tasks = []
    semaphore = asyncio.Semaphore(MAX_DOWNLOAD_WORKERS)

    async for message in iterator:
        scan_cursor["id"] = message.id

        if message.id in done_ids:      # already uploaded in a previous run
            continue
        if not in_source_topic(message, task.get("source_topic_id")):
            continue
        if not message.media:           # same rule as your bot: media messages only
            continue

        size = getattr(getattr(message, "document", None), "size", 0) or 0
        if size > 13 * 1024 ** 3:       # runner only has ~14GB disk
            log(f"[SKIP] msg {message.id}: {size / 1e9:.1f}GB too big for runner disk")
            continue

        async def wrapped_download(msg):
            async with semaphore:
                await download_worker(msg, destination, task.get("target_topic_id"))

        tasks.append(asyncio.create_task(wrapped_download(message)))

    log(f"Scan finished — {len(tasks)} media found this pass. Draining downloads/uploads...")
    await asyncio.gather(*tasks)
    await upload_queue.join()

    clear_task_file()
    log("ALL DONE — task.json cleared, nothing left to resume")
    try:
        await client.send_message("me", "✅ **Mirror finished!**", parse_mode="md")
    except Exception:
        pass


# =========================================================
# HEARTBEAT — proves the run isn't frozen during long scans
# =========================================================

async def heartbeat():
    mins = 0
    while True:
        await asyncio.sleep(60)
        mins += 1
        log(f"[alive {mins}min | disk {free_gb():.1f}GB | done this run: {stats['uploaded']} "
            f"| in queue: {upload_queue.qsize()} | scanning at msg {scan_cursor['id']}]")


# =========================================================
# MAIN
# =========================================================

async def main():
    me = await client.get_me()
    full_name = " ".join(x for x in [me.first_name, me.last_name] if x)
    log("=" * 60)
    log(f"✅ Logged in as: {full_name} (@{me.username}) | ID: {me.id}")
    log(f"⚡ {MAX_DOWNLOAD_WORKERS} parallel downloads | ⏳ {RATE_LIMIT}s upload delay")
    log("=" * 60)

    task = load_task()
    if task:
        log(f"Found unfinished task in {TASK_FILE} — RESUMING it")
        log(f"   destination: {task.get('destination_name')} | last uploaded msg: {task.get('last_done_id')}")
    else:
        task = {
            "source_ref": SOURCE_GROUP,
            "destination_ref": TARGET_GROUP,
            "start_id": START_ID,
            "end_id": END_ID,
            "source_topic_id": SOURCE_TOPIC_ID,
            "target_topic_id": TARGET_TOPIC_ID,
            "last_done_id": START_ID - 1,
            "done_recent": [],
        }
        log(f"New task from env: SOURCE {SOURCE_GROUP} -> TARGET {TARGET_GROUP}")
        log(f"   from msg {START_ID} -> " + (f"{END_ID}" if END_ID else "newest (oldest first)"))

    asyncio.create_task(upload_worker())
    hb = asyncio.create_task(heartbeat())
    try:
        await execute_task(task)
    finally:
        hb.cancel()
        shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    await client.disconnect()


with client:
    client.loop.run_until_complete(main())
