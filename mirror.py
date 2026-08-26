import asyncio
import json
import os
import shutil
import time

from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
SOURCE_GROUP = os.environ["SOURCE_GROUP"]
TARGET_GROUP = os.environ["TARGET_GROUP"]

TIME_BUDGET_MIN = float(os.environ.get("TIME_BUDGET_MIN", "240"))
MAX_VIDEOS_PER_RUN = int(os.environ.get("MAX_VIDEOS_PER_RUN", "0"))
DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "1"))
MIN_FREE_GB = float(os.environ.get("MIN_FREE_GB", "2"))

PROGRESS_FILE = "progress.json"
DOWNLOAD_DIR = "downloads"
MB = 1024 * 1024


def log(msg):
    # timestamped + flush=True so it appears in the GitHub Actions live log instantly
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"last_id": 0, "total_done": 0, "failed": []}


def save_progress(p):
    with open(PROGRESS_FILE + ".tmp", "w") as f:
        json.dump(p, f, indent=2)
    os.replace(PROGRESS_FILE + ".tmp", PROGRESS_FILE)


def free_gb():
    return shutil.disk_usage(".").free / 1e9


def is_video(msg):
    if msg.video:
        return True
    if msg.document:
        return any(isinstance(a, DocumentAttributeVideo) for a in msg.document.attributes)
    return False


def make_progress(label):
    state = {"last": 0}
    def cb(current, total):
        if not total:
            return
        step = max(total // 10, 50 * MB)  # print every 10% or every 50MB
        if current - state["last"] >= step or current >= total:
            state["last"] = current
            log(f"      {label}: {current/MB:.0f}/{total/MB:.0f} MB")
    return cb


async def heartbeat():
    mins = 0
    while True:
        await asyncio.sleep(60)
        mins += 1
        log(f"[alive {mins}min | disk {free_gb():.1f}GB free | total done so far: OK]")


async def resolve_chat(client, value, label):
    value = value.strip()
    log(f"Resolving {label} chat: {value}")
    try:
        value = int(value)
    except ValueError:
        entity = await client.get_entity(value)
        log(f"{label} resolved -> '{getattr(entity, 'title', entity.id)}'")
        return entity

    try:
        entity = await client.get_entity(value)
    except ValueError:
        log(f"{label} id not in session cache, scanning all dialogs (can take a minute)...")
        entity = None
        async for d in client.iter_dialogs():
            if d.id == value or getattr(d.entity, "id", None) == value:
                entity = d.entity
                break
        if entity is None:
            raise RuntimeError(
                f"{label} chat {value} NOT FOUND. Is this account a member of it? "
                f"Run list_chats.py and copy the EXACT id from there."
            )
    log(f"{label} resolved -> '{getattr(entity, 'title', entity.id)}' (id: {entity.id})")
    return entity


async def transfer(client, msg, dst):
    size = getattr(getattr(msg, "document", None), "size", 0) or 0
    log(f"  msg {msg.id}: DOWNLOADING video ({size/MB:.1f} MB)...")
    path = await msg.download_media(file=DOWNLOAD_DIR, progress_callback=make_progress("dl"))
    if not path:
        raise RuntimeError("download_media returned nothing")

    log(f"  msg {msg.id}: UPLOADING to target group...")
    kwargs = {"caption": msg.message or "", "supports_streaming": True,
              "parse_mode": None, "progress_callback": make_progress("ul")}
    try:
        await client.send_file(dst, path, formatting_entities=msg.entities, **kwargs)
    except TypeError:
        await client.send_file(dst, path, **kwargs)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


async def main():
    log("=" * 60)
    log("TELEGRAM VIDEO MIRROR — starting")
    log(f"Time budget: {TIME_BUDGET_MIN} min | delay: {DELAY_SECONDS}s | "
        f"video cap: {MAX_VIDEOS_PER_RUN or 'unlimited'}")
    log("=" * 60)

    progress = load_progress()
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    log("Connecting to Telegram...")
    async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
        me = await client.get_me()
        full_name = " ".join(x for x in [me.first_name, me.last_name] if x)
        uname = f"@{me.username}" if me.username else "(no username)"
        phone = f"+{me.phone}" if me.phone else "(phone hidden)"
        log(f"Logged in as: {full_name} ({uname}) | ID: {me.id} | {phone}")

        src = await resolve_chat(client, SOURCE_GROUP, "SOURCE")
        dst = await resolve_chat(client, TARGET_GROUP, "TARGET")

        if src.id == dst.id:
            log("!!! WARNING: source and target are THE SAME CHAT — check your secrets !!!")

        total_messages = await client.get_messages(src, limit=0)
        log(f"Source chat contains ~{total_messages.total} messages total")

        log(f"Resuming after message id {progress['last_id']} | videos done so far: {progress['total_done']}")
        log("Scanning history oldest -> newest... (first video can take a while on big chats)")

        hb = asyncio.create_task(heartbeat())
        deadline = time.time() + TIME_BUDGET_MIN * 60
        done_this_run = 0
        skipped = 0

        try:
            async for msg in client.iter_messages(src, reverse=True, min_id=progress["last_id"]):
                if not is_video(msg):
                    progress["last_id"] = msg.id
                    skipped += 1
                    if skipped % 500 == 0:
                        log(f"  ...skipped {skipped} non-video messages so far (at msg {msg.id})")
                        save_progress(progress)
                    continue

                if time.time() > deadline:
                    log("Time budget reached — stopping. Next run continues from here.")
                    break
                if MAX_VIDEOS_PER_RUN and done_this_run >= MAX_VIDEOS_PER_RUN:
                    log("Video cap for this run reached — stopping.")
                    break
                if free_gb() < MIN_FREE_GB:
                    log("Runner disk almost full — stopping.")
                    break

                log(f"--- Video #{progress['total_done'] + 1} (msg {msg.id}) ---")
                try:
                    await transfer(client, msg, dst)
                    done_this_run += 1
                    progress["total_done"] += 1
                    log(f"[{progress['total_done']}] msg {msg.id} -> UPLOADED SUCCESSFULLY")
                except errors.FloodWaitError as fw:
                    wait = min(int(fw.seconds) + 5, 3600)
                    log(f"Telegram flood wait {fw.seconds}s -> sleeping {wait}s")
                    save_progress(progress)
                    await asyncio.sleep(wait)
                    try:
                        await transfer(client, msg, dst)
                        done_this_run += 1
                        progress["total_done"] += 1
                        log(f"[{progress['total_done']}] msg {msg.id} -> UPLOADED (after flood wait)")
                    except Exception as e:
                        log(f"msg {msg.id} FAILED after flood retry: {e}")
                        progress["failed"].append(msg.id)
                except Exception as e:
                    log(f"msg {msg.id} FAILED ({type(e).__name__}): {e}")
                    progress["failed"].append(msg.id)

                progress["last_id"] = msg.id
                save_progress(progress)
                await asyncio.sleep(DELAY_SECONDS)
        finally:
            hb.cancel()
            save_progress(progress)

        shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
        log("=" * 60)
        log(f"RUN SUMMARY: {done_this_run} uploaded this run | {progress['total_done']} total | "
            f"last_id={progress['last_id']} | failed={len(progress['failed'])}")
        if progress["failed"]:
            log(f"Failed message ids: {progress['failed'][-20:]}")
        log("Done.")
        log("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

