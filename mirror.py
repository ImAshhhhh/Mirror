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
SOURCE_GROUP = os.environ["SOURCE_GROUP"]   # e.g. -1001234567890
TARGET_GROUP = os.environ["TARGET_GROUP"]

# knobs (can override from the workflow)
TIME_BUDGET_MIN = float(os.environ.get("TIME_BUDGET_MIN", "240"))   # stop before GH kills job at 6h
MAX_VIDEOS_PER_RUN = int(os.environ.get("MAX_VIDEOS_PER_RUN", "0")) # 0 = no cap
DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "1"))
MIN_FREE_GB = float(os.environ.get("MIN_FREE_GB", "2"))

PROGRESS_FILE = "progress.json"
DOWNLOAD_DIR = "downloads"


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"last_id": 0, "total_done": 0, "failed": []}


def save_progress(p):
    # atomic write so a killed job can't corrupt it
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


async def resolve_chat(client, value):
    """Accepts @username or numeric id; falls back to scanning your dialogs."""
    value = value.strip()
    try:
        value = int(value)
    except ValueError:
        return await client.get_entity(value)

    try:
        return await client.get_entity(value)
    except ValueError:
        pass
    async for d in client.iter_dialogs():
        if d.id == value or getattr(d.entity, "id", None) == value:
            return d.entity
    raise RuntimeError(f"Chat not found: {value} — run list_chats.py to get the right id")


async def transfer(client, msg, dst):
    path = await msg.download_media(file=DOWNLOAD_DIR)
    if not path:
        raise RuntimeError("download_media returned nothing")

    kwargs = {"caption": msg.message or "", "supports_streaming": True, "parse_mode": None}
    try:
        # formatting_entities keeps bold/links/etc. from the original caption
        await client.send_file(dst, path, formatting_entities=msg.entities, **kwargs)
    except TypeError:  # older telethon without formatting_entities
        await client.send_file(dst, path, **kwargs)
    finally:
        try:
            os.remove(path)  # free disk immediately
        except OSError:
            pass


async def main():
    progress = load_progress()
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
        src = await resolve_chat(client, SOURCE_GROUP)
        dst = await resolve_chat(client, TARGET_GROUP)

        print(f"Resuming after message id {progress['last_id']} | done so far: {progress['total_done']}")

        deadline = time.time() + TIME_BUDGET_MIN * 60
        done_this_run = 0

        # reverse=True -> oldest to newest, min_id -> skip everything already done
        async for msg in client.iter_messages(src, reverse=True, min_id=progress["last_id"]):
            if not is_video(msg):
                progress["last_id"] = msg.id  # pass over non-video msgs but remember them
                continue

            if time.time() > deadline:
                print("Time budget reached — stopping, next run continues from here.")
                break
            if MAX_VIDEOS_PER_RUN and done_this_run >= MAX_VIDEOS_PER_RUN:
                print("Video cap for this run reached — stopping.")
                break
            if free_gb() < MIN_FREE_GB:
                print("Runner disk almost full — stopping.")
                break

            try:
                await transfer(client, msg, dst)
                done_this_run += 1
                progress["total_done"] += 1
                print(f"[{progress['total_done']}] msg {msg.id} -> uploaded")
            except errors.FloodWaitError as fw:
                wait = min(int(fw.seconds) + 5, 3600)
                print(f"Telegram flood wait {fw.seconds}s -> sleeping {wait}s")
                save_progress(progress)
                await asyncio.sleep(wait)
                try:
                    await transfer(client, msg, dst)
                    done_this_run += 1
                    progress["total_done"] += 1
                    print(f"[{progress['total_done']}] msg {msg.id} -> uploaded (after wait)")
                except Exception as e:
                    print(f"msg {msg.id} failed after flood retry: {e}")
                    progress["failed"].append(msg.id)
            except Exception as e:
                # don't let one broken video block 90k others
                print(f"msg {msg.id} FAILED ({type(e).__name__}): {e}")
                progress["failed"].append(msg.id)

            progress["last_id"] = msg.id
            save_progress(progress)
            await asyncio.sleep(DELAY_SECONDS)

        save_progress(progress)
        shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
        print(f"RUN SUMMARY: {done_this_run} this run | {progress['total_done']} total | "
              f"last_id={progress['last_id']} | failed={len(progress['failed'])}")


if __name__ == "__main__":
    asyncio.run(main())
