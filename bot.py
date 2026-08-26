# -*- coding: utf-8 -*-

import os
import json
import time
import shutil
import asyncio
import mimetypes

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# =========================================================
# CONFIG — all from environment variables now
# =========================================================
# Required:  API_ID, API_HASH, SESSION_STRING
# Optional:  RATE_LIMIT (default 2), MAX_DOWNLOAD_WORKERS (default 5),
#            DOWNLOAD_DIR (default "downloads"),
#            IDLE_EXIT_MIN (auto-exit when nothing is running; 0 = never)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
STRING_SESSION = os.environ["SESSION_STRING"]

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
RATE_LIMIT = int(os.environ.get("RATE_LIMIT", "2"))
MAX_DOWNLOAD_WORKERS = int(os.environ.get("MAX_DOWNLOAD_WORKERS", "5"))
IDLE_EXIT_MIN = float(os.environ.get("IDLE_EXIT_MIN", "0"))

TASK_FILE = "task.json"   # resume state — committed back to the repo by the workflow

# =========================================================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# =========================================================
# GLOBALS
# =========================================================

task_running = False
user_state = {}
upload_queue = asyncio.Queue()
current_task = None   # mirror of TASK_FILE — this is what makes resume work


def log(msg):
    # timestamped + flushed -> shows LIVE in the GitHub Actions log
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


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
    os.replace(tmp, TASK_FILE)   # atomic — can't corrupt if the job is killed mid-write


def clear_task_file():
    try:
        os.remove(TASK_FILE)
    except FileNotFoundError:
        pass


def free_gb():
    return shutil.disk_usage(".").free / 1e9

# =========================================================
# PARSE LINK (unchanged logic)
# =========================================================

def parse_link(link):
    link = link.strip()
    if not link:
        raise Exception("Empty link")

    if link.startswith("-100"):
        chat, msg_part = link.split("/")
        if "-" in msg_part:
            start_id, end_id = map(int, msg_part.split("-"))
            return {"chat": int(chat), "range": True, "start_id": start_id, "end_id": end_id}
        else:
            return {"chat": int(chat), "range": False, "start_id": int(msg_part)}

    if link.endswith("/"):
        link = link[:-1]
    parts = link.split("/")

    if "/c/" in link:
        if len(parts) < 6:
            raise Exception("Invalid private link")
        group_id = parts[-2]
        msg_part = parts[-1]
        chat = int(f"-100{group_id}")
    else:
        if len(parts) < 5:
            raise Exception("Invalid Telegram link")
        chat = parts[-2]
        msg_part = parts[-1]

    if "-" in msg_part:
        start_id, end_id = map(int, msg_part.split("-"))
        return {"chat": chat, "range": True, "start_id": start_id, "end_id": end_id}
    else:
        return {"chat": chat, "range": False, "start_id": int(msg_part)}

# =========================================================
# RESOLVE ENTITY (works even on a fresh runner with empty cache)
# =========================================================

async def resolve_entity(chat):
    try:
        return await client.get_entity(chat)
    except Exception:
        pass
    async for dialog in client.iter_dialogs():
        try:
            ent_id = getattr(dialog.entity, "id", None)
            if (str(dialog.id) == str(chat)
                    or (ent_id is not None and (str(ent_id) == str(chat)
                        or f"-100{ent_id}" == str(chat)))):
                return dialog.entity
        except Exception:
            pass
    return None

# =========================================================
# DOWNLOAD WORKER
# =========================================================

async def download_worker(message, destination):
    try:
        # wait if disk is nearly full — finished uploads free space as they go
        while free_gb() < 1.5 and not upload_queue.empty():
            await asyncio.sleep(10)

        log(f"[DOWNLOAD] msg {message.id}")
        file_path = await message.download_media(file=DOWNLOAD_DIR)
        if not file_path:
            log(f"[SKIP] msg {message.id} — no media")
            return
        caption = (message.message or "")[:1024]
        await upload_queue.put({
            "file_path": file_path,
            "caption": caption,
            "destination": destination,
            "msg_id": message.id,   # needed so resume knows how far we got
        })
        log(f"[QUEUED] msg {message.id} -> {os.path.basename(file_path)}")
    except Exception as e:
        log(f"[DOWNLOAD ERROR] {e}")

# =========================================================
# UPLOAD WORKER
# =========================================================

async def upload_worker():
    while True:
        data = await upload_queue.get()
        try:
            file_path = data["file_path"]
            caption = data["caption"]
            destination = data["destination"]
            log(f"[UPLOAD] msg {data['msg_id']}")

            mime_type, _ = mimetypes.guess_type(file_path)
            supports_streaming = bool(mime_type and mime_type.startswith("video"))
            # NOTE: removed the fake DocumentAttributeVideo(duration=0, w=1280, h=720)
            # — that wrote wrong metadata. Let Telegram detect real duration/size itself.

            async def send_it():
                return await client.send_file(
                    destination,
                    file=file_path,
                    caption=caption,
                    supports_streaming=supports_streaming,
                    force_document=False,
                    allow_cache=False,
                    part_size_kb=512,
                )

            try:
                await send_it()
            except FloodWaitError as e:
                wait_time = int(e.seconds)
                log(f"[FLOOD WAIT] {wait_time}s")
                await asyncio.sleep(wait_time)
                await send_it()

            log(f"[UPLOADED] msg {data['msg_id']}")

            # RESUME POINT — remember this message is fully done
            if current_task is not None:
                recent = current_task.setdefault("done_recent", [])
                if data["msg_id"] not in recent:
                    recent.append(data["msg_id"])
                del recent[:-50]  # keep only the last 50 (small file, safe resume window)
                current_task["last_done_id"] = max(current_task.get("last_done_id", 0),
                                                   data["msg_id"])
                save_task(current_task)

            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass

            await asyncio.sleep(RATE_LIMIT)

        except Exception as e:
            log(f"[UPLOAD ERROR] {e}")
        finally:
            upload_queue.task_done()

# =========================================================
# TASK RUNNER (shared by interactive start AND auto-resume)
# =========================================================

async def run_task(entity, chat_ref, destination, dest_name,
                   start_id, range_mode, end_id, topic_mode, topic_id,
                   resume_min_id=None, done_ids=None):
    global task_running, current_task

    task_running = True
    done_ids = done_ids or set()
    min_id = resume_min_id if resume_min_id is not None else start_id - 1

    current_task = {
        "chat": str(chat_ref),
        "chat_id": entity.id,
        "destination": destination,
        "dest_name": dest_name,
        "start_id": start_id,
        "end_id": end_id,
        "range_mode": range_mode,
        "topic_mode": topic_mode,
        "topic_id": topic_id,
        "last_done_id": min_id,
        "done_recent": [],
    }
    save_task(current_task)

    text = f"🚀 **Task started**\n📤 `{dest_name}`\n"
    if range_mode:
        text += f"🔢 `{start_id}` → `{end_id}`\n"
    else:
        text += f"🔢 From `{start_id}` → Latest\n"
    if resume_min_id is not None:
        text += f"♻️ Resumed from msg `{resume_min_id}`\n"
    if topic_mode:
        text += f"🧵 Topic: `{topic_id}`\n"
    try:
        await client.send_message("me", text, parse_mode="md")
    except Exception:
        pass

    if range_mode:
        iterator = client.iter_messages(entity, min_id=min_id, max_id=end_id + 1, reverse=True)
    else:
        iterator = client.iter_messages(entity, min_id=min_id, reverse=True)

    tasks = []
    semaphore = asyncio.Semaphore(MAX_DOWNLOAD_WORKERS)

    async for message in iterator:
        if not task_running:
            break

        if message.id in done_ids:   # already uploaded before the last restart
            continue

        if topic_mode:
            try:
                if not message.reply_to:
                    continue
                if message.reply_to.reply_to_top_id != topic_id:
                    continue
            except Exception:
                continue

        if not message.media:
            continue

        async def wrapped_download(msg):
            async with semaphore:
                await download_worker(msg, destination)

        tasks.append(asyncio.create_task(wrapped_download(message)))

    await asyncio.gather(*tasks)
    await upload_queue.join()

    task_running = False
    clear_task_file()   # finished (or /stop) — nothing to resume next time
    log("TASK FINISHED — task.json cleared")
    try:
        await client.send_message("me", "✅ **Done!**", parse_mode="md")
    except Exception:
        pass

# =========================================================
# IDLE WATCHDOG — exit cleanly when nothing is happening
# (stops Actions runs from sitting zombie for 6 hours)
# =========================================================

async def idle_watchdog():
    if not IDLE_EXIT_MIN:
        return
    idle_since = time.time()
    while True:
        await asyncio.sleep(60)
        busy = task_running or (not upload_queue.empty()) or bool(user_state)
        if busy:
            idle_since = time.time()
            continue
        if time.time() - idle_since > IDLE_EXIT_MIN * 60:
            log(f"Idle for {IDLE_EXIT_MIN:.0f} min with no active task — exiting cleanly")
            await client.disconnect()
            return

# =========================================================
# COMMANDS
# =========================================================

@client.on(events.NewMessage(pattern=r"^/start$"))
async def start(event):
    me = await client.get_me()
    if event.chat_id != me.id:
        return
    await event.reply(
        "👋 **Telegram Mirror**\n\n"
        "Send Telegram link:\n"
        "`https://t.me/c/123/100`\n"
        "`https://t.me/c/123/100-200`\n"
        "`-100123/100`\n\n"
        f"⚡ {MAX_DOWNLOAD_WORKERS} parallel downloads\n"
        f"⏳ {RATE_LIMIT}s upload delay",
        parse_mode="md")


@client.on(events.NewMessage(pattern=r"^/stop$"))
async def stop(event):
    global task_running
    me = await client.get_me()
    if event.chat_id != me.id:
        return
    task_running = False
    log("Received /stop — draining queue, then stopping")
    await event.reply("⏹ Stopped (queued uploads will finish)", parse_mode="md")

# =========================================================
# HELPER: Send long message in chunks
# =========================================================

async def send_long_message(event, text, max_len=4000):
    if len(text) <= max_len:
        await event.reply(text, parse_mode="md")
        return

    lines = text.split("\n")
    chunks = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 <= max_len:
            current += line + "\n"
        else:
            chunks.append(current.strip())
            current = line + "\n"
    if current:
        chunks.append(current.strip())

    for chunk in chunks:
        await event.reply(chunk, parse_mode="md")
        await asyncio.sleep(0.5)

# =========================================================
# HANDLER
# =========================================================

@client.on(events.NewMessage)
async def handler(event):
    global task_running

    me = await client.get_me()
    if event.chat_id != me.id:
        return
    if (event.raw_text or "").startswith("/"):
        return

    user_id = event.sender_id

    if user_id not in user_state:
        link = (event.raw_text or "").strip()
        try:
            parsed = parse_link(link)

            entity = await resolve_entity(parsed["chat"])
            if not entity:
                return await event.reply(
                    "❌ Cannot access chat.\nCheck: joined? opened? link valid?",
                    parse_mode="md")

            parsed["entity"] = entity

            topic_mode = False
            topic_id = None
            try:
                msg = await client.get_messages(entity, ids=parsed["start_id"])
                if msg and getattr(msg, "reply_to", None) and getattr(msg.reply_to, "reply_to_top_id", None):
                    topic_mode = True
                    topic_id = msg.reply_to.reply_to_top_id
                    log(f"[TOPIC MODE] {topic_id}")
            except Exception as e:
                log(f"[TOPIC DETECT] {e}")

            parsed["topic_mode"] = topic_mode
            parsed["topic_id"] = topic_id
            user_state[user_id] = parsed

            dialogs = []
            text = "📤 **Choose destination (send NUMBER)**\n\n"
            index = 1
            async for dialog in client.iter_dialogs():
                try:
                    ent = dialog.entity
                    if getattr(ent, "megagroup", False) or getattr(ent, "broadcast", False):
                        dialogs.append(dialog)
                        text += f"{index}. {dialog.name}\n"
                        index += 1
                except Exception:
                    pass

            if not dialogs:
                await event.reply("❌ No groups/channels to send to!")
                return

            user_state[user_id]["dialogs"] = dialogs
            await send_long_message(event, text)

        except Exception as e:
            await event.reply(f"❌ Error: `{str(e)}`", parse_mode="md")
        return

    data = user_state[user_id]
    choice = (event.raw_text or "").strip()

    if "/" in choice or "t.me/" in choice or choice.startswith("-100"):
        del user_state[user_id]
        return await handler(event)

    if not choice.isdigit():
        return await event.reply("❌ Send a valid NUMBER")

    choice = int(choice)
    dialogs = data["dialogs"]
    if choice < 1 or choice > len(dialogs):
        return await event.reply("❌ Invalid choice")

    selected_dialog = dialogs[choice - 1]
    user_state.pop(user_id, None)

    await run_task(
        entity=data["entity"],
        chat_ref=data["chat"],
        destination=selected_dialog.id,
        dest_name=selected_dialog.name,
        start_id=data["start_id"],
        range_mode=data["range"],
        end_id=data.get("end_id"),
        topic_mode=data["topic_mode"],
        topic_id=data["topic_id"],
    )

# =========================================================
# MAIN
# =========================================================

async def main():
    me = await client.get_me()
    full_name = " ".join(x for x in [me.first_name, me.last_name] if x)
    log("=" * 60)
    log(f"✅ Logged in as: {full_name} (@{me.username}) | ID: {me.id}")
    log(f"⚡ {MAX_DOWNLOAD_WORKERS} parallel downloads | ⏳ {RATE_LIMIT}s upload delay")
    log("🚀 Send a link in Saved Messages to start a new task")
    log("=" * 60)

    asyncio.create_task(upload_worker())
    asyncio.create_task(idle_watchdog())

    # AUTO-RESUME: if a previous run was killed mid-task, continue it
    task = load_task()
    if task:
        log(f"Found unfinished task in {TASK_FILE} — AUTO-RESUMING")
        log(f"  destination: {task.get('dest_name')} | last uploaded msg: {task.get('last_done_id')}")
        entity = await resolve_entity(task.get("chat_id") or task.get("chat"))
        if entity is None:
            log("!! Could not resolve source chat from task.json — send a new link to start over")
            clear_task_file()
        else:
            done_recent = task.get("done_recent") or []
            if done_recent:
                resume_min = min(done_recent) - 1
            else:
                resume_min = task.get("last_done_id") or (task.get("start_id", 1) - 1)
            await run_task(
                entity=entity,
                chat_ref=task.get("chat"),
                destination=task["destination"],
                dest_name=task.get("dest_name", str(task["destination"])),
                start_id=task.get("start_id", resume_min + 1),
                range_mode=task.get("range_mode", False),
                end_id=task.get("end_id"),
                topic_mode=task.get("topic_mode", False),
                topic_id=task.get("topic_id"),
                resume_min_id=resume_min,
                done_ids=set(done_recent),
            )


with client:
    client.loop.run_until_complete(main())
    client.run_until_disconnected()
