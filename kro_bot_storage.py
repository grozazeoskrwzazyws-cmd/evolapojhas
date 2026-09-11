# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import gzip
import base64
import random
import string
import sqlite3
import threading
import tempfile
from datetime import datetime, timedelta

import telebot
from telebot import types
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    FloodWaitError,
)

try:
    import fcntl
except ImportError:
    fcntl = None


# ============================================================
# KRO Telegram Bot V2
# ============================================================
# Environment:
#   BOT_TOKEN
#   STORAGE_CHAT_ID
#   API_ID
#   API_HASH
#
# Storage format:
#   KRO_DB
#   VERSION=2
#   RECORD_ID=<unique id>
#   TYPE=<record type>
#   CREATED_AT=<original record time>
#
# Recovery:
#   - Uses local SQLite as fast cache.
#   - Storage Group is the recovery source.
#   - Links/albums keep their original CREATED_AT.
#   - Albums are stored as ONE MEDIA_GROUP record.
#   - RECORD_ID makes queued records idempotent.
#   - Snapshot records can speed up future recovery.
#
# Security:
#   - Never store BOT_TOKEN/API_HASH/Telethon StringSession in Storage.
#   - Recovery user session stays in local SQLite only.
#   - TELETHON_ADMIN_ID is the only user allowed to add Recovery number.
#
# IMPORTANT FOR RAILWAY:
#   Set service Replicas = 1. The local process lock cannot protect
#   two different containers from polling the same BOT_TOKEN.
# ============================================================


BOT_TOKEN = os.getenv("BOT_TOKEN", "")
STORAGE_CHAT_ID_RAW = os.getenv("STORAGE_CHAT_ID", "0")
API_ID_RAW = os.getenv("API_ID", "0")
API_HASH = os.getenv("API_HASH", "")

try:
    STORAGE_CHAT_ID = int(STORAGE_CHAT_ID_RAW)
except ValueError:
    STORAGE_CHAT_ID = 0

try:
    API_ID = int(API_ID_RAW)
except ValueError:
    API_ID = 0


OWNERS = [
    8223922043,
    7247497156,
    913352843,
]

TELETHON_ADMIN_ID = 913352843

DATABASE_FILE = "database.db"
PROCESS_LOCK_FILE = "kro_bot.lock"

STORAGE_VERSION = 2

CONTENT_DELETE_SECONDS = 10
PREVENT_DUPLICATE_FILE_IDS = True

STORAGE_QUEUE_MAX = 5000
STORAGE_QUEUE_MAX_ATTEMPTS = 8
STORAGE_RETRY_BASE = 30
STORAGE_RETRY_MAX = 3600
STORAGE_ALERT_AFTER = 5

RECOVERY_PROGRESS_EVERY = 1000

SNAPSHOT_ENABLED = True
SNAPSHOT_INTERVAL_SECONDS = 6 * 60 * 60
SNAPSHOT_CHUNK_SIZE = 200


if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is missing.")

if not STORAGE_CHAT_ID:
    raise SystemExit("STORAGE_CHAT_ID is missing or invalid.")

if not API_ID or not API_HASH:
    raise SystemExit("API_ID and API_HASH are required for Recovery.")


# ============================================================
# Process lock
# ============================================================

_process_lock_handle = None


def acquire_process_lock():
    global _process_lock_handle

    if fcntl is None:
        print("WARNING: fcntl unavailable; process lock disabled.")
        return True

    try:
        _process_lock_handle = open(PROCESS_LOCK_FILE, "w")
        fcntl.flock(
            _process_lock_handle.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )
        _process_lock_handle.write(str(os.getpid()))
        _process_lock_handle.flush()
        return True

    except BlockingIOError:
        print(
            "FATAL: Another KRO bot process is already running "
            "with this database."
        )
        return False

    except Exception as e:
        print("PROCESS LOCK ERROR:", repr(e))
        return False


# ============================================================
# Bot
# ============================================================

bot = telebot.TeleBot(BOT_TOKEN)

ME = bot.get_me()
BOT_ID = ME.id
BOT_USERNAME = ME.username or ""


# ============================================================
# SQLite
# ============================================================

conn = sqlite3.connect(
    DATABASE_FILE,
    check_same_thread=False,
    timeout=30,
)

db_lock = threading.RLock()


def db_execute(query, params=(), fetchone=False, fetchall=False):
    with db_lock:
        cur = conn.cursor()
        cur.execute(query, params)

        if fetchone:
            return cur.fetchone()

        if fetchall:
            return cur.fetchall()

        conn.commit()
        return None


def initialize_database():
    with db_lock:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS links(
            code TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            content TEXT NOT NULL,
            caption TEXT DEFAULT '',
            creator_id INTEGER,
            creator_username TEXT,
            creator_name TEXT,
            created_at TEXT NOT NULL,
            deleted INTEGER DEFAULT 0
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            date_join TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS start_msg(
            id INTEGER PRIMARY KEY CHECK(id = 1),
            type TEXT,
            content TEXT,
            caption TEXT DEFAULT ''
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS admins(
            user_id INTEGER PRIMARY KEY,
            added_by INTEGER,
            date_added TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS banned_users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            reason TEXT,
            banned_by INTEGER,
            date_banned TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS system_state(
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS storage_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id TEXT UNIQUE NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            last_error TEXT DEFAULT '',
            next_attempt_at TEXT NOT NULL,
            status TEXT DEFAULT 'pending'
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS review_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            last_error TEXT DEFAULT '',
            next_attempt_at TEXT NOT NULL,
            status TEXT DEFAULT 'pending'
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS file_index(
            file_id TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            media_type TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots(
            snapshot_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            last_storage_message_id INTEGER DEFAULT 0,
            status TEXT DEFAULT 'created'
        )
        """)

        # Backward-compatible migrations.
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(links)").fetchall()
        }

        for column, definition in [
            ("creator_id", "INTEGER"),
            ("creator_username", "TEXT"),
            ("creator_name", "TEXT"),
            ("deleted", "INTEGER DEFAULT 0"),
        ]:
            if column not in existing:
                conn.execute(
                    f"ALTER TABLE links ADD COLUMN {column} {definition}"
                )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_links_file ON links(content)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_links_created ON links(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_banned ON banned_users(user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_storage_queue_retry "
            "ON storage_queue(status,next_attempt_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_review_queue_retry "
            "ON review_queue(status,next_attempt_at)"
        )

        conn.commit()


initialize_database()


# ============================================================
# Runtime state
# ============================================================

admin_steps = {}
broadcast_data = {}

pending_start = {}
admins_seen_start = set()

media_groups = {}
media_group_timers = {}
media_group_lock = threading.RLock()

recovery_lock = threading.Lock()

telethon_login_sessions = {}

storage_failure_count = 0
storage_alert_lock = threading.Lock()

last_snapshot_time = 0


# ============================================================
# General helpers
# ============================================================

def now():
    return datetime.now().isoformat(timespec="seconds")


def utc_now():
    return datetime.utcnow().isoformat(timespec="seconds")


def generate_id(prefix="01"):
    """
    Sortable-enough unique local record ID.
    Does not require an external ULID package.
    """
    timestamp = int(time.time() * 1000)
    random_part = "".join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(10)
    )
    return f"{prefix}{timestamp:X}{random_part}"


def generate_code(length=8):
    chars = string.ascii_letters + string.digits

    while True:
        code = "".join(random.choice(chars) for _ in range(length))

        if not db_execute(
            "SELECT 1 FROM links WHERE code=?",
            (code,),
            fetchone=True,
        ):
            return code


def encode_text(value):
    return json.dumps(
        value if value is not None else "",
        ensure_ascii=False,
    )


def decode_text(value):
    try:
        return json.loads(value)
    except Exception:
        return ""


def delete_after(chat_id, message_ids, delay=CONTENT_DELETE_SECONDS):
    def job():
        time.sleep(delay)

        for message_id in message_ids:
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass

    threading.Thread(target=job, daemon=True).start()


# ============================================================
# System state
# ============================================================

def get_state(key):
    row = db_execute(
        "SELECT value FROM system_state WHERE key=?",
        (key,),
        fetchone=True,
    )
    return row[0] if row else None


def set_state(key, value):
    db_execute(
        """
        INSERT OR REPLACE INTO system_state(key,value)
        VALUES (?,?)
        """,
        (key, str(value)),
    )


def get_telethon_session_string():
    # NEVER send this to Storage.
    return get_state("telethon_session_string") or ""


def set_telethon_session_string(value):
    set_state("telethon_session_string", value)


# ============================================================
# Permissions / Users / Bans
# ============================================================

def is_owner(user_id):
    return user_id in OWNERS


def is_admin(user_id):
    if is_owner(user_id):
        return True

    return bool(
        db_execute(
            "SELECT 1 FROM admins WHERE user_id=?",
            (user_id,),
            fetchone=True,
        )
    )


def is_banned(user_id):
    return bool(
        db_execute(
            "SELECT 1 FROM banned_users WHERE user_id=?",
            (user_id,),
            fetchone=True,
        )
    )


def ensure_user(user):
    row = db_execute(
        "SELECT 1 FROM users WHERE user_id=?",
        (user.id,),
        fetchone=True,
    )

    if row:
        db_execute(
            """
            UPDATE users
            SET username=?, first_name=?
            WHERE user_id=?
            """,
            (
                user.username or "",
                user.first_name or "",
                user.id,
            ),
        )
        return False

    joined = now()

    db_execute(
        """
        INSERT INTO users
        (user_id,username,first_name,date_join)
        VALUES (?,?,?,?)
        """,
        (
            user.id,
            user.username or "",
            user.first_name or "",
            joined,
        ),
    )

    # V2 still writes individual users for exact incremental recovery.
    # USER_SNAPSHOT can later compact the historical data.
    storage_record(
        "user",
        USER_ID=user.id,
        USERNAME=user.username or "",
        FIRST_NAME=user.first_name or "",
        DATE_JOIN=joined,
    )

    return True


def guard_message(message):
    ensure_user(message.from_user)

    if is_banned(message.from_user.id):
        try:
            bot.send_message(
                message.chat.id,
                "أنت محظور من استخدام البوت.",
            )
        except Exception:
            pass

        return True

    return False


def ban_user(user_id, username, reason, banned_by):
    date_banned = now()

    db_execute(
        """
        INSERT OR REPLACE INTO banned_users
        (user_id,username,reason,banned_by,date_banned)
        VALUES (?,?,?,?,?)
        """,
        (
            user_id,
            username or "",
            reason or "مخالفة",
            banned_by,
            date_banned,
        ),
    )

    storage_record(
        "ban",
        USER_ID=user_id,
        USERNAME=username or "",
        REASON=reason or "مخالفة",
        BANNED_BY=banned_by,
        DATE_BANNED=date_banned,
    )


def unban_user(user_id, unbanned_by):
    db_execute(
        "DELETE FROM banned_users WHERE user_id=?",
        (user_id,),
    )

    storage_record(
        "unban",
        USER_ID=user_id,
        UNBANNED_BY=unbanned_by,
        DATE=now(),
    )


# ============================================================
# Storage V2
# ============================================================

def storage_send(text):
    global storage_failure_count

    try:
        msg = bot.send_message(
            STORAGE_CHAT_ID,
            text,
            disable_notification=True,
        )

        with storage_alert_lock:
            storage_failure_count = 0

        return msg

    except Exception as e:
        print("STORAGE WRITE ERROR:", repr(e))

        with storage_alert_lock:
            storage_failure_count += 1
            failures = storage_failure_count

        if failures == STORAGE_ALERT_AFTER:
            notify_storage_failure(failures, e)

        return None


def notify_storage_failure(failures, error):
    for owner_id in OWNERS:
        try:
            bot.send_message(
                owner_id,
                "⚠ تنبيه Storage\n\n"
                f"عدد محاولات الفشل المتتالية: {failures}\n"
                f"الخطأ: {str(error)[:500]}",
            )
        except Exception:
            pass


def queue_storage_payload(
    record_id,
    payload,
    error="",
):
    """
    Idempotent queue:
    RECORD_ID is UNIQUE, so the same record cannot be queued twice.
    """
    existing = db_execute(
        "SELECT id,status FROM storage_queue WHERE record_id=?",
        (record_id,),
        fetchone=True,
    )

    if existing:
        return

    count = db_execute(
        """
        SELECT COUNT(*)
        FROM storage_queue
        WHERE status IN ('pending','retry')
        """,
        fetchone=True,
    )[0]

    if count >= STORAGE_QUEUE_MAX:
        # Drop the oldest permanently failed/retried record first.
        db_execute(
            """
            DELETE FROM storage_queue
            WHERE id = (
                SELECT id FROM storage_queue
                ORDER BY id ASC LIMIT 1
            )
            """
        )

    db_execute(
        """
        INSERT OR IGNORE INTO storage_queue
        (record_id,payload,created_at,attempts,last_error,
         next_attempt_at,status)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            record_id,
            payload,
            now(),
            0,
            str(error)[:1000],
            now(),
            "pending",
        ),
    )


def retry_delay(attempts):
    delay = STORAGE_RETRY_BASE * (2 ** max(0, attempts - 1))
    return min(delay, STORAGE_RETRY_MAX)


def storage_record(record_type, created_at=None, record_id=None, **fields):
    """
    Every Storage record has:
      KRO_DB
      VERSION
      RECORD_ID
      TYPE
      CREATED_AT
    """
    record_id = record_id or generate_id()
    created_at = created_at or now()

    lines = [
        "KRO_DB",
        f"VERSION={STORAGE_VERSION}",
        f"RECORD_ID={record_id}",
        f"TYPE={record_type}",
        f"CREATED_AT={created_at}",
    ]

    for key, value in fields.items():
        if value is None:
            value = ""

        if key in {
            "CAPTION",
            "CONTENT",
            "USERNAME",
            "FIRST_NAME",
            "REASON",
            "TITLE",
            "DATA",
        }:
            value = encode_text(value)

        lines.append(f"{key}={value}")

    payload = "\n".join(lines)

    if storage_send(payload) is None:
        queue_storage_payload(
            record_id,
            payload,
            "Telegram Storage write failed",
        )
        return False

    return True


def flush_storage_queue(limit=50):
    rows = db_execute(
        """
        SELECT id,record_id,payload,attempts
        FROM storage_queue
        WHERE status IN ('pending','retry')
          AND next_attempt_at<=?
        ORDER BY id ASC
        LIMIT ?
        """,
        (now(), limit),
        fetchall=True,
    )

    sent = 0

    for row_id, record_id, payload, attempts in rows:
        try:
            msg = bot.send_message(
                STORAGE_CHAT_ID,
                payload,
                disable_notification=True,
            )

            if msg:
                db_execute(
                    "DELETE FROM storage_queue WHERE id=?",
                    (row_id,),
                )
                sent += 1

        except Exception as e:
            new_attempts = attempts + 1

            if new_attempts >= STORAGE_QUEUE_MAX_ATTEMPTS:
                status = "dead"
            else:
                status = "retry"

            next_time = datetime.now() + timedelta(
                seconds=retry_delay(new_attempts)
            )

            db_execute(
                """
                UPDATE storage_queue
                SET attempts=?,
                    last_error=?,
                    next_attempt_at=?,
                    status=?
                WHERE id=?
                """,
                (
                    new_attempts,
                    str(e)[:1000],
                    next_time.isoformat(timespec="seconds"),
                    status,
                    row_id,
                ),
            )

    return sent


# ============================================================
# Review Queue
# ============================================================

def queue_review(code):
    db_execute(
        """
        INSERT OR IGNORE INTO review_queue
        (code,created_at,next_attempt_at,status)
        VALUES (?,?,?,?,?)
        """,
        (
            code,
            now(),
            now(),
            "pending",
        ),
    )


def send_review_message(code):
    """
    Review is linked to CODE.
    The media is NOT copied to Storage again.
    """
    row = db_execute(
        """
        SELECT type,content,caption,
               creator_id,creator_username,creator_name
        FROM links
        WHERE code=? AND deleted=0
        """,
        (code,),
        fetchone=True,
    )

    if not row:
        return True

    msg_type, content, caption, creator_id, creator_username, creator_name = row

    username_text = (
        f"@{creator_username}"
        if creator_username
        else "غير موجود"
    )

    info = (
        "📥 محتوى جديد بانتظار المراجعة\n\n"
        f"الكود: {code}\n"
        f"المنشئ: {creator_name or 'غير معروف'}\n"
        f"Username: {username_text}\n"
        f"User ID: {creator_id or 'غير معروف'}\n\n"
        "المحتوى محفوظ في Storage كسجل KRO_DB، "
        "والمراجعة مرتبطة بالكود فقط."
    )

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "⚠ مخالفة",
            callback_data=f"report:{code}",
        )
    )

    try:
        bot.send_message(
            STORAGE_CHAT_ID,
            info,
            reply_markup=markup,
            disable_notification=True,
        )
        return True

    except Exception as e:
        print("REVIEW SEND ERROR:", repr(e))
        return False


def flush_review_queue(limit=50):
    rows = db_execute(
        """
        SELECT id,code,attempts
        FROM review_queue
        WHERE status IN ('pending','retry')
          AND next_attempt_at<=?
        ORDER BY id ASC
        LIMIT ?
        """,
        (now(), limit),
        fetchall=True,
    )

    sent = 0

    for row_id, code, attempts in rows:
        if send_review_message(code):
            db_execute(
                "DELETE FROM review_queue WHERE id=?",
                (row_id,),
            )
            sent += 1
            continue

        new_attempts = attempts + 1

        if new_attempts >= STORAGE_QUEUE_MAX_ATTEMPTS:
            status = "dead"
        else:
            status = "retry"

        next_time = datetime.now() + timedelta(
            seconds=retry_delay(new_attempts)
        )

        db_execute(
            """
            UPDATE review_queue
            SET attempts=?,
                last_error=?,
                next_attempt_at=?,
                status=?
            WHERE id=?
            """,
            (
                new_attempts,
                "Review send failed",
                next_time.isoformat(timespec="seconds"),
                status,
                row_id,
            ),
        )

    return sent


# ============================================================
# Storage worker
# ============================================================

def storage_sync_loop():
    while True:
        try:
            flush_storage_queue(50)
            flush_review_queue(50)
            cleanup_storage_queue()
        except Exception as e:
            print("STORAGE WORKER ERROR:", repr(e))

        time.sleep(30)


def cleanup_storage_queue():
    # Dead entries are kept for 30 days for diagnostics, then removed.
    cutoff = (
        datetime.now() - timedelta(days=30)
    ).isoformat(timespec="seconds")

    db_execute(
        """
        DELETE FROM storage_queue
        WHERE status='dead'
          AND created_at<?
        """,
        (cutoff,),
    )

    db_execute(
        """
        DELETE FROM review_queue
        WHERE status='dead'
          AND created_at<?
        """,
        (cutoff,),
    )


# ============================================================
# Forced subscription
# ============================================================

def get_forced_channel():
    raw = get_state("forced_channel")

    if not raw:
        return None

    try:
        return json.loads(raw)
    except Exception:
        return None


def set_forced_channel(channel):
    set_state(
        "forced_channel",
        json.dumps(channel, ensure_ascii=False),
    )

    storage_record(
        "forced_channel",
        DATA=json.dumps(channel, ensure_ascii=False),
    )


def is_subscribed(user_id):
    channel = get_forced_channel()

    if not channel:
        return True

    try:
        member = bot.get_chat_member(
            channel["chat_id"],
            user_id,
        )

        return member.status in (
            "member",
            "administrator",
            "creator",
        )

    except Exception as e:
        print("SUB CHECK ERROR:", repr(e))
        return True


def send_subscribe_prompt(user_id, pending_code=None):
    channel = get_forced_channel()

    if not channel:
        return

    if pending_code:
        pending_start[user_id] = pending_code

    link = channel.get("invite_link")

    if not link and channel.get("username"):
        link = f"https://t.me/{channel['username']}"

    markup = types.InlineKeyboardMarkup()

    if link:
        markup.add(
            types.InlineKeyboardButton(
                "📢 اشترك في القناة",
                url=link,
            )
        )

    markup.add(
        types.InlineKeyboardButton(
            "✅ تحقق من الاشتراك",
            callback_data="check_sub",
        )
    )

    bot.send_message(
        user_id,
        "⚠ يجب عليك الاشتراك في القناة أولاً لاستخدام البوت",
        reply_markup=markup,
    )


# ============================================================
# File Index
# ============================================================

def find_existing_file(file_id):
    if not PREVENT_DUPLICATE_FILE_IDS:
        return None

    row = db_execute(
        """
        SELECT code
        FROM file_index
        WHERE file_id=?
        """,
        (file_id,),
        fetchone=True,
    )

    return row[0] if row else None


def index_file(file_id, code, media_type):
    if not file_id:
        return True

    try:
        db_execute(
            """
            INSERT INTO file_index
            (file_id,code,media_type,created_at)
            VALUES (?,?,?,?)
            """,
            (
                file_id,
                code,
                media_type,
                now(),
            ),
        )
        return True

    except sqlite3.IntegrityError:
        return False


def index_link_files(code, msg_type, content):
    if msg_type in ("photo", "video"):
        index_file(content, code, msg_type)
        return

    if msg_type == "media":
        try:
            items = json.loads(content)
        except Exception:
            return

        for item in items:
            index_file(
                item.get("file_id", ""),
                code,
                item.get("type", ""),
            )


# ============================================================
# Link creation
# ============================================================

def create_link(msg_type, content, caption, creator):
    file_ids = []

    if msg_type in ("photo", "video"):
        file_ids = [content]

    elif msg_type == "media":
        try:
            file_ids = [
                item["file_id"]
                for item in json.loads(content)
            ]
        except Exception:
            file_ids = []

    # Fast duplicate check.
    if PREVENT_DUPLICATE_FILE_IDS:
        for file_id in file_ids:
            existing = find_existing_file(file_id)

            if existing:
                return None, existing, True

    created_at = now()
    code = generate_code()

    try:
        with db_lock:
            conn.execute(
                """
                INSERT INTO links
                (code,type,content,caption,
                 creator_id,creator_username,creator_name,
                 created_at,deleted)
                VALUES (?,?,?,?,?,?,?,?,0)
                """,
                (
                    code,
                    msg_type,
                    content,
                    caption or "",
                    creator.id,
                    creator.username or "",
                    creator.first_name or "",
                    created_at,
                ),
            )

            # File index in same transaction.
            for file_id in file_ids:
                media_type = msg_type

                if msg_type == "media":
                    try:
                        item = next(
                            x for x in json.loads(content)
                            if x["file_id"] == file_id
                        )
                        media_type = item["type"]
                    except Exception:
                        pass

                conn.execute(
                    """
                    INSERT INTO file_index
                    (file_id,code,media_type,created_at)
                    VALUES (?,?,?,?)
                    """,
                    (
                        file_id,
                        code,
                        media_type,
                        created_at,
                    ),
                )

            conn.commit()

    except sqlite3.IntegrityError:
        # Duplicate file race.
        for file_id in file_ids:
            existing = find_existing_file(file_id)
            if existing:
                return None, existing, True

        return None, None, False

    # Persist exact original creation time.
    storage_write_link(
        code,
        msg_type,
        content,
        caption or "",
        creator.id,
        creator.username or "",
        creator.first_name or "",
        created_at,
    )

    # Review is retryable and does NOT duplicate the media.
    queue_review(code)

    return code, None, False


def storage_write_link(
    code,
    msg_type,
    content,
    caption,
    creator_id,
    creator_username,
    creator_name,
    created_at,
):
    if msg_type == "media":
        try:
            items = json.loads(content)
        except Exception:
            return False

        # ONE Storage record for the complete album.
        fields = {
            "CODE": code,
            "MEDIA_GROUP": 1,
            "TOTAL": len(items),
            "CREATOR_ID": creator_id,
            "CREATOR_USERNAME": creator_username or "",
            "CREATOR_NAME": creator_name or "",
        }

        for index, item in enumerate(items):
            fields[f"ITEM_{index}"] = json.dumps(
                {
                    "type": item["type"],
                    "file_id": item["file_id"],
                    "caption": item.get("caption", ""),
                },
                ensure_ascii=False,
            )

        return storage_record(
            "media_group",
            created_at=created_at,
            **fields,
        )

    return storage_record(
        msg_type,
        created_at=created_at,
        CODE=code,
        FILE_ID=(
            content
            if msg_type in ("photo", "video")
            else ""
        ),
        CONTENT=(
            content
            if msg_type == "text"
            else ""
        ),
        CAPTION=caption or "",
        CREATOR_ID=creator_id,
        CREATOR_USERNAME=creator_username or "",
        CREATOR_NAME=creator_name or "",
    )


def delete_link(code, deleted_by):
    row = db_execute(
        """
        SELECT 1
        FROM links
        WHERE code=? AND deleted=0
        """,
        (code,),
        fetchone=True,
    )

    if not row:
        return False

    db_execute(
        "UPDATE links SET deleted=1 WHERE code=?",
        (code,),
    )

    storage_record(
        "link_deleted",
        CODE=code,
        DELETED_BY=deleted_by,
        DATE=now(),
    )

    return True


# ============================================================
# Delivery
# ============================================================

def deliver_content(user_id, code):
    row = db_execute(
        """
        SELECT type,content,caption
        FROM links
        WHERE code=? AND deleted=0
        """,
        (code,),
        fetchone=True,
    )

    if not row:
        bot.send_message(user_id, "الرابط غير صالح")
        return

    msg_type, content, caption = row
    sent = []

    try:
        if msg_type == "text":
            msg = bot.send_message(user_id, content)
            sent.append(msg.message_id)

        elif msg_type == "photo":
            msg = bot.send_photo(
                user_id,
                content,
                caption=caption or "",
            )
            sent.append(msg.message_id)

        elif msg_type == "video":
            msg = bot.send_video(
                user_id,
                content,
                caption=caption or "",
            )
            sent.append(msg.message_id)

        elif msg_type == "media":
            items = json.loads(content)

            for start in range(0, len(items), 10):
                chunk = items[start:start + 10]
                media = []

                for index, item in enumerate(chunk):
                    item_caption = (
                        item.get("caption", "")
                        if index == 0
                        else ""
                    )

                    if item["type"] == "photo":
                        media.append(
                            types.InputMediaPhoto(
                                item["file_id"],
                                caption=item_caption,
                            )
                        )
                    else:
                        media.append(
                            types.InputMediaVideo(
                                item["file_id"],
                                caption=item_caption,
                            )
                        )

                msgs = bot.send_media_group(
                    user_id,
                    media,
                )

                sent.extend(
                    m.message_id
                    for m in msgs
                )

    except Exception as e:
        print("DELIVER ERROR:", repr(e))
        bot.send_message(
            user_id,
            "⚠ تعذر إرسال المحتوى.",
        )
        return

    delete_after(user_id, sent)


# ============================================================
# Start message
# ============================================================

def send_start(user_id):
    row = db_execute(
        """
        SELECT type,content,caption
        FROM start_msg
        WHERE id=1
        """,
        fetchone=True,
    )

    if not row:
        bot.send_message(user_id, "اهلا بك 👋")
        return

    msg_type, content, caption = row

    try:
        if msg_type == "photo":
            bot.send_photo(
                user_id,
                content,
                caption=caption or "",
            )

        elif msg_type == "video":
            bot.send_video(
                user_id,
                content,
                caption=caption or "",
            )

        else:
            bot.send_message(
                user_id,
                caption or "اهلا بك 👋",
            )

    except Exception:
        bot.send_message(
            user_id,
            caption or "اهلا بك 👋",
        )


# ============================================================
# Keyboards
# ============================================================

def admin_keyboard(user_id):
    markup = types.InlineKeyboardMarkup()

    if is_admin(user_id):
        markup.add(
            types.InlineKeyboardButton(
                "توليد رابط",
                callback_data="create",
            )
        )

        markup.add(
            types.InlineKeyboardButton(
                "⚠ حظر مستخدم",
                callback_data="ban_user",
            )
        )

    if is_owner(user_id):
        markup.add(
            types.InlineKeyboardButton(
                "إذاعة",
                callback_data="broadcast",
            )
        )

        markup.add(
            types.InlineKeyboardButton(
                "إضافة مشرف",
                callback_data="add_admin",
            ),
            types.InlineKeyboardButton(
                "حذف مشرف",
                callback_data="remove_admin",
            ),
        )

        markup.add(
            types.InlineKeyboardButton(
                "حذف رابط",
                callback_data="delete_link",
            ),
            types.InlineKeyboardButton(
                "عدد المستخدمين",
                callback_data="users",
            ),
        )

        markup.add(
            types.InlineKeyboardButton(
                "تعديل start",
                callback_data="edit_start",
            )
        )

        markup.add(
            types.InlineKeyboardButton(
                "قناة الاشتراك الإجباري",
                callback_data="set_channel",
            )
        )

        markup.add(
            types.InlineKeyboardButton(
                "فك حظر",
                callback_data="unban_user",
            )
        )

        markup.add(
            types.InlineKeyboardButton(
                "فحص Storage",
                callback_data="storage_check",
            )
        )

        markup.add(
            types.InlineKeyboardButton(
                "Recovery",
                callback_data="recovery",
            )
        )

        markup.add(
            types.InlineKeyboardButton(
                "Snapshot",
                callback_data="snapshot",
            )
        )

    if user_id == TELETHON_ADMIN_ID:
        markup.add(
            types.InlineKeyboardButton(
                "➕ إضافة رقم Recovery",
                callback_data="telethon_add_phone",
            )
        )

    return markup


def report_keyboard(code):
    markup = types.InlineKeyboardMarkup()

    markup.add(
        types.InlineKeyboardButton(
            "⚠ مخالفة",
            callback_data=f"report:{code}",
        )
    )

    return markup


# ============================================================
# Storage helpers for settings
# ============================================================

def storage_write_start(msg_type, file_id, caption):
    storage_record(
        "start",
        MEDIA_TYPE=msg_type,
        FILE_ID=file_id,
        CAPTION=caption or "",
    )


def storage_write_admin(user_id, added_by, date_added):
    storage_record(
        "admin",
        USER_ID=user_id,
        ADDED_BY=added_by,
        DATE_ADDED=date_added,
    )


# ============================================================
# Start
# ============================================================

@bot.message_handler(commands=["start"])
def start(message):
    if guard_message(message):
        return

    user_id = message.from_user.id
    args = message.text.split()
    code = args[1] if len(args) > 1 else None

    if (
        user_id not in OWNERS
        and not is_admin(user_id)
    ):
        if not is_subscribed(user_id):
            send_subscribe_prompt(
                user_id,
                code,
            )
            return

    if code:
        deliver_content(user_id, code)
        return

    if is_admin(user_id):
        if user_id not in admins_seen_start:
            send_start(user_id)
            admins_seen_start.add(user_id)

        bot.send_message(
            user_id,
            "لوحة التحكم",
            reply_markup=admin_keyboard(user_id),
        )

    else:
        send_start(user_id)


# ============================================================
# Telethon login
# ============================================================

def cleanup_telethon_login(user_id):
    session_data = telethon_login_sessions.pop(
        user_id,
        None,
    )

    if session_data:
        try:
            session_data["client"].disconnect()
        except Exception:
            pass

    admin_steps[user_id] = None


def finish_telethon_login(user_id, client):
    try:
        # Local SQLite ONLY.
        set_telethon_session_string(
            client.session.save()
        )

        bot.send_message(
            user_id,
            "✅ تم تسجيل الدخول وحفظ جلسة Recovery محليًا.",
        )

    finally:
        cleanup_telethon_login(user_id)


# ============================================================
# Callback dispatcher
# ============================================================

@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    user_id = call.from_user.id
    data = call.data or ""

    if is_banned(user_id):
        bot.answer_callback_query(
            call.id,
            "أنت محظور من استخدام البوت.",
            show_alert=True,
        )
        return

    if data == "check_sub":
        if is_subscribed(user_id):
            bot.answer_callback_query(
                call.id,
                "تم التحقق بنجاح ✅",
            )

            try:
                bot.delete_message(
                    user_id,
                    call.message.message_id,
                )
            except Exception:
                pass

            code = pending_start.pop(
                user_id,
                None,
            )

            if code:
                deliver_content(user_id, code)

            elif is_admin(user_id):
                bot.send_message(
                    user_id,
                    "لوحة التحكم",
                    reply_markup=admin_keyboard(user_id),
                )

            else:
                send_start(user_id)

        else:
            bot.answer_callback_query(
                call.id,
                "لم تشترك بعد ⚠",
                show_alert=True,
            )

        return

    # Recovery number button.
    if data == "telethon_add_phone":
        if user_id != TELETHON_ADMIN_ID:
            bot.answer_callback_query(
                call.id,
                "هذا الزر مخصص فقط للمعرف المحدد.",
                show_alert=True,
            )
            return

        bot.answer_callback_query(call.id)

        admin_steps[user_id] = "telethon_phone"

        bot.send_message(
            user_id,
            "أرسل رقم الهاتف بصيغة دولية.\n"
            "مثال: +9647701234567\n\n"
            "يجب أن يكون الحساب عضوًا في Storage Group.",
        )
        return

    owner_only = {
        "add_admin",
        "remove_admin",
        "unban_user",
        "set_channel",
        "disable_channel",
        "storage_check",
        "recovery",
        "delete_link",
        "snapshot",
    }

    admin_operations = {
        "create",
        "broadcast",
        "add_admin",
        "remove_admin",
        "delete_link",
        "users",
        "edit_start",
        "set_channel",
        "disable_channel",
        "ban_user",
        "unban_user",
        "storage_check",
        "recovery",
        "snapshot",
    }

    if data in admin_operations and not is_admin(user_id):
        bot.answer_callback_query(
            call.id,
            "ليس لديك صلاحية.",
            show_alert=True,
        )
        return

    if data in owner_only and not is_owner(user_id):
        bot.answer_callback_query(
            call.id,
            "هذه الصلاحية للـOwner فقط.",
            show_alert=True,
        )
        return

    if data.startswith("report:"):
        handle_report(
            call,
            data.split(":", 1)[1],
        )
        return

    if data.startswith("ban_confirm:"):
        handle_ban_confirm(
            call,
            data.split(":", 1)[1],
        )
        return

    if data.startswith("unban_confirm:"):
        handle_unban_confirm(
            call,
            data.split(":", 1)[1],
        )
        return

    if data == "ban_cancel":
        bot.answer_callback_query(
            call.id,
            "تم الإلغاء",
        )
        bot.send_message(
            user_id,
            "تم إلغاء الحظر.",
        )
        return

    bot.answer_callback_query(call.id)

    if data == "create":
        admin_steps[user_id] = "create"
        bot.send_message(
            user_id,
            "ارسل المحتوى",
        )

    elif data == "broadcast":
        admin_steps[user_id] = "broadcast_msg"
        bot.send_message(
            user_id,
            "ارسل رسالة الإذاعة",
        )

    elif data == "add_admin":
        admin_steps[user_id] = "add_admin"
        bot.send_message(
            user_id,
            "ارسل الايدي",
        )

    elif data == "remove_admin":
        admin_steps[user_id] = "remove_admin"
        bot.send_message(
            user_id,
            "ارسل الايدي",
        )

    elif data == "delete_link":
        admin_steps[user_id] = "delete_link"
        bot.send_message(
            user_id,
            "ارسل كود الرابط",
        )

    elif data == "users":
        total = db_execute(
            "SELECT COUNT(*) FROM users",
            fetchone=True,
        )[0]

        bot.send_message(
            user_id,
            f"عدد المستخدمين: {total}",
        )

    elif data == "edit_start":
        admin_steps[user_id] = "edit_start"

        bot.send_message(
            user_id,
            "ارسل صورة أو فيديو مع وصف.",
        )

    elif data == "ban_user":
        admin_steps[user_id] = "ban_user"

        bot.send_message(
            user_id,
            "ارسل User ID",
        )

    elif data == "unban_user":
        admin_steps[user_id] = "unban_user"

        bot.send_message(
            user_id,
            "ارسل User ID",
        )

    elif data == "set_channel":
        admin_steps[user_id] = "set_channel"

        channel = get_forced_channel()

        current = (
            f"\n\nالقناة الحالية: "
            f"{channel.get('title', '—')}"
            if channel
            else "\n\nلا توجد قناة مفعّلة حاليًا"
        )

        markup = types.InlineKeyboardMarkup()

        markup.add(
            types.InlineKeyboardButton(
                "❌ إلغاء الاشتراك الإجباري",
                callback_data="disable_channel",
            )
        )

        bot.send_message(
            user_id,
            "لتفعيل الاشتراك الإجباري:\n"
            "1) اجعل البوت مشرفًا في القناة\n"
            "2) أرسل @username للقناة\n"
            + current,
            reply_markup=markup,
        )

    elif data == "disable_channel":
        set_state("forced_channel", "")

        storage_record(
            "forced_channel_disabled",
            DATE=now(),
        )

        admin_steps[user_id] = None

        bot.send_message(
            user_id,
            "تم إلغاء الاشتراك الإجباري ✅",
        )

    elif data == "storage_check":
        check_storage(user_id)

    elif data == "recovery":
        bot.send_message(
            user_id,
            "بدأ Recovery...\n"
            "سأرسل Progress أثناء القراءة.",
        )

        threading.Thread(
            target=run_recovery,
            args=(user_id,),
            daemon=True,
        ).start()

    elif data == "snapshot":
        bot.send_message(
            user_id,
            "بدأ إنشاء Snapshot...",
        )

        threading.Thread(
            target=run_snapshot,
            args=(user_id,),
            daemon=True,
        ).start()


# ============================================================
# Text handler
# ============================================================

@bot.message_handler(content_types=["text"])
def admin_text(message):
    if guard_message(message):
        return

    user_id = message.from_user.id
    step = admin_steps.get(user_id)

    # --------------------------------------------
    # Telethon phone
    # --------------------------------------------

    if step == "telethon_phone":
        if user_id != TELETHON_ADMIN_ID:
            admin_steps[user_id] = None
            return

        phone = message.text.strip()

        try:
            client = TelegramClient(
                StringSession(),
                API_ID,
                API_HASH,
            )

            client.connect()

            sent = client.send_code_request(phone)

            telethon_login_sessions[user_id] = {
                "client": client,
                "phone": phone,
                "phone_code_hash": sent.phone_code_hash,
            }

            admin_steps[user_id] = "telethon_code"

            bot.send_message(
                user_id,
                "تم إرسال رمز التحقق إلى حسابك.\n"
                "أرسله هنا وسيتم حذف الرسالة تلقائيًا.",
            )

        except FloodWaitError as e:
            bot.send_message(
                user_id,
                f"يجب الانتظار {e.seconds} ثانية.",
            )
            admin_steps[user_id] = None

        except Exception as e:
            bot.send_message(
                user_id,
                f"فشل إرسال الرمز:\n{e}",
            )
            admin_steps[user_id] = None

        return

    # --------------------------------------------
    # Telethon code
    # --------------------------------------------

    if step == "telethon_code":
        if user_id != TELETHON_ADMIN_ID:
            admin_steps[user_id] = None
            return

        try:
            bot.delete_message(
                message.chat.id,
                message.message_id,
            )
        except Exception:
            pass

        session_data = telethon_login_sessions.get(
            user_id
        )

        if not session_data:
            bot.send_message(
                user_id,
                "انتهت الجلسة. اضغط الزر وابدأ من جديد.",
            )
            admin_steps[user_id] = None
            return

        client = session_data["client"]

        try:
            client.sign_in(
                phone=session_data["phone"],
                code=message.text.strip(),
                phone_code_hash=session_data["phone_code_hash"],
            )

            finish_telethon_login(
                user_id,
                client,
            )

        except SessionPasswordNeededError:
            admin_steps[user_id] = "telethon_password"

            bot.send_message(
                user_id,
                "الحساب محمي بـ2FA.\n"
                "أرسل كلمة المرور.",
            )

        except (
            PhoneCodeInvalidError,
            PhoneCodeExpiredError,
        ):
            bot.send_message(
                user_id,
                "الرمز خاطئ أو منتهي.",
            )
            cleanup_telethon_login(user_id)

        except Exception as e:
            bot.send_message(
                user_id,
                f"فشل تسجيل الدخول:\n{e}",
            )
            cleanup_telethon_login(user_id)

        return

    # --------------------------------------------
    # Telethon password
    # --------------------------------------------

    if step == "telethon_password":
        if user_id != TELETHON_ADMIN_ID:
            admin_steps[user_id] = None
            return

        try:
            bot.delete_message(
                message.chat.id,
                message.message_id,
            )
        except Exception:
            pass

        session_data = telethon_login_sessions.get(
            user_id
        )

        if not session_data:
            bot.send_message(
                user_id,
                "انتهت الجلسة.",
            )
            admin_steps[user_id] = None
            return

        try:
            session_data["client"].sign_in(
                password=message.text.strip()
            )

            finish_telethon_login(
                user_id,
                session_data["client"],
            )

        except Exception as e:
            bot.send_message(
                user_id,
                f"كلمة المرور خاطئة أو فشل الدخول:\n{e}",
            )
            cleanup_telethon_login(user_id)

        return

    # --------------------------------------------
    # Normal Admin
    # --------------------------------------------

    if not is_admin(user_id):
        return

    if step == "create":
        code, existing, duplicate = create_link(
            "text",
            message.text,
            "",
            message.from_user,
        )

        if duplicate:
            bot.send_message(
                user_id,
                "هذا المحتوى موجود مسبقًا ضمن الرابط:\n"
                f"https://t.me/{BOT_USERNAME}?start={existing}",
            )

        else:
            send_created_link_message(
                user_id,
                code,
            )

        admin_steps[user_id] = None

    elif step == "broadcast_msg":
        broadcast_data[user_id] = message.text
        admin_steps[user_id] = "broadcast_time"

        bot.send_message(
            user_id,
            "كم ثانية قبل حذف الرسالة؟",
        )

    elif step == "broadcast_time":
        try:
            delay = int(message.text.strip())

            users = db_execute(
                "SELECT user_id FROM users",
                fetchall=True,
            )

            sent = []

            for row in users:
                target_id = row[0]

                if is_banned(target_id):
                    continue

                try:
                    msg = bot.send_message(
                        target_id,
                        broadcast_data[user_id],
                    )

                    sent.append(
                        (target_id, msg.message_id)
                    )

                except Exception:
                    pass

            def delete_broadcast():
                time.sleep(max(0, delay))

                for chat_id, message_id in sent:
                    try:
                        bot.delete_message(
                            chat_id,
                            message_id,
                        )
                    except Exception:
                        pass

            threading.Thread(
                target=delete_broadcast,
                daemon=True,
            ).start()

            bot.send_message(
                user_id,
                "تمت الإذاعة",
            )

        except ValueError:
            bot.send_message(
                user_id,
                "رقم فقط",
            )

        finally:
            admin_steps[user_id] = None

    elif step == "add_admin":
        if not is_owner(user_id):
            return

        try:
            new_admin = int(
                message.text.strip()
            )

            if new_admin in OWNERS:
                bot.send_message(
                    user_id,
                    "هذا المستخدم Owner بالفعل.",
                )

            else:
                date_added = now()

                db_execute(
                    """
                    INSERT OR REPLACE INTO admins
                    (user_id,added_by,date_added)
                    VALUES(?,?,?)
                    """,
                    (
                        new_admin,
                        user_id,
                        date_added,
                    ),
                )

                storage_write_admin(
                    new_admin,
                    user_id,
                    date_added,
                )

                bot.send_message(
                    user_id,
                    "تمت إضافة المشرف.",
                )

        except ValueError:
            bot.send_message(
                user_id,
                "ايدي خطأ",
            )

        admin_steps[user_id] = None

    elif step == "remove_admin":
        if not is_owner(user_id):
            return

        try:
            admin_id = int(
                message.text.strip()
            )

            if admin_id in OWNERS:
                bot.send_message(
                    user_id,
                    "لا يمكن حذف Owner.",
                )

            else:
                db_execute(
                    "DELETE FROM admins WHERE user_id=?",
                    (admin_id,),
                )

                storage_record(
                    "admin_removed",
                    USER_ID=admin_id,
                    REMOVED_BY=user_id,
                    DATE=now(),
                )

                bot.send_message(
                    user_id,
                    "تم الحذف.",
                )

        except ValueError:
            bot.send_message(
                user_id,
                "ايدي خطأ",
            )

        admin_steps[user_id] = None

    elif step == "delete_link":
        code = message.text.strip()

        if delete_link(code, user_id):
            bot.send_message(
                user_id,
                "تم حذف الرابط.",
            )
        else:
            bot.send_message(
                user_id,
                "غير موجود.",
            )

        admin_steps[user_id] = None

    elif step == "ban_user":
        try:
            target_id = int(
                message.text.strip()
            )

        except ValueError:
            bot.send_message(
                user_id,
                "ايدي خطأ",
            )
            admin_steps[user_id] = None
            return

        if target_id in OWNERS:
            bot.send_message(
                user_id,
                "لا يمكن حظر Owner.",
            )
            admin_steps[user_id] = None
            return

        row = db_execute(
            """
            SELECT username,first_name
            FROM users
            WHERE user_id=?
            """,
            (target_id,),
            fetchone=True,
        )

        username = row[0] if row else ""
        first_name = (
            row[1]
            if row
            else "غير معروف"
        )

        markup = types.InlineKeyboardMarkup()

        markup.add(
            types.InlineKeyboardButton(
                "حظر المستخدم",
                callback_data=f"ban_confirm:{target_id}",
            ),
            types.InlineKeyboardButton(
                "إلغاء",
                callback_data="ban_cancel",
            ),
        )

        username_text = (
            f"@{username}"
            if username
            else "غير موجود"
        )

        bot.send_message(
            user_id,
            f"الاسم: {first_name}\n"
            f"Username: {username_text}\n"
            f"User ID: {target_id}\n\n"
            "هل تريد حظر هذا المستخدم؟",
            reply_markup=markup,
        )

        admin_steps[user_id] = None

    elif step == "unban_user":
        if not is_owner(user_id):
            return

        try:
            target_id = int(
                message.text.strip()
            )

            unban_user(
                target_id,
                user_id,
            )

            bot.send_message(
                user_id,
                "تم فك الحظر.",
            )

        except ValueError:
            bot.send_message(
                user_id,
                "ايدي خطأ",
            )

        admin_steps[user_id] = None

    elif step == "set_channel":
        handle_forced_channel_input(
            message
        )


# ============================================================
# Media handler
# ============================================================

@bot.message_handler(
    content_types=["photo", "video"]
)
def media_handler(message):
    if guard_message(message):
        return

    user_id = message.from_user.id

    if not is_admin(user_id):
        return

    step = admin_steps.get(user_id)

    # Start message.
    if step == "edit_start":
        if message.content_type == "photo":
            msg_type = "photo"
            file_id = message.photo[-1].file_id
        else:
            msg_type = "video"
            file_id = message.video.file_id

        db_execute(
            "DELETE FROM start_msg"
        )

        db_execute(
            """
            INSERT INTO start_msg
            (id,type,content,caption)
            VALUES(1,?,?,?)
            """,
            (
                msg_type,
                file_id,
                message.caption or "",
            ),
        )

        storage_write_start(
            msg_type,
            file_id,
            message.caption or "",
        )

        bot.send_message(
            user_id,
            "تم تحديث start.",
        )

        admin_steps[user_id] = None
        return

    if step != "create":
        return

    # Album.
    if message.media_group_id:
        gid = message.media_group_id

        item = {
            "type": message.content_type,
            "file_id": (
                message.photo[-1].file_id
                if message.content_type == "photo"
                else message.video.file_id
            ),
            "caption": message.caption or "",
        }

        with media_group_lock:
            media_groups.setdefault(
                gid,
                [],
            )

            media_groups[gid].append(item)

            old_timer = media_group_timers.get(gid)

            if old_timer:
                old_timer.cancel()

            creator_snapshot = {
                "id": message.from_user.id,
                "username": message.from_user.username or "",
                "first_name": message.from_user.first_name or "",
                "chat_id": user_id,
            }

            def process_album():
                with media_group_lock:
                    items = media_groups.pop(
                        gid,
                        [],
                    )

                    media_group_timers.pop(
                        gid,
                        None,
                    )

                if not items:
                    return

                # Telegram albums are max 10 for one group.
                if len(items) > 10:
                    items = items[:10]

                content = json.dumps(
                    items,
                    ensure_ascii=False,
                )

                creator = type(
                    "Creator",
                    (),
                    {
                        "id": creator_snapshot["id"],
                        "username": creator_snapshot["username"],
                        "first_name": creator_snapshot["first_name"],
                    },
                )()

                code, existing, duplicate = create_link(
                    "media",
                    content,
                    "",
                    creator,
                )

                if duplicate:
                    bot.send_message(
                        creator_snapshot["chat_id"],
                        "هذه الوسائط موجودة مسبقًا ضمن الرابط:\n"
                        f"https://t.me/{BOT_USERNAME}?start={existing}",
                    )

                else:
                    send_created_link_message(
                        creator_snapshot["chat_id"],
                        code,
                    )

                admin_steps[
                    creator_snapshot["id"]
                ] = None

            timer = threading.Timer(
                2.5,
                process_album,
            )

            timer.daemon = True

            media_group_timers[gid] = timer
            timer.start()

        return

    # Single photo/video.
    if message.content_type == "photo":
        msg_type = "photo"
        file_id = message.photo[-1].file_id
    else:
        msg_type = "video"
        file_id = message.video.file_id

    code, existing, duplicate = create_link(
        msg_type,
        file_id,
        message.caption or "",
        message.from_user,
    )

    if duplicate:
        bot.send_message(
            user_id,
            "هذه الوسائط موجودة مسبقًا ضمن الرابط:\n"
            f"https://t.me/{BOT_USERNAME}?start={existing}",
        )

    else:
        send_created_link_message(
            user_id,
            code,
        )

    admin_steps[user_id] = None


# ============================================================
# Other content
# ============================================================

OTHER_CONTENT_TYPES = [
    "document",
    "audio",
    "voice",
    "sticker",
    "animation",
    "video_note",
    "contact",
    "location",
    "venue",
    "poll",
]


@bot.message_handler(
    content_types=OTHER_CONTENT_TYPES
)
def other_content_handler(message):
    guard_message(message)


# ============================================================
# Created link
# ============================================================

def send_created_link_message(admin_id, code):
    bot.send_message(
        admin_id,
        f"https://t.me/{BOT_USERNAME}?start={code}",
    )


# ============================================================
# Reports
# ============================================================

def handle_report(call, code):
    owner_id = call.from_user.id

    if not is_owner(owner_id):
        bot.answer_callback_query(
            call.id,
            "هذه الصلاحية للـOwner فقط.",
            show_alert=True,
        )
        return

    row = db_execute(
        """
        SELECT type,creator_id,
               creator_username,creator_name
        FROM links
        WHERE code=? AND deleted=0
        """,
        (code,),
        fetchone=True,
    )

    if not row:
        bot.answer_callback_query(
            call.id,
            "الرابط غير موجود.",
            show_alert=True,
        )
        return

    msg_type, creator_id, creator_username, creator_name = row

    if not creator_id:
        bot.answer_callback_query(
            call.id,
            "لا توجد هوية منشئ محفوظة.",
            show_alert=True,
        )
        return

    username_text = (
        f"@{creator_username}"
        if creator_username
        else "غير موجود"
    )

    markup = types.InlineKeyboardMarkup()

    markup.add(
        types.InlineKeyboardButton(
            "حظر المستخدم",
            callback_data=f"ban_confirm:{creator_id}",
        ),
        types.InlineKeyboardButton(
            "إلغاء",
            callback_data="ban_cancel",
        ),
    )

    bot.answer_callback_query(call.id)

    bot.send_message(
        call.message.chat.id,
        "⚠ محتوى مخالف\n\n"
        f"الاسم: {creator_name or 'غير معروف'}\n"
        f"Username: {username_text}\n"
        f"User ID: {creator_id}\n\n"
        "هل تريد حظر هذا المستخدم؟",
        reply_markup=markup,
    )


def handle_ban_confirm(call, target_id_raw):
    admin_id = call.from_user.id

    if not is_admin(admin_id):
        bot.answer_callback_query(
            call.id,
            "ليس لديك صلاحية.",
            show_alert=True,
        )
        return

    try:
        target_id = int(target_id_raw)
    except ValueError:
        bot.answer_callback_query(
            call.id,
            "ID غير صحيح.",
            show_alert=True,
        )
        return

    if target_id in OWNERS:
        bot.answer_callback_query(
            call.id,
            "لا يمكن حظر Owner.",
            show_alert=True,
        )
        return

    row = db_execute(
        "SELECT username FROM users WHERE user_id=?",
        (target_id,),
        fetchone=True,
    )

    username = row[0] if row else ""

    ban_user(
        target_id,
        username,
        "مخالفة",
        admin_id,
    )

    bot.answer_callback_query(
        call.id,
        "تم الحظر.",
    )

    try:
        bot.edit_message_reply_markup(
            call.message.chat.id,
            call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass

    markup = types.InlineKeyboardMarkup()

    markup.add(
        types.InlineKeyboardButton(
            "↩ إلغاء الحظر",
            callback_data=f"unban_confirm:{target_id}",
        )
    )

    bot.send_message(
        call.message.chat.id,
        f"تم حظر المستخدم {target_id} فورًا.",
        reply_markup=markup,
    )


def handle_unban_confirm(call, target_id_raw):
    owner_id = call.from_user.id

    if not is_owner(owner_id):
        bot.answer_callback_query(
            call.id,
            "هذه الصلاحية للـOwner فقط.",
            show_alert=True,
        )
        return

    try:
        target_id = int(target_id_raw)
    except ValueError:
        bot.answer_callback_query(
            call.id,
            "ID غير صحيح.",
            show_alert=True,
        )
        return

    unban_user(
        target_id,
        owner_id,
    )

    bot.answer_callback_query(
        call.id,
        "تم إلغاء الحظر.",
    )

    try:
        bot.edit_message_reply_markup(
            call.message.chat.id,
            call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass

    bot.send_message(
        call.message.chat.id,
        f"تم إلغاء حظر المستخدم {target_id}.",
    )


# ============================================================
# Forced channel
# ============================================================

def handle_forced_channel_input(message):
    user_id = message.from_user.id

    if not is_owner(user_id):
        admin_steps[user_id] = None
        return

    channel_info = None
    text = (message.text or "").strip()

    if (
        message.forward_from_chat
        and message.forward_from_chat.type == "channel"
    ):
        chat = message.forward_from_chat

        channel_info = {
            "chat_id": chat.id,
            "username": chat.username,
            "title": chat.title,
        }

    elif text.startswith("@"):
        try:
            chat = bot.get_chat(text)

            channel_info = {
                "chat_id": chat.id,
                "username": chat.username,
                "title": chat.title,
            }

        except Exception:
            bot.send_message(
                user_id,
                "تعذر العثور على القناة.",
            )
            return

    else:
        bot.send_message(
            user_id,
            "أرسل منشورًا Forward من القناة، "
            "أو يوزر يبدأ بـ @",
        )
        return

    try:
        member = bot.get_chat_member(
            channel_info["chat_id"],
            BOT_ID,
        )

        if member.status not in (
            "administrator",
            "creator",
        ):
            bot.send_message(
                user_id,
                "⚠ يجب أن يكون البوت مشرفًا.",
            )
            return

    except Exception:
        bot.send_message(
            user_id,
            "⚠ تعذر التحقق من صلاحية البوت.",
        )
        return

    if not channel_info.get("username"):
        try:
            channel_info["invite_link"] = (
                bot.export_chat_invite_link(
                    channel_info["chat_id"]
                )
            )
        except Exception as e:
            print(
                "INVITE LINK ERROR:",
                repr(e),
            )

    set_forced_channel(
        channel_info
    )

    admin_steps[user_id] = None

    bot.send_message(
        user_id,
        "✅ تم تفعيل الاشتراك الإجباري في: "
        f"{channel_info.get('title')}",
    )


# ============================================================
# Storage health
# ============================================================

def check_storage(owner_id):
    try:
        chat = bot.get_chat(
            STORAGE_CHAT_ID
        )

        member = bot.get_chat_member(
            STORAGE_CHAT_ID,
            BOT_ID,
        )

        queue_count = db_execute(
            """
            SELECT COUNT(*)
            FROM storage_queue
            WHERE status IN ('pending','retry')
            """,
            fetchone=True,
        )[0]

        review_count = db_execute(
            """
            SELECT COUNT(*)
            FROM review_queue
            WHERE status IN ('pending','retry')
            """,
            fetchone=True,
        )[0]

        bot.send_message(
            owner_id,
            "Storage Group يعمل.\n\n"
            f"الاسم: {chat.title}\n"
            f"ID: {chat.id}\n"
            f"Bot status: {member.status}\n"
            f"Storage Queue: {queue_count}\n"
            f"Review Queue: {review_count}",
        )

    except Exception as e:
        bot.send_message(
            owner_id,
            "فشل فحص Storage Group:\n"
            + str(e),
        )


# ============================================================
# Recovery parser
# ============================================================

def parse_storage_message(text):
    if not text:
        return None

    lines = text.splitlines()

    if not lines or lines[0].strip() != "KRO_DB":
        return None

    data = {}

    for line in lines[1:]:
        if "=" not in line:
            continue

        key, value = line.split(
            "=",
            1,
        )

        data[key.strip()] = value

    # V2 only.
    # This intentionally does not silently interpret V1 as V2.
    if data.get("VERSION") != str(STORAGE_VERSION):
        return None

    if not data.get("RECORD_ID"):
        return None

    if not data.get("TYPE"):
        return None

    if not data.get("CREATED_AT"):
        return None

    return data


# ============================================================
# Recovery helpers
# ============================================================

def insert_user_if_missing(
    user_id,
    username,
    first_name,
    date_join,
):
    if db_execute(
        "SELECT 1 FROM users WHERE user_id=?",
        (user_id,),
        fetchone=True,
    ):
        return False

    db_execute(
        """
        INSERT OR IGNORE INTO users
        (user_id,username,first_name,date_join)
        VALUES(?,?,?,?)
        """,
        (
            user_id,
            username,
            first_name,
            date_join,
        ),
    )

    return True


def recover_link_record(data, result):
    code = data.get("CODE")

    if not code:
        return

    if db_execute(
        "SELECT 1 FROM links WHERE code=?",
        (code,),
        fetchone=True,
    ):
        return

    record_type = data.get("TYPE")

    if record_type == "media_group":
        recover_media_group_record(
            data,
            result,
        )
        return

    if record_type not in (
        "text",
        "photo",
        "video",
    ):
        return

    if record_type == "text":
        content = decode_text(
            data.get("CONTENT", "")
        )
    else:
        content = data.get(
            "FILE_ID",
            "",
        )

    if not content:
        return

    try:
        creator_id = int(
            data.get(
                "CREATOR_ID",
                "0",
            )
        ) or None
    except Exception:
        creator_id = None

    caption = decode_text(
        data.get("CAPTION", "")
    )

    created_at = data.get(
        "CREATED_AT",
        now(),
    )

    db_execute(
        """
        INSERT OR IGNORE INTO links
        (code,type,content,caption,
         creator_id,creator_username,creator_name,
         created_at,deleted)
        VALUES(?,?,?,?,?,?,?,?,0)
        """,
        (
            code,
            record_type,
            content,
            caption,
            creator_id,
            decode_text(
                data.get(
                    "CREATOR_USERNAME",
                    "",
                )
            ),
            decode_text(
                data.get(
                    "CREATOR_NAME",
                    "",
                )
            ),
            created_at,
        ),
    )

    if record_type in ("photo", "video"):
        index_file(
            content,
            code,
            record_type,
        )

    result["links"] += 1


def recover_media_group_record(data, result):
    code = data.get("CODE")

    if not code:
        return

    if db_execute(
        "SELECT 1 FROM links WHERE code=?",
        (code,),
        fetchone=True,
    ):
        return

    try:
        total = int(
            data.get(
                "TOTAL",
                "0",
            )
        )
    except Exception:
        return

    if total <= 0 or total > 10:
        return

    items = []

    # Complete-group validation.
    for index in range(total):
        raw = data.get(
            f"ITEM_{index}"
        )

        if not raw:
            print(
                "RECOVERY: incomplete album:",
                code,
            )
            return

        try:
            item = json.loads(raw)

            if item.get("type") not in (
                "photo",
                "video",
            ):
                return

            if not item.get("file_id"):
                return

            items.append(
                {
                    "type": item["type"],
                    "file_id": item["file_id"],
                    "caption": item.get(
                        "caption",
                        "",
                    ),
                }
            )

        except Exception:
            print(
                "RECOVERY: damaged album:",
                code,
            )
            return

    try:
        creator_id = int(
            data.get(
                "CREATOR_ID",
                "0",
            )
        ) or None
    except Exception:
        creator_id = None

    created_at = data.get(
        "CREATED_AT",
        now(),
    )

    db_execute(
        """
        INSERT OR IGNORE INTO links
        (code,type,content,caption,
         creator_id,creator_username,creator_name,
         created_at,deleted)
        VALUES(?,?,?,?,?,?,?,?,0)
        """,
        (
            code,
            "media",
            json.dumps(
                items,
                ensure_ascii=False,
            ),
            "",
            creator_id,
            decode_text(
                data.get(
                    "CREATOR_USERNAME",
                    "",
                )
            ),
            decode_text(
                data.get(
                    "CREATOR_NAME",
                    "",
                )
            ),
            created_at,
        ),
    )

    for item in items:
        index_file(
            item["file_id"],
            code,
            item["type"],
        )

    result["links"] += 1


# ============================================================
# Recovery
# ============================================================

def restore_from_storage(owner_id):
    result = {
        "links": 0,
        "users": 0,
        "admins": 0,
        "bans": 0,
        "processed": 0,
        "skipped": 0,
    }

    session_string = get_telethon_session_string()

    if not session_string:
        raise RuntimeError(
            "لا توجد جلسة Telethon. "
            "استخدم «إضافة رقم Recovery» أولاً."
        )

    client = TelegramClient(
        StringSession(session_string),
        API_ID,
        API_HASH,
    )

    client.connect()

    if not client.is_user_authorized():
        client.disconnect()

        raise RuntimeError(
            "جلسة Telethon منتهية أو غير صالحة."
        )

    try:
        # Telegram does not expose an exact cheap COUNT here.
        # We show processed progress instead of pretending an exact total.
        bot.send_message(
            owner_id,
            "Recovery started...\n"
            "0 records processed.",
        )

        deleted_codes = set()

        for message in client.iter_messages(
            STORAGE_CHAT_ID,
            limit=None,
            reverse=True,
        ):
            result["processed"] += 1

            try:
                data = parse_storage_message(
                    message.message or ""
                )

                if not data:
                    continue

                record_type = data.get("TYPE")

                if record_type in (
                    "text",
                    "photo",
                    "video",
                    "media_group",
                ):
                    recover_link_record(
                        data,
                        result,
                    )

                elif record_type == "link_deleted":
                    code = data.get("CODE")

                    if code:
                        deleted_codes.add(code)

                elif record_type == "user":
                    try:
                        user_id = int(
                            data["USER_ID"]
                        )
                    except Exception:
                        continue

                    if insert_user_if_missing(
                        user_id,
                        decode_text(
                            data.get(
                                "USERNAME",
                                "",
                            )
                        ),
                        decode_text(
                            data.get(
                                "FIRST_NAME",
                                "",
                            )
                        ),
                        data.get(
                            "DATE_JOIN",
                            data.get(
                                "CREATED_AT",
                                now(),
                            ),
                        ),
                    ):
                        result["users"] += 1

                elif record_type == "admin":
                    try:
                        user_id = int(
                            data["USER_ID"]
                        )

                        added_by = int(
                            data.get(
                                "ADDED_BY",
                                "0",
                            )
                        )

                    except Exception:
                        continue

                    if user_id in OWNERS:
                        continue

                    if not db_execute(
                        "SELECT 1 FROM admins WHERE user_id=?",
                        (user_id,),
                        fetchone=True,
                    ):
                        db_execute(
                            """
                            INSERT OR IGNORE INTO admins
                            (user_id,added_by,date_added)
                            VALUES(?,?,?)
                            """,
                            (
                                user_id,
                                added_by,
                                data.get(
                                    "DATE_ADDED",
                                    data.get(
                                        "CREATED_AT",
                                        now(),
                                    ),
                                ),
                            ),
                        )

                        result["admins"] += 1

                elif record_type == "admin_removed":
                    try:
                        user_id = int(
                            data["USER_ID"]
                        )

                        db_execute(
                            "DELETE FROM admins WHERE user_id=?",
                            (user_id,),
                        )

                    except Exception:
                        pass

                elif record_type == "ban":
                    try:
                        user_id = int(
                            data["USER_ID"]
                        )

                        banned_by = int(
                            data.get(
                                "BANNED_BY",
                                "0",
                            )
                        )

                    except Exception:
                        continue

                    if user_id in OWNERS:
                        continue

                    db_execute(
                        """
                        INSERT OR REPLACE INTO banned_users
                        (user_id,username,reason,
                         banned_by,date_banned)
                        VALUES(?,?,?,?,?)
                        """,
                        (
                            user_id,
                            decode_text(
                                data.get(
                                    "USERNAME",
                                    "",
                                )
                            ),
                            decode_text(
                                data.get(
                                    "REASON",
                                    "",
                                )
                            ),
                            banned_by,
                            data.get(
                                "DATE_BANNED",
                                data.get(
                                    "CREATED_AT",
                                    now(),
                                ),
                            ),
                        ),
                    )

                    result["bans"] += 1

                elif record_type == "unban":
                    try:
                        user_id = int(
                            data["USER_ID"]
                        )

                        db_execute(
                            "DELETE FROM banned_users WHERE user_id=?",
                            (user_id,),
                        )

                    except Exception:
                        pass

                elif record_type == "start":
                    media_type = data.get(
                        "MEDIA_TYPE"
                    )

                    file_id = data.get(
                        "FILE_ID"
                    )

                    if (
                        media_type in (
                            "photo",
                            "video",
                        )
                        and file_id
                    ):
                        db_execute(
                            """
                            INSERT OR REPLACE INTO start_msg
                            (id,type,content,caption)
                            VALUES(1,?,?,?)
                            """,
                            (
                                media_type,
                                file_id,
                                decode_text(
                                    data.get(
                                        "CAPTION",
                                        "",
                                    )
                                ),
                            ),
                        )

                elif record_type == "forced_channel":
                    raw = decode_text(
                        data.get(
                            "DATA",
                            "",
                        )
                    )

                    try:
                        channel = json.loads(raw)

                        set_state(
                            "forced_channel",
                            json.dumps(
                                channel,
                                ensure_ascii=False,
                            ),
                        )

                    except Exception:
                        pass

                elif record_type == "forced_channel_disabled":
                    set_state(
                        "forced_channel",
                        "",
                    )

            except Exception as record_error:
                result["skipped"] += 1

                print(
                    "RECOVERY RECORD ERROR:",
                    repr(record_error),
                )

            if (
                result["processed"]
                % RECOVERY_PROGRESS_EVERY
                == 0
            ):
                bot.send_message(
                    owner_id,
                    "Recovery progress:\n"
                    f"{result['processed']:,} records processed\n"
                    f"Links restored: {result['links']:,}\n"
                    f"Users restored: {result['users']:,}",
                )

        # Deleted tombstones always win.
        for code in deleted_codes:
            db_execute(
                "UPDATE links SET deleted=1 WHERE code=?",
                (code,),
            )

        return result

    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def run_recovery(owner_id):
    if not is_owner(owner_id):
        return

    if not recovery_lock.acquire(
        blocking=False
    ):
        bot.send_message(
            owner_id,
            "Recovery يعمل بالفعل.",
        )
        return

    try:
        result = restore_from_storage(
            owner_id
        )

        set_state(
            "database_recovered",
            "1",
        )

        set_state(
            "last_recovery",
            now(),
        )

        bot.send_message(
            owner_id,
            "✅ Recovery completed.\n\n"
            f"Records: {result['processed']:,}\n"
            f"Links: {result['links']:,}\n"
            f"Users: {result['users']:,}\n"
            f"Admins: {result['admins']:,}\n"
            f"Bans: {result['bans']:,}\n"
            f"Skipped/damaged: {result['skipped']:,}",
        )

    except Exception as e:
        print(
            "RECOVERY ERROR:",
            repr(e),
        )

        bot.send_message(
            owner_id,
            "❌ Recovery failed:\n"
            + str(e),
        )

    finally:
        recovery_lock.release()


# ============================================================
# Snapshot
# ============================================================

def make_snapshot_payload(snapshot_id):
    """
    Creates a compressed logical snapshot of important SQLite state.
    It is split into multiple Storage records to avoid Telegram
    message-size problems.
    """
    snapshot = {
        "snapshot_id": snapshot_id,
        "created_at": now(),
        "users": db_execute(
            """
            SELECT user_id,username,first_name,date_join
            FROM users
            """,
            fetchall=True,
        ),
        "links": db_execute(
            """
            SELECT code,type,content,caption,
                   creator_id,creator_username,
                   creator_name,created_at,deleted
            FROM links
            """,
            fetchall=True,
        ),
        "admins": db_execute(
            """
            SELECT user_id,added_by,date_added
            FROM admins
            """,
            fetchall=True,
        ),
        "bans": db_execute(
            """
            SELECT user_id,username,reason,
                   banned_by,date_banned
            FROM banned_users
            """,
            fetchall=True,
        ),
        "start": db_execute(
            """
            SELECT type,content,caption
            FROM start_msg
            WHERE id=1
            """,
            fetchone=True,
        ),
        "forced_channel": get_forced_channel(),
    }

    raw = json.dumps(
        snapshot,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    compressed = gzip.compress(
        raw,
        compresslevel=6,
    )

    encoded = base64.b64encode(
        compressed
    ).decode("ascii")

    chunks = [
        encoded[i:i + SNAPSHOT_CHUNK_SIZE]
        for i in range(
            0,
            len(encoded),
            SNAPSHOT_CHUNK_SIZE,
        )
    ]

    return chunks


def run_snapshot(owner_id=None):
    global last_snapshot_time

    if not SNAPSHOT_ENABLED:
        return

    snapshot_id = generate_id(
        "SNAP"
    )

    try:
        chunks = make_snapshot_payload(
            snapshot_id
        )

        total = len(chunks)

        for index, chunk in enumerate(chunks):
            storage_record(
                "snapshot",
                SNAPSHOT_ID=snapshot_id,
                INDEX=index,
                TOTAL=total,
                DATA=chunk,
            )

        db_execute(
            """
            INSERT OR REPLACE INTO snapshots
            (snapshot_id,created_at,last_storage_message_id,status)
            VALUES(?,?,?,?,?)
            """,
            (
                snapshot_id,
                now(),
                0,
                "created",
            ),
        )

        last_snapshot_time = time.time()

        if owner_id:
            bot.send_message(
                owner_id,
                "✅ Snapshot completed.\n"
                f"ID: {snapshot_id}\n"
                f"Chunks: {total}",
            )

    except Exception as e:
        print(
            "SNAPSHOT ERROR:",
            repr(e),
        )

        if owner_id:
            bot.send_message(
                owner_id,
                "❌ Snapshot failed:\n"
                + str(e),
            )


def snapshot_loop():
    global last_snapshot_time

    while True:
        try:
            if (
                SNAPSHOT_ENABLED
                and (
                    time.time()
                    - last_snapshot_time
                    >= SNAPSHOT_INTERVAL_SECONDS
                )
            ):
                run_snapshot()

        except Exception as e:
            print(
                "SNAPSHOT LOOP ERROR:",
                repr(e),
            )

        time.sleep(300)


# ============================================================
# Boot
# ============================================================

def boot():
    if not acquire_process_lock():
        raise SystemExit(
            "Another KRO bot process is already running."
        )

    print("========================================")
    print("KRO BOT V2 STARTING")
    print(f"BOT: @{BOT_USERNAME}")
    print(f"STORAGE: {STORAGE_CHAT_ID}")
    print(f"PID: {os.getpid()}")
    print("========================================")

    try:
        chat = bot.get_chat(
            STORAGE_CHAT_ID
        )

        member = bot.get_chat_member(
            STORAGE_CHAT_ID,
            BOT_ID,
        )

        print(
            f"Storage: {chat.title} | "
            f"Bot status: {member.status}"
        )

    except Exception as e:
        print(
            "STORAGE HEALTH ERROR:",
            repr(e),
        )

    try:
        queued = flush_storage_queue(
            100
        )

        if queued:
            print(
                f"Storage queue flushed: {queued}"
            )

    except Exception as e:
        print(
            "INITIAL QUEUE FLUSH ERROR:",
            repr(e),
        )

    threading.Thread(
        target=storage_sync_loop,
        daemon=True,
    ).start()

    threading.Thread(
        target=snapshot_loop,
        daemon=True,
    ).start()

    print("BOT STARTED")

    try:
        bot.infinity_polling(
            skip_pending=True,
            timeout=30,
            long_polling_timeout=30,
        )

    except Exception as e:
        print(
            "POLLING ERROR:",
            repr(e),
        )
        raise


if __name__ == "__main__":
    boot()
