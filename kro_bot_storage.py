import os
import json
import random
import string
import sqlite3
import threading
import time
from datetime import datetime

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
from types import SimpleNamespace as PySimpleNamespace

# ============================================================
# KRO Telegram Bot
# SQLite cache + Telegram Storage Group + Telethon Recovery
# ============================================================
# Required Environment Variables:
# BOT_TOKEN       = Telegram Bot Token
# STORAGE_CHAT_ID = ID of the private Telegram Storage Group
# API_ID          = Telegram API ID (for Recovery only)
# API_HASH        = Telegram API Hash (for Recovery only)
#
# OWNERS are configured below.
#
# IMPORTANT:
# - Never put BOT_TOKEN/API_HASH in GitHub.
# - Media is NOT downloaded to the server. Telegram file_id is stored.
# - SQLite is the fast local cache.
# - Storage Group is the persistent source for recovery.
# - Telethon is used only to read old Storage Group messages during Recovery.
# - Recovery requires a logged-in USER account (not a bot) that is a member
#   of the Storage Group, because Telegram forbids bot accounts from calling
#   GetHistoryRequest. That user session is created in-bot via the
#   "Add Recovery Number" button, restricted to TELETHON_ADMIN_ID, and the
#   resulting session string is stored ONLY in local SQLite — never sent to
#   Storage Group or anywhere over Telegram.
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
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

# Only this ID can see/use the "Add Recovery Number" button and complete
# the Telethon user-account login flow.
TELETHON_ADMIN_ID = 913352843

DATABASE_FILE = "database.db"
TELETHON_SESSION = "kro_storage_recovery"
CONTENT_DELETE_SECONDS = 10
PREVENT_DUPLICATE_FILE_IDS = True
STORAGE_VERSION = 1

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is missing.")
if not STORAGE_CHAT_ID:
    raise SystemExit("STORAGE_CHAT_ID is missing or invalid.")
if not API_ID or not API_HASH:
    raise SystemExit("API_ID and API_HASH are required for Recovery.")

bot = telebot.TeleBot(BOT_TOKEN)
ME = bot.get_me()
BOT_ID = ME.id
BOT_USERNAME = ME.username

# ============================================================
# SQLite
# ============================================================

conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
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

        # Backward-compatible migration for the original database.
        existing_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(links)").fetchall()
        }
        for column, definition in [
            ("creator_id", "INTEGER"),
            ("creator_username", "TEXT"),
            ("creator_name", "TEXT"),
            ("deleted", "INTEGER DEFAULT 0"),
        ]:
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE links ADD COLUMN {column} {definition}")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_links_file ON links(content)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_banned ON banned_users(user_id)")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS storage_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            last_error TEXT DEFAULT ''
        )
        """)
        conn.commit()

initialize_database()

# ============================================================
# Runtime state
# ============================================================

admin_steps = {}
broadcast_data = {}
media_groups = {}
media_group_timers = {}
media_group_lock = threading.RLock()
processing_groups = set()
pending_start = {}
admins_seen_start = set()
ban_pending = {}
recovery_lock = threading.Lock()

# Holds the in-progress Telethon user-login client/state while
# TELETHON_ADMIN_ID is going through phone -> code -> (2FA password).
telethon_login_sessions = {}

# ============================================================
# General helpers
# ============================================================

def now():
    return datetime.now().isoformat(timespec="seconds")

def generate_code(length=8):
    chars = string.ascii_letters + string.digits
    while True:
        code = "".join(random.choice(chars) for _ in range(length))
        exists = db_execute(
            "SELECT 1 FROM links WHERE code=?",
            (code,),
            fetchone=True,
        )
        if not exists:
            return code

def delete_after(chat_id, message_ids, delay=CONTENT_DELETE_SECONDS):
    def job():
        time.sleep(delay)
        for message_id in message_ids:
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass

    threading.Thread(target=job, daemon=True).start()

def encode_text(value):
    # JSON is safer than hand-escaping arbitrary Telegram text.
    return json.dumps(value if value is not None else "", ensure_ascii=False)

def decode_text(value):
    try:
        return json.loads(value)
    except Exception:
        return ""

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
        "INSERT OR REPLACE INTO system_state(key,value) VALUES (?,?)",
        (key, str(value)),
    )

def get_telethon_session_string():
    # Stored ONLY in local SQLite. Never passed to storage_record / Storage
    # Group / any Telegram message — leaking it grants full account control.
    return get_state("telethon_session_string") or ""

def set_telethon_session_string(value):
    set_state("telethon_session_string", value)

# ============================================================
# Permissions / users / bans
# ============================================================

def is_owner(user_id):
    return user_id in OWNERS

def is_admin(user_id):
    if is_owner(user_id):
        return True
    return bool(db_execute(
        "SELECT 1 FROM admins WHERE user_id=?",
        (user_id,),
        fetchone=True,
    ))

def is_banned(user_id):
    return bool(db_execute(
        "SELECT 1 FROM banned_users WHERE user_id=?",
        (user_id,),
        fetchone=True,
    ))

def ensure_user(user):
    user_id = user.id
    row = db_execute(
        "SELECT 1 FROM users WHERE user_id=?",
        (user_id,),
        fetchone=True,
    )
    if row:
        db_execute(
            "UPDATE users SET username=?, first_name=? WHERE user_id=?",
            (user.username, user.first_name, user_id),
        )
        return False

    joined = now()
    db_execute(
        "INSERT INTO users(user_id,username,first_name,date_join) VALUES (?,?,?,?)",
        (user_id, user.username, user.first_name, joined),
    )
    storage_write_user(user_id, user.username, user.first_name, joined)
    return True

def guard_message(message):
    ensure_user(message.from_user)
    if is_banned(message.from_user.id):
        try:
            bot.send_message(message.chat.id, "أنت محظور من استخدام البوت.")
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
        (user_id, username or "", reason or "مخالفة", banned_by, date_banned),
    )
    storage_write_ban(user_id, username, reason, banned_by, date_banned)

def unban_user(user_id, unbanned_by):
    db_execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
    storage_write_unban(user_id, unbanned_by)

# ============================================================
# Storage Group
# ============================================================

def storage_send(text):
    try:
        return bot.send_message(
            STORAGE_CHAT_ID,
            text,
            disable_notification=True,
        )
    except Exception as e:
        print("STORAGE WRITE ERROR:", repr(e))
        return None

def queue_storage_payload(payload, error=""):
    db_execute(
        "INSERT INTO storage_queue(payload,created_at,attempts,last_error) VALUES(?,?,0,?)",
        (payload, now(), str(error)[:1000]),
    )

def storage_record(record_type, **fields):
    lines = ["KRO_DB", f"VERSION={STORAGE_VERSION}", f"TYPE={record_type}"]
    for key, value in fields.items():
        if value is None:
            value = ""
        if key in {"CAPTION", "CONTENT", "USERNAME", "FIRST_NAME", "REASON", "TITLE"}:
            value = encode_text(value)
        lines.append(f"{key}={value}")

    payload = "\n".join(lines)
    if storage_send(payload) is None:
        queue_storage_payload(payload, "Telegram Storage write failed")
        return False
    return True

def flush_storage_queue(limit=50):
    rows = db_execute(
        "SELECT id,payload,attempts FROM storage_queue ORDER BY id ASC LIMIT ?",
        (limit,),
        fetchall=True,
    )
    sent = 0
    for row_id, payload, attempts in rows:
        try:
            msg = bot.send_message(
                STORAGE_CHAT_ID,
                payload,
                disable_notification=True,
            )
            if msg:
                db_execute("DELETE FROM storage_queue WHERE id=?", (row_id,))
                sent += 1
        except Exception as e:
            db_execute(
                "UPDATE storage_queue SET attempts=attempts+1,last_error=? WHERE id=?",
                (str(e)[:1000], row_id),
            )
    return sent

def storage_sync_loop():
    while True:
        try:
            flush_storage_queue()
        except Exception as e:
            print("STORAGE QUEUE ERROR:", repr(e))
        time.sleep(60)

def storage_write_user(user_id, username, first_name, date_join):
    storage_record(
        "user",
        USER_ID=user_id,
        USERNAME=username or "",
        FIRST_NAME=first_name or "",
        DATE_JOIN=date_join,
    )

def storage_write_admin(user_id, added_by, date_added):
    storage_record(
        "admin",
        USER_ID=user_id,
        ADDED_BY=added_by,
        DATE_ADDED=date_added,
    )

def storage_write_ban(user_id, username, reason, banned_by, date_banned):
    storage_record(
        "ban",
        USER_ID=user_id,
        USERNAME=username or "",
        REASON=reason or "مخالفة",
        BANNED_BY=banned_by,
        DATE_BANNED=date_banned,
    )

def storage_write_unban(user_id, unbanned_by):
    storage_record(
        "unban",
        USER_ID=user_id,
        UNBANNED_BY=unbanned_by,
        DATE=now(),
    )

def storage_write_start(msg_type, file_id, caption):
    storage_record(
        "start",
        MEDIA_TYPE=msg_type,
        FILE_ID=file_id,
        CAPTION=caption or "",
    )

def storage_write_link(code, msg_type, content, caption, creator_id, creator_username, creator_name):
    if msg_type == "media":
        items = json.loads(content)
        total = len(items)
        for index, item in enumerate(items):
            storage_record(
                "media_item",
                CODE=code,
                INDEX=index,
                TOTAL=total,
                MEDIA_TYPE=item["type"],
                FILE_ID=item["file_id"],
                CAPTION=item.get("caption", ""),
                CREATOR_ID=creator_id,
                CREATOR_USERNAME=creator_username or "",
                CREATOR_NAME=creator_name or "",
            )
        return

    fields = {
        "CODE": code,
        "FILE_ID": content if msg_type in ("photo", "video") else "",
        "CONTENT": content if msg_type == "text" else "",
        "CAPTION": caption or "",
        "CREATOR_ID": creator_id,
        "CREATOR_USERNAME": creator_username or "",
        "CREATOR_NAME": creator_name or "",
    }
    storage_record(msg_type, **fields)

def storage_write_link_deleted(code, deleted_by):
    storage_record(
        "link_deleted",
        CODE=code,
        DELETED_BY=deleted_by,
        DATE=now(),
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
    set_state("forced_channel", json.dumps(channel, ensure_ascii=False))
    storage_record("forced_channel", DATA=json.dumps(channel, ensure_ascii=False))

def is_subscribed(user_id):
    channel = get_forced_channel()
    if not channel:
        return True
    try:
        member = bot.get_chat_member(channel["chat_id"], user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        print("SUB CHECK ERROR:", repr(e))
        # Preserve the original behavior: do not lock out users if Telegram check fails.
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
        markup.add(types.InlineKeyboardButton("📢 اشترك في القناة", url=link))
    markup.add(types.InlineKeyboardButton("✅ تحقق من الاشتراك", callback_data="check_sub"))
    bot.send_message(
        user_id,
        "⚠ يجب عليك الاشتراك في القناة أولاً لاستخدام البوت",
        reply_markup=markup,
    )

# ============================================================
# Link creation / duplicate protection
# ============================================================

def find_existing_file(file_id):
    if not PREVENT_DUPLICATE_FILE_IDS:
        return None

    rows = db_execute(
        "SELECT code,type,content FROM links WHERE deleted=0",
        fetchall=True,
    )
    for code, msg_type, content in rows:
        if msg_type in ("photo", "video") and content == file_id:
            return code
        if msg_type == "media":
            try:
                for item in json.loads(content):
                    if item.get("file_id") == file_id:
                        return code
            except Exception:
                pass
    return None

def create_link(msg_type, content, caption, creator):
    # For a media group, the group is one logical content item.
    if PREVENT_DUPLICATE_FILE_IDS:
        file_ids = []
        if msg_type in ("photo", "video"):
            file_ids = [content]
        elif msg_type == "media":
            try:
                file_ids = [item["file_id"] for item in json.loads(content)]
            except Exception:
                file_ids = []

        for file_id in file_ids:
            existing = find_existing_file(file_id)
            if existing:
                return None, existing, True

    # SQLite is checked inside the generation loop, so CODE collisions are not accepted.
    while True:
        code = generate_code()
        try:
            with db_lock:
                conn.execute(
                    """
                    INSERT INTO links
                    (code,type,content,caption,creator_id,creator_username,creator_name,created_at,deleted)
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
                        now(),
                    ),
                )
                conn.commit()
            break
        except sqlite3.IntegrityError:
            continue

    # Telegram is the persistent copy. If it fails, local SQLite remains usable,
    # and the Owner can retry storage synchronization later.
    storage_write_link(
        code,
        msg_type,
        content,
        caption or "",
        creator.id,
        creator.username or "",
        creator.first_name or "",
    )
    return code, None, False

def delete_link(code, deleted_by):
    row = db_execute(
        "SELECT 1 FROM links WHERE code=? AND deleted=0",
        (code,),
        fetchone=True,
    )
    if not row:
        return False

    db_execute(
        "UPDATE links SET deleted=1 WHERE code=?",
        (code,),
    )
    storage_write_link_deleted(code, deleted_by)
    return True

# ============================================================
# Delivery
# ============================================================

def deliver_content(user_id, code):
    row = db_execute(
        "SELECT type,content,caption FROM links WHERE code=? AND deleted=0",
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
            msg = bot.send_photo(user_id, content, caption=caption or "")
            sent.append(msg.message_id)
        elif msg_type == "video":
            msg = bot.send_video(user_id, content, caption=caption or "")
            sent.append(msg.message_id)
        elif msg_type == "media":
            items = json.loads(content)
            for start in range(0, len(items), 10):
                chunk = items[start:start + 10]
                media = []
                for index, item in enumerate(chunk):
                    item_caption = item.get("caption", "") if index == 0 else ""
                    if item["type"] == "photo":
                        media.append(types.InputMediaPhoto(item["file_id"], caption=item_caption))
                    else:
                        media.append(types.InputMediaVideo(item["file_id"], caption=item_caption))
                msgs = bot.send_media_group(user_id, media)
                sent.extend(m.message_id for m in msgs)
    except Exception as e:
        print("DELIVER ERROR:", repr(e))
        bot.send_message(user_id, "⚠ تعذر إرسال المحتوى.")
        return

    delete_after(user_id, sent)

# ============================================================
# Start message
# ============================================================

def send_start(user_id):
    row = db_execute(
        "SELECT type,content,caption FROM start_msg WHERE id=1",
        fetchone=True,
    )
    if not row:
        bot.send_message(user_id, "اهلا بك 👋")
        return

    msg_type, content, caption = row
    try:
        if msg_type == "photo":
            bot.send_photo(user_id, content, caption=caption or "")
        elif msg_type == "video":
            bot.send_video(user_id, content, caption=caption or "")
        else:
            bot.send_message(user_id, caption or "اهلا بك 👋")
    except Exception:
        bot.send_message(user_id, caption or "اهلا بك 👋")

# ============================================================
# Keyboards
# ============================================================

def admin_keyboard(user_id):
    markup = types.InlineKeyboardMarkup()

    if is_admin(user_id):
        markup.add(types.InlineKeyboardButton("توليد رابط", callback_data="create"))
        markup.add(types.InlineKeyboardButton("⚠ حظر مستخدم", callback_data="ban_user"))

    if is_owner(user_id):
        markup.add(types.InlineKeyboardButton("إذاعة", callback_data="broadcast"))
        markup.add(
            types.InlineKeyboardButton("إضافة مشرف", callback_data="add_admin"),
            types.InlineKeyboardButton("حذف مشرف", callback_data="remove_admin"),
        )
        markup.add(
            types.InlineKeyboardButton("حذف رابط", callback_data="delete_link"),
            types.InlineKeyboardButton("عدد المستخدمين", callback_data="users"),
        )
        markup.add(types.InlineKeyboardButton("تعديل start", callback_data="edit_start"))
        markup.add(types.InlineKeyboardButton("قناة الاشتراك الإجباري", callback_data="set_channel"))
        markup.add(types.InlineKeyboardButton("فك حظر", callback_data="unban_user"))
        markup.add(types.InlineKeyboardButton("فحص Storage", callback_data="storage_check"))
        markup.add(types.InlineKeyboardButton("Recovery", callback_data="recovery"))

    if user_id == TELETHON_ADMIN_ID:
        markup.add(types.InlineKeyboardButton("➕ إضافة رقم Recovery", callback_data="telethon_add_phone"))

    return markup

def report_keyboard(code):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⚠ مخالفة", callback_data=f"report:{code}"))
    return markup

# ============================================================
# /start
# ============================================================

@bot.message_handler(commands=["start"])
def start(message):
    if guard_message(message):
        return

    user_id = message.from_user.id
    args = message.text.split()
    code = args[1] if len(args) > 1 else None

    if user_id not in OWNERS and not is_admin(user_id):
        if not is_subscribed(user_id):
            send_subscribe_prompt(user_id, code)
            return

    if code:
        deliver_content(user_id, code)
        return

    if is_admin(user_id):
        if user_id not in admins_seen_start:
            send_start(user_id)
            admins_seen_start.add(user_id)
        bot.send_message(user_id, "لوحة التحكم", reply_markup=admin_keyboard(user_id))
    else:
        send_start(user_id)

# ============================================================
# Callbacks - single dispatcher
# ============================================================

@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    user_id = call.from_user.id
    data = call.data or ""

    if is_banned(user_id):
        bot.answer_callback_query(call.id, "أنت محظور من استخدام البوت.", show_alert=True)
        return

    # Subscription check is available to everybody.
    if data == "check_sub":
        if is_subscribed(user_id):
            bot.answer_callback_query(call.id, "تم التحقق بنجاح ✅")
            try:
                bot.delete_message(user_id, call.message.message_id)
            except Exception:
                pass
            code = pending_start.pop(user_id, None)
            if code:
                deliver_content(user_id, code)
            elif is_admin(user_id):
                bot.send_message(user_id, "لوحة التحكم", reply_markup=admin_keyboard(user_id))
            else:
                send_start(user_id)
        else:
            bot.answer_callback_query(call.id, "لم تشترك بعد ⚠", show_alert=True)
        return

    # Telethon user-account login flow - restricted strictly to TELETHON_ADMIN_ID.
    if data == "telethon_add_phone":
        if user_id != TELETHON_ADMIN_ID:
            bot.answer_callback_query(call.id, "هذا الزر مخصص فقط لهذا المعرف.", show_alert=True)
            return
        bot.answer_callback_query(call.id)
        admin_steps[user_id] = "telethon_phone"
        bot.send_message(
            user_id,
            "أرسل رقم الهاتف بصيغة دولية، مثال:\n+9647701234567\n\n"
            "ملاحظة: يجب أن يكون هذا الرقم عضوًا في Storage Group حتى يعمل Recovery.",
        )
        return

    # Owner-only operations.
    owner_only = {
        "add_admin", "remove_admin", "unban_user", "set_channel",
        "disable_channel", "storage_check", "recovery", "delete_link",
    }
    admin_operations = {
        "create", "broadcast", "add_admin", "remove_admin", "delete_link",
        "users", "edit_start", "set_channel", "disable_channel", "ban_user",
        "unban_user", "storage_check", "recovery",
    }

    if data in admin_operations and not is_admin(user_id):
        bot.answer_callback_query(call.id, "ليس لديك صلاحية.", show_alert=True)
        return

    if data in owner_only and not is_owner(user_id):
        bot.answer_callback_query(call.id, "هذه الصلاحية للـ Owner فقط.", show_alert=True)
        return

    # Reports are allowed for Admin/Owner.
    if data.startswith("report:"):
        handle_report(call, data.split(":", 1)[1])
        return
    if data.startswith("ban_confirm:"):
        handle_ban_confirm(call, data.split(":", 1)[1])
        return
    if data.startswith("unban_confirm:"):
        handle_unban_confirm(call, data.split(":", 1)[1])
        return
    if data == "ban_cancel":
        bot.answer_callback_query(call.id, "تم الإلغاء")
        bot.send_message(user_id, "تم إلغاء الحظر.")
        return

    bot.answer_callback_query(call.id)

    if data == "create":
        admin_steps[user_id] = "create"
        bot.send_message(user_id, "ارسل المحتوى")
    elif data == "broadcast":
        admin_steps[user_id] = "broadcast_msg"
        bot.send_message(user_id, "ارسل رسالة الإذاعة")
    elif data == "add_admin":
        admin_steps[user_id] = "add_admin"
        bot.send_message(user_id, "ارسل الايدي")
    elif data == "remove_admin":
        admin_steps[user_id] = "remove_admin"
        bot.send_message(user_id, "ارسل الايدي")
    elif data == "delete_link":
        admin_steps[user_id] = "delete_link"
        bot.send_message(user_id, "ارسل كود الرابط")
    elif data == "users":
        total = db_execute("SELECT COUNT(*) FROM users", fetchone=True)[0]
        bot.send_message(user_id, f"عدد المستخدمين: {total}")
    elif data == "edit_start":
        admin_steps[user_id] = "edit_start"
        bot.send_message(user_id, "ارسل صورة او فيديو مع وصف")
    elif data == "ban_user":
        admin_steps[user_id] = "ban_user"
        bot.send_message(user_id, "ارسل User ID")
    elif data == "unban_user":
        admin_steps[user_id] = "unban_user"
        bot.send_message(user_id, "ارسل User ID")
    elif data == "set_channel":
        admin_steps[user_id] = "set_channel"
        channel = get_forced_channel()
        current = f"\n\nالقناة الحالية: {channel.get('title', '—')}" if channel else "\n\nلا توجد قناة مفعّلة حاليًا"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ إلغاء الاشتراك الإجباري", callback_data="disable_channel"))
        bot.send_message(
            user_id,
            "لتفعيل الاشتراك الإجباري:\n"
            "1) اجعل البوت مشرفًا في القناة\n"
            "2) وجّه منشورًا من القناة إلى هنا، أو أرسل @username" + current,
            reply_markup=markup,
        )
    elif data == "disable_channel":
        set_state("forced_channel", "")
        storage_record("forced_channel_disabled", DATE=now())
        admin_steps[user_id] = None
        bot.send_message(user_id, "تم إلغاء الاشتراك الإجباري ✅")
    elif data == "storage_check":
        check_storage(user_id)
    elif data == "recovery":
        bot.send_message(user_id, "بدأ Recovery... لا تغلق البوت حتى تنتهي العملية.")
        threading.Thread(target=run_recovery, args=(user_id,), daemon=True).start()

# ============================================================
# Telethon user-account login (Recovery number)
# ============================================================

def finish_telethon_login(user_id, client):
    try:
        # Saved ONLY in local SQLite via set_state - never sent through
        # storage_record / Storage Group / any Telegram message.
        set_telethon_session_string(client.session.save())
        bot.send_message(user_id, "✅ تم تسجيل الدخول وحفظ الجلسة. يمكنك الآن استخدام Recovery.")
    finally:
        cleanup_telethon_login(user_id)

def cleanup_telethon_login(user_id):
    session_data = telethon_login_sessions.pop(user_id, None)
    if session_data:
        try:
            session_data["client"].disconnect()
        except Exception:
            pass
    admin_steps[user_id] = None

# ============================================================
# Report / violation handling
# ============================================================

def handle_report(call, code):
    owner_id = call.from_user.id
    if not is_owner(owner_id):
        bot.answer_callback_query(call.id, "هذه الصلاحية للـ Owner فقط.", show_alert=True)
        return

    row = db_execute(
        """
        SELECT type,creator_id,creator_username,creator_name
        FROM links WHERE code=? AND deleted=0
        """,
        (code,),
        fetchone=True,
    )
    if not row:
        bot.answer_callback_query(call.id, "الرابط غير موجود.", show_alert=True)
        return

    msg_type, creator_id, creator_username, creator_name = row
    if not creator_id:
        bot.answer_callback_query(call.id, "لا توجد هوية منشئ محفوظة لهذا الرابط.", show_alert=True)
        return

    username_text = f"@{creator_username}" if creator_username else "غير موجود"
    text = (
        "⚠ محتوى مخالف\n\n"
        f"الاسم: {creator_name or 'غير معروف'}\n"
        f"Username: {username_text}\n"
        f"User ID: {creator_id}\n\n"
        "هل تريد حظر هذا المستخدم؟"
    )
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("حظر المستخدم", callback_data=f"ban_confirm:{creator_id}"),
        types.InlineKeyboardButton("إلغاء", callback_data="ban_cancel"),
    )
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, text, reply_markup=markup)

def handle_ban_confirm(call, target_id_raw):
    admin_id = call.from_user.id
    if not is_admin(admin_id):
        bot.answer_callback_query(call.id, "ليس لديك صلاحية.", show_alert=True)
        return

    try:
        target_id = int(target_id_raw)
    except ValueError:
        bot.answer_callback_query(call.id, "ID غير صحيح.", show_alert=True)
        return

    if target_id in OWNERS:
        bot.answer_callback_query(call.id, "لا يمكن حظر Owner.", show_alert=True)
        return

    row = db_execute(
        "SELECT username FROM users WHERE user_id=?",
        (target_id,),
        fetchone=True,
    )
    username = row[0] if row else ""
    ban_user(target_id, username, "مخالفة", admin_id)

    bot.answer_callback_query(call.id, "تم الحظر.")
    try:
        bot.edit_message_reply_markup(
            call.message.chat.id,
            call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass

    unban_markup = types.InlineKeyboardMarkup()
    unban_markup.add(
        types.InlineKeyboardButton("↩ إلغاء الحظر", callback_data=f"unban_confirm:{target_id}")
    )
    bot.send_message(
        call.message.chat.id,
        f"تم حظر المستخدم {target_id} فورًا.",
        reply_markup=unban_markup,
    )

def handle_unban_confirm(call, target_id_raw):
    owner_id = call.from_user.id
    if not is_owner(owner_id):
        bot.answer_callback_query(call.id, "هذه الصلاحية للـ Owner فقط.", show_alert=True)
        return

    try:
        target_id = int(target_id_raw)
    except ValueError:
        bot.answer_callback_query(call.id, "ID غير صحيح.", show_alert=True)
        return

    unban_user(target_id, owner_id)

    bot.answer_callback_query(call.id, "تم إلغاء الحظر.")
    try:
        bot.edit_message_reply_markup(
            call.message.chat.id,
            call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass
    bot.send_message(call.message.chat.id, f"تم إلغاء حظر المستخدم {target_id}.")

# ============================================================
# Media handlers
# ============================================================

@bot.message_handler(content_types=["photo", "video"])
def media_handler(message):
    if guard_message(message):
        return

    user_id = message.from_user.id
    if not is_admin(user_id):
        return

    step = admin_steps.get(user_id)

    if step == "edit_start":
        if message.content_type == "photo":
            msg_type = "photo"
            file_id = message.photo[-1].file_id
        else:
            msg_type = "video"
            file_id = message.video.file_id

        db_execute("DELETE FROM start_msg")
        db_execute(
            "INSERT INTO start_msg(id,type,content,caption) VALUES(1,?,?,?)",
            (msg_type, file_id, message.caption or ""),
        )
        storage_write_start(msg_type, file_id, message.caption or "")
        bot.send_message(user_id, "تم تحديث start")
        admin_steps[user_id] = None
        return

    if step != "create":
        return

    if message.media_group_id:
        gid = message.media_group_id
        item = {
            "type": message.content_type,
            "file_id": message.photo[-1].file_id if message.content_type == "photo" else message.video.file_id,
            "caption": message.caption or "",
        }

        with media_group_lock:
            media_groups.setdefault(gid, [])
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
                    items = media_groups.pop(gid, [])
                    media_group_timers.pop(gid, None)
                if not items:
                    return

                content = json.dumps(items, ensure_ascii=False)
                creator = PySimpleNamespace(
                    id=creator_snapshot["id"],
                    username=creator_snapshot["username"],
                    first_name=creator_snapshot["first_name"],
                )
                code, existing, duplicate = create_link("media", content, "", creator)

                if duplicate:
                    bot.send_message(
                        creator_snapshot["chat_id"],
                        "هذه الوسائط موجودة مسبقًا ضمن الرابط:\n"
                        f"https://t.me/{BOT_USERNAME}?start={existing}",
                    )
                else:
                    send_created_link_message(creator_snapshot["chat_id"], code)
                    notify_owners_for_review(code, "media", content, "", creator)
                admin_steps[creator_snapshot["id"]] = None

            timer = threading.Timer(2.5, process_album)
            timer.daemon = True
            media_group_timers[gid] = timer
            timer.start()
        return

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
        send_created_link_message(user_id, code)
        notify_owners_for_review(code, msg_type, file_id, message.caption or "", message.from_user)
    admin_steps[user_id] = None

# ============================================================
# Other content types (documents, stickers, voice, etc.)
# ============================================================
# These do not create links, but they must still register the user so
# that broadcasting and user statistics stay accurate for anyone who
# interacts with the bot without ever sending plain text/photo/video.

OTHER_CONTENT_TYPES = [
    "document", "audio", "voice", "sticker",
    "animation", "video_note", "contact", "location", "venue", "poll",
]

@bot.message_handler(content_types=OTHER_CONTENT_TYPES)
def other_content_handler(message):
    guard_message(message)

# ============================================================
# Text handler
# ============================================================

@bot.message_handler(content_types=["text"])
def admin_text(message):
    if guard_message(message):
        return

    user_id = message.from_user.id
    step = admin_steps.get(user_id)

    # Telethon login steps are available only to TELETHON_ADMIN_ID and must
    # be checked before the "not is_admin -> return" gate below, since this
    # ID is not necessarily a bot Admin/Owner.
    if step == "telethon_phone":
        if user_id != TELETHON_ADMIN_ID:
            admin_steps[user_id] = None
            return
        phone = message.text.strip()
        try:
            client = TelegramClient(StringSession(), API_ID, API_HASH)
            client.connect()
            sent = client.send_code_request(phone)
            telethon_login_sessions[user_id] = {
                "client": client,
                "phone": phone,
                "phone_code_hash": sent.phone_code_hash,
            }
            admin_steps[user_id] = "telethon_code"
            bot.send_message(user_id, "تم إرسال رمز التحقق إلى حسابك، أرسله هنا.")
        except FloodWaitError as e:
            bot.send_message(user_id, f"يجب الانتظار {e.seconds} ثانية قبل المحاولة.")
            admin_steps[user_id] = None
        except Exception as e:
            bot.send_message(user_id, f"فشل إرسال الرمز:\n{e}")
            admin_steps[user_id] = None
        return

    if step == "telethon_code":
        if user_id != TELETHON_ADMIN_ID:
            admin_steps[user_id] = None
            return
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        session_data = telethon_login_sessions.get(user_id)
        if not session_data:
            bot.send_message(user_id, "انتهت الجلسة، اضغط الزر وابدأ من جديد.")
            admin_steps[user_id] = None
            return
        client = session_data["client"]
        try:
            client.sign_in(
                phone=session_data["phone"],
                code=message.text.strip(),
                phone_code_hash=session_data["phone_code_hash"],
            )
            finish_telethon_login(user_id, client)
        except SessionPasswordNeededError:
            admin_steps[user_id] = "telethon_password"
            bot.send_message(user_id, "الحساب محمي بكلمة مرور (2FA)، أرسلها الآن.")
        except (PhoneCodeInvalidError, PhoneCodeExpiredError):
            bot.send_message(user_id, "الرمز خاطئ أو منتهي، اضغط الزر وابدأ من جديد.")
            cleanup_telethon_login(user_id)
        except Exception as e:
            bot.send_message(user_id, f"فشل تسجيل الدخول:\n{e}")
            cleanup_telethon_login(user_id)
        return

    if step == "telethon_password":
        if user_id != TELETHON_ADMIN_ID:
            admin_steps[user_id] = None
            return
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        session_data = telethon_login_sessions.get(user_id)
        if not session_data:
            bot.send_message(user_id, "انتهت الجلسة، اضغط الزر وابدأ من جديد.")
            admin_steps[user_id] = None
            return
        try:
            session_data["client"].sign_in(password=message.text.strip())
            finish_telethon_login(user_id, session_data["client"])
        except Exception as e:
            bot.send_message(user_id, f"كلمة المرور خاطئة أو فشل الدخول:\n{e}")
            cleanup_telethon_login(user_id)
        return

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
            send_created_link_message(user_id, code)
            notify_owners_for_review(code, "text", message.text, "", message.from_user)
        admin_steps[user_id] = None

    elif step == "broadcast_msg":
        broadcast_data[user_id] = message.text
        admin_steps[user_id] = "broadcast_time"
        bot.send_message(user_id, "كم ثانية قبل حذف الرسالة؟")

    elif step == "broadcast_time":
        try:
            delay = int(message.text.strip())
            users = db_execute("SELECT user_id FROM users", fetchall=True)
            sent = []
            for row in users:
                target_id = row[0]
                if is_banned(target_id):
                    continue
                try:
                    msg = bot.send_message(target_id, broadcast_data[user_id])
                    sent.append((target_id, msg.message_id))
                except Exception:
                    pass

            def delete_broadcast():
                time.sleep(max(0, delay))
                for chat_id, message_id in sent:
                    try:
                        bot.delete_message(chat_id, message_id)
                    except Exception:
                        pass

            threading.Thread(target=delete_broadcast, daemon=True).start()
            bot.send_message(user_id, "تمت الإذاعة")
        except ValueError:
            bot.send_message(user_id, "رقم فقط")
        finally:
            admin_steps[user_id] = None

    elif step == "add_admin":
        if not is_owner(user_id):
            return
        try:
            new_admin = int(message.text.strip())
            if new_admin in OWNERS:
                bot.send_message(user_id, "هذا المستخدم Owner بالفعل.")
            else:
                date_added = now()
                db_execute(
                    "INSERT OR REPLACE INTO admins(user_id,added_by,date_added) VALUES(?,?,?)",
                    (new_admin, user_id, date_added),
                )
                storage_write_admin(new_admin, user_id, date_added)
                bot.send_message(user_id, "تمت إضافة المشرف.")
        except ValueError:
            bot.send_message(user_id, "ايدي خطأ")
        admin_steps[user_id] = None

    elif step == "remove_admin":
        if not is_owner(user_id):
            return
        try:
            admin_id = int(message.text.strip())
            if admin_id in OWNERS:
                bot.send_message(user_id, "لا يمكن حذف Owner.")
            else:
                db_execute("DELETE FROM admins WHERE user_id=?", (admin_id,))
                storage_record("admin_removed", USER_ID=admin_id, REMOVED_BY=user_id, DATE=now())
                bot.send_message(user_id, "تم الحذف")
        except ValueError:
            bot.send_message(user_id, "ايدي خطأ")
        admin_steps[user_id] = None

    elif step == "delete_link":
        code = message.text.strip()
        if delete_link(code, user_id):
            bot.send_message(user_id, "تم حذف الرابط.")
        else:
            bot.send_message(user_id, "غير موجود")
        admin_steps[user_id] = None

    elif step == "ban_user":
        try:
            target_id = int(message.text.strip())
        except ValueError:
            bot.send_message(user_id, "ايدي خطأ")
            admin_steps[user_id] = None
            return

        if target_id in OWNERS:
            bot.send_message(user_id, "لا يمكن حظر Owner.")
            admin_steps[user_id] = None
            return

        row = db_execute(
            "SELECT username,first_name FROM users WHERE user_id=?",
            (target_id,),
            fetchone=True,
        )
        username = row[0] if row else ""
        first_name = row[1] if row else "غير معروف"
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("حظر المستخدم", callback_data=f"ban_confirm:{target_id}"),
            types.InlineKeyboardButton("إلغاء", callback_data="ban_cancel"),
        )
        username_text = f"@{username}" if username else "غير موجود"
        text = (
            f"الاسم: {first_name}\n"
            f"Username: {username_text}\n"
            f"User ID: {target_id}\n\n"
            "هل تريد حظر هذا المستخدم؟"
        )
        bot.send_message(user_id, text, reply_markup=markup)
        admin_steps[user_id] = None

    elif step == "unban_user":
        if not is_owner(user_id):
            return
        try:
            target_id = int(message.text.strip())
            unban_user(target_id, user_id)
            bot.send_message(user_id, "تم فك الحظر.")
        except ValueError:
            bot.send_message(user_id, "ايدي خطأ")
        admin_steps[user_id] = None

    elif step == "set_channel":
        handle_forced_channel_input(message)

# ============================================================
# Link result + violation button
# ============================================================

def send_created_link_message(admin_id, code):
    # Only the link goes back to the Admin who created it.
    # The violation-review button is sent to Owners in the Storage group instead.
    bot.send_message(
        admin_id,
        f"https://t.me/{BOT_USERNAME}?start={code}",
    )

def notify_owners_for_review(code, msg_type, content, caption, creator):
    # Sends the media/content itself + the violation button to the Storage
    # group, where only Owners can act on it. Admins never see this.
    username_text = f"@{creator.username}" if getattr(creator, "username", "") else "غير موجود"
    info = (
        "📥 محتوى جديد بانتظار المراجعة\n\n"
        f"الكود: {code}\n"
        f"المنشئ: {getattr(creator, 'first_name', '') or 'غير معروف'}\n"
        f"Username: {username_text}\n"
        f"User ID: {creator.id}"
    )

    try:
        if msg_type == "photo":
            bot.send_photo(STORAGE_CHAT_ID, content, caption=caption or "")
        elif msg_type == "video":
            bot.send_video(STORAGE_CHAT_ID, content, caption=caption or "")
        elif msg_type == "media":
            items = json.loads(content)
            for start in range(0, len(items), 10):
                chunk = items[start:start + 10]
                media = []
                for index, item in enumerate(chunk):
                    item_caption = item.get("caption", "") if index == 0 else ""
                    if item["type"] == "photo":
                        media.append(types.InputMediaPhoto(item["file_id"], caption=item_caption))
                    else:
                        media.append(types.InputMediaVideo(item["file_id"], caption=item_caption))
                bot.send_media_group(STORAGE_CHAT_ID, media)
        elif msg_type == "text":
            bot.send_message(STORAGE_CHAT_ID, content)
    except Exception as e:
        print("REVIEW SEND ERROR:", repr(e))

    try:
        bot.send_message(STORAGE_CHAT_ID, info, reply_markup=report_keyboard(code))
    except Exception as e:
        print("REVIEW INFO SEND ERROR:", repr(e))

# ============================================================
# Forced channel input
# ============================================================

def handle_forced_channel_input(message):
    user_id = message.from_user.id
    if not is_owner(user_id):
        admin_steps[user_id] = None
        return

    channel_info = None
    text = (message.text or "").strip()

    if message.forward_from_chat and message.forward_from_chat.type == "channel":
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
            bot.send_message(user_id, "تعذر العثور على القناة، تأكد من اليوزر")
            return
    else:
        bot.send_message(user_id, "أرسل منشورًا Forward من القناة، أو يوزر يبدأ بـ @")
        return

    try:
        member = bot.get_chat_member(channel_info["chat_id"], BOT_ID)
        if member.status not in ("administrator", "creator"):
            bot.send_message(user_id, "⚠ يجب أن يكون البوت مشرفًا في القناة أولاً")
            return
    except Exception:
        bot.send_message(user_id, "⚠ تعذر التحقق، تأكد أن البوت مشرف في القناة")
        return

    if not channel_info.get("username"):
        try:
            channel_info["invite_link"] = bot.export_chat_invite_link(channel_info["chat_id"])
        except Exception as e:
            print("INVITE LINK ERROR:", repr(e))

    set_forced_channel(channel_info)
    admin_steps[user_id] = None
    bot.send_message(user_id, f"✅ تم تفعيل الاشتراك الإجباري في: {channel_info.get('title')}")

# ============================================================
# Storage health check
# ============================================================

def check_storage(owner_id):
    try:
        chat = bot.get_chat(STORAGE_CHAT_ID)
        member = bot.get_chat_member(STORAGE_CHAT_ID, BOT_ID)
        bot.send_message(
            owner_id,
            "Storage Group يعمل.\n\n"
            f"الاسم: {chat.title}\n"
            f"ID: {chat.id}\n"
            f"Bot status: {member.status}",
        )
    except Exception as e:
        bot.send_message(owner_id, "فشل فحص Storage Group:\n" + str(e))

# ============================================================
# Recovery parser
# ============================================================

def parse_storage_message(text):
    if not text or (not text.startswith("KRO_DB\n") and text.strip() != "KRO_DB"):
        return None
    data = {}
    for line in text.splitlines()[1:]:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value
    if data.get("VERSION") != str(STORAGE_VERSION):
        return None
    return data

def insert_user_if_missing(user_id, username, first_name, date_join):
    if db_execute("SELECT 1 FROM users WHERE user_id=?", (user_id,), fetchone=True):
        return False
    db_execute(
        "INSERT OR IGNORE INTO users(user_id,username,first_name,date_join) VALUES(?,?,?,?)",
        (user_id, username, first_name, date_join),
    )
    return True

def restore_from_storage():
    """
    Reads only KRO_DB records from Storage Group.
    No media is downloaded. file_id remains plain metadata.
    Damaged records are ignored.
    Existing SQLite records are never duplicated.

    Requires a logged-in USER account session (StringSession stored locally
    via set_telethon_session_string), because Telegram forbids bot accounts
    from calling GetHistoryRequest to read old messages. That user account
    must be a member of the Storage Group.
    """
    result = {
        "links": 0,
        "users": 0,
        "admins": 0,
        "bans": 0,
    }

    media_groups_recovery = {}
    deleted_codes = set()

    session_string = get_telethon_session_string()
    if not session_string:
        raise RuntimeError(
            "لا توجد جلسة Telethon مسجّلة. استخدم زر «إضافة رقم Recovery» أولًا "
            "(متاح فقط للمعرف المخصص لذلك)."
        )

    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    client.connect()
    if not client.is_user_authorized():
        raise RuntimeError(
            "جلسة Telethon منتهية أو غير صالحة. استخدم زر «إضافة رقم Recovery» "
            "لإعادة تسجيل الدخول."
        )

    try:
        for message in client.iter_messages(STORAGE_CHAT_ID, limit=None, reverse=True):
            try:
                data = parse_storage_message(message.message or "")
                if not data:
                    continue

                record_type = data.get("TYPE")

                if record_type in ("text", "photo", "video"):
                    code = data.get("CODE")
                    if not code:
                        continue
                    if db_execute("SELECT 1 FROM links WHERE code=?", (code,), fetchone=True):
                        continue

                    if record_type == "text":
                        content = decode_text(data.get("CONTENT", ""))
                    else:
                        content = data.get("FILE_ID", "")
                    if not content:
                        continue

                    caption = decode_text(data.get("CAPTION", ""))
                    try:
                        creator_id = int(data.get("CREATOR_ID", "0")) or None
                    except Exception:
                        creator_id = None

                    db_execute(
                        """
                        INSERT OR IGNORE INTO links
                        (code,type,content,caption,creator_id,creator_username,creator_name,created_at,deleted)
                        VALUES(?,?,?,?,?,?,?,?,0)
                        """,
                        (
                            code,
                            record_type,
                            content,
                            caption,
                            creator_id,
                            decode_text(data.get("CREATOR_USERNAME", "")),
                            decode_text(data.get("CREATOR_NAME", "")),
                            now(),
                        ),
                    )
                    result["links"] += 1

                elif record_type == "media_item":
                    code = data.get("CODE")
                    if not code:
                        continue
                    try:
                        index = int(data.get("INDEX", "-1"))
                        total = int(data.get("TOTAL", "0"))
                    except Exception:
                        continue
                    if index < 0 or total <= 0 or index >= total:
                        continue
                    file_id = data.get("FILE_ID", "")
                    media_type = data.get("MEDIA_TYPE", "")
                    if not file_id or media_type not in ("photo", "video"):
                        continue
                    group = media_groups_recovery.setdefault(
                        code,
                        {
                            "total": total,
                            "items": {},
                            "creator_id": None,
                            "creator_username": "",
                            "creator_name": "",
                            "created_at": now(),
                        },
                    )
                    group["items"][index] = {
                        "type": media_type,
                        "file_id": file_id,
                        "caption": decode_text(data.get("CAPTION", "")),
                    }
                    if group["creator_id"] is None:
                        try:
                            group["creator_id"] = int(data.get("CREATOR_ID", "0")) or None
                        except Exception:
                            group["creator_id"] = None
                    group["creator_username"] = decode_text(data.get("CREATOR_USERNAME", ""))
                    group["creator_name"] = decode_text(data.get("CREATOR_NAME", ""))

                elif record_type == "link_deleted":
                    code = data.get("CODE")
                    if code:
                        deleted_codes.add(code)

                elif record_type == "user":
                    try:
                        user_id = int(data["USER_ID"])
                    except Exception:
                        continue
                    if insert_user_if_missing(
                        user_id,
                        decode_text(data.get("USERNAME", "")),
                        decode_text(data.get("FIRST_NAME", "")),
                        data.get("DATE_JOIN", now()),
                    ):
                        result["users"] += 1

                elif record_type == "admin":
                    try:
                        user_id = int(data["USER_ID"])
                        added_by = int(data.get("ADDED_BY", "0"))
                    except Exception:
                        continue
                    if user_id in OWNERS:
                        continue
                    if not db_execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,), fetchone=True):
                        db_execute(
                            "INSERT OR IGNORE INTO admins(user_id,added_by,date_added) VALUES(?,?,?)",
                            (user_id, added_by, data.get("DATE_ADDED", now())),
                        )
                        result["admins"] += 1

                elif record_type == "admin_removed":
                    try:
                        user_id = int(data["USER_ID"])
                        db_execute("DELETE FROM admins WHERE user_id=?", (user_id,))
                    except Exception:
                        pass

                elif record_type == "ban":
                    try:
                        user_id = int(data["USER_ID"])
                        banned_by = int(data.get("BANNED_BY", "0"))
                    except Exception:
                        continue
                    if user_id in OWNERS:
                        continue
                    if not db_execute("SELECT 1 FROM banned_users WHERE user_id=?", (user_id,), fetchone=True):
                        db_execute(
                            """
                            INSERT OR IGNORE INTO banned_users
                            (user_id,username,reason,banned_by,date_banned)
                            VALUES(?,?,?,?,?)
                            """,
                            (
                                user_id,
                                decode_text(data.get("USERNAME", "")),
                                decode_text(data.get("REASON", "")),
                                banned_by,
                                data.get("DATE_BANNED", now()),
                            ),
                        )
                        result["bans"] += 1

                elif record_type == "unban":
                    try:
                        user_id = int(data["USER_ID"])
                        db_execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
                    except Exception:
                        pass

                elif record_type == "start":
                    media_type = data.get("MEDIA_TYPE")
                    file_id = data.get("FILE_ID")
                    if media_type in ("photo", "video") and file_id:
                        db_execute(
                            "INSERT OR REPLACE INTO start_msg(id,type,content,caption) VALUES(1,?,?,?)",
                            (media_type, file_id, decode_text(data.get("CAPTION", ""))),
                        )

                elif record_type == "forced_channel":
                    raw = data.get("DATA", "")
                    try:
                        channel = json.loads(raw)
                        # Recovery must not write a new record back to Storage.
                        set_state("forced_channel", json.dumps(channel, ensure_ascii=False))
                    except Exception:
                        pass

                elif record_type == "forced_channel_disabled":
                    set_state("forced_channel", "")

            except Exception as record_error:
                print("RECOVERY RECORD ERROR:", repr(record_error))
                continue

        # Build each media group only when all expected indexes exist.
        for code, group in media_groups_recovery.items():
            if db_execute("SELECT 1 FROM links WHERE code=?", (code,), fetchone=True):
                continue
            if code in deleted_codes:
                continue
            total = group["total"]
            items = group["items"]
            if len(items) != total:
                print(f"RECOVERY: damaged album skipped: {code}")
                continue
            ordered = []
            valid = True
            for index in range(total):
                if index not in items:
                    valid = False
                    break
                ordered.append(items[index])
            if not valid:
                continue

            first = ordered[0]
            try:
                creator_id = int(group.get("creator_id") or 0) or None
            except Exception:
                creator_id = None
            creator_username = group.get("creator_username", "")
            creator_name = group.get("creator_name", "")
            created_at = group.get("created_at") or now()
            db_execute(
                """
                INSERT OR IGNORE INTO links
                (code,type,content,caption,creator_id,creator_username,creator_name,created_at,deleted)
                VALUES(?,?,?,?,?,?,?,?,0)
                """,
                (
                    code, "media", json.dumps(ordered, ensure_ascii=False), "",
                    creator_id, creator_username, creator_name, created_at,
                ),
            )
            result["links"] += 1

        # Tombstones always win over older link records.
        for code in deleted_codes:
            db_execute("UPDATE links SET deleted=1 WHERE code=?", (code,))

        return result

    finally:
        try:
            client.disconnect()
        except Exception:
            pass

def database_needs_recovery():
    # A newly-created SQLite database has no recovery marker.
    # Existing databases are left alone; Owner can always run manual Recovery.
    if not os.path.exists(DATABASE_FILE):
        return True
    return get_state("database_recovered") != "1"

def run_recovery(owner_id):
    if not is_owner(owner_id):
        return
    if not recovery_lock.acquire(blocking=False):
        bot.send_message(owner_id, "Recovery يعمل بالفعل.")
        return

    try:
        result = restore_from_storage()
        set_state("database_recovered", "1")
        set_state("last_recovery", now())
        bot.send_message(
            owner_id,
            "تمت استعادة البيانات.\n\n"
            f"الروابط: {result['links']}\n"
            f"المستخدمون: {result['users']}\n"
            f"المشرفون: {result['admins']}\n"
            f"المحظورون: {result['bans']}",
        )
    except Exception as e:
        print("RECOVERY ERROR:", repr(e))
        bot.send_message(owner_id, "فشل Recovery:\n" + str(e))
    finally:
        recovery_lock.release()

# ============================================================
# Boot
# ============================================================

def boot():
    print("========================================")
    print("KRO BOT STARTING")
    print(f"BOT: @{BOT_USERNAME}")
    print(f"STORAGE: {STORAGE_CHAT_ID}")
    print("========================================")

    try:
        chat = bot.get_chat(STORAGE_CHAT_ID)
        member = bot.get_chat_member(STORAGE_CHAT_ID, BOT_ID)
        print(f"Storage: {chat.title} | Bot status: {member.status}")
    except Exception as e:
        print("STORAGE HEALTH ERROR:", repr(e))

    if database_needs_recovery():
        if get_telethon_session_string():
            print("SQLite is new/uninitialized. Starting Storage Recovery...")
            try:
                result = restore_from_storage()
                set_state("database_recovered", "1")
                set_state("last_recovery", now())
                print("Initial Recovery:", result)
            except Exception as e:
                print("INITIAL RECOVERY FAILED:", repr(e))
        else:
            print(
                "SQLite is new/uninitialized, but no Telethon user session is "
                "saved yet. Skipping automatic Recovery on boot - use the "
                "'Add Recovery Number' button, then run Recovery manually."
            )

    try:
        queued = flush_storage_queue()
        if queued:
            print(f"Storage queue flushed: {queued}")
    except Exception as e:
        print("INITIAL STORAGE QUEUE FLUSH FAILED:", repr(e))

    threading.Thread(target=storage_sync_loop, daemon=True).start()

    print("BOT STARTED")
    bot.infinity_polling(
        skip_pending=True,
        timeout=30,
        long_polling_timeout=30,
    )

if __name__ == "__main__":
    boot()
