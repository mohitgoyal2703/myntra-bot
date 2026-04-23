import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
# 🔴 STOP CONTROL
if os.getenv("STOP_BOT") == "1":
    print("Bot is stopped via env")
    exit()
    
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("myntra_bot")

# =========================
# ENV / CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_PATH = os.getenv("RAZORPAY_WEBHOOK_PATH", "/razorpay/webhook")

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data" if Path("/app").exists() else "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

PRODUCTS_FILE = DATA_DIR / "products.json"
ORDER_HISTORY_FILE = DATA_DIR / "order_history.json"
USERS_FILE = DATA_DIR / "users.json"
PROCESSED_WEBHOOKS_FILE = DATA_DIR / "processed_webhooks.json"
APPROVED_UTRS_FILE = DATA_DIR / "approved_utrs.json"
MAINTENANCE_FILE = DATA_DIR / "maintenance.json"

LOW_UPI = os.getenv("LOW_UPI", "Q703671699@ybl")
HIGH_UPI = os.getenv("HIGH_UPI", "mohitgoy1al21-1@okhdfcbank")
LOW_QR = os.getenv("LOW_QR", "qr.png")
HIGH_QR = os.getenv("HIGH_QR", "qr2.png")

MAIN_ADMINS = [1232325263]
APPROVE_ADMINS = [1232325263,791363068]
VIEW_ADMINS = [791363068]
ALL_ADMINS = list(dict.fromkeys(MAIN_ADMINS + APPROVE_ADMINS + VIEW_ADMINS))

DEFAULT_PRODUCTS = {
     "p1": {"name": "Myntra 100 Off on 199", "price": 35, "file": str(DATA_DIR / "myntra100on199.txt"), "reserve": 2},
    "p2": {"name": "Myntra 100 Off on 649", "price": 20, "file": str(DATA_DIR / "myntra100on649.txt"), "reserve": 2},
    "p3": {
        "name": "Myntra Combo(100+100)",
        "price": 55,
        "combo_files": [str(DATA_DIR / "myntra100on199.txt"), str(DATA_DIR / "myntra100on649.txt")],
        "reserve": 2,
    },
      "p4": {"name": "Myntra FWD 100 off on 399", "price": 25, "file": str(DATA_DIR / "myntra100on399.txt"), "reserve": 2},
    
   "p5": {"name": "Myntra 50% OFF code", "price": 80, "file": str(DATA_DIR / "myntra50.txt"), "reserve": 2},
    
}

# =========================
# GLOBALS
# =========================
DATA_LOCK = threading.RLock()
BOT_APP: Optional[Application] = None
BOT_LOOP: Optional[asyncio.AbstractEventLoop] = None


# =========================
# BASIC HELPERS
# =========================
def now_str() -> str:
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")

ORDER_EXPIRY_MINUTES = int(os.getenv("ORDER_EXPIRY_MINUTES", "30"))


def parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            pass
    return None


def is_order_expired(order: Optional[Dict[str, Any]]) -> bool:
    if not order:
        return True

    status = (order.get("status") or "").lower()
    if status in {"approved", "payment_received", "manual_review_pending", "rejected", "cancelled", "expired"}:
        return False

    created_at = parse_time(order.get("created_at"))
    if not created_at:
        return False

    return (datetime.now() - created_at).total_seconds() > ORDER_EXPIRY_MINUTES * 60


def atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def load_json(path: Path, default: Any) -> Any:
    with DATA_LOCK:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default


def save_json(path: Path, data: Any) -> None:
    with DATA_LOCK:
        atomic_write_text(path, json.dumps(data, indent=2, ensure_ascii=False))


def ensure_file(path: Path, default_text: str = "") -> None:
    with DATA_LOCK:
        if not path.exists():
            atomic_write_text(path, default_text)


def sanitize_data_filename(name: str) -> Optional[str]:
    cleaned = Path(name or "").name.strip()
    if not cleaned or cleaned in {".", ".."}:
        return None
    return cleaned


def list_data_files() -> List[Path]:
    try:
        return sorted([p for p in DATA_DIR.iterdir() if p.is_file()], key=lambda p: p.name.lower())
    except Exception:
        return []


def count_file_lines(path: Path) -> int:
    try:
        return len(path.read_text(encoding="utf-8").splitlines())
    except Exception:
        return 0


def is_protected_data_file(path: Path) -> bool:
    protected = {
        PRODUCTS_FILE.resolve(),
        ORDER_HISTORY_FILE.resolve(),
        USERS_FILE.resolve(),
        PROCESSED_WEBHOOKS_FILE.resolve(),
        APPROVED_UTRS_FILE.resolve(),
        MAINTENANCE_FILE.resolve(),
    }
    try:
        return path.resolve() in protected
    except Exception:
        return False


def load_maintenance_state() -> Dict[str, Any]:
    return load_json(MAINTENANCE_FILE, {"enabled": False, "reason": "Upgrading, try again later."})


def save_maintenance_state(enabled: bool, reason: Optional[str] = None, changed_by: Optional[str] = None) -> None:
    current = load_maintenance_state()
    payload = {
        "enabled": bool(enabled),
        "reason": reason or current.get("reason") or "Upgrading, try again later.",
        "changed_by": changed_by or current.get("changed_by") or "system",
        "updated_at": now_str(),
    }
    save_json(MAINTENANCE_FILE, payload)


def maintenance_enabled() -> bool:
    return bool(load_maintenance_state().get("enabled"))


def maintenance_reason() -> str:
    return str(load_maintenance_state().get("reason") or "Upgrading, try again later.")


def user_blocked_by_maintenance(user_id: int) -> bool:
    return maintenance_enabled() and user_id not in MAIN_ADMINS


# =========================
# DATA INIT
# =========================
def normalize_product_paths(product: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(product)
    if "file" in normalized:
        normalized["file"] = str(Path(normalized["file"]))
    if "combo_files" in normalized:
        normalized["combo_files"] = [str(Path(x)) for x in normalized["combo_files"]]
    try:
        normalized["reserve"] = max(0, int(normalized.get("reserve", 2)))
    except Exception:
        normalized["reserve"] = 2
    return normalized


def ensure_products() -> None:
    with DATA_LOCK:
        current = load_json(PRODUCTS_FILE, {}) if PRODUCTS_FILE.exists() else {}
        changed = False

        for pid, pdata in DEFAULT_PRODUCTS.items():
            if pid not in current:
                current[pid] = normalize_product_paths(pdata)
                changed = True

        # normalize live paths so repo-relative old files are migrated to data dir once
        for pid, pdata in list(current.items()):
            if not isinstance(pdata, dict):
                continue
            updated = dict(pdata)
            if pid in DEFAULT_PRODUCTS:
                default_item = DEFAULT_PRODUCTS[pid]
                if "file" in default_item:
                    updated["file"] = default_item["file"]
                if "combo_files" in default_item:
                    updated["combo_files"] = default_item["combo_files"]
                if "reserve" not in updated:
                    updated["reserve"] = default_item.get("reserve", 2)
            elif "reserve" not in updated:
                updated["reserve"] = 2
            normalized_updated = normalize_product_paths(updated)
            if normalized_updated != pdata:
                changed = True
            current[pid] = normalized_updated

        if changed or not PRODUCTS_FILE.exists():
            save_json(PRODUCTS_FILE, current)

        # create stock files if missing
        for pdata in current.values():
            if isinstance(pdata, dict):
                if pdata.get("file"):
                    ensure_file(Path(pdata["file"]))
                for fname in pdata.get("combo_files", []):
                    ensure_file(Path(fname))


ensure_products()


# =========================
# PRODUCT / STOCK HELPERS
# =========================
def load_products() -> Dict[str, Dict[str, Any]]:
    ensure_products()
    return load_json(PRODUCTS_FILE, DEFAULT_PRODUCTS)


def save_products(products: Dict[str, Dict[str, Any]]) -> None:
    normalized_products = {pid: normalize_product_paths(pdata) for pid, pdata in products.items()}
    save_json(PRODUCTS_FILE, normalized_products)


products = load_products()


def reload_products() -> None:
    global products
    products = load_products()


def refresh_runtime_file_state() -> None:
    """Reload file-backed runtime state after any admin file change."""
    ensure_products()
    reload_products()


def read_codes_file(path_str: str) -> List[str]:
    path = Path(path_str)
    try:
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except Exception:
        return []


def write_codes_file(path_str: str, codes: List[str]) -> None:
    path = Path(path_str)
    text = "\n".join(codes)
    if text:
        text += "\n"
    atomic_write_text(path, text)


def get_stock(filename: str) -> int:
    with DATA_LOCK:
        return len(read_codes_file(filename))


def get_product_stock(product: Dict[str, Any]) -> int:
    if product.get("combo_files"):
        stocks = [get_stock(f) for f in product["combo_files"]]
        return min(stocks) if stocks else 0
    return get_stock(product["file"])


def get_product_reserve(product: Dict[str, Any]) -> int:
    try:
        return max(0, int(product.get("reserve", 2)))
    except Exception:
        return 2


def get_visible_product_stock(product: Dict[str, Any]) -> int:
    return max(0, get_product_stock(product) - get_product_reserve(product))


def get_codes_for_product(product: Dict[str, Any], qty: int) -> Optional[List[str]]:
    with DATA_LOCK:
        if product.get("combo_files"):
            files = product["combo_files"]
            pools = {fname: read_codes_file(fname) for fname in files}
            if any(len(codes) < qty for codes in pools.values()):
                return None

            delivered = []
            for i in range(qty):
                parts = []
                for idx, fname in enumerate(files):
                    code = pools[fname][i]
                    label = (
                        "₹100 Off" if "100" in fname else "₹150 Off" if "150" in fname else f"Code {idx + 1}"
                    )
                    parts.append(f"{label}: {code}")
                delivered.append(" | ".join(parts))

            for fname in files:
                write_codes_file(fname, pools[fname][qty:])
            return delivered

        file_name = product["file"]
        codes = read_codes_file(file_name)
        if len(codes) < qty:
            return None
        selected = codes[:qty]
        write_codes_file(file_name, codes[qty:])
        return selected


# =========================
# ORDER / USER HELPERS
# =========================
def load_order_history() -> List[Dict[str, Any]]:
    return load_json(ORDER_HISTORY_FILE, [])


def save_order_history(history: List[Dict[str, Any]]) -> None:
    save_json(ORDER_HISTORY_FILE, history)


def save_user(user) -> None:
    users = load_json(USERS_FILE, [])
    if user.id not in users:
        users.append(user.id)
        save_json(USERS_FILE, users)


def add_order_history(entry: Dict[str, Any]) -> None:
    history = load_order_history()
    history.append(entry)
    save_order_history(history)


def find_order(order_id: str) -> Optional[Dict[str, Any]]:
    history = load_order_history()
    for item in history:
        if item.get("order_id") == order_id:
            return item
    return None


def find_order_by_payment_link(payment_link_id: str) -> Optional[Dict[str, Any]]:
    history = load_order_history()
    for item in history:
        if item.get("razorpay_payment_link_id") == payment_link_id:
            return item
    return None


def update_order(order_id: str, **updates: Any) -> Optional[Dict[str, Any]]:
    history = load_order_history()
    for item in history:
        if item.get("order_id") == order_id:
            item.update(updates)
            item["updated_at"] = now_str()
            save_order_history(history)
            return item
    return None


def mark_webhook_processed(event_key: str) -> bool:
    processed = set(load_json(PROCESSED_WEBHOOKS_FILE, []))
    if event_key in processed:
        return False
    processed.add(event_key)
    save_json(PROCESSED_WEBHOOKS_FILE, sorted(processed))
    return True


def load_approved_utrs() -> List[Dict[str, Any]]:
    return load_json(APPROVED_UTRS_FILE, [])


def save_approved_utrs(data: List[Dict[str, Any]]) -> None:
    save_json(APPROVED_UTRS_FILE, data)


def normalize_utr(raw: str) -> str:
    return "".join(ch for ch in (raw or "").strip().upper() if ch.isalnum())


def validate_payment_reference(raw: str) -> Optional[Dict[str, str]]:
    normalized = normalize_utr(raw)
    if not normalized:
        return None

    if normalized.isdigit() and len(normalized) in (12, 15):
        return {"type": "utr", "value": normalized}

    if re.fullmatch(r"T260[A-Z0-9]{4,20}", normalized):
        return {"type": "transaction_id", "value": normalized}

    return None


def find_duplicate_utr(utr: str, exclude_order_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    normalized = normalize_utr(utr)
    if not normalized:
        return None

    for item in load_approved_utrs():
        approved_value = item.get("payment_reference") or item.get("utr", "")
        if normalize_utr(approved_value) == normalized:
            return {"source": "approved_payment_reference", **item}

    return None


def save_approved_utr(order: Dict[str, Any], approved_by: str) -> None:
    payment_reference = normalize_utr(order.get("payment_reference") or order.get("utr", ""))
    if not payment_reference:
        return

    payment_reference_type = order.get("payment_reference_type") or ("utr" if payment_reference.isdigit() else "transaction_id")

    data = load_approved_utrs()
    for item in data:
        approved_value = item.get("payment_reference") or item.get("utr", "")
        if normalize_utr(approved_value) == payment_reference:
            return

    data.append({
        "payment_reference": payment_reference,
        "type": payment_reference_type,
        "utr": payment_reference,
        "order_id": order.get("order_id"),
        "user_id": order.get("user_id"),
        "product_name": order.get("product_name"),
        "amount": order.get("total"),
        "approved_by": approved_by,
        "approved_at": now_str(),
    })
    save_approved_utrs(data)


# =========================
# RAZORPAY API
# =========================
def has_razorpay_config() -> bool:
    return bool(RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET and RAZORPAY_WEBHOOK_SECRET and PUBLIC_BASE_URL)


def razorpay_request(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"https://api.razorpay.com{path}"
    body = None
    headers = {"Content-Type": "application/json"}

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    token = base64.b64encode(f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}".encode()).decode()
    headers["Authorization"] = f"Basic {token}"

    req = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
    ctx = ssl.create_default_context()

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Razorpay API error {e.code}: {raw}")


def create_razorpay_payment_link(order: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "amount": int(order["total"] * 100),
        "currency": "INR",
        "accept_partial": False,
        "reference_id": order["order_id"],
        "description": f"{order['product_name']} x {order['qty']}",
        "notify": {"sms": False, "email": False},
        "reminder_enable": False,
        "callback_method": "get",
        "callback_url": f"{PUBLIC_BASE_URL}/paid?order_id={urllib.parse.quote(order['order_id'])}",
        "notes": {
            "order_id": order["order_id"],
            "telegram_user_id": str(order["user_id"]),
            "product_id": order["product_id"],
            "qty": str(order["qty"]),
        },
    }
    return razorpay_request("POST", "/v1/payment_links", payload)


def fetch_razorpay_payment_link(payment_link_id: str) -> Dict[str, Any]:
    return razorpay_request("GET", f"/v1/payment_links/{payment_link_id}")


# =========================
# TELEGRAM UI
# =========================

def user_panel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["🛒 Buy Codes", "♻️ Recovery"]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def main_menu_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("🛒 Buy Codes", callback_data="menu_buy"),
            InlineKeyboardButton("♻️ Recovery", callback_data="menu_recovery"),
        ]]
    )

def back_to_menu_inline_keyboard() -> Optional[InlineKeyboardMarkup]:
    return None


def normalize_order_id(raw: str) -> str:
    return "".join(ch for ch in (raw or "").strip().upper() if ch.isalnum())


def find_order_flexible(raw_order_id: str) -> Optional[Dict[str, Any]]:
    normalized = normalize_order_id(raw_order_id)
    if not normalized:
        return None

    history = load_order_history()
    for item in history:
        if normalize_order_id(item.get("order_id", "")) == normalized:
            return item
    return None


def clear_user_input_states(context: ContextTypes.DEFAULT_TYPE, *, keep: Optional[set] = None) -> None:
    keep = keep or set()
    state_keys = {
        "awaiting_recovery_order_id",
        "awaiting_qty",
        "pending_order_qty",
        "custom_qty_prompt_id",
        "awaiting_payment_reference_order_id",
        "awaiting_file_create_name",
        "awaiting_file_rename",
        "awaiting_file_append",
        "awaiting_file_replace",
        "awaiting_file_duplicate",
        "selected_manage_file",
        "pending_file_delete",
        "pending_file_clear",
    }
    for key in state_keys - keep:
        context.user_data.pop(key, None)


def order_is_recoverable(order: Optional[Dict[str, Any]]) -> bool:
    if not order:
        return False
    return order.get("status") in {"approved", "payment_received"}


async def send_recovered_codes(chat_id: int, order: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> None:
    codes = order.get("delivered_codes") or []
    if codes:
        await context.bot.send_message(
            chat_id,
            f"✅ Recovery Successful\n\nOrder ID: {order.get('order_id')}\n\nYour Codes:\n" + "\n".join(codes),
        )
        return

    if order.get("status") == "payment_received":
        ok = await deliver_order_automatically(order["order_id"], "Recovery by user", order.get("payment_data"))
        refreshed = find_order(order["order_id"]) or order
        if ok and refreshed.get("delivered_codes"):
            await context.bot.send_message(
                chat_id,
                f"✅ Recovery Successful\n\nOrder ID: {refreshed.get('order_id')}\n\nYour Codes:\n" + "\n".join(refreshed.get("delivered_codes", [])),
            )
            return

    await context.bot.send_message(chat_id, "Payment not received for this order.")


async def safe_answer(query, text: Optional[str] = None, show_alert: bool = False) -> None:
    try:
        await query.answer(text=text, show_alert=show_alert)
    except Exception:
        pass


def disclaimer_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I Agree and Pay Now", callback_data="disc_pay")],
        [InlineKeyboardButton("🚫 Cancel Order", callback_data="disc_cancel")],
    ])


def disclaimer_text() -> str:
    return (
        "⚠️ Important Notice\n\n"
        "Please read before proceeding:\n\n"
        "• This is a digital product (coupon/code)\n"
        "• No refund / no replacement once codes are delivered\n"
        "• Make sure you are purchasing the correct product & quantity\n"
        "• Do not share your codes with anyone\n"
        "• For manual payments, wrong or duplicate UTR will lead to rejection\n\n"
        "By continuing, you agree to all the above terms."
    )

async def show_disclaimer_screen(chat_id: int, context: ContextTypes.DEFAULT_TYPE, qty: int) -> None:
    product_id = context.user_data.get("product")
    reload_products()

    if product_id not in products:
        await context.bot.send_message(chat_id, "Invalid product", reply_markup=back_to_menu_inline_keyboard())
        return

    product = products[product_id]
    stock = get_visible_product_stock(product)
    if stock <= 0:
        await context.bot.send_message(chat_id, "This product is out of stock", reply_markup=back_to_menu_inline_keyboard())
        return
    if qty > stock:
        await context.bot.send_message(chat_id, f"Only {stock} left", reply_markup=back_to_menu_inline_keyboard())
        return

    context.user_data["pending_order_qty"] = qty
    await context.bot.send_message(
        chat_id,
        disclaimer_text(),
        reply_markup=disclaimer_keyboard(),
    )


async def disclaimer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer(query)

    action = query.data.split("_", 1)[1]
    qty = context.user_data.get("pending_order_qty")
    product_id = context.user_data.get("product")

    try:
        await query.message.delete()
    except Exception:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

    if action == "cancel":
        context.user_data.pop("pending_order_qty", None)
        await context.bot.send_message(query.message.chat_id, "🚫 Order Cancelled")
        return

    if action == "pay":
        if not qty or not product_id:
            context.user_data.pop("pending_order_qty", None)
            await context.bot.send_message(query.message.chat_id, "Order session expired. Please start again.")
            return
        context.user_data.pop("pending_order_qty", None)
        await process_order(query.message.chat_id, context, int(qty))
        return



def build_products_keyboard() -> List[List[InlineKeyboardButton]]:
    keyboard = []
    for product_id, product in products.items():
        visible_stock = get_visible_product_stock(product)
        if visible_stock <= 0:
            text = f"❌ {product['name']} | ₹{product['price']} | Stock: OUT"
            callback = f"outofstock_{product_id}"
        else:
            text = f"{product['name']} | ₹{product['price']} | Stock: {visible_stock}"
            callback = f"select_{product_id}"
        keyboard.append([InlineKeyboardButton(text, callback_data=callback)])
    return keyboard


def order_action_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I Paid", callback_data=f"paid_{order_id}")],
    ])


def build_admin_review_caption(order: Dict[str, Any], status_line: Optional[str] = None) -> str:
    username = order.get("username")
    username_text = f"@{username}" if username else "NoUsername"
    lines = [
        "🧾 New Payment",
        "",
        f"Order ID: {order.get('order_id')}",
        f"User: {order.get('name', 'Unknown')}",
        f"Username: {username_text}",
        f"User ID: {order.get('user_id')}",
        f"Product: {order.get('product_name')}",
        f"Qty: {order.get('qty')}",
        f"Total: ₹{order.get('total')}",
    ]
    payment_reference = order.get("payment_reference") or order.get("utr")
    payment_reference_type = order.get("payment_reference_type") or ("utr" if (payment_reference or "").isdigit() else "transaction_id")
    if payment_reference:
        label = "UTR" if payment_reference_type == "utr" else "TRANSACTION ID"
        lines.append(f"🔥 PAYMENT REFERENCE: {payment_reference}")
        lines.append(f"Type: {label}")
    if status_line:
        lines.extend(["", status_line])
    return "\n".join(lines)


async def update_admin_review_messages(
    context: ContextTypes.DEFAULT_TYPE,
    order: Dict[str, Any],
    status_line: Optional[str] = None,
) -> None:
    text = build_admin_review_caption(order, status_line)
    for item in order.get("admin_messages", []):
        try:
            await context.bot.edit_message_text(
                chat_id=item["admin_id"],
                message_id=item["message_id"],
                text=text,
                reply_markup=None,
            )
        except Exception:
            try:
                await context.bot.edit_message_reply_markup(
                    chat_id=item["admin_id"],
                    message_id=item["message_id"],
                    reply_markup=None,
                )
            except Exception:
                pass


# =========================
# BOT HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and user_blocked_by_maintenance(update.message.from_user.id):
        await update.message.reply_text(f"⚠️ {maintenance_reason()}")
        return
    context.user_data.clear()
    if update.message:
        save_user(update.message.from_user)
        user = update.message.from_user.first_name
        await update.message.reply_text(
            f"━━━━━━━━━━━━━━\n"
            f"✨ MYNTRA CODES\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"Welcome, {user} 👋\n\n"
            f"Fast delivery ⚡\n"
            f"Easy payment 💳\n"
            f"Instant support 💬",
            reply_markup=user_panel_keyboard(),
        )

async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query and user_blocked_by_maintenance(update.callback_query.from_user.id):
        await safe_answer(update.callback_query, text=maintenance_reason(), show_alert=True)
        return
    query = update.callback_query
    await safe_answer(query)

    if query.data in ["menu_home", "menu_buy"]:
        clear_user_input_states(context)
        reload_products()
        keyboard = build_products_keyboard()
        if not keyboard:
            try:
                await query.message.edit_text("No products available right now")
            except Exception:
                await query.message.reply_text("No products available right now", reply_markup=back_to_menu_inline_keyboard())
            return
        try:
            await query.message.edit_text("Select product:", reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            await query.message.reply_text("Select product:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if query.data == "menu_recovery":
        clear_user_input_states(context, keep={"awaiting_recovery_order_id"})
        context.user_data["awaiting_recovery_order_id"] = True
        try:
            await query.message.edit_text("Send your Order ID to recover codes.")
        except Exception:
            await query.message.reply_text("Send your Order ID to recover codes.")
        return

async def show_user_orders(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    if user_blocked_by_maintenance(user_id):
        return
    history = load_order_history()
    user_orders = [x for x in history if x.get("user_id") == user_id]

    if not user_orders:
        await context.bot.send_message(user_id, "No order history found", reply_markup=back_to_menu_inline_keyboard())
        return

    lines = ["📦 Your Order History\n"]
    for item in user_orders[-10:][::-1]:
        order_id = item.get("order_id", "Old order")
        payment_link = item.get("payment_link_short_url")
        payment_line = f"Payment Link: {payment_link}\n" if payment_link else ""
        payment_reference = item.get("payment_reference") or item.get("utr")
        reference_line = f"Payment Reference: {payment_reference}\n" if payment_reference else ""
        lines.append(
            f"🧾 {item.get('product_name')}\n"
            f"Order ID: {order_id}\n"
            f"Qty: {item.get('qty')} | Total: ₹{item.get('total')}\n"
            f"Status: {item.get('status')}\n"
            f"{reference_line}"
            f"{payment_line}"
            f"Time: {item.get('created_at')}\n"
        )

    await context.bot.send_message(user_id, "\n".join(lines), reply_markup=back_to_menu_inline_keyboard())


async def select_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query and user_blocked_by_maintenance(update.callback_query.from_user.id):
        await safe_answer(update.callback_query, text=maintenance_reason(), show_alert=True)
        return
    query = update.callback_query
    await safe_answer(query)

    product_id = query.data.split("_", 1)[1]
    reload_products()

    if product_id not in products:
        await query.message.reply_text("Invalid product", reply_markup=back_to_menu_inline_keyboard())
        return

    stock = get_visible_product_stock(products[product_id])
    if stock <= 0:
        await query.message.reply_text("This product is out of stock", reply_markup=back_to_menu_inline_keyboard())
        return

    context.user_data["product"] = product_id

    keyboard = [
        [InlineKeyboardButton("1", callback_data="qty_1"), InlineKeyboardButton("2", callback_data="qty_2"), InlineKeyboardButton("3", callback_data="qty_3")],
        [InlineKeyboardButton("5", callback_data="qty_5"), InlineKeyboardButton("10", callback_data="qty_10")],
        [InlineKeyboardButton("✍️ Custom", callback_data="qty_custom")],
        [InlineKeyboardButton("⬅ Back", callback_data="menu_buy")],
    ]
    await query.message.edit_text(
        f"{stock} in stock\n\nSelect quantity:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def paid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query and user_blocked_by_maintenance(update.callback_query.from_user.id):
        await safe_answer(update.callback_query, text=maintenance_reason(), show_alert=True)
        return
    query = update.callback_query
    await safe_answer(query)

    order_id = query.data.split("_", 1)[1]
    order = find_order_flexible(order_id)
    if not order:
        await query.message.reply_text("Order not found", reply_markup=back_to_menu_inline_keyboard())
        return

    if order.get("user_id") != query.from_user.id:
        await safe_answer(query, text="This button is not for you", show_alert=True)
        return

    if order.get("status") == "cancelled":
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("This order is already cancelled.")
        return

    if order.get("status") == "approved":
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("This order is already approved and delivered.")
        return

    if is_order_expired(order):
        update_order(order_id, status="expired", expired_at=now_str())
        context.user_data.pop("awaiting_payment_reference_order_id", None)
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("⌛ This order has expired.\nPlease create a new order.")
        return

    clear_user_input_states(context, keep={"awaiting_payment_reference_order_id"})

    if order.get("payment_mode") == "razorpay_payment_link":
        refreshed_order = order
        payment_link_id = order.get("razorpay_payment_link_id")

        if payment_link_id and has_razorpay_config():
            try:
                remote_link = fetch_razorpay_payment_link(payment_link_id)
                remote_status = (remote_link.get("status") or "").lower()
                payments = remote_link.get("payments") or []
                paid_payment = payments[0] if payments else {}
                if remote_status == "paid" or paid_payment.get("id"):
                    refreshed_order = update_order(
                        order_id,
                        status="payment_received",
                        payment_received_at=now_str(),
                        razorpay_payment_id=paid_payment.get("id") or order.get("razorpay_payment_id"),
                        payment_data=remote_link,
                    ) or refreshed_order
            except Exception as e:
                logger.exception("Failed checking Razorpay payment link status")
                await query.message.reply_text(f"Could not verify payment right now: {e}")
                return

        if refreshed_order.get("status") == "payment_received" or refreshed_order.get("razorpay_payment_id"):
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass

            await query.message.reply_text("🔍 Verifying payment...")
            ok = await deliver_order_automatically(order_id, "Razorpay payment verified", refreshed_order.get("payment_data"))
            if not ok:
                await query.message.reply_text("Payment found, but delivery could not be completed right now. Admin has been notified.")
            return

        await query.message.reply_text("Payment not detected yet.\n\nIf already paid, please wait a few seconds and tap 'I Paid' again.")
        return

    update_order(
        order_id,
        status="awaiting_payment_reference",
        username=query.from_user.username,
        name=query.from_user.first_name,
    )
    context.user_data["awaiting_payment_reference_order_id"] = order_id

    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="Enter your UTR / Transaction ID"
    )

async def cancel_order_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query and user_blocked_by_maintenance(update.callback_query.from_user.id):
        await safe_answer(update.callback_query, text=maintenance_reason(), show_alert=True)
        return
    query = update.callback_query
    await safe_answer(query)

    order_id = query.data.split("_", 1)[1]
    order = find_order_flexible(order_id)
    if not order:
        await query.message.reply_text("Order not found")
        return

    if order.get("user_id") != query.from_user.id:
        await safe_answer(query, text="This button is not for you", show_alert=True)
        return

    if order.get("status") in {"approved", "cancelled"}:
        await query.message.reply_text(
            "This order is already approved and cannot be cancelled."
            if order.get("status") == "approved" else
            "This order is already cancelled."
        )
        return

    update_order(order_id, status="cancelled", cancelled_at=now_str())
    context.user_data.pop("awaiting_payment_reference_order_id", None)

    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await context.bot.send_message(query.message.chat_id, f"🚫 Order Cancelled\n\nOrder ID: {order_id}")


async def out_of_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query and user_blocked_by_maintenance(update.callback_query.from_user.id):
        await safe_answer(update.callback_query, text=maintenance_reason(), show_alert=True)
        return
    query = update.callback_query
    await safe_answer(query, text="This product is out of stock", show_alert=True)


async def select_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query and user_blocked_by_maintenance(update.callback_query.from_user.id):
        await safe_answer(update.callback_query, text=maintenance_reason(), show_alert=True)
        return
    query = update.callback_query
    await safe_answer(query)

    if query.data == "qty_custom":
        context.user_data["awaiting_qty"] = True
        try:
            await query.message.delete()
        except Exception:
            pass
        prompt = await context.bot.send_message(
            query.message.chat_id,
            "Enter quantity:",
        )
        context.user_data["custom_qty_prompt_id"] = prompt.message_id
        return

    qty = int(query.data.split("_")[1])
    context.user_data["qty"] = qty
    try:
        await query.message.delete()
    except Exception:
        pass
    await show_disclaimer_screen(query.message.chat_id, context, qty)


async def custom_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        qty = int(update.message.text)
        if qty <= 0:
            await update.message.reply_text("Invalid quantity")
            return

        context.user_data["awaiting_qty"] = False
        context.user_data["qty"] = qty
        context.user_data.pop("custom_qty_prompt_id", None)
        await show_disclaimer_screen(update.message.chat_id, context, qty)
    except Exception:
        await update.message.reply_text("Send number only")


async def process_order(chat_id: int, context: ContextTypes.DEFAULT_TYPE, qty: int) -> None:
    user = context._user_id_and_data[0] if hasattr(context, "_user_id_and_data") else None
    tg_user_id = chat_id
    product_id = context.user_data.get("product")
    reload_products()

    if product_id not in products:
        await context.bot.send_message(chat_id, "Invalid product", reply_markup=back_to_menu_inline_keyboard())
        return

    product = products[product_id]
    stock = get_visible_product_stock(product)
    if stock <= 0:
        await context.bot.send_message(chat_id, "This product is out of stock", reply_markup=back_to_menu_inline_keyboard())
        return
    if qty > stock:
        await context.bot.send_message(chat_id, f"Only {stock} left", reply_markup=back_to_menu_inline_keyboard())
        return

    total = product["price"] * qty
    order_id = f"ORD{datetime.now().strftime('%d%m%H%M%S')}{str(chat_id)[-4:]}"
    context.user_data["last_order_id"] = order_id
    context.user_data["total"] = total

    order = {
        "order_id": order_id,
        "user_id": tg_user_id,
        "username": None,
        "name": None,
        "product_id": product_id,
        "product_name": product["name"],
        "qty": qty,
        "total": total,
        "status": "creating_payment_link",
        "created_at": now_str(),
        "delivered_codes": [],
        "admin_messages": [],
        "payment_mode": "razorpay_payment_link",
    }
    add_order_history(order)

    if has_razorpay_config():
        try:
            payment_link = create_razorpay_payment_link(order)
            update_order(
                order_id,
                status="payment_link_created",
                razorpay_payment_link_id=payment_link.get("id"),
                payment_link_short_url=payment_link.get("short_url"),
                payment_link_status=payment_link.get("status"),
                payment_link_created_at=now_str(),
            )
            await context.bot.send_message(
                chat_id,
                (
                    f"🧾 Order Summary\n\n"
                    f"{product['name']}\n"
                    f"Qty: {qty}\n"
                    f"Stock: {stock}\n"
                    f"Total: ₹{total}\n"
                    f"Order ID: {order_id}\n\n"
                    f"💳 Pay here:\n{payment_link.get('short_url')}"
                ),
                reply_markup=order_action_keyboard(order_id),
            )
            return
        except Exception as e:
            logger.exception("Failed creating payment link")
            update_order(order_id, status="payment_link_failed", payment_link_error=str(e))
            await context.bot.send_message(
                chat_id,
                (
                    f"Automatic payment link could not be created right now.\n\n"
                    f"Order ID: {order_id}\n"
                    f"You can pay manually using the UPI below."
                ),
            )

    # manual backup
    upi = HIGH_UPI if total >= 400 else LOW_UPI
    qr = HIGH_QR if total >= 400 else LOW_QR
    update_order(order_id, status="manual_payment_pending", upi=upi, payment_mode="manual_upi")

    caption = (
        f"🧾 Order Summary\n\n"
        f"{product['name']}\n"
        f"Qty: {qty}\n"
        f"Total: ₹{total}\n"
        f"Order ID: {order_id}\n\n"
        f"UPI: `{upi}` (tap to copy)"
    )

    try:
        with open(qr, "rb") as photo:
            await context.bot.send_photo(
                chat_id,
                photo,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=order_action_keyboard(order_id),
            )
    except Exception:
        await context.bot.send_message(
            chat_id,
            caption,
            parse_mode="Markdown",
            reply_markup=order_action_keyboard(order_id),
        )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_data_file_upload"):
        await update.message.reply_text("Please send the file as a document, not as a photo.", reply_markup=admin_data_files_keyboard())
        return

    await update.message.reply_text(
        "❌ Please send only your UTR / Transaction ID in text.\nPhotos or screenshots are not accepted."
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        await update.message.reply_text("Document received, but no upload action is active right now.")
        return

    document = update.message.document

    if context.user_data.get("awaiting_file_replace"):
        file_name = context.user_data.pop("awaiting_file_replace", None)
        safe_target = sanitize_data_filename(file_name)
        if not safe_target:
            await update.message.reply_text("Invalid file")
            return
        if not document:
            await update.message.reply_text("Invalid document")
            return

        target_path = DATA_DIR / safe_target
        tmp_path = target_path.with_suffix(target_path.suffix + ".upload")
        try:
            telegram_file = await context.bot.get_file(document.file_id)
            await telegram_file.download_to_drive(str(tmp_path))
            os.replace(tmp_path, target_path)
            refresh_runtime_file_state()
        except Exception as e:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            logger.exception("Failed replacing data file from document")
            await update.message.reply_text(f"Replace failed: {e}")
            return

        await update.message.reply_text(
            f"✅ Replaced content of {safe_target}",
            reply_markup=build_single_file_manage_keyboard(safe_target, is_protected_data_file(target_path)),
        )
        return

    if not context.user_data.get("awaiting_data_file_upload"):
        await update.message.reply_text("Document received, but no upload action is active right now.")
        return

    safe_name = sanitize_data_filename(document.file_name if document else "")
    if not document or not safe_name:
        await update.message.reply_text("Invalid filename. Please rename the file and try again.", reply_markup=admin_data_files_keyboard())
        return

    target_path = DATA_DIR / safe_name
    existed_before = target_path.exists()
    tmp_path = target_path.with_suffix(target_path.suffix + ".upload")

    try:
        telegram_file = await context.bot.get_file(document.file_id)
        await telegram_file.download_to_drive(str(tmp_path))
        os.replace(tmp_path, target_path)
        refresh_runtime_file_state()
    except Exception as e:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        logger.exception("Failed saving uploaded data file")
        await update.message.reply_text(f"Upload failed: {e}", reply_markup=admin_data_files_keyboard())
        return

    context.user_data.pop("awaiting_data_file_upload", None)
    action_text = "overwritten" if existed_before else "uploaded"
    await update.message.reply_text(
        f"✅ File {action_text} in data folder\nName: {safe_name}",
        reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(target_path)),
    )


async def clear_admin_buttons(context: ContextTypes.DEFAULT_TYPE, order: Dict[str, Any]) -> None:
    for item in order.get("admin_messages", []):
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=item["admin_id"],
                message_id=item["message_id"],
                reply_markup=None,
            )
        except Exception:
            pass


async def deliver_order_automatically(order_id: str, source: str, payment_data: Optional[Dict[str, Any]] = None) -> bool:
    reload_products()
    order = find_order(order_id)
    if not order:
        logger.warning("Order not found for delivery: %s", order_id)
        return False

    if order.get("status") == "approved":
        logger.info("Order already approved: %s", order_id)
        return True

    product_id = order["product_id"]
    if product_id not in products:
        update_order(order_id, status="failed_invalid_product")
        if BOT_APP:
            await BOT_APP.bot.send_message(order["user_id"], "Sorry, product is invalid now.")
        return False

    product = products[product_id]
    stock = get_product_stock(product)
    if stock < order["qty"]:
        update_order(order_id, status="paid_but_no_stock", payment_data=payment_data or {})
        if BOT_APP:
            await BOT_APP.bot.send_message(
                order["user_id"],
                "Payment received, but stock is currently insufficient. Admin has been notified.",
                
            )
            for admin in ALL_ADMINS:
                try:
                    await BOT_APP.bot.send_message(admin, f"⚠️ Paid but no stock\nOrder ID: {order_id}")
                except Exception:
                    pass
        return False

    codes = get_codes_for_product(product, order["qty"])
    if not codes:
        update_order(order_id, status="paid_but_no_stock", payment_data=payment_data or {})
        return False

    update_order(
        order_id,
        status="approved",
        delivered_codes=codes,
        auto_approved=True,
        approved_by_name=source,
        payment_data=payment_data or {},
    )
    order = find_order(order_id) or order
    save_approved_utr(order, source)

    if BOT_APP:
        await BOT_APP.bot.send_message(
            order["user_id"],
            "✅ Payment Approved\n\n🎟 Your Codes:\n" + "\n".join(codes) + "\n\nThank you for your purchase 🙌",
        )
    return True


async def admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer(query)

    if query.from_user.id not in APPROVE_ADMINS:
        await query.answer("Not allowed", show_alert=True)
        return

    action, order_id = query.data.split("_", 1)
    order = find_order(order_id)
    if not order:
        await query.message.edit_reply_markup(reply_markup=None)
        await query.message.reply_text("Order not found or already processed")
        return

    if order.get("status") not in ["manual_review_pending", "manual_payment_pending", "payment_link_failed"]:
        await query.message.edit_reply_markup(reply_markup=None)
        await query.answer(f"Already {order.get('status')}", show_alert=True)
        return

    if action == "approve":
        ok = await deliver_order_automatically(order_id, f"manual approval by {query.from_user.first_name}")
        order = find_order(order_id) or order
        if ok:
            save_approved_utr(order, query.from_user.first_name)
            await update_admin_review_messages(
                context,
                order,
                f"✅ Approved by {query.from_user.first_name}",
            )
            return
        await clear_admin_buttons(context, order)
        await query.message.reply_text("Could not deliver due to stock/problem.")
        return

    if action == "reject":
        update_order(
            order_id,
            status="rejected",
            rejected_by=query.from_user.id,
            rejected_by_name=query.from_user.first_name,
        )
        order = find_order(order_id) or order
        await update_admin_review_messages(
            context,
            order,
            f"❌ Rejected by {query.from_user.first_name}",
        )
        await context.bot.send_message(
            order["user_id"],
            "❌ Payment Rejected\n\nIf you believe this is a mistake, please contact support."
        )


async def add_codes_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return

    try:
        reload_products()
        product_id = context.args[0]
        if product_id not in products:
            await update.message.reply_text("Invalid product ID")
            return

        product = products[product_id]
        if product.get("combo_files"):
            files_text = ", ".join(product["combo_files"])
            await update.message.reply_text(f"This is a combo product.\nAdd codes separately in these files:\n{files_text}")
            return

        context.user_data["adding_codes"] = product_id
        await update.message.reply_text("Send codes (one per line)")
    except Exception:
        await update.message.reply_text("Usage: /addcodes p1")


async def add_codes_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    product_id = context.user_data.get("adding_codes")
    if not product_id:
        return False

    reload_products()
    if product_id not in products:
        context.user_data["adding_codes"] = None
        await update.message.reply_text("Invalid product ID")
        return True

    file_name = products[product_id]["file"]
    codes = [code.strip() for code in update.message.text.strip().split("\n") if code.strip()]

    with DATA_LOCK:
        existing = read_codes_file(file_name)
        existing.extend(codes)
        write_codes_file(file_name, existing)

    context.user_data["adding_codes"] = None
    await update.message.reply_text(f"Added {len(codes)} codes")
    return True


async def add_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return

    try:
        raw = " ".join(context.args)
        product_id, name, price, filename = [x.strip() for x in raw.split("|", 3)]
        price = int(price)

        reload_products()
        path = str(DATA_DIR / filename)
        ensure_file(Path(path))
        products[product_id] = {"name": name, "price": price, "file": path, "reserve": 2}
        save_products(products)
        await update.message.reply_text(
            f"Product added\nID: {product_id}\nName: {name}\nPrice: ₹{price}\nFile: {path}"
        )
    except Exception:
        await update.message.reply_text("Usage:\n/addproduct p4|Myntra ₹200 Off|60|myntra200.txt")


async def delete_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return

    if not context.args:
        await update.message.reply_text("Usage: /delproduct PRODUCT_ID")
        return

    product_id = context.args[0].strip()
    reload_products()

    if product_id not in products:
        await update.message.reply_text("Invalid product ID")
        return

    if product_id in DEFAULT_PRODUCTS:
        await update.message.reply_text("Default product cannot be deleted")
        return

    product = products.pop(product_id)
    save_products(products)

    await update.message.reply_text(
        f"✅ Product deleted\nID: {product_id}\nName: {product.get('name', '-')}"
    )


async def list_products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reload_products()
    lines = ["📦 Products\n"]
    for product_id, product in products.items():
        real_stock = get_product_stock(product)
        reserve = get_product_reserve(product)
        visible_stock = get_visible_product_stock(product)
        lines.append(
            f"{product_id} | {product['name']} | ₹{product['price']} | Real: {real_stock} | Reserve: {reserve} | Visible: {visible_stock}"
        )
    await update.message.reply_text("\n".join(lines))


async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and user_blocked_by_maintenance(update.message.from_user.id):
        await update.message.reply_text(f"⚠️ {maintenance_reason()}")
        return
    reload_products()
    keyboard = build_products_keyboard()
    if not keyboard:
        await update.message.reply_text("No products available right now")
        return
    await update.message.reply_text("Select product:", reply_markup=InlineKeyboardMarkup(keyboard))


async def orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and user_blocked_by_maintenance(update.message.from_user.id):
        await update.message.reply_text(f"⚠️ {maintenance_reason()}")
        return
    await show_user_orders(update.message.chat_id, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and user_blocked_by_maintenance(update.message.from_user.id):
        await update.message.reply_text(f"⚠️ {maintenance_reason()}")
        return
    await update.message.reply_text(
        "❓ Need Help?\n\n📩 Contact: @myntracodes\n\nPlz contact for any support or help or feedback\n\n Thank you.",
        reply_markup=ReplyKeyboardRemove(),
    )

async def utrs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    await send_approved_utrs(update.message.chat_id, context)

async def lockbot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    reason = " ".join(context.args).strip() or "Bot under maintenance. Try again later."
    save_maintenance_state(True, reason, update.message.from_user.first_name)
    await update.message.reply_text(f"🔒 Bot locked\nReason: {reason}")


async def unlockbot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    save_maintenance_state(False, maintenance_reason(), update.message.from_user.first_name)
    await update.message.reply_text("🔓 Bot unlocked")


async def botstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    state = load_maintenance_state()
    await update.message.reply_text(
        f"Maintenance: {'ON' if state.get('enabled') else 'OFF'}\nReason: {state.get('reason')}\nUpdated: {state.get('updated_at', '-') }"
    )



async def admin_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return

    history = load_order_history()
    if not history:
        await update.message.reply_text("No orders found")
        return

    lines = ["📊 All Orders\n"]
    for item in history[-25:][::-1]:
        username = item.get("username")
        username_text = f"@{username}" if username else "NoUsername"
        delivered_codes = item.get("delivered_codes", [])
        delivered_info = f"\n🎟 Delivered Codes: {len(delivered_codes)}" if delivered_codes else ""
        payment_reference = item.get("payment_reference") or item.get("utr")
        payment_reference_type = item.get("payment_reference_type")
        reference_info = ""
        if payment_reference:
            label = "UTR" if payment_reference_type == "utr" else "TRANSACTION ID"
            reference_info = f"\n🔥 {label}: {payment_reference}"
        lines.append(
            f"👤 {item.get('name', 'Unknown')} | {username_text}\n"
            f"🆔 {item.get('user_id')}\n"
            f"📦 {item.get('product_name')}\n"
            f"🧾 Order ID: {item.get('order_id', 'Old order')}\n"
            f"🔢 Qty: {item.get('qty')} | ₹{item.get('total')}\n"
            f"📌 Status: {item.get('status')}{delivered_info}{reference_info}\n"
            f"🕒 {item.get('created_at')}\n"
        )
    await update.message.reply_text("\n".join(lines))


async def delivered_codes_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    if not context.args:
        await update.message.reply_text("Usage: /delivered ORDER_ID")
        return

    order_id = context.args[0].strip()
    order = find_order_flexible(order_id)
    if not order:
        await update.message.reply_text("Order not found")
        return

    codes = order.get("delivered_codes", [])
    if not codes:
        await update.message.reply_text("No delivered codes found for this order")
        return

    await update.message.reply_text(f"Order ID: {order_id}\n\nDelivered Codes:\n" + "\n".join(codes))


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📦 Products", callback_data="admin_products"), InlineKeyboardButton("📊 Stock", callback_data="admin_stock")],
            [InlineKeyboardButton("🧾 Orders", callback_data="admin_orders"), InlineKeyboardButton("🎟 Delivered", callback_data="admin_delivered")],
            [InlineKeyboardButton("📄 Approved UTRs", callback_data="admin_utrs"), InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
            [InlineKeyboardButton("🗂 Data Files", callback_data="admin_datafiles")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_home")],
        ]
    )


def admin_products_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for pid, product in products.items():
        visible_stock = get_visible_product_stock(product)
        reserve = get_product_reserve(product)
        buttons.append([
            InlineKeyboardButton(
                f"💰 {pid} | ₹{product['price']} | V:{visible_stock} | R:{reserve}",
                callback_data=f"admin_setprice_{pid}"
            ),
            InlineKeyboardButton("🗑 Delete", callback_data=f"admin_deleteproduct_{pid}"),
        ])
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data="admin_home")])
    return InlineKeyboardMarkup(buttons)


def admin_stock_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for pid in products.keys():
        buttons.append(
            [
                InlineKeyboardButton(f"📤 Export {pid}", callback_data=f"admin_export_{pid}"),
                InlineKeyboardButton(f"🗑 Clear {pid}", callback_data=f"admin_clear_{pid}"),
            ]
        )
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data="admin_home")])
    return InlineKeyboardMarkup(buttons)


def admin_data_files_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📋 List / Manage Files", callback_data="admin_datafiles_list")],
            [InlineKeyboardButton("📥 Upload / Overwrite", callback_data="admin_datafiles_upload"), InlineKeyboardButton("➕ Create Empty", callback_data="admin_datafile_create")],
            [InlineKeyboardButton("📤 Export All Files", callback_data="admin_datafiles_exportall")],
            [InlineKeyboardButton("⬅ Back", callback_data="admin_home")],
        ]
    )


def build_data_file_export_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for file_path in list_data_files():
        buttons.append([InlineKeyboardButton(f"📄 {file_path.name}", callback_data=f"admin_datafile_export::{file_path.name}")])
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data="admin_datafiles")])
    return InlineKeyboardMarkup(buttons)


def build_data_file_manage_list_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for file_path in list_data_files():
        size = 0
        try:
            size = file_path.stat().st_size
        except Exception:
            pass
        buttons.append([InlineKeyboardButton(f"📄 {file_path.name} | {size}B", callback_data=f"admin_datafile_manage::{file_path.name}")])
    buttons.append([InlineKeyboardButton("➕ Create Empty File", callback_data="admin_datafile_create")])
    buttons.append([InlineKeyboardButton("📤 Export All Files", callback_data="admin_datafiles_exportall")])
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data="admin_datafiles")])
    return InlineKeyboardMarkup(buttons)


def build_single_file_manage_keyboard(file_name: str, protected: bool = False) -> InlineKeyboardMarkup:
    warn = "⚠️" if protected else ""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Preview", callback_data=f"admin_datafile_preview::{file_name}"), InlineKeyboardButton("📤 Export", callback_data=f"admin_datafile_export::{file_name}")],
        [InlineKeyboardButton("✍️ Append", callback_data=f"admin_datafile_append::{file_name}"), InlineKeyboardButton("🧾 Replace", callback_data=f"admin_datafile_replace::{file_name}")],
        [InlineKeyboardButton("✏️ Rename", callback_data=f"admin_datafile_rename::{file_name}"), InlineKeyboardButton("📄 Duplicate", callback_data=f"admin_datafile_duplicate::{file_name}")],
        [InlineKeyboardButton(f"{warn}🗑 Delete", callback_data=f"admin_datafile_deleteask::{file_name}"), InlineKeyboardButton(f"{warn}🧹 Clear", callback_data=f"admin_datafile_clearask::{file_name}")],
        [InlineKeyboardButton("⬅ Back", callback_data="admin_datafiles_list")],
    ])


def build_confirm_keyboard(action: str, file_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm", callback_data=f"admin_datafile_confirm::{action}::{file_name}"), InlineKeyboardButton("❌ Cancel", callback_data=f"admin_datafile_cancel::{file_name}")]
    ])


async def send_data_file(chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_name: str) -> bool:
    safe_name = sanitize_data_filename(file_name)
    if not safe_name:
        await context.bot.send_message(chat_id, "Invalid file name", reply_markup=admin_data_files_keyboard())
        return False

    file_path = DATA_DIR / safe_name
    if not file_path.exists() or not file_path.is_file():
        await context.bot.send_message(chat_id, f"File not found: {safe_name}", reply_markup=admin_data_files_keyboard())
        return False

    with open(file_path, "rb") as f:
        await context.bot.send_document(chat_id, f, filename=safe_name)
    return True


async def send_data_file_preview(chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_name: str) -> bool:
    safe_name = sanitize_data_filename(file_name)
    if not safe_name:
        await context.bot.send_message(chat_id, "Invalid file name")
        return False
    file_path = DATA_DIR / safe_name
    if not file_path.exists() or not file_path.is_file():
        await context.bot.send_message(chat_id, f"File not found: {safe_name}")
        return False
    try:
        raw = file_path.read_text(encoding="utf-8")
    except Exception:
        await context.bot.send_message(chat_id, f"Preview not available for {safe_name}. Export the file instead.")
        return False
    preview = raw[:3500] if raw else "<empty file>"
    await context.bot.send_message(
        chat_id,
        f"📄 {safe_name}\nSize: {file_path.stat().st_size} bytes\nLines: {count_file_lines(file_path)}\nProtected: {'Yes' if is_protected_data_file(file_path) else 'No'}\n\nPreview:\n{preview}",
        reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(file_path)),
    )
    return True


async def send_data_files_list(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    files = list_data_files()
    if not files:
        await context.bot.send_message(chat_id, "Data folder is empty", reply_markup=admin_data_files_keyboard())
        return

    lines = ["🗂 Data Folder Files\n"]
    for file_path in files:
        try:
            size = file_path.stat().st_size
        except Exception:
            size = 0
        lines.append(f"{file_path.name} | {size} bytes")

    await context.bot.send_message(chat_id, "\n".join(lines), reply_markup=build_data_file_manage_list_keyboard())


async def show_admin_panel(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str = "👑 Admin Panel") -> None:
    await context.bot.send_message(chat_id, text, reply_markup=admin_panel_keyboard())


async def admin_panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    context.user_data.pop("awaiting_admin_price", None)
    context.user_data.pop("awaiting_admin_delivered", None)
    context.user_data.pop("awaiting_broadcast", None)
    context.user_data.pop("awaiting_data_file_upload", None)
    context.user_data.pop("awaiting_file_create_name", None)
    context.user_data.pop("awaiting_file_rename", None)
    context.user_data.pop("awaiting_file_append", None)
    context.user_data.pop("awaiting_file_replace", None)
    context.user_data.pop("awaiting_file_duplicate", None)
    context.user_data.pop("selected_manage_file", None)
    context.user_data.pop("pending_file_delete", None)
    context.user_data.pop("pending_file_clear", None)
    await show_admin_panel(update.message.chat_id, context)


async def show_stock_text(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    reload_products()
    lines = ["📊 Stock Overview\n"]
    for pid, product in products.items():
        real_stock = get_product_stock(product)
        reserve = get_product_reserve(product)
        visible_stock = get_visible_product_stock(product)
        lines.append(
            f"{pid} | {product['name']} | ₹{product['price']} | Real: {real_stock} | Reserve: {reserve} | Visible: {visible_stock}"
        )
    await context.bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_stock_keyboard())


async def send_recent_orders(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    history = load_order_history()
    if not history:
        await context.bot.send_message(chat_id, "No orders found", reply_markup=admin_panel_keyboard())
        return

    lines = ["🧾 Recent Orders\n"]
    for item in history[-15:][::-1]:
        payment_reference = item.get("payment_reference") or item.get("utr")
        payment_reference_type = item.get("payment_reference_type")
        reference_line = ""
        if payment_reference:
            label = "UTR" if payment_reference_type == "utr" else "TRANSACTION ID"
            reference_line = f" | 🔥 {label}: {payment_reference}"
        lines.append(
            f"{item.get('order_id', '-')} | {item.get('product_name')} | Qty {item.get('qty')} | ₹{item.get('total')} | {item.get('status')}{reference_line}"
        )
    await context.bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_panel_keyboard())


async def send_approved_utrs(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = load_approved_utrs()
    if not data:
        await context.bot.send_message(chat_id, "No approved payment references found", reply_markup=admin_panel_keyboard())
        return

    lines = ["📄 Approved Payment References\n"]
    for item in data[-25:][::-1]:
        payment_reference = item.get("payment_reference") or item.get("utr")
        payment_type = item.get("type") or ("utr" if str(payment_reference).isdigit() else "transaction_id")
        label = "UTR" if payment_type == "utr" else "TRANSACTION ID"
        lines.append(
            f"🔥 {label}: {payment_reference} | {item.get('order_id')} | ₹{item.get('amount')} | {item.get('approved_at')}"
        )
    await context.bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_panel_keyboard())


async def admin_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer(query)

    if query.from_user.id not in MAIN_ADMINS:
        await safe_answer(query, text="Not allowed", show_alert=True)
        return

    reload_products()
    data = query.data

    if data == "admin_home":
        context.user_data.pop("awaiting_admin_price", None)
        context.user_data.pop("awaiting_admin_delivered", None)
        context.user_data.pop("awaiting_broadcast", None)
        context.user_data.pop("awaiting_data_file_upload", None)
        context.user_data.pop("awaiting_file_create_name", None)
        context.user_data.pop("awaiting_file_rename", None)
        context.user_data.pop("awaiting_file_append", None)
        context.user_data.pop("awaiting_file_replace", None)
        context.user_data.pop("awaiting_file_duplicate", None)
        context.user_data.pop("selected_manage_file", None)
        context.user_data.pop("pending_file_delete", None)
        context.user_data.pop("pending_file_clear", None)
        await query.message.reply_text("👑 Admin Panel", reply_markup=admin_panel_keyboard())
        return

    if data == "admin_products":
        await query.message.reply_text("Tap a product to edit price or delete it", reply_markup=admin_products_keyboard())
        return

    if data == "admin_stock":
        await show_stock_text(query.message.chat_id, context)
        return

    if data == "admin_orders":
        await send_recent_orders(query.message.chat_id, context)
        return

    if data == "admin_delivered":
        context.user_data["awaiting_admin_delivered"] = True
        await query.message.reply_text("Send Order ID to view delivered codes")
        return

    if data == "admin_utrs":
        await send_approved_utrs(query.message.chat_id, context)
        return

    if data == "admin_broadcast":
        context.user_data["awaiting_broadcast"] = True
        await query.message.reply_text("Send broadcast message now")
        return

    if data == "admin_datafiles":
        context.user_data.pop("awaiting_data_file_upload", None)
        await query.message.reply_text("🗂 Data Files", reply_markup=admin_data_files_keyboard())
        return

    if data == "admin_datafiles_list":
        await send_data_files_list(query.message.chat_id, context)
        return

    if data == "admin_datafiles_upload":
        context.user_data["awaiting_data_file_upload"] = True
        await query.message.reply_text(
            "Send the file now as a Telegram document. If a file with the same name already exists in the data folder, it will be overwritten.",
            reply_markup=admin_data_files_keyboard(),
        )
        return

    if data == "admin_datafiles_exportall":
        files = list_data_files()
        if not files:
            await query.message.reply_text("Data folder is empty", reply_markup=admin_data_files_keyboard())
            return
        sent = 0
        for file_path in files:
            try:
                with open(file_path, "rb") as f:
                    await context.bot.send_document(query.message.chat_id, f, filename=file_path.name)
                sent += 1
            except Exception:
                logger.exception("Failed exporting data file: %s", file_path)
        await query.message.reply_text(f"✅ Exported {sent} file(s) from data folder", reply_markup=admin_data_files_keyboard())
        return

    if data.startswith("admin_datafile_export::"):
        file_name = data.split("::", 1)[1]
        ok = await send_data_file(query.message.chat_id, context, file_name)
        if ok:
            await query.message.reply_text(f"✅ Exported {file_name}", reply_markup=build_data_file_export_keyboard())
        return

    if data == "admin_datafile_create":
        context.user_data["awaiting_file_create_name"] = True
        await query.message.reply_text("Send new file name with extension, for example: test.txt")
        return

    if data.startswith("admin_datafile_manage::"):
        file_name = data.split("::", 1)[1]
        safe_name = sanitize_data_filename(file_name)
        if not safe_name:
            await query.message.reply_text("Invalid file name")
            return
        file_path = DATA_DIR / safe_name
        if not file_path.exists():
            await query.message.reply_text("File not found")
            return
        context.user_data["selected_manage_file"] = safe_name
        await query.message.reply_text(
            f"Manage file: {safe_name}\nSize: {file_path.stat().st_size} bytes\nLines: {count_file_lines(file_path)}\nProtected: {'Yes' if is_protected_data_file(file_path) else 'No'}",
            reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(file_path)),
        )
        return

    if data.startswith("admin_datafile_preview::"):
        file_name = data.split("::", 1)[1]
        await send_data_file_preview(query.message.chat_id, context, file_name)
        return

    if data.startswith("admin_datafile_append::"):
        file_name = data.split("::", 1)[1]
        context.user_data["awaiting_file_append"] = file_name
        await query.message.reply_text(f"Send text to append into {file_name}")
        return

    if data.startswith("admin_datafile_replace::"):
        file_name = data.split("::", 1)[1]
        context.user_data["awaiting_file_replace"] = file_name
        await query.message.reply_text(f"Send full replacement content for {file_name} as text or document.")
        return

    if data.startswith("admin_datafile_rename::"):
        file_name = data.split("::", 1)[1]
        context.user_data["awaiting_file_rename"] = file_name
        await query.message.reply_text(f"Send new file name for {file_name}")
        return

    if data.startswith("admin_datafile_duplicate::"):
        file_name = data.split("::", 1)[1]
        context.user_data["awaiting_file_duplicate"] = file_name
        await query.message.reply_text(f"Send duplicate file name for {file_name}")
        return

    if data.startswith("admin_datafile_deleteask::"):
        file_name = data.split("::", 1)[1]
        context.user_data["pending_file_delete"] = file_name
        p = DATA_DIR / sanitize_data_filename(file_name)
        extra_warning = "\n⚠️ Protected system file." if p.exists() and is_protected_data_file(p) else ""
        await query.message.reply_text(f"Confirm delete: {file_name}{extra_warning}", reply_markup=build_confirm_keyboard("delete", file_name))
        return

    if data.startswith("admin_datafile_clearask::"):
        file_name = data.split("::", 1)[1]
        context.user_data["pending_file_clear"] = file_name
        p = DATA_DIR / sanitize_data_filename(file_name)
        extra_warning = "\n⚠️ Protected system file." if p.exists() and is_protected_data_file(p) else ""
        await query.message.reply_text(f"Confirm clear: {file_name}{extra_warning}", reply_markup=build_confirm_keyboard("clear", file_name))
        return

    if data.startswith("admin_datafile_confirm::"):
        _, action, file_name = data.split("::", 2)
        safe_name = sanitize_data_filename(file_name)
        file_path = DATA_DIR / safe_name
        if not safe_name or not file_path.exists():
            await query.message.reply_text("File not found")
            return
        if action == "delete":
            try:
                file_path.unlink()
                context.user_data.pop("pending_file_delete", None)
                refresh_runtime_file_state()
                await query.message.reply_text(f"✅ Deleted {safe_name}", reply_markup=build_data_file_manage_list_keyboard())
            except Exception as e:
                await query.message.reply_text(f"Delete failed: {e}")
            return
        if action == "clear":
            try:
                atomic_write_text(file_path, "")
                context.user_data.pop("pending_file_clear", None)
                refresh_runtime_file_state()
                await query.message.reply_text(f"✅ Cleared {safe_name}", reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(file_path)))
            except Exception as e:
                await query.message.reply_text(f"Clear failed: {e}")
            return

    if data.startswith("admin_datafile_cancel::"):
        file_name = data.split("::", 1)[1]
        context.user_data.pop("pending_file_delete", None)
        context.user_data.pop("pending_file_clear", None)
        safe_name = sanitize_data_filename(file_name)
        await query.message.reply_text(f"Cancelled for {safe_name}", reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(DATA_DIR / safe_name)))
        return

    if data.startswith("admin_deleteproduct_"):
        pid = data.split("admin_deleteproduct_", 1)[1]
        if pid not in products:
            await query.message.reply_text("Invalid product", reply_markup=admin_panel_keyboard())
            return
        deleted = products.pop(pid)
        save_products(products)
        await query.message.reply_text(f"✅ Product deleted\nID: {pid}\nName: {deleted.get('name', '-')}", reply_markup=admin_products_keyboard())
        return

    if data.startswith("admin_setprice_"):
        pid = data.split("admin_setprice_", 1)[1]
        if pid not in products:
            await query.message.reply_text("Invalid product", reply_markup=admin_panel_keyboard())
            return
        context.user_data["awaiting_admin_price"] = pid
        await query.message.reply_text(
            f"Send new price for {pid} ({products[pid]['name']})\nCurrent: ₹{products[pid]['price']}"
        )
        return

    if data.startswith("admin_export_"):
        pid = data.split("admin_export_", 1)[1]
        product = products.get(pid)
        if not product:
            await query.message.reply_text("Invalid product", reply_markup=admin_panel_keyboard())
            return
        if product.get("combo_files"):
            for idx, file_path in enumerate(product["combo_files"], start=1):
                with open(file_path, "rb") as f:
                    await context.bot.send_document(query.message.chat_id, f, filename=f"{pid}_part{idx}.txt")
            await query.message.reply_text("Combo stock exported", reply_markup=admin_stock_keyboard())
            return
        with open(product["file"], "rb") as f:
            await context.bot.send_document(query.message.chat_id, f, filename=f"{pid}.txt")
        await query.message.reply_text("Stock exported", reply_markup=admin_stock_keyboard())
        return

    if data.startswith("admin_clear_"):
        pid = data.split("admin_clear_", 1)[1]
        product = products.get(pid)
        if not product:
            await query.message.reply_text("Invalid product", reply_markup=admin_panel_keyboard())
            return
        if product.get("combo_files"):
            for file_path in product["combo_files"]:
                write_codes_file(file_path, [])
        else:
            write_codes_file(product["file"], [])
        await query.message.reply_text(f"✅ Stock cleared for {pid}", reply_markup=admin_stock_keyboard())
        return


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast your message")
        return

    msg = " ".join(context.args)
    users = load_json(USERS_FILE, [])
    if not users:
        await update.message.reply_text("No users found")
        return

    sent, failed = 0, 0
    for user_id in users:
        try:
            await context.bot.send_message(user_id, msg)
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"Broadcast completed\nSent: {sent}\nFailed: {failed}")


async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /checkpayment ORDER_ID")
        return
    order_id = context.args[0].strip()
    order = find_order_flexible(order_id)
    if not order:
        await update.message.reply_text("Order not found")
        return
    await update.message.reply_text(
        f"Order ID: {order_id}\nStatus: {order.get('status')}\nPayment Link: {order.get('payment_link_short_url', '-') }"
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    lower_text = text.lower()

    if user_blocked_by_maintenance(update.message.from_user.id):
        await update.message.reply_text(f"⚠️ {maintenance_reason()}")
        return

    if context.user_data.get("adding_codes"):
        handled = await add_codes_save(update, context)
        if handled:
            return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_data_file_upload"):
        await update.message.reply_text("Please send the file as a document.", reply_markup=admin_data_files_keyboard())
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_file_create_name"):
        context.user_data.pop("awaiting_file_create_name", None)
        safe_name = sanitize_data_filename(text)
        if not safe_name:
            await update.message.reply_text("Invalid file name")
            return
        target = DATA_DIR / safe_name
        if target.exists():
            await update.message.reply_text("File already exists", reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(target)))
            return
        atomic_write_text(target, "")
        refresh_runtime_file_state()
        await update.message.reply_text(f"✅ Created empty file: {safe_name}", reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(target)))
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_file_rename"):
        old_name = context.user_data.pop("awaiting_file_rename", None)
        safe_old = sanitize_data_filename(old_name)
        safe_new = sanitize_data_filename(text)
        if not safe_old or not safe_new:
            await update.message.reply_text("Invalid file name")
            return
        old_path = DATA_DIR / safe_old
        new_path = DATA_DIR / safe_new
        if not old_path.exists():
            await update.message.reply_text("Source file not found")
            return
        if new_path.exists():
            await update.message.reply_text("Target file already exists")
            return
        old_path.rename(new_path)
        refresh_runtime_file_state()
        await update.message.reply_text(f"✅ Renamed {safe_old} -> {safe_new}", reply_markup=build_single_file_manage_keyboard(safe_new, is_protected_data_file(new_path)))
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_file_duplicate"):
        source_name = context.user_data.pop("awaiting_file_duplicate", None)
        safe_source = sanitize_data_filename(source_name)
        safe_new = sanitize_data_filename(text)
        if not safe_source or not safe_new:
            await update.message.reply_text("Invalid file name")
            return
        src_path = DATA_DIR / safe_source
        dst_path = DATA_DIR / safe_new
        if not src_path.exists():
            await update.message.reply_text("Source file not found")
            return
        if dst_path.exists():
            await update.message.reply_text("Target file already exists")
            return
        atomic_write_text(dst_path, src_path.read_text(encoding="utf-8"))
        refresh_runtime_file_state()
        await update.message.reply_text(f"✅ Duplicated {safe_source} -> {safe_new}", reply_markup=build_single_file_manage_keyboard(safe_new, is_protected_data_file(dst_path)))
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_file_append"):
        file_name = context.user_data.pop("awaiting_file_append", None)
        safe_name = sanitize_data_filename(file_name)
        file_path = DATA_DIR / safe_name
        if not safe_name or not file_path.exists():
            await update.message.reply_text("File not found")
            return
        old = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
        suffix = text if old.endswith("\n") or not old else "\n" + text
        atomic_write_text(file_path, old + suffix + ("\n" if not text.endswith("\n") else ""))
        refresh_runtime_file_state()
        await update.message.reply_text(f"✅ Appended content to {safe_name}", reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(file_path)))
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_file_replace"):
        file_name = context.user_data.pop("awaiting_file_replace", None)
        safe_name = sanitize_data_filename(file_name)
        file_path = DATA_DIR / safe_name
        if not safe_name:
            await update.message.reply_text("Invalid file")
            return
        atomic_write_text(file_path, text + ("\n" if text and not text.endswith("\n") else ""))
        refresh_runtime_file_state()
        await update.message.reply_text(f"✅ Replaced content of {safe_name}", reply_markup=build_single_file_manage_keyboard(safe_name, is_protected_data_file(file_path)))
        return

    navigation_texts = {
        "buy", "my orders", "help", "/start", "/help", "/orders", "/buy", "/admin",
        "🛒 buy codes", "♻️ recovery",
    }

    if context.user_data.get("awaiting_recovery_order_id"):
        if lower_text in navigation_texts:
            if lower_text == "♻️ recovery":
                await update.message.reply_text("Send your Order ID to recover codes.")
                return
            context.user_data.pop("awaiting_recovery_order_id", None)
        else:
            context.user_data.pop("awaiting_recovery_order_id", None)
            order = find_order_flexible(text)
            if not order:
                await update.message.reply_text("Order not found. Please send a valid Order ID.")
                return
            if order.get("user_id") != update.message.from_user.id:
                await update.message.reply_text("❌ You can recover codes only for your own orders.")
                return
            if order_is_recoverable(order):
                await send_recovered_codes(update.message.chat_id, order, context)
            else:
                await update.message.reply_text("Payment not received for this order.")
            return

    if context.user_data.get("awaiting_qty"):
        if lower_text in navigation_texts:
            context.user_data["awaiting_qty"] = False
        else:
            await custom_quantity(update, context)
            return

    # Admin actions first so they do not get trapped in UTR validation
    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_admin_price"):
        try:
            new_price = int(text)
            if new_price <= 0:
                raise ValueError
            pid = context.user_data.pop("awaiting_admin_price")
            reload_products()
            if pid not in products:
                await update.message.reply_text("Invalid product")
                return
            products[pid]["price"] = new_price
            save_products(products)
            await update.message.reply_text(f"✅ Price updated for {pid}: ₹{new_price}", reply_markup=admin_products_keyboard())
            return
        except Exception:
            await update.message.reply_text("Send valid price in number only")
            return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_admin_delivered"):
        context.user_data.pop("awaiting_admin_delivered", None)
        order = find_order_flexible(text)
        if not order:
            await update.message.reply_text("Order not found")
            return
        codes = order.get("delivered_codes", [])
        if not codes:
            await update.message.reply_text("No delivered codes found for this order")
            return
        await update.message.reply_text(
            f"Order ID: {order.get('order_id')}\n\nDelivered Codes:\n" + "\n".join(codes),
        )
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_broadcast"):

        context.user_data.pop("awaiting_broadcast", None)
        users = load_json(USERS_FILE, [])
        if not users:
            await update.message.reply_text("No users found")
            return
        sent, failed = 0, 0
        for user_id in users:
            try:
                await context.bot.send_message(user_id, text)
                sent += 1
            except Exception:
                failed += 1
        await update.message.reply_text(
            f"Broadcast completed\nSent: {sent}\nFailed: {failed}",
        )
        return

    awaiting_order_id = context.user_data.get("awaiting_payment_reference_order_id")
    awaiting_order = find_order(awaiting_order_id) if awaiting_order_id else None

    if awaiting_order_id and lower_text in navigation_texts:
        context.user_data.pop("awaiting_payment_reference_order_id", None)
        awaiting_order_id = None
        awaiting_order = None

    if awaiting_order and awaiting_order.get("user_id") == update.message.from_user.id:
        if is_order_expired(awaiting_order):
            update_order(awaiting_order_id, status="expired", expired_at=now_str())
            context.user_data.pop("awaiting_payment_reference_order_id", None)
            await update.message.reply_text("⌛ This order has expired.\nPlease create a new order.")
            return

        payment_reference_result = validate_payment_reference(text)
        if not payment_reference_result:
            update_order(
                awaiting_order_id,
                status="awaiting_payment_reference",
                rejected_by_name="Auto invalid payment reference check",
            )
            await update.message.reply_text(
                "❌ Invalid UTR / Transaction ID.\nPlease enter a valid payment reference."
            )
            return

        payment_reference = payment_reference_result["value"]
        payment_reference_type = payment_reference_result["type"]

        duplicate = find_duplicate_utr(payment_reference, exclude_order_id=awaiting_order_id)
        if duplicate:
            update_order(
                awaiting_order_id,
                status="awaiting_payment_reference",
                payment_reference=payment_reference,
                payment_reference_type=payment_reference_type,
                rejected_by_name="Auto duplicate approved payment reference check",
            )
            await update.message.reply_text(
                "❌ This UTR / Transaction ID has already been used.\n\nPlease avoid fake payments, otherwise you may be permanently banned."
            )
            return

        update_order(
            awaiting_order_id,
            status="manual_review_pending",
            payment_reference=payment_reference,
            payment_reference_type=payment_reference_type,
            username=update.message.from_user.username,
            name=update.message.from_user.first_name,
        )
        order = find_order(awaiting_order_id) or awaiting_order
        caption = build_admin_review_caption(order)
        admin_messages = []

        for admin in ALL_ADMINS:
            markup = None
            if admin in APPROVE_ADMINS:
                keyboard = [[
                    InlineKeyboardButton("✅ Approve", callback_data=f"approve_{awaiting_order_id}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"reject_{awaiting_order_id}"),
                ]]
                markup = InlineKeyboardMarkup(keyboard)

            sent_msg = await context.bot.send_message(admin, caption, reply_markup=markup)
            admin_messages.append({"admin_id": admin, "message_id": sent_msg.message_id})

        update_order(awaiting_order_id, admin_messages=admin_messages)
        context.user_data.pop("awaiting_payment_reference_order_id", None)
        await update.message.reply_text(
            f"⏳ Payment under review\n\nOrder ID: {awaiting_order_id}\nWaiting for admin approval"
        )
        return

    save_user(update.message.from_user)

    if text in ["🛒 Buy Codes", "🛒 Buy", "buy", "Buy"]:
        clear_user_input_states(context)
        reload_products()
        keyboard = build_products_keyboard()
        if not keyboard:
            await update.message.reply_text("No products available right now")
            return
        await update.message.reply_text("Select product:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if text in ["♻️ Recovery", "recovery", "Recovery"]:
        clear_user_input_states(context, keep={"awaiting_recovery_order_id"})
        context.user_data["awaiting_recovery_order_id"] = True
        await update.message.reply_text("Send your Order ID to recover codes.")
        return
def verify_razorpay_signature(body: bytes, received_sig: str) -> bool:
    expected = hmac.new(
        RAZORPAY_WEBHOOK_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, received_sig or "")


class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "MyntraBotHTTP/1.0"

    def _send_json(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path.startswith("/health") or self.path == "/":
            self._send_json(200, {"ok": True, "service": "telegram-bot", "time": now_str()})
            return
        if self.path.startswith("/paid"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h2>Payment received.</h2><p>Return to Telegram and tap I Paid. For Razorpay orders, the bot will verify payment and deliver automatically. For manual orders, it will ask for your UTR / Transaction ID.</p></body></html>"
            )
            return
        self._send_json(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:
        if self.path != WEBHOOK_PATH:
            self._send_json(404, {"ok": False, "error": "not_found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        signature = self.headers.get("X-Razorpay-Signature", "")

        if not verify_razorpay_signature(body, signature):
            self._send_json(400, {"ok": False, "error": "invalid_signature"})
            return

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._send_json(400, {"ok": False, "error": "invalid_json"})
            return

        event = payload.get("event")
        if event != "payment_link.paid":
            self._send_json(200, {"ok": True, "ignored": event})
            return

        entity = payload.get("payload", {}).get("payment_link", {}).get("entity", {})
        payment_entity = payload.get("payload", {}).get("payment", {}).get("entity", {})
        payment_link_id = entity.get("id")
        order_id = (entity.get("notes") or {}).get("order_id") or entity.get("reference_id")
        payment_id = payment_entity.get("id")
        event_key = f"{event}:{payment_link_id}:{payment_id}"

        if not mark_webhook_processed(event_key):
            self._send_json(200, {"ok": True, "duplicate": True})
            return

        if not order_id and payment_link_id:
            order = find_order_by_payment_link(payment_link_id)
            order_id = order.get("order_id") if order else None

        if not order_id:
            self._send_json(200, {"ok": False, "error": "order_not_found"})
            return

        update_order(
            order_id,
            status="payment_received",
            razorpay_payment_link_id=payment_link_id,
            razorpay_payment_id=payment_id,
            razorpay_event=event,
            payment_received_at=now_str(),
        )

        self._send_json(200, {"ok": True, "order_id": order_id})

    def log_message(self, fmt: str, *args) -> None:
        logger.info("HTTP | " + fmt, *args)


def start_http_server() -> threading.Thread:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), WebhookHandler)

    def serve() -> None:
        logger.info("Webhook server started on port %s path %s", PORT, WEBHOOK_PATH)
        server.serve_forever()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return thread


# =========================
# MAIN
# =========================
async def post_init(app: Application) -> None:
    logger.info("Bot initialized")
    logger.info("Data directory: %s", DATA_DIR)
    logger.info("Webhook enabled: %s", has_razorpay_config())
    if has_razorpay_config():
        logger.info("Set Razorpay webhook URL to: %s%s", PUBLIC_BASE_URL, WEBHOOK_PATH)

    await app.bot.set_my_commands([
        BotCommand("start", "Start bot"),
        BotCommand("buy", "Buy product"),
        BotCommand("orders", "Order history"),
        BotCommand("help", "Help and support"),
        BotCommand("admin", "Admin panel"),
        BotCommand("lockbot", "Lock bot for users"),
        BotCommand("unlockbot", "Unlock bot"),
        BotCommand("botstatus", "Bot maintenance status"),
    ])


async def run_bot() -> None:
    global BOT_APP, BOT_LOOP
    BOT_LOOP = asyncio.get_running_loop()
    BOT_APP = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    BOT_APP.add_handler(CommandHandler("start", start))
    BOT_APP.add_handler(CommandHandler("buy", buy_command))
    BOT_APP.add_handler(CommandHandler("orders", orders_command))
    BOT_APP.add_handler(CommandHandler("help", help_command))
    BOT_APP.add_handler(CommandHandler("admin", admin_panel_command))
    BOT_APP.add_handler(CommandHandler("addcodes", add_codes_start))
    BOT_APP.add_handler(CommandHandler("addproduct", add_product))
    BOT_APP.add_handler(CommandHandler("delproduct", delete_product))
    BOT_APP.add_handler(CommandHandler("products", list_products))
    BOT_APP.add_handler(CommandHandler("allorders", admin_history))
    BOT_APP.add_handler(CommandHandler("delivered", delivered_codes_command))
    BOT_APP.add_handler(CommandHandler("broadcast", broadcast))
    BOT_APP.add_handler(CommandHandler("checkpayment", check_payment))
    BOT_APP.add_handler(CommandHandler("utrs", utrs_command))
    BOT_APP.add_handler(CommandHandler("lockbot", lockbot_command))
    BOT_APP.add_handler(CommandHandler("unlockbot", unlockbot_command))
    BOT_APP.add_handler(CommandHandler("botstatus", botstatus_command))

    BOT_APP.add_handler(CallbackQueryHandler(admin_menu_handler, pattern=r"^admin_"))
    BOT_APP.add_handler(CallbackQueryHandler(menu_handler, pattern=r"^menu_"))
    BOT_APP.add_handler(CallbackQueryHandler(disclaimer_handler, pattern=r"^disc_"))
    BOT_APP.add_handler(CallbackQueryHandler(paid_handler, pattern=r"^paid_"))
    BOT_APP.add_handler(CallbackQueryHandler(cancel_order_handler, pattern=r"^cancel_"))
    BOT_APP.add_handler(CallbackQueryHandler(out_of_stock, pattern=r"^outofstock_"))
    BOT_APP.add_handler(CallbackQueryHandler(select_product, pattern=r"^select_"))
    BOT_APP.add_handler(CallbackQueryHandler(select_quantity, pattern=r"^qty_"))
    BOT_APP.add_handler(CallbackQueryHandler(admin_action, pattern=r"^(approve_|reject_)"))

    BOT_APP.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    BOT_APP.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    BOT_APP.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    start_http_server()

    await BOT_APP.initialize()
    await BOT_APP.start()
    await BOT_APP.updater.start_polling(drop_pending_updates=False)
    logger.info("Telegram polling started")

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await BOT_APP.updater.stop()
        await BOT_APP.stop()
        await BOT_APP.shutdown()


if __name__ == "__main__":
    asyncio.run(run_bot())
