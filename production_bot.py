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

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update
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
BOT_TOKEN = os.getenv("BOT_TOKEN_TEST")
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

LOW_UPI = os.getenv("LOW_UPI", "Q703671699@ybl")
HIGH_UPI = os.getenv("HIGH_UPI", "mohitgoy1al21-1@okhdfcbank")
LOW_QR = os.getenv("LOW_QR", "qr.png")
HIGH_QR = os.getenv("HIGH_QR", "qr2.png")

MAIN_ADMINS = [1232325263]
APPROVE_ADMINS = [1232325263]
VIEW_ADMINS = [791363068]
ALL_ADMINS = list(dict.fromkeys(MAIN_ADMINS + APPROVE_ADMINS + VIEW_ADMINS))

DEFAULT_PRODUCTS = {
    "p1": {"name": "Myntra ₹100 Off", "price": 30, "file": str(DATA_DIR / "myntra100.txt")},
    "p2": {"name": "Myntra ₹150 Off", "price": 25, "file": str(DATA_DIR / "myntra150.txt")},
    "p3": {
        "name": "Myntra Combo",
        "price": 55,
        "combo_files": [str(DATA_DIR / "myntra100.txt"), str(DATA_DIR / "myntra150.txt")],
    },
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


# =========================
# DATA INIT
# =========================
def normalize_product_paths(product: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(product)
    if "file" in normalized:
        normalized["file"] = str(Path(normalized["file"]))
    if "combo_files" in normalized:
        normalized["combo_files"] = [str(Path(x)) for x in normalized["combo_files"]]
    return normalized


def ensure_products() -> None:
    with DATA_LOCK:
        current = load_json(PRODUCTS_FILE, {}) if PRODUCTS_FILE.exists() else {}
        changed = False

        for pid, pdata in DEFAULT_PRODUCTS.items():
            if pid not in current:
                current[pid] = pdata
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
            current[pid] = updated

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
    save_json(PRODUCTS_FILE, products)


products = load_products()


def reload_products() -> None:
    global products
    products = load_products()


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


# =========================
# TELEGRAM UI
# =========================

def main_menu_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🛒 Buy", callback_data="menu_buy")],
            [InlineKeyboardButton("📦 My Orders", callback_data="menu_history")],
            [InlineKeyboardButton("❓ Help", callback_data="menu_help")],
        ]
    )


def back_to_menu_inline_keyboard() -> Optional[InlineKeyboardMarkup]:
    return None


async def safe_answer(query, text: Optional[str] = None, show_alert: bool = False) -> None:
    try:
        await query.answer(text=text, show_alert=show_alert)
    except Exception:
        pass


def build_products_keyboard() -> List[List[InlineKeyboardButton]]:
    keyboard = []
    for product_id, product in products.items():
        stock = get_product_stock(product)
        if stock <= 0:
            text = f"❌ {product['name']} | ₹{product['price']} | Stock: OUT"
            callback = f"outofstock_{product_id}"
        else:
            text = f"{product['name']} | ₹{product['price']} | Stock: {stock}"
            callback = f"select_{product_id}"
        keyboard.append([InlineKeyboardButton(text, callback_data=callback)])
    return keyboard


def paid_button_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ I Paid", callback_data=f"paid_{order_id}")]])


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
            f"Instant support 💬\n\n"
            f"Use the menu button below to continue.",
            reply_markup=ReplyKeyboardRemove(),
        )


async def show_main_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(chat_id, "Choose option:", reply_markup=main_menu_inline_keyboard())


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer(query)

    if query.data in ["menu_home", "menu_buy"]:
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

    elif query.data == "menu_history":
        await show_user_orders(query.message.chat_id, context)

    elif query.data == "menu_help":
        text = (
            "❓ Need Help?\n\n"
            "📩 Contact: @myntracodes\n\n"
            "Use Buy button to purchase.\n"
            "For automatic delivery, complete payment through the Razorpay link sent by the bot."
        )
        await query.message.reply_text(text, reply_markup=back_to_menu_inline_keyboard())


async def show_user_orders(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    query = update.callback_query
    await safe_answer(query)

    product_id = query.data.split("_", 1)[1]
    reload_products()

    if product_id not in products:
        await query.message.reply_text("Invalid product", reply_markup=back_to_menu_inline_keyboard())
        return

    stock = get_product_stock(products[product_id])
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
    query = update.callback_query
    await safe_answer(query)

    order_id = query.data.split("_", 1)[1]
    order = find_order(order_id)
    if not order:
        await query.message.reply_text("Order not found", reply_markup=back_to_menu_inline_keyboard())
        return

    if order.get("user_id") != query.from_user.id:
        await safe_answer(query, text="This button is not for you", show_alert=True)
        return

    if order.get("status") == "approved":
        await query.message.reply_text("This order is already approved and delivered.")
        return

    update_order(
        order_id,
        status="awaiting_payment_reference",
        username=query.from_user.username,
        name=query.from_user.first_name,
    )
    context.user_data["awaiting_payment_reference_order_id"] = order_id
    
    # ❗ remove button by editing message
    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    
    await context.bot.send_message(
    chat_id=query.message.chat_id,
    text="Enter your UTR/ Transaction ID"
)


async def out_of_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer(query, text="This product is out of stock", show_alert=True)


async def select_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    await process_order(query.message.chat_id, context, qty)


async def custom_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        qty = int(update.message.text)
        if qty <= 0:
            await update.message.reply_text("Invalid quantity")
            return

        context.user_data["awaiting_qty"] = False
        context.user_data["qty"] = qty

        prompt_id = context.user_data.pop("custom_qty_prompt_id", None)
        if prompt_id:
            try:
                await context.bot.delete_message(update.message.chat_id, prompt_id)
            except Exception:
                pass

        try:
            await update.message.delete()
        except Exception:
            pass

        await process_order(update.message.chat_id, context, qty)
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
    stock = get_product_stock(product)
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
                reply_markup=paid_button_keyboard(order_id),
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
    update_order(order_id, status="manual_payment_pending", upi=upi)

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
                reply_markup=paid_button_keyboard(order_id),
            )
    except Exception:
        await context.bot.send_message(
            chat_id,
            caption,
            parse_mode="Markdown",
            reply_markup=paid_button_keyboard(order_id),
        )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "❌ Please send only your UTR / Transaction ID in text.\nPhotos or screenshots are not accepted."
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
        products[product_id] = {"name": name, "price": price, "file": path}
        save_products(products)
        await update.message.reply_text(
            f"Product added\nID: {product_id}\nName: {name}\nPrice: ₹{price}\nFile: {path}"
        )
    except Exception:
        await update.message.reply_text("Usage:\n/addproduct p4|Myntra ₹200 Off|60|myntra200.txt")


async def list_products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reload_products()
    lines = ["📦 Products\n"]
    for product_id, product in products.items():
        lines.append(f"{product_id} | {product['name']} | ₹{product['price']} | Stock: {get_product_stock(product)}")
    await update.message.reply_text("\n".join(lines))


async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reload_products()
    keyboard = build_products_keyboard()
    if not keyboard:
        await update.message.reply_text("No products available right now")
        return
    await update.message.reply_text("Select product:", reply_markup=InlineKeyboardMarkup(keyboard))


async def orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_user_orders(update.message.chat_id, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "❓ Need Help?\n\n📩 Contact: @myntracodes\n\nUse /buy to purchase",
        reply_markup=ReplyKeyboardRemove(),
    )


async def utrs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    await send_approved_utrs(update.message.chat_id, context)


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
    order = find_order(order_id)
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
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_home")],
        ]
    )


def admin_products_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for pid, product in products.items():
        buttons.append([InlineKeyboardButton(f"💰 {pid} | ₹{product['price']} | {product['name'][:18]}", callback_data=f"admin_setprice_{pid}")])
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


async def show_admin_panel(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str = "👑 Admin Panel") -> None:
    await context.bot.send_message(chat_id, text, reply_markup=admin_panel_keyboard())


async def admin_panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id not in MAIN_ADMINS:
        return
    context.user_data.pop("awaiting_admin_price", None)
    context.user_data.pop("awaiting_admin_delivered", None)
    context.user_data.pop("awaiting_broadcast", None)
    await show_admin_panel(update.message.chat_id, context)


async def show_stock_text(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    reload_products()
    lines = ["📊 Stock Overview\n"]
    for pid, product in products.items():
        lines.append(f"{pid} | {product['name']} | ₹{product['price']} | Stock: {get_product_stock(product)}")
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
        await query.message.reply_text("👑 Admin Panel", reply_markup=admin_panel_keyboard())
        return

    if data == "admin_products":
        await query.message.reply_text("Tap product to edit price", reply_markup=admin_products_keyboard())
        return

    if data == "admin_stock":
        await show_stock_text(query.message.chat_id, context)
        return

    if data == "admin_orders":
        await send_recent_orders(query.message.chat_id, context)
        return

    if data == "admin_delivered":
        context.user_data["awaiting_admin_delivered"] = True
        await query.message.reply_text("Send Order ID to view delivered codes", reply_markup=admin_panel_keyboard())
        return

    if data == "admin_utrs":
        await send_approved_utrs(query.message.chat_id, context)
        return

    if data == "admin_broadcast":
        context.user_data["awaiting_broadcast"] = True
        await query.message.reply_text("Send broadcast message now", reply_markup=admin_panel_keyboard())
        return

    if data.startswith("admin_setprice_"):
        pid = data.split("admin_setprice_", 1)[1]
        if pid not in products:
            await query.message.reply_text("Invalid product", reply_markup=admin_panel_keyboard())
            return
        context.user_data["awaiting_admin_price"] = pid
        await query.message.reply_text(
            f"Send new price for {pid} ({products[pid]['name']})\nCurrent: ₹{products[pid]['price']}",
            reply_markup=admin_panel_keyboard(),
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
    order = find_order(order_id)
    if not order:
        await update.message.reply_text("Order not found")
        return
    await update.message.reply_text(
        f"Order ID: {order_id}\nStatus: {order.get('status')}\nPayment Link: {order.get('payment_link_short_url', '-') }"
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    lower_text = text.lower()

    if context.user_data.get("adding_codes"):
        handled = await add_codes_save(update, context)
        if handled:
            return

    navigation_texts = {"buy", "my orders", "help", "/start", "/help", "/orders", "/buy", "/admin"}

    if context.user_data.get("awaiting_qty"):
        if lower_text in navigation_texts:
            context.user_data["awaiting_qty"] = False
        else:
            await custom_quantity(update, context)
            return

    awaiting_order_id = context.user_data.get("awaiting_payment_reference_order_id")
    awaiting_order = find_order(awaiting_order_id) if awaiting_order_id else None
    if awaiting_order and awaiting_order.get("user_id") == update.message.from_user.id:
        payment_reference_result = validate_payment_reference(text)
        if not payment_reference_result:
            update_order(
                awaiting_order_id,
                status="awaiting_payment_reference",
                rejected_by_name="Auto invalid payment reference check",
            )
            await update.message.reply_text(
                "❌ Please send a valid UTR / Transaction ID.\n"
                "Invalid or duplicate payment references are strictly prohibited."
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
                "❌ Please send a valid UTR / Transaction ID.\n"
                "Invalid or duplicate payment references are strictly prohibited."
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
            f"⏳ Payment under review\n\nOrder ID: {awaiting_order_id}\nCodes will be delivered shortly"
        )
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_admin_price"):
        try:
            new_price = int(text)
            if new_price <= 0:
                raise ValueError
            pid = context.user_data.pop("awaiting_admin_price")
            reload_products()
            if pid not in products:
                await update.message.reply_text("Invalid product", reply_markup=admin_panel_keyboard())
                return
            products[pid]["price"] = new_price
            save_products(products)
            await update.message.reply_text(f"✅ Price updated for {pid}: ₹{new_price}", reply_markup=admin_products_keyboard())
            return
        except Exception:
            await update.message.reply_text("Send valid price in number only", reply_markup=admin_panel_keyboard())
            return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_admin_delivered"):
        context.user_data.pop("awaiting_admin_delivered", None)
        order = find_order(text)
        if not order:
            await update.message.reply_text("Order not found", reply_markup=admin_panel_keyboard())
            return
        codes = order.get("delivered_codes", [])
        if not codes:
            await update.message.reply_text("No delivered codes found for this order", reply_markup=admin_panel_keyboard())
            return
        await update.message.reply_text(f"Order ID: {text}\n\nDelivered Codes:\n" + "\n".join(codes), reply_markup=admin_panel_keyboard())
        return

    if update.message.from_user.id in MAIN_ADMINS and context.user_data.get("awaiting_broadcast"):
        context.user_data.pop("awaiting_broadcast", None)
        users = load_json(USERS_FILE, [])
        if not users:
            await update.message.reply_text("No users found", reply_markup=admin_panel_keyboard())
            return
        sent, failed = 0, 0
        for user_id in users:
            try:
                await context.bot.send_message(user_id, text)
                sent += 1
            except Exception:
                failed += 1
        await update.message.reply_text(f"Broadcast completed\nSent: {sent}\nFailed: {failed}", reply_markup=admin_panel_keyboard())
        return

    save_user(update.message.from_user)

    if text in ["🛒 Buy", "buy", "Buy"]:
        reload_products()
        keyboard = build_products_keyboard()
        if not keyboard:
            await update.message.reply_text("No products available right now")
            return
        await update.message.reply_text("Select product:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if text in ["📦 My Orders", "my orders", "My Orders"]:
        await show_user_orders(update.message.chat_id, context)
        return

    if text in ["❓ Help", "help", "Help"]:
        await update.message.reply_text(
            "❓ Need Help?\n\n📩 Contact: @myntracodes\n\nUse Buy button to purchase",
            
        )
        return



# =========================
# WEBHOOK SERVER
# =========================
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
                b"<html><body><h2>Payment received.</h2><p>You can return to Telegram, tap I Paid, and send your UTR / Transaction ID for review.</p></body></html>"
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
    BOT_APP.add_handler(CommandHandler("products", list_products))
    BOT_APP.add_handler(CommandHandler("allorders", admin_history))
    BOT_APP.add_handler(CommandHandler("delivered", delivered_codes_command))
    BOT_APP.add_handler(CommandHandler("broadcast", broadcast))
    BOT_APP.add_handler(CommandHandler("checkpayment", check_payment))
    BOT_APP.add_handler(CommandHandler("utrs", utrs_command))

    BOT_APP.add_handler(CallbackQueryHandler(admin_menu_handler, pattern=r"^admin_"))
    BOT_APP.add_handler(CallbackQueryHandler(menu_handler, pattern=r"^menu_"))
    BOT_APP.add_handler(CallbackQueryHandler(paid_handler, pattern=r"^paid_"))
    BOT_APP.add_handler(CallbackQueryHandler(out_of_stock, pattern=r"^outofstock_"))
    BOT_APP.add_handler(CallbackQueryHandler(select_product, pattern=r"^select_"))
    BOT_APP.add_handler(CallbackQueryHandler(select_quantity, pattern=r"^qty_"))
    BOT_APP.add_handler(CallbackQueryHandler(admin_action, pattern=r"^(approve_|reject_)"))

    BOT_APP.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    BOT_APP.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    start_http_server()

    await BOT_APP.initialize()
    await BOT_APP.start()
    await BOT_APP.updater.start_polling(drop_pending_updates=True)
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