import os
import re
import time
import asyncio
import logging
from typing import Union, Any, Dict
from aiogram.filters import Command, CommandStart 
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, Filter, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from typing import List, Dict, Any
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove, ChatJoinRequest
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from dotenv import load_dotenv
from keep_alive import keep_alive
from database import (
    init_db, add_user, get_user_count, get_kino_by_code, get_all_codes,
    delete_kino_code, get_code_stat, increment_stat, get_all_user_ids,
    update_anime_code, get_today_users, add_anime, add_part_to_anime,
    search_anime_by_title, get_channels, add_channel, remove_channel,
    delete_part_from_anime, get_all_admins, add_admin, remove_admin, db_pool, check_user_request, add_join_request
)

logging.basicConfig(level=logging.INFO)

API_TOKEN = os.getenv("API_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME")

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

START_ADMINS = [6486825926, 5492962467]
ADMINS = set(START_ADMINS)
BOT_ACTIVE = True

load_dotenv()
keep_alive()

class AdminStates(StatesGroup):
    waiting_for_kino_data = State()
    waiting_for_delete_code = State()
    waiting_for_stat_code = State()
    waiting_for_broadcast_data = State()
    waiting_for_admin_id = State()
    waiting_for_remove_id = State()
    waiting_for_new_sub_channel = State()
    waiting_for_new_main_channel = State()
    waiting_for_broadcast_type = State()
    waiting_for_forward_data = State()
    waiting_for_simple_message = State()

class AddAnimeStates(StatesGroup):
    waiting_for_code = State()
    waiting_for_title = State()
    waiting_for_genre = State()
    waiting_for_season = State()
    waiting_for_quality = State()
    waiting_for_channel_name = State()
    waiting_for_dubbed_by = State()
    waiting_for_total_parts = State()
    waiting_for_poster = State()
    waiting_for_parts = State()

class PostStates(StatesGroup):
    waiting_for_code = State()

class SearchStates(StatesGroup):
    waiting_for_anime_name = State()

class PartPostStates(StatesGroup):
    waiting_for_anime_code = State()
    waiting_for_part_number = State()
    waiting_for_channel_username = State()



class UserStates(StatesGroup):
    waiting_for_admin_message = State()

class AdminReplyStates(StatesGroup):
    waiting_for_reply_message = State()

class KanalStates(StatesGroup):
    waiting_for_channel_id = State()
    waiting_for_channel_link = State()
    waiting_for_channel_title = State()
    waiting_for_channel_type = State()

# ========== STATELAR ==========
class EditAnimeStates(StatesGroup):
    waiting_for_code = State()
    menu = State()
    waiting_for_new_title = State()
    waiting_for_new_part = State()
    waiting_for_part_delete = State()
    waiting_for_field_value = State()

# ========== INLINE KEYBOARD FUNKSIYALARI ==========

def edit_main_menu_inline_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🎞 Qismlarni tahrirlash", callback_data="edit:parts"))
    builder.row(InlineKeyboardButton(text="📝 Ma'lumotlarni tahrirlash", callback_data="edit:info"))
    return builder.as_markup()

def edit_parts_menu_inline_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Qism qo‘shish", callback_data="edit_parts:add"))
    builder.row(InlineKeyboardButton(text="❌ Qism o‘chirish", callback_data="edit_parts:delete"))
    builder.row(InlineKeyboardButton(text="🔙 Orqaga", callback_data="edit:back_to_main"))
    return builder.as_markup()

def get_broadcast_type_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="📣 Kanaldan yuborish")
    builder.button(text="📰 Oddiy xabar")
    builder.button(text="📡 Boshqarish")
    builder.adjust(2, 1)  # 2 ta tugma bir qatorda, keyin 1 ta
    return builder.as_markup(resize_keyboard=True)

def edit_info_fields_inline_keyboard():
    fields = [
        ("Nomi", "title"),
        ("Janr", "genre"),
        ("Mavsum", "season"),
        ("Sifati", "quality"),
        ("Kanal nomi", "channel_name"),
        ("Ovoz bergan", "dubbed_by"),
        ("Qismlar soni", "total_parts"),
    ]
    builder = InlineKeyboardBuilder()
    for label, key in fields:
        builder.row(InlineKeyboardButton(text=f"✏️ {label}", callback_data=f"edit_field:{key}"))
    builder.row(InlineKeyboardButton(text="🔙 Orqaga", callback_data="edit:back_to_main"))
    return builder.as_markup()

# --- Keyboards (Aiogram 3 uslubida) ---
def start_keyboard_user():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🔍 Anime qidirish"))
    builder.row(
        KeyboardButton(text="🎞 Barcha animelar"),
        KeyboardButton(text="✉️ Admin bilan bog‘lanish")
    )
    return builder.as_markup(resize_keyboard=True)

def admin_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📡 Kanal boshqaruvi"))
    builder.row(
        KeyboardButton(text="❌ Kodni o‘chirish"),
        KeyboardButton(text="➕ Anime qo‘shish"),
        KeyboardButton(text="✏️ Kodni tahrirlash")
    )
    builder.row(
        KeyboardButton(text="📄 Kodlar ro‘yxati"),
        KeyboardButton(text="📈 Kod statistikasi"),
        KeyboardButton(text="📊 Statistika")
    )
    builder.row(
        KeyboardButton(text="👥 Adminlar"),
        KeyboardButton(text="📢 Habar yuborish"),
        KeyboardButton(text="📤 Post qilish")
    )
    builder.row(
        KeyboardButton(text="🎞 Qism post qilish"),
        KeyboardButton(text="🤖 Bot holati")
    )
    return builder.as_markup(resize_keyboard=True)

async def send_admin_panel(message: Message):
    await message.answer("👮 Admin panel:", reply_markup=admin_keyboard())

# --- DeepLink Filter ---
class DeepLinkFilter(Filter):
    def __init__(self, pattern: Union[str, re.Pattern]):
        if isinstance(pattern, str):
            self.pattern = re.compile(pattern)
        else:
            self.pattern = pattern

    async def __call__(self, message: Message, command: CommandObject) -> Union[bool, Dict[str, Any]]:
        if not command or not command.args:
            return False
        
        match = self.pattern.match(command.args)
        if match:
            return {"match": match, "args": command.args}
        return False

# --- Helper functions ---
async def get_unsubscribed_channels(user_id: int):
    # Adminlarni o'tkazib yuborish
    admins = await get_all_admins()
    if user_id in admins:
        return []  # Adminlar uchun hech qanday kanal talab qilinmaydi

    unsubscribed = []
    channels_data = await get_channels('sub')
    for channel in channels_data:
        channel_id = channel['cid']
        channel_link = channel['link']
        channel_title = channel['title']
        mode = channel.get('mode', 'ochiq')

        if mode == 'sorovli':
            is_requested = await check_user_request(user_id, channel_id)
            if not is_requested:
                unsubscribed.append((channel_id, channel_link, channel_title))
        else:
            try:
                member = await bot.get_chat_member(channel_id, user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    unsubscribed.append((channel_id, channel_link, channel_title))
            except Exception as e:
                logging.error(f"Obuna tekshirishda xato: {e}")
                unsubscribed.append((channel_id, channel_link, channel_title))
    return unsubscribed

async def make_unsubscribed_markup(user_id, code):
    unsubscribed = await get_unsubscribed_channels(user_id)
    builder = InlineKeyboardBuilder()
    for _, link, title in unsubscribed:
        builder.row(InlineKeyboardButton(text=f"➕ {title}", url=link))
    
    builder.row(InlineKeyboardButton(text="✅ Tekshirish", callback_data=f"checksub:{code}"))
    return builder.as_markup()

@dp.chat_join_request()
async def on_join_request(event: ChatJoinRequest):
    user_id = event.from_user.id
    channel_id = event.chat.id
    await add_join_request(user_id, channel_id)  # ✅ Xavfsiz, allaqachon mavjud

# --- Handlers ---

@dp.message(Command("start"), DeepLinkFilter(re.compile(r'part_(\d+)_(\d+)')))
async def download_part_by_deeplink(message: Message, match: re.Match, args: str):
    user_id = message.from_user.id
    await add_user(user_id)
    
    code = match.group(1)
    part_number = int(match.group(2))
    
    unsubscribed = await get_unsubscribed_channels(user_id)
    if unsubscribed:
        builder = InlineKeyboardBuilder()
        for _, link, title in unsubscribed:
            builder.row(InlineKeyboardButton(text=f"➕ {title}", url=link))
        builder.row(InlineKeyboardButton(text="✅ Tekshirish", callback_data=f"check_part_sub:{code}_{part_number}"))
        
        await message.answer("🛑 Davom etish uchun quyidagi kanallarga obuna bo‘lishingiz shart:", reply_markup=builder.as_markup())
        return

    kino = await get_kino_by_code(code)
    if not kino:
        await message.answer("❌ Anime topilmadi.")
        return

    parts_file_ids = kino.get("parts_file_ids", [])
    if not (0 < part_number <= len(parts_file_ids)):
        await message.answer("❌ So‘ralgan qism mavjud emas.")
        return

    file_id = parts_file_ids[part_number - 1]
    msg = await message.answer("⏳ Qism yuklanmoqda, iltimos kuting...")
    
    try:
        await message.answer_document(document=file_id, caption=f"{kino.get('title')} [{part_number}-qism]")
        await msg.delete()
        
        if user_id in ADMINS:
            await send_admin_panel(message)
        else:
            await message.answer("Sizning panelingiz:", reply_markup=start_keyboard_user())
    except Exception as e:
        await message.answer("❌ Fayl yuborishda xatolik.")
        logging.error(f"Fayl yuborishda xato: {e}")

@dp.message(Command("start"), DeepLinkFilter(re.compile(r'^\d+$')))
async def download_all_by_deeplink(message: Message, args: str):
    user_id = message.from_user.id
    await add_user(user_id)
    code = args

    if not BOT_ACTIVE and user_id not in ADMINS:
        await message.answer("📴 Bot hozircha o'chirilgan.")
        return

    await increment_stat(code, "init")
    unsubscribed = await get_unsubscribed_channels(user_id)
    if unsubscribed:
        markup = await make_unsubscribed_markup(user_id, code)
        await message.answer("❗ Animeni olishdan oldin quyidagi kanallarga obuna bo‘ling:", reply_markup=markup)
    else:
        # send_reklama_post funksiyasini chaqirish (o'zingizda bor deb hisoblaymiz)
        await send_reklama_post(user_id, code)
        await increment_stat(code, "viewed")

START_CAPTION = "✨"

@dp.message(Command("start"))
async def start_handler(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    
    if user_id in ADMINS:
        await send_admin_panel(message)
        return
    
    if not BOT_ACTIVE:
        await message.answer("📴 Bot hozircha o'chirilgan.")
    else:
        await message.answer(START_CAPTION, reply_markup=start_keyboard_user())

@dp.callback_query(F.data.startswith("checksub:"))
async def check_subscription_callback(call: CallbackQuery):
    code = call.data.split(":")[1]
    unsubscribed = await get_unsubscribed_channels(call.from_user.id)

    if unsubscribed:
        builder = InlineKeyboardBuilder()
        for _, link, title in unsubscribed:
            builder.row(InlineKeyboardButton(text=f"➕ {title}", url=link))
        builder.row(InlineKeyboardButton(text="✅ Yana tekshirish", callback_data=f"checksub:{code}"))
        
        await call.message.edit_text("❗ Hali ham obuna bo‘lmagan kanal(lar):", reply_markup=builder.as_markup())
        await call.answer("Obuna bo'lmagansiz!", show_alert=True)
    else:
        await call.message.delete()
        # await send_reklama_post(call.from_user.id, code)
        await call.message.answer("Obuna tasdiqlandi! Anime Kodini yuboring!")

# --- Bot holati (Admin Panel) ---

@dp.message(F.text == "🤖 Bot holati")
async def show_bot_status(message: Message):
    if message.from_user.id not in ADMINS:
        return

    status_text = "🟢 Yoqilgan" if BOT_ACTIVE else "🔴 O'chirilgan"
    text = f"🤖 Bot holati: <b>{status_text}</b>"

    builder = InlineKeyboardBuilder()
    if BOT_ACTIVE:
        builder.row(InlineKeyboardButton(text="🔴 O'chirish", callback_data="bot_toggle:off"))
    else:
        builder.row(InlineKeyboardButton(text="🟢 Yoqish", callback_data="bot_toggle:on"))
    
    builder.row(InlineKeyboardButton(text="⬅️ Orqaga", callback_data="bot_status_back"))

    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("bot_toggle:"))
async def toggle_bot_status(callback: CallbackQuery):
    global BOT_ACTIVE
    action = callback.data.split(":")[1]

    if action == "on":
        BOT_ACTIVE = True
        text = "✅ Bot yoqildi!"
    else:
        BOT_ACTIVE = False
        text = "📴 Bot o'chirildi!"

    await callback.answer(text)
    # Yangilangan holatni qayta ko'rsatish
    await show_bot_status(callback.message)

@dp.callback_query(F.data == "bot_status_back")
async def back_from_bot_status(callback: CallbackQuery):
    await callback.message.delete()
    # send_admin_panel funksiyasini chaqirish
    await send_admin_panel(callback.message)

# --- 🔍 Anime qidirish ---

@dp.message(F.text == "🔍 Anime qidirish")
async def start_search(message: Message, state: FSMContext):
    if not BOT_ACTIVE and message.from_user.id not in ADMINS:
        await message.answer("📴 Bot hozircha o'chirilgan.")
        return
    
    await state.set_state(SearchStates.waiting_for_anime_name)
    await message.answer("🔍 Qidirish uchun anime nomini yozing:")

@dp.message(SearchStates.waiting_for_anime_name)
async def handle_search(message: Message, state: FSMContext):
    query = message.text.strip()
    if not query:
        await message.answer("❗ Iltimos, qidiruv so‘rovini kiriting.")
        return

    results = await search_anime_by_title(query)
    if not results:
        await state.clear() # Natija topilmasa stateni tozalash
        await message.answer("❌ Hech narsa topilmadi.")
        return

    builder = InlineKeyboardBuilder()
    for item in results:
        builder.row(InlineKeyboardButton(
            text=f"{item['title']}",
            callback_data=f"show_anime:{item['code']}"
        ))

    await state.clear()
    await message.answer("🔎 *Topilgan animelar:*", reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("show_anime:"))
async def show_full_anime(callback: CallbackQuery):
    code = callback.data.split(":")[1]
    user_id = callback.from_user.id

    unsubscribed = await get_unsubscribed_channels(user_id)
    if unsubscribed:
        markup = await make_unsubscribed_markup(user_id, code)
        await callback.message.edit_text(
            "❗ Anime olishdan oldin quyidagi kanal(lar)ga obuna bo‘ling:",
            reply_markup=markup
        )
        return

    await increment_stat(code, "init")
    await increment_stat(code, "searched")
    
    # Reklama postini yuborish funksiyasi
    await send_reklama_post(user_id, code)
    await callback.answer()

# --- 🎞 Barcha animelar ---

@dp.message(F.text == "🎞 Barcha animelar")
async def show_all_animes(message: Message):
    kodlar = await get_all_codes()
    if not kodlar:
        await message.answer("⛔️ Hozircha animelar yoʻq.")
        return

    # Kodlar bo'yicha tartiblash
    kodlar = sorted(kodlar, key=lambda x: int(x["code"]))

    chunk_size = 100
    for i in range(0, len(kodlar), chunk_size):
        chunk = kodlar[i:i + chunk_size]
        text = "📄 *Barcha animelar:*\n\n"
        for row in chunk:
            text += f"`{row['code']}` – *{row['title']}*\n"
        
        await message.answer(text, parse_mode="Markdown")

# --- ✉️ Admin bilan bog‘lanish ---

def cancel_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="❌ Bekor qilish"))
    return builder.as_markup(resize_keyboard=True)

@dp.message(F.text == "✉️ Admin bilan bog‘lanish")
async def contact_admin(message: Message, state: FSMContext):
    await state.set_state(UserStates.waiting_for_admin_message)
    await message.answer(
        "✍️ Adminlarga yubormoqchi bo‘lgan xabaringizni yozing.\n\n❌ Bekor qilish tugmasini bosing agar ortga qaytmoqchi bo‘lsangiz.",
        reply_markup=cancel_keyboard()
    )

@dp.message(UserStates.waiting_for_admin_message)
async def forward_to_admins(message: Message, state: FSMContext):
    if message.text == "❌ Bekor qilish":
        await state.clear()
        await message.answer("🏠 Asosiy menyuga qaytdingiz.", reply_markup=start_keyboard_user())
        return

    await state.clear()
    user = message.from_user

    for admin_id in ADMINS:
        try:
            builder = InlineKeyboardBuilder()
            builder.add(InlineKeyboardButton(text="✉️ Javob yozish", callback_data=f"reply_user:{user.id}"))

            await bot.send_message(
                admin_id,
                f"📩 <b>Yangi xabar:</b>\n\n"
                f"<b>👤 Foydalanuvchi:</b> {user.full_name} | <code>{user.id}</code>\n"
                f"<b>💬 Xabar:</b> {message.text}",
                parse_mode="HTML",
                reply_markup=builder.as_markup()
            )
        except Exception as e:
            print(f"Adminga yuborishda xatolik: {e}")

    await message.answer(
        "✅ Xabaringiz yuborildi. Tez orada admin siz bilan bog‘lanadi.",
        reply_markup=start_keyboard_user()
    )

@dp.callback_query(F.data.startswith("reply_user:"), F.from_user.id.in_(ADMINS))
async def start_admin_reply(callback: CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split(":")[1])
    await state.update_data(reply_user_id=user_id)
    await state.set_state(AdminReplyStates.waiting_for_reply_message)
    await callback.message.answer("✍️ Endi foydalanuvchiga yubormoqchi bo‘lgan xabaringizni yozing.")
    await callback.answer()

@dp.message(AdminReplyStates.waiting_for_reply_message, F.from_user.id.in_(ADMINS))
async def send_admin_reply(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = data.get("reply_user_id")

    try:
        await bot.send_message(user_id, f"✉️ Admindan javob:\n\n{message.text}")
        await message.answer("✅ Javob foydalanuvchiga yuborildi.")
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")
    finally:
        await state.clear()

# === 📡 Kanal boshqaruvi menyusi ===

@dp.message(F.text == "📡 Kanal boshqaruvi")
async def kanal_boshqaruvi(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        return
    await state.set_data({})  # Ma'lumotlarni tozalash
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔗 Majburiy obuna", callback_data="channel_type:sub"),
        InlineKeyboardButton(text="📌 Asosiy kanallar", callback_data="channel_type:main")
    )
    await message.answer("📡 Qaysi kanal turini boshqarasiz?", reply_markup=builder.as_markup())


@dp.callback_query(F.data.startswith("channel_type:"))
async def select_channel_type(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMINS:
        await callback.answer("🚫 Sizga ruxsat yo‘q.", show_alert=True)
        return

    ctype = callback.data.split(":")[1]
    await state.update_data(channel_type=ctype)

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Kanal qo‘shish", callback_data="action:add"),
        InlineKeyboardButton(text="📋 Kanal ro‘yxati", callback_data="action:list")
    )
    builder.row(
        InlineKeyboardButton(text="❌ Kanal o‘chirish", callback_data="action:delete"),
        InlineKeyboardButton(text="⬅️ Orqaga", callback_data="action:back")
    )

    text = "📡 Majburiy obuna kanallari menyusi:" if ctype == "sub" else "📌 Asosiy kanallar menyusi:"
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()


@dp.callback_query(F.data.startswith("action:"))
async def channel_actions(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMINS:
        await callback.answer("🚫 Sizga ruxsat yo‘q.", show_alert=True)
        return

    action = callback.data.split(":")[1]
    data = await state.get_data()
    ctype = data.get("channel_type")

    if not ctype and action != "back_to_menu":
        await callback.answer("❗ Xatolik: Avval kanal turini tanlang.", show_alert=True)
        return

    if action == "add":
        # 🔵 So'rovli yoki Ochiq rejim tanlash
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="Ochiq 🟢", callback_data="chan_mode:ochiq"),
            InlineKeyboardButton(text="So'rovli 🔵", callback_data="chan_mode:sorovli")
        )
        await callback.message.edit_text("Kanal ish rejimini tanlang:", reply_markup=builder.as_markup())
        await state.set_state(KanalStates.waiting_for_channel_type)

    elif action == "list":
        channels = await get_channels(ctype)
        if not channels:
            text = "📭 Hali kanal yo‘q."
        else:
            # Endi get_channels() lug'atlar qaytaradi: {cid, link, title, mode}
            text = ("📋 Majburiy obuna kanallari:\n\n" if ctype == "sub" else "📌 Asosiy kanallar:\n\n") + "\n".join(
                f"{i}. {ch['title']} ({ch.get('mode', 'ochiq')})\n   🆔 {ch['cid']}\n   🔗 {ch['link']}"
                for i, ch in enumerate(channels, 1)
            )
        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text="⬅️ Orqaga", callback_data="action:back"))
        await callback.message.edit_text(text, reply_markup=builder.as_markup())

    elif action == "delete":
        channels = await get_channels(ctype)
        if not channels:
            await callback.answer("📭 Hali kanal yo‘q.", show_alert=True)
            return
        builder = InlineKeyboardBuilder()
        for ch in channels:
            builder.row(InlineKeyboardButton(text=f"❌ {ch['title']} ({ch['cid']})", callback_data=f"del_ch:{ch['cid']}"))
        builder.row(InlineKeyboardButton(text="⬅️ Orqaga", callback_data="action:back"))
        await callback.message.edit_text("❌ Qaysi kanalni o‘chirmoqchisiz?", reply_markup=builder.as_markup())

    elif action in ["back", "back_to_menu"]:
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🔗 Majburiy obuna", callback_data="channel_type:sub"),
            InlineKeyboardButton(text="📌 Asosiy kanallar", callback_data="channel_type:main")
        )
        await callback.message.edit_text("📡 Qaysi kanal turini boshqarasiz?", reply_markup=builder.as_markup())

    await callback.answer()


# === Rejim tanlash ===
@dp.callback_query(F.data.startswith("chan_mode:"))
async def set_channel_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split(":")[1]
    await state.update_data(channel_mode=mode)
    await state.set_state(KanalStates.waiting_for_channel_id)
    await callback.message.edit_text(f"Kanal rejimi: {mode.upper()}\n🆔 Kanal ID yuboring (masalan: -1001234567890):")


# === Kanal qo'shish jarayoni ===
@dp.message(KanalStates.waiting_for_channel_id)
async def add_channel_id(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        await state.clear()
        return

    raw = message.text.strip()
    if raw.startswith("-100") and raw[4:].isdigit():
        channel_id = int(raw)
    elif raw.isdigit():
        channel_id = int("-100" + raw)
    else:
        await message.answer("❗ Noto‘g‘ri ID. Faqat raqam yuboring.")
        return

    try:
        chat = await bot.get_chat(channel_id)
        bot_member = await bot.get_chat_member(channel_id, (await bot.get_me()).id)
        if bot_member.status not in ["administrator", "creator"]:
            await message.answer("❗ Bot ushbu kanalda admin emas!")
            return
        await state.update_data(channel_id=channel_id, channel_title=chat.title)
        await state.set_state(KanalStates.waiting_for_channel_link)
        await message.answer(f"🔗 Kanal linkini yuboring (masalan: https://t.me/{chat.username or 'kanal_nomi'}):")
    except Exception as e:
        await message.answer(f"❗ Xato: {e}")


@dp.message(KanalStates.waiting_for_channel_link)
async def add_channel_finish(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        await state.clear()
        return

    link = message.text.strip()
    if not link.startswith("http"):
        await message.answer("❗ To‘liq link yuboring.")
        return

    data = await state.get_data()
    await add_channel(
        cid=data['channel_id'],
        link=link,
        title=data['channel_title'],
        ctype=data['channel_type'],
        mode=data['channel_mode']
    )
    await message.answer(f"✅ Kanal ({data['channel_mode']}) muvaffaqiyatli saqlandi!", reply_markup=admin_keyboard())
    await state.clear()


@dp.callback_query(F.data.startswith("del_ch:"))
async def delete_channel_process(callback: CallbackQuery, state: FSMContext):
    cid = int(callback.data.split(":")[1])
    data = await state.get_data()
    ctype = data.get('channel_type', 'sub')  # 'sub' yoki 'main'
    await remove_channel(cid, ctype=ctype)  # ✅ Faqat tanlangan tur o'chiriladi
    await callback.answer("✅ Kanal o‘chirildi!")
    # Menyuni qayta ko'rsatish
    await select_channel_type(callback, state)

# === 👥 Adminlar boshqaruvi ===

def admin_menu_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Admin qo‘shish"), KeyboardButton(text="➖ Admin o‘chirish"))
    builder.row(KeyboardButton(text="👥 Adminlar ro‘yxati"), KeyboardButton(text="⬅️ Ortga"))
    return builder.as_markup(resize_keyboard=True)

def control_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="📡 Boshqarish"))
    return builder.as_markup(resize_keyboard=True)

@dp.message(F.text == "👥 Adminlar")
async def open_admins_menu(message: Message):
    if message.from_user.id not in START_ADMINS:
        return
    await message.answer("👥 Adminlarni boshqarish menyusi:", reply_markup=admin_menu_keyboard())

@dp.message(F.text == "➕ Admin qo‘shish")
async def start_add_admin(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS: return
    await state.set_state(AdminStates.waiting_for_admin_id)
    await message.answer("Yangi adminning Telegram ID raqamini yuboring:", reply_markup=control_keyboard())

@dp.message(AdminStates.waiting_for_admin_id)
async def add_admin_process(message: Message, state: FSMContext):
    text = message.text.strip()
    if text in ["📡 Boshqarish", "⬅️ Ortga"]:
        await state.clear()
        await send_admin_panel(message)
        return

    if not text.isdigit():
        await message.answer("❗ Faqat raqam (ID) yuboring.")
        return

    new_id = int(text)
    ADMINS.add(new_id)
    # await add_admin(new_id) # Agar bazada saqlasangiz
    await message.answer(f"✅ {new_id} admin qilindi.", reply_markup=admin_menu_keyboard())
    await state.clear()

@dp.message(F.text == "👥 Adminlar ro‘yxati")
async def show_admins(message: Message):
    if message.from_user.id not in ADMINS: return
    admins_list = "\n".join([f"• <code>{a}</code>" for a in sorted(ADMINS)])
    await message.answer(f"👥 Adminlar:\n\n{admins_list}", parse_mode="HTML")

@dp.message(F.text == "➖ Admin o‘chirish")
async def start_remove_admin(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS: return
    await state.set_state(AdminStates.waiting_for_remove_id)
    await message.answer("O'chirish uchun admin ID raqamini yuboring:", reply_markup=control_keyboard())

@dp.message(AdminStates.waiting_for_remove_id)
async def remove_admin_process(message: Message, state: FSMContext):
    text = message.text.strip()
    if text in ["📡 Boshqarish", "⬅️ Ortga"]:
        await state.clear()
        await send_admin_panel(message)
        return

    if text.isdigit() and int(text) in ADMINS:
        ADMINS.remove(int(text))
        await message.answer(f"✅ {text} o'chirildi.", reply_markup=admin_menu_keyboard())
    else:
        await message.answer("❌ ID topilmadi.")
    await state.clear()

@dp.message(F.text == "⬅️ Ortga")
async def back_to_admin_panel_msg(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS: return
    await state.clear()
    await send_admin_panel(message)

# === 📈 Kod statistikasi ===

@dp.message(F.text == "📈 Kod statistikasi", F.from_user.id.in_(ADMINS))
async def ask_stat_code(message: Message, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_stat_code)
    await message.answer("📥 Kod raqamini yuboring:", reply_markup=control_keyboard())

@dp.message(AdminStates.waiting_for_stat_code)
async def show_code_stat(message: Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return

    code = message.text.strip()
    stat = await get_code_stat(code)
    await state.clear()

    if not stat:
        await message.answer("❗ Bunday kod statistikasi topilmadi.", reply_markup=control_keyboard())
        await send_admin_panel(message)
        return

    await message.answer(
        f"📊 <b>{code} statistikasi:</b>\n"
        f"🔍 Qidirilgan: <b>{stat['searched']}</b>\n",
        parse_mode="HTML"
    )
    await send_admin_panel(message)

# === ✏️ Kodni tahrirlash (ANIME) ===

@dp.message(F.text == "✏️ Kodni tahrirlash", F.from_user.id.in_(ADMINS))
async def edit_anime_start(message: Message, state: FSMContext):
    await state.set_state(EditAnimeStates.waiting_for_code)
    await message.answer("📝 Qaysi anime KODini tahrirlamoqchisiz?", reply_markup=control_keyboard())

@dp.message(EditAnimeStates.waiting_for_code)
async def edit_anime_code(message: Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return
    code = message.text.strip()
    anime = await get_kino_by_code(code)
    if not anime:
        await message.answer("❌ Bunday kod topilmadi.", reply_markup=control_keyboard())
        return
    await state.update_data(code=code, anime=anime)
    await message.answer(
        f"🔎 Kod: {code}\n📌 Nomi: {anime['title']}\nNima qilmoqchisiz?",
        reply_markup=edit_main_menu_inline_keyboard()
    )

@dp.callback_query(F.data.startswith("edit:"))
async def handle_edit_main_menu(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":")[1]
    if action == "parts":
        await callback.message.edit_text("🎞 Qaysi amalni bajarmoqchisiz?", reply_markup=edit_parts_menu_inline_keyboard())
    elif action == "info":
        await callback.message.edit_text("📝 Qaysi maydonni tahrirlamoqchisiz?", reply_markup=edit_info_fields_inline_keyboard())
    elif action == "back_to_main":
        data = await state.get_data()
        anime = data["anime"]
        code = data["code"]
        await callback.message.edit_text(
            f"🔎 Kod: {code}\n📌 Nomi: {anime['title']}\nNima qilmoqchisiz?",
            reply_markup=edit_main_menu_inline_keyboard()
        )
    await callback.answer()

# QISM QO'SHISH
@dp.callback_query(F.data == "edit_parts:add")
async def start_adding_parts(callback: CallbackQuery, state: FSMContext):
    await state.set_state(EditAnimeStates.waiting_for_new_part)
    await callback.message.edit_text("🎞 Yangi qism(lar)ni yuboring. Bir yoki bir nechta fayl yuborishingiz mumkin:\n\n⚠️ Tugatganda /done yozing yoki '📡 Boshqarish' tugmasini bosing.")
    await callback.answer()

@dp.message(EditAnimeStates.waiting_for_new_part, F.video | F.document)
async def receive_new_parts(message: Message, state: FSMContext):
    data = await state.get_data()
    parts = data.get("new_parts", [])
    file_id = message.video.file_id if message.video else message.document.file_id
    parts.append(file_id)
    await state.update_data(new_parts=parts)
    await message.answer(f"✅ Qism qo‘shildi. Jami: {len(parts)} ta.")

@dp.message(F.text == "/done", EditAnimeStates.waiting_for_new_part)
async def finish_adding_parts(message: Message, state: FSMContext):
    data = await state.get_data()
    new_parts = data.get("new_parts", [])
    if not new_parts:
        await message.answer("❗ Hech qanday qism qo‘shilmadi.", reply_markup=admin_keyboard())
    else:
        # Mavjud qismlarga qo'shamiz
        anime = await get_kino_by_code(data["code"])
        old_parts = anime.get("parts_file_ids", [])
        all_parts = old_parts + new_parts
        await update_anime_code(data["code"], data["code"], anime["title"], parts_file_ids=all_parts)
        await message.answer(f"✅ {len(new_parts)} ta qism qo‘shildi.", reply_markup=admin_keyboard())
    await state.clear()

# QISM O'CHIRISH
@dp.callback_query(F.data == "edit_parts:delete")
async def ask_part_to_delete(callback: CallbackQuery, state: FSMContext):
    await state.set_state(EditAnimeStates.waiting_for_part_delete)
    await callback.message.edit_text("🔢 O‘chirmoqchi bo‘lgan qism raqamini yozing:")
    await callback.answer()

@dp.message(EditAnimeStates.waiting_for_part_delete)
async def delete_part_by_number(message: Message, state: FSMContext):
    data = await state.get_data()
    try:
        part_num = int(message.text.strip())
        anime = await get_kino_by_code(data["code"])
        parts = anime.get("parts_file_ids", [])
        if not (1 <= part_num <= len(parts)):
            raise ValueError
        parts.pop(part_num - 1)
        await update_anime_code(data["code"], data["code"], anime["title"], parts_file_ids=parts)
        await message.answer(f"✅ {part_num}-qism o‘chirildi.", reply_markup=admin_keyboard())
    except (ValueError, IndexError):
        await message.answer("❌ Noto‘g‘ri qism raqami.")
    await state.clear()
@dp.callback_query(F.data.startswith("edit_field:"))
async def start_edit_field(callback: CallbackQuery, state: FSMContext):
    field_key = callback.data.split(":")[1]
    field_labels = {
        "title": "Nomi",
        "genre": "Janr",
        "season": "Mavsum",
        "quality": "Sifati",
        "channel_name": "Kanal nomi",
        "dubbed_by": "Ovoz bergan",
        "total_parts": "Qismlar soni",
    }
    label = field_labels.get(field_key, "Maydon")
    await state.update_data(editing_field=field_key)
    await state.set_state(EditAnimeStates.waiting_for_field_value)
    await callback.message.edit_text(f"📝 {label} uchun yangi qiymat kiriting:")
    await callback.answer()

@dp.message(EditAnimeStates.waiting_for_field_value)
async def save_edited_field(message: Message, state: FSMContext):
    data = await state.get_data()
    field = data["editing_field"]
    new_value = message.text.strip()

    # Integer qilish kerak bo'lgan maydonlar
    if field == "total_parts":
        try:
            new_value = int(new_value)
            if new_value <= 0:
                raise ValueError
        except ValueError:
            await message.answer("❗ Faqat musbat butun son kiriting.")
            return

    # Baza yangilash
    anime = await get_kino_by_code(data["code"])
    update_kwargs = {field: new_value}
    await update_anime_code(old_code=data["code"], new_code=data["code"], **update_kwargs)

    await message.answer("✅ Ma’lumot muvaffaqiyatli yangilandi.", reply_markup=admin_keyboard())
    await state.clear()

@dp.message(F.text == "📡 Boshqarish")
async def cancel_via_control(message: Message, state: FSMContext):
    await state.clear()
    await send_admin_panel(message)
# === ❌ Kodni o'chirish ===

@dp.message(F.text == "❌ Kodni o‘chirish", F.from_user.id.in_(ADMINS))
async def ask_delete_code(message: Message, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_delete_code)
    await message.answer("🗑 Qaysi kodni o‘chirmoqchisiz? Kodni yuboring.", reply_markup=control_keyboard())

@dp.message(AdminStates.waiting_for_delete_code)
async def delete_code_handler(message: Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return

    code = message.text.strip()
    if not code.isdigit():
        await message.answer("❗ Noto‘g‘ri format. Kod raqamini yuboring.")
        return

    deleted = await delete_kino_code(code)
    await state.clear()
    text = f"✅ Kod {code} o‘chirildi." if deleted else "❌ Kod topilmadi."
    await message.answer(text, reply_markup=admin_keyboard())

# === 🎞 Qism post qilish (Deep Link yaratish) ===

@dp.message(F.text == "🎞 Qism post qilish", F.from_user.id.in_(ADMINS))
async def start_part_posting(message: Message, state: FSMContext):
    await state.set_state(PartPostStates.waiting_for_anime_code)
    await message.answer("📌 Qaysi animeni qismini post qilmoqchisiz? Kodni yuboring:", reply_markup=control_keyboard())

@dp.message(PartPostStates.waiting_for_anime_code)
async def part_post_code_handler(message: Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return

    code = message.text.strip()
    kino = await get_kino_by_code(code)
    if not kino:
        await message.answer("❌ Bunday kod topilmadi.")
        return
    
    parts = kino.get('parts_file_ids', [])
    await state.update_data(code=code, title=kino['title'], parts_file_ids=parts)
    await state.set_state(PartPostStates.waiting_for_part_number)
    await message.answer(f"✅ {kino['title']} (jami {len(parts)} qism).\nNechinchi qismni post qilmoqchisiz?")

@dp.message(PartPostStates.waiting_for_part_number)
async def part_post_number_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    try:
        part_num = int(message.text.strip())
        if not (1 <= part_num <= len(data['parts_file_ids'])): raise ValueError
    except:
        await message.answer("❌ Noto'g'ri qism raqami.")
        return

    await state.update_data(part_number=part_num)
    await state.set_state(PartPostStates.waiting_for_channel_username)
    await message.answer("📌 Kanal username yuboring (@username):")

@dp.message(PartPostStates.waiting_for_channel_username)
async def part_post_finish(message: Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.clear(); await send_admin_panel(message); return

    channel = message.text.strip()
    data = await state.get_data()
    bot_info = await bot.get_me()
    
    deep_link = f"https://t.me/{bot_info.username}?start=part_{data['code']}_{data['part_number']}"
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="✨Yuklab olish✨", url=deep_link))

    try:
        await bot.send_message(
            chat_id=channel,
            text=f"🎬 Anime: {data['title']}\n✨ Qism: {data['part_number']}-qism",
            reply_markup=builder.as_markup()
        )
        await message.answer("✅ Post kanalga yuborildi.", reply_markup=admin_keyboard())
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")
    await state.clear()

# === ⚡ Deep Link Handler (Foydalanuvchi start bossa) ===

@dp.message(CommandStart(deep_link=True))
async def download_part_by_deeplink(message: Message, command: CommandObject):
    args = command.args # Masalan: "part_123_5"
    if not args or not args.startswith("part_"):
        return

    user_id = message.from_user.id
    await add_user(user_id)
    
    try:
        _, code, part_number = args.split('_')
        part_number = int(part_number)
    except:
        await message.answer("❌ Havola noto'g'ri.")
        return

    unsubscribed = await get_unsubscribed_channels(user_id)
    if unsubscribed:
        builder = InlineKeyboardBuilder()
        for _, link, title in unsubscribed:
            builder.row(InlineKeyboardButton(text=f"➕ {title}", url=link))
        builder.row(InlineKeyboardButton(text="✅ Tekshirish", callback_data=f"check_part_sub:{code}_{part_number}"))
        
        await message.answer("🛑 Davom etish uchun kanallarga obuna bo'ling:", reply_markup=builder.as_markup())
        return

    # Obuna bo'lsa darhol yuborish
    await send_anime_part(message, code, part_number)

# === 🛠 Yordamchi funksiya: Qismni yuborish ===

async def send_anime_part(event, code, part_number):
    kino = await get_kino_by_code(code)
    if not kino or part_number > len(kino['parts_file_ids']):
        target = event.message if isinstance(event, CallbackQuery) else event
        await target.answer("❌ Qism topilmadi.")
        return

    file_id = kino['parts_file_ids'][part_number - 1]
    msg = event.message if isinstance(event, CallbackQuery) else event
    
    await msg.answer("⏳ Yuklanmoqda...")

# === Obunani qayta tekshirish (YAGONA QISM UCHUN) ===
@dp.callback_query(F.data.startswith("check_part_sub:"))
async def check_part_subscription(callback: CallbackQuery):
    user_id = callback.from_user.id
    # data format: code_part_number (masalan: 123_5)
    data = callback.data.split(":")[1] 
    
    # 1. Obuna tekshiruvi
    unsubscribed = await get_unsubscribed_channels(user_id)
    
    if unsubscribed:
        await callback.answer("❌ Hali obuna bo‘lmadingiz. Kanallarga a’zo bo‘lib, qayta urinib ko‘ring.", show_alert=True)
        
        builder = InlineKeyboardBuilder()
        for channel_id, channel_link, channel_title in unsubscribed:
            builder.row(InlineKeyboardButton(text=f"➕ {channel_title}", url=channel_link))
            
        builder.row(InlineKeyboardButton(text="✅ Tekshirish", callback_data=f"check_part_sub:{data}"))
        
        try:
            await callback.message.edit_text(
                "🛑 Davom etish uchun quyidagi kanallarga obuna bo‘lishingiz shart:",
                reply_markup=builder.as_markup()
            )
        except Exception:
            pass 
        return

    await callback.answer("✅ Obuna muvaffaqiyatli tekshirildi.")
    
    try:
        await callback.message.delete()
    except Exception:
        pass

    try:
        code, part_number_str = data.split('_')
        part_number = int(part_number_str)

        kino = await get_kino_by_code(code)
        if not kino:
            await callback.message.answer("❌ Anime topilmadi.")
            return

        title = kino.get("title", "Anime")
        parts_file_ids = kino.get("parts_file_ids", [])
        
        if not parts_file_ids or part_number > len(parts_file_ids) or part_number < 1:
            await callback.message.answer("❌ So‘ralgan qism mavjud emas.")
            return
            
        file_id = parts_file_ids[part_number - 1]
        caption = f"{title} [{part_number}-qism]"
        
        # 3. Qismni yuborish
        await callback.message.answer("⏳ Qism yuklanmoqda, iltimos kuting...")
        await bot.send_document(
            chat_id=user_id,
            document=file_id,
            caption=caption
        )
        await callback.message.answer("✅ Qism yuborildi. Bosh menyu:", reply_markup=start_keyboard_user()) 
        
    except Exception as e:
        await callback.message.answer("❌ Qismni yuborishda xatolik yuz berdi. Iltimos, qayta urinib ko‘ring.")

# === ➕ Anime qo‘shish ===
@dp.message(F.text == "➕ Anime qo‘shish", F.from_user.id.in_(ADMINS))
async def start_add_anime(message: Message, state: FSMContext):
    await message.answer("📝 Kodni kiriting (faqat raqam):", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_code)

# Universal "Boshqarish" tugmasi
@dp.message(F.text == "📡 Boshqarish")
async def cancel_via_control(message: Message, state: FSMContext):
    await state.clear()
    await send_admin_panel(message)

# Kodni qabul qilish
@dp.message(AddAnimeStates.waiting_for_code)
async def anime_code_handler(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❗ Faqat raqam kiriting (masalan: 12345).", reply_markup=control_keyboard())
        return

    await state.update_data(code=message.text.strip())
    await message.answer("📝 Anime nomini kiriting:", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_title)

@dp.message(AddAnimeStates.waiting_for_title)
async def anime_title_handler(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await message.answer("🎭 Janrini kiriting:", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_genre)

@dp.message(AddAnimeStates.waiting_for_genre)
async def anime_genre_handler(message: Message, state: FSMContext):
    await state.update_data(genre=message.text.strip())
    await message.answer("📺 Sezonni kiriting:", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_season)

@dp.message(AddAnimeStates.waiting_for_season)
async def anime_season_handler(message: Message, state: FSMContext):
    await state.update_data(season=message.text.strip())
    await message.answer("🎬 Sifatini kiriting:", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_quality)

@dp.message(AddAnimeStates.waiting_for_quality)
async def anime_quality_handler(message: Message, state: FSMContext):
    await state.update_data(quality=message.text.strip())
    await message.answer("📡 Kanal nomini kiriting:", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_channel_name)

@dp.message(AddAnimeStates.waiting_for_channel_name)
async def anime_channel_handler(message: Message, state: FSMContext):
    await state.update_data(channel_name=message.text.strip())
    await message.answer("🎙 Ovoz berganini kiriting:", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_dubbed_by)

@dp.message(AddAnimeStates.waiting_for_dubbed_by)
async def anime_dubbed_handler(message: Message, state: FSMContext):
    await state.update_data(dubbed_by=message.text.strip())
    await message.answer("🔢 Umumiy qismlar sonini kiriting:", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_total_parts)

@dp.message(AddAnimeStates.waiting_for_total_parts)
async def anime_total_parts_handler(message: Message, state: FSMContext):
    try:
        total_parts = int(message.text.strip())
        if total_parts <= 0: raise ValueError
    except ValueError:
        await message.answer("❗ Faqat musbat son kiriting.", reply_markup=control_keyboard())
        return

    await state.update_data(total_parts=total_parts)
    await message.answer("📸 Reklama postini yuboring (rasm/video/file):", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_poster)

@dp.message(AddAnimeStates.waiting_for_poster, F.photo | F.video | F.document)
async def anime_poster_handler(message: Message, state: FSMContext):
    file_id = ""
    poster_type = ""

    if message.photo:
        file_id = message.photo[-1].file_id
        poster_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        poster_type = "video"
    elif message.document:
        file_id = message.document.file_id
        poster_type = "document"

    await state.update_data(
        poster_file_id=file_id,
        poster_type=poster_type,
        caption=message.caption or "",
        parts_file_ids=[]
    )
    await message.answer("📥 Endi qismlarni yuboring. Oxirida /done yuboring.", reply_markup=control_keyboard())
    await state.set_state(AddAnimeStates.waiting_for_parts)

@dp.message(AddAnimeStates.waiting_for_parts, F.video | F.document)
async def anime_parts_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    parts = data.get("parts_file_ids", [])
    file_id = message.video.file_id if message.video else message.document.file_id
    parts.append(file_id)
    await state.update_data(parts_file_ids=parts)
    await message.answer(f"✅ Qism saqlandi. Jami: {len(parts)} ta.")

@dp.message(F.text == "/done", AddAnimeStates.waiting_for_parts)
async def anime_done_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    parts_file_ids = data.get("parts_file_ids", [])
    if not parts_file_ids:
        await message.answer("❗ Hech qanday qism yuborilmadi!")
        return

    # Database saqlash
    await add_anime(
        data["code"], data["title"], data["poster_file_id"], parts_file_ids, 
        "", # Caption keyinroq formatlanadi
        data.get("genre", ""), data.get("season", "1"), data.get("quality", ""),
        data.get("channel_name", ""), data.get("dubbed_by", ""), data.get("total_parts"),
        poster_type=data.get("poster_type", "photo")
    )

    await message.answer(f"✅ Anime saqlandi!\n📌 Kod: <b>{data['code']}</b>", reply_markup=admin_keyboard(), parse_mode="HTML")
    await state.clear()

# === 📤 Post qilish (menyudan kirish) ===
@dp.message(F.text == "📤 Post qilish", F.from_user.id.in_(ADMINS))
async def start_posting(message: Message, state: FSMContext):
    await state.set_state(PostStates.waiting_for_code)
    await message.answer(
        "📌 Qaysi animeni post qilmoqchisiz? Kodni yuboring:", 
        reply_markup=control_keyboard()
    )

# === 📤 Post qilish (To'liq tuzatilgan versiya) ===
@dp.message(PostStates.waiting_for_code)
async def send_post_by_code(message: Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return

    code = message.text.strip()
    if not code.isdigit():
        await message.answer("❌ Kod faqat raqamlardan iborat bo‘lishi kerak.", reply_markup=control_keyboard())
        return

    kino = await get_kino_by_code(code)
    if not kino:
        await message.answer("❌ Bunday kod topilmadi.", reply_markup=control_keyboard())
        return

    # ✅ Captionni avtomatik yaratish
    title = kino.get('title', 'Noma’lum')
    season = kino.get('season', '1')
    quality = kino.get('quality', '')
    channel = kino.get('channel_name', '')
    genre = kino.get('genre', '').strip()
    dubbed = kino.get('dubbed_by', '')
    parts_file_ids = kino.get('parts_file_ids', [])
    parts_count = len(parts_file_ids)

    if genre:
        genre = ", ".join(genre.split())
    else:
        genre = "—"

    caption = (
        f"{title}\n"
        f"──────────────────────\n"
        f"➤ Mavsum: {season}\n"
        f"➤ Qismlar: {parts_count}\n"
        f"➤ Sifati: {quality or '—'}\n"
        f"➤ Ovoz berdi: {dubbed or '—'}\n"
        f"➤ Kanal: {channel or '—'}\n"
        f"➤ Janri: {genre}\n"
        f"──────────────────────"
    )

    # ✅ Yuklab olish tugmasi — URL dagi bo'sh joyni tozalash
    bot_info = await bot.get_me()
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="✨Yuklab olish✨",
        url=f"https://t.me/{bot_info.username}?start={code}"  # ⚠️ bo'sh joysiz!
    ))

    # Asosiy kanallarga post qilish — ENDI `get_channels` lug'at qaytaradi
    main_channels_data = await get_channels('main')
    if not main_channels_data:
        await message.answer("❌ Hech qanday asosiy kanal topilmadi.", reply_markup=admin_keyboard())
        await state.clear()
        return

    successful = 0
    failed = 0
    for ch in main_channels_data:
        ch_id = ch['cid']
        ch_title = ch['title']

        try:
            file_id = kino['poster_file_id']
            poster_type = kino.get('poster_type', 'photo')
            
            if poster_type == "video":
                await bot.send_video(chat_id=ch_id, video=file_id, caption=caption, reply_markup=builder.as_markup())
            elif poster_type == "document":
                await bot.send_document(chat_id=ch_id, document=file_id, caption=caption, reply_markup=builder.as_markup())
            else:  # photo
                await bot.send_photo(chat_id=ch_id, photo=file_id, caption=caption, reply_markup=builder.as_markup())
                
            successful += 1
        except Exception as e:
            logging.error(f"Kanal {ch_id} ({ch_title}) ga post yuborishda xato: {e}")
            failed += 1

    await message.answer(
        f"✅ Post yuborildi.\n"
        f"✅ Muvaffaqiyatli: {successful}\n"
        f"❌ Xatolik: {failed}",
        reply_markup=admin_keyboard()
    )
    await state.clear()

# === 📄 Kodlar ro'yxati ===
@dp.message(F.text == "📄 Kodlar ro‘yxati", F.from_user.id.in_(ADMINS))
async def show_all_animes(message: Message):
    kodlar = await get_all_codes()
    if not kodlar:
        await message.answer("Ba'zada hech qanday kodlar yo'q!")
        return

    # Kodlarni tartiblash
    kodlar = sorted(kodlar, key=lambda x: int(x["code"]))
    
    # Telegram xabar limiti (4096 belgi) sababli bo'lib yuborish
    chunk_size = 50 
    for i in range(0, len(kodlar), chunk_size):
        chunk = kodlar[i:i + chunk_size]
        text = "📄 <b>Barcha animelar:</b>\n\n"
        for row in chunk:
            text += f"<code>{row['code']}</code> – <i>{row['title']}</i>\n"
        await message.answer(text, parse_mode="HTML")

# === 📊 Statistika ===
@dp.message(F.text == "📊 Statistika", F.from_user.id.in_(ADMINS))
async def stats(message: Message):
    # Bazaga ping (ulanish tezligi) tekshiruvi
    from database import db_pool
    async with db_pool.acquire() as conn:
        start_time = time.perf_counter()
        await conn.fetch("SELECT 1;")
        ping = (time.perf_counter() - start_time) * 1000

    kodlar = await get_all_codes()
    foydalanuvchilar = await get_user_count()
    today_users = await get_today_users()
    
    text = (
        f"⚡️ <b>Ulanish tezligi:</b> {ping:.2f} ms\n"
        f"👥 <b>Jami foydalanuvchilar:</b> {foydalanuvchilar} ta\n"
        f"📅 <b>Bugun qo'shilganlar:</b> {today_users} ta\n"
        f"📂 <b>Baza hajmi:</b> {len(kodlar)} ta anime"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=admin_keyboard())

# === ⬅️ Orqaga tugmasi ===
@dp.message(F.text == "⬅️ Orqaga", F.from_user.id.in_(ADMINS))
async def back_to_admin_menu(message: Message, state: FSMContext):
    await state.clear()
    await send_admin_panel(message)

# === 📢 Habar yuborish ===

async def background_broadcast(
    message: Message,
    users: list[int],
    broadcast_info: dict[str, Any],
    bot: Bot
):
    """
    Flood control bilan himoyalangan xabar yuborish (HTML parse mode bilan)
    """
    success = 0
    fail = 0
    total_users = len(users)

    BATCH_SIZE = 15
    BATCH_DELAY = 2
    PER_USER_DELAY = 0.1

    admin_id = message.chat.id

    # Habar yuborish funksiyasi (avvalgiday)
    if broadcast_info.get('type') == 'forward':
        channel_username = broadcast_info['channel_username']
        msg_id = broadcast_info['msg_id']

        async def send_func(user_id: int) -> bool:
            retries = 0
            while retries < 5:
                try:
                    await bot.forward_message(user_id, channel_username, msg_id)
                    return True
                except Exception as e:
                    error_text = str(e).lower()
                    if "flood control" in error_text or "too many requests" in error_text:
                        match = re.search(r"retry in (\d+)", error_text)
                        wait_time = int(match.group(1)) if match else 1
                        await asyncio.sleep(wait_time + 1)
                        retries += 1
                    else:
                        return False
            return False
    else:
        source_chat_id = admin_id
        message_id = broadcast_info['message_id']

        async def send_func(user_id: int) -> bool:
            retries = 0
            while retries < 5:
                try:
                    await bot.copy_message(user_id, source_chat_id, message_id)
                    return True
                except Exception as e:
                    error_text = str(e).lower()
                    if "flood control" in error_text or "too many requests" in error_text:
                        match = re.search(r"retry in (\d+)", error_text)
                        wait_time = int(match.group(1)) if match else 1
                        await asyncio.sleep(wait_time + 1)
                        retries += 1
                    else:
                        return False
            return False

    # Boshlanish xabari (HTML bilan)
    status_msg = await bot.send_message(
        admin_id,
        f"⏳ <b>Boshlandi!</b> Jami {total_users} ta foydalanuvchiga yuborish.",
        parse_mode="HTML"
    )
    
    for i in range(0, total_users, BATCH_SIZE):
        batch = users[i:i + BATCH_SIZE]
        for user_id in batch:
            if user_id == admin_id:
                continue
                
            ok = await send_func(user_id)
            if ok:
                success += 1
            else:
                fail += 1
            await asyncio.sleep(PER_USER_DELAY)

        await asyncio.sleep(BATCH_DELAY)

        # Progress yangilash (HTML bilan)
        remaining = total_users - (i + len(batch))
        try:
            await bot.edit_message_text(
                chat_id=admin_id,
                message_id=status_msg.message_id,
                text=(
                    f"📤 Yuborilmoqda...\n\n"
                    f"👥 Jami: {total_users}\n"
                    f"✅ Yuborildi: {success}\n"
                    f"❌ Xatolik: {fail}\n"
                    f"⏳ Kutilmoqda: {remaining}"
                ),
                parse_mode="HTML"
            )
        except Exception:
            pass

    # Yakuniy hisobot (HTML bilan)
    try:
        await bot.edit_message_text(
            chat_id=admin_id,
            message_id=status_msg.message_id,
            text=(
                f"✅ <b>Yuborish tugadi!</b>\n"
                f"Jami foydalanuvchilar: {total_users}\n"
                f"Muvaqqiyatli: {success}\n"
                f"Xato: {fail}"
            ),
            parse_mode="HTML",
            reply_markup=admin_keyboard()
        )
    except Exception:
        await bot.send_message(
            admin_id,
            f"✅ <b>Yuborish tugadi!</b>\n"
            f"Jami: {total_users} | Muvaffaqiyatli: {success} | Xato: {fail}",
            parse_mode="HTML",
            reply_markup=admin_keyboard()
        )

# 1. Habar yuborish tugmasi bosilganda
@dp.message(F.text == "📢 Habar yuborish", F.from_user.id.in_(ADMINS))
async def ask_broadcast_type(message: Message, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_broadcast_type)
    await message.answer(
        "Qanday turdagi xabar yubormoqchisiz?",
        reply_markup=get_broadcast_type_keyboard()
    )

# 2. Habar turini tanlash
@dp.message(AdminStates.waiting_for_broadcast_type, F.from_user.id.in_(ADMINS))
async def process_broadcast_type(message: Message, state: FSMContext):
    # MUHIM: Avval Boshqarish tugmasini tekshirish
    if message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return

    if message.text == "📣 Kanaldan yuborish":
        await state.set_state(AdminStates.waiting_for_forward_data)
        await message.answer(
            "📨 **Kanaldan yuborish** uchun format:\n`@kanal_username xabar_id`\n\nMasalan: `@kanalim 123`",
            parse_mode="MarkdownV2",
            reply_markup=control_keyboard()
        )
    elif message.text == "📰 Oddiy xabar":
        await state.set_state(AdminStates.waiting_for_simple_message)
        await message.answer(
            "📨 **Oddiy xabar** (rasm, matn, video,...) yuboring\\.\n"
            "Bu xabar foydalanuvchilarga `copy_message` orqali yuboriladi\\.",
            parse_mode="MarkdownV2",
            reply_markup=control_keyboard()
        )
    else:
        await message.answer(
            "❗ Noto'g'ri tanlov\\.",
            parse_mode="MarkdownV2",
            reply_markup=get_broadcast_type_keyboard()
        )

# 3a. Kanaldan yuborish ma'lumotlarini qabul qilish
@dp.message(AdminStates.waiting_for_forward_data, F.from_user.id.in_(ADMINS))
async def start_forward_broadcast(message: Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return

    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.answer(
            "❗ Format noto'g'ri\\. Masalan: `@kanalim 123`",
            parse_mode="MarkdownV2",
            reply_markup=control_keyboard()
        )
        return

    channel_username, msg_id_str = parts
    if not msg_id_str.isdigit():
        await message.answer(
            "❗ Xabar ID raqam bo'lishi kerak\\.",
            parse_mode="MarkdownV2",
            reply_markup=control_keyboard()
        )
        return

    # State'ni tozalash
    await state.clear()

    # Yuborishni asinxron boshlash
    users = await get_all_user_ids()
    broadcast_info = {
        'type': 'forward',
        'channel_username': channel_username,
        'msg_id': int(msg_id_str)
    }
    asyncio.create_task(background_broadcast(message, users, broadcast_info, bot))

# 3b. Oddiy xabar ma'lumotlarini qabul qilish (har qanday kontent turi)
@dp.message(AdminStates.waiting_for_simple_message, F.from_user.id.in_(ADMINS))
async def start_simple_broadcast(message: Message, state: FSMContext):
    # "Boshqarish" tugmasini tekshirish (faqat text xabarlarda)
    if message.content_type == ContentType.TEXT and message.text == "📡 Boshqarish":
        await state.clear()
        await send_admin_panel(message)
        return

    # Matnli xabar uchun tasdiqlash (ixtiyoriy - kerak bo'lsa qo'shishingiz mumkin)
    if message.content_type == ContentType.TEXT:
        await message.answer(
            "Siz yuborgan matnli xabar barcha foydalanuvchilarga yuboriladi\\. Tasdiqlaysizmi\\?",
            parse_mode="MarkdownV2",
            reply_markup=control_keyboard()
        )
        # Tasdiqlash qo'shish uchun state saqlash mumkin, lekin hozircha bevosita yuboramiz
        # await state.update_data(message_id=message.message_id)
        # await state.set_state(AdminStates.confirming_broadcast)
        # return

    # State'ni tozalash va yuborishni boshlash
    await state.clear()

    users = await get_all_user_ids()
    broadcast_info = {
        'type': 'copy',
        'message_id': message.message_id
    }
    asyncio.create_task(background_broadcast(message, users, broadcast_info, bot))

# === 🔢 Kodni qidirish (Faqat raqam yuborilganda) ===
@dp.message(F.text.isdigit())
async def handle_code_message(message: Message):
    # Bot holatini tekshirish (global o'zgaruvchi deb hisoblandi)
    if not BOT_ACTIVE and message.from_user.id not in ADMINS:
        await message.answer("📴 Bot hozircha o'chirilgan.")
        return

    user_id = message.from_user.id
    unsubscribed = await get_unsubscribed_channels(user_id)
    
    if unsubscribed:
        # Avvalgi kodda yozilgan make_unsubscribed_markup funksiyasi
        markup = await make_unsubscribed_markup(user_id, message.text)
        await message.answer(
            "❗ Anime olishdan oldin quyidagi kanal(lar)ga obuna bo‘ling:",
            reply_markup=markup
        )
        return

    code = message.text
    # Statistika yuritish
    await increment_stat(code, "init")
    await increment_stat(code, "searched")
    
    # Reklama postini chiqarish
    await send_reklama_post(user_id, code)
    await increment_stat(code, "viewed")

# === 🖼 Reklama post yuborish funksiyasi ===
async def send_reklama_post(user_id, code):
    data = await get_kino_by_code(code)
    if not data:
        await bot.send_message(user_id, "❌ Kod topilmadi.")
        return

    title = data.get('title', 'Noma’lum')
    season = data.get('season', '1')
    quality = data.get('quality', '')
    channel = data.get('channel_name', '')
    genre = data.get('genre', '').strip()
    dubbed = data.get('dubbed_by', '')
    parts_file_ids = data.get('parts_file_ids', [])
    parts_count = len(parts_file_ids)

    # Ko'rish statistikasi
    stat_data = await get_code_stat(code)
    view_count = stat_data["viewed"] if stat_data else 0  # dict deb faraz qilamiz

    # ✅ Janrni sozlash: agar bo'sh bo'lsa "—", agar bir nechta so'z bo'lsa vergul qo'yish
    if genre:
        # Agar foydalanuvchi "Drama Ekshin Sarguzasht" deb yozgan bo'lsa — ajratish
        genre = ", ".join(genre.split())
    else:
        genre = "—"

    # ✅ Caption — Siz xohlagan ko'rinishda
    caption = (
        f"{title}\n"
        f"──────────────────────\n"
        f"➤ Mavsum: {season}\n"
        f"➤ Qismlar: {parts_count}\n"
        f"➤ Sifati: {quality or '—'}\n"
        f"➤ Ovoz berdi: {dubbed or '—'}\n"
        f"➤ Kanal: {channel or '—'}\n"
        f"➤ Janri: {genre}\n"
        f"──────────────────────\n"
        f"🔍 Ko'rishlar soni: {view_count}"
    )

    # Tugmalar
    builder = InlineKeyboardBuilder()
    for i in range(1, parts_count + 1):
        builder.button(text=str(i), callback_data=f"part_download:{code}:{i}")
    builder.adjust(5)

    poster_file_id = data.get("poster_file_id")
    poster_type = data.get("poster_type", "photo")

    try:
        if not poster_file_id:
            await bot.send_message(user_id, caption, reply_markup=builder.as_markup())
        elif poster_type == "video":
            await bot.send_video(user_id, poster_file_id, caption=caption, reply_markup=builder.as_markup())
        elif poster_type == "document":
            await bot.send_document(user_id, poster_file_id, caption=caption, reply_markup=builder.as_markup())
        else:  # photo
            await bot.send_photo(user_id, poster_file_id, caption=caption, reply_markup=builder.as_markup())
    except Exception as e:
        logging.error(f"Reklama post yuborishda xato: {e}")
        await bot.send_message(user_id, "❌ Post yuborishda xatolik yuz berdi. Adminlarga xabar bering.")

# === 📥 Qism tugmasi bosilganda ===
@dp.callback_query(F.data.startswith("part_download:"))
async def download_single_part(callback: CallbackQuery):
    try:
        _, code, part_number = callback.data.split(":")
        part_number = int(part_number)
    except:
        await callback.answer("❌ Ma'lumotda xatolik.", show_alert=True)
        return

    await callback.answer(f"⏳ {part_number}-qism yuborilmoqda...")
    
    result = await get_kino_by_code(code)
    if not result:
        await callback.message.answer("❌ Anime topilmadi.")
        return

    parts_file_ids = result.get("parts_file_ids", [])
    if part_number < 1 or part_number > len(parts_file_ids):
        await callback.message.answer("❌ Bu qism mavjud emas.")
        return
        
    file_id = parts_file_ids[part_number - 1]
    
    try:
        await bot.send_document(
            chat_id=callback.from_user.id,
            document=file_id,
            caption=f"{result.get('title', 'Anime')} [{part_number}-qism]"
        )
    except Exception as e:
        await callback.message.answer("❌ Faylni yuborishda xatolik. Botni bloklamaganingizga ishonch hosil qiling.")

# === 🚀 MAIN ===
async def main():
    await init_db() # Baza ishga tushishi
    print("✅ Bot ishga tushdi!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot to'xtatildi!")
