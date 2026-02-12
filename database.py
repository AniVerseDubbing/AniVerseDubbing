import json
import aiosqlite
import asyncpg
import os
import asyncio
from dotenv import load_dotenv
from datetime import date
from typing import Optional

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]  # Majburiy

db_pool: Optional[asyncpg.pool.Pool] = None


# === Databasega ulanish ===
async def init_db(retries: int = 5, delay: int = 2):
    global db_pool

    for attempt in range(retries):
        try:
            db_pool = await asyncpg.create_pool(
                dsn=DATABASE_URL,
                ssl="require",              # Supabase uchun kerak
                statement_cache_size=0,     # Transaction pooler uchun muhim
                min_size=1,
                max_size=15                 # Ko‘p so‘rov uchun optimal
            )

            async with db_pool.acquire() as conn:
                # === Foydalanuvchilar ===
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # === Anime kodlari ===
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS kino_codes (
                        code TEXT PRIMARY KEY,
                        title TEXT,
                        channel TEXT,
                        message_id INTEGER,
                        post_count INTEGER,
                        poster_file_id TEXT,
                        caption TEXT,
                        parts_file_ids TEXT,
                        genre TEXT,
                        season TEXT,
                        quality TEXT,
                        channel_name TEXT,
                        dubbed_by TEXT,
                        total_parts INTEGER,
                        poster_type TEXT
                    );
                """)

                # === Statistika ===
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS stats (
                        code TEXT PRIMARY KEY,
                        searched INTEGER DEFAULT 0,
                        viewed INTEGER DEFAULT 0
                    );
                """)

                # === Adminlar ===
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS admins (
                        user_id BIGINT PRIMARY KEY
                    );
                """)

                # === Kanallar — yangilangan PRIMARY KEY ===
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS channels (
                        channel_id BIGINT NOT NULL,
                        title TEXT,
                        link TEXT,
                        type TEXT NOT NULL CHECK (type IN ('sub', 'main')),
                        mode TEXT DEFAULT 'ochiq',
                        PRIMARY KEY (channel_id, type)
                    );
                """)

                # === So'rovli obuna uchun: foydalanuvchi so'rovlari ===
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS join_requests (
                        user_id BIGINT,
                        channel_id BIGINT,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (user_id, channel_id)
                    );
                """)

                # Dastlabki admin
                default_admins = [6486825926]
                for admin_id in default_admins:
                    await conn.execute(
                        "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
                        admin_id
                    )

            print("[DB] Ulanish muvaffaqiyatli")
            break
        except Exception as e:
            print(f"[DB] Ulanish xatosi ({i+1}/{retries}): {e}")
            if i + 1 == retries:
                raise
            await asyncio.sleep(delay)

async def get_conn() -> asyncpg.pool.Pool:
    global db_pool
    if db_pool is None:
        await init_db()
        return db_pool
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("SELECT 1;")
    except (asyncpg.InterfaceError, asyncpg.PostgresError):
        print("[DB] Pool uzildi, qayta ulanmoqda…")
        await init_db()
    return db_pool


# === Foydalanuvchilar ===
async def add_user(user_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
        )

async def get_user_count():
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM users")
        return row[0]

async def get_today_users():
    pool = await get_conn()
    async with pool.acquire() as conn:
        today = date.today()
        row = await conn.fetchrow("SELECT COUNT(*) FROM users WHERE DATE(created_at) = $1", today)
        return row[0] if row else 0


# === Anime kodlari ===
async def add_anime(code, title, poster_file_id, parts_file_ids, caption="", genre="", season="1", quality="", channel_name="", dubbed_by="", total_parts=0, poster_type="photo"):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO kino_codes (
                code, title, poster_file_id, caption, parts_file_ids, post_count,
                genre, season, quality, channel_name, dubbed_by, total_parts, poster_type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (code) DO UPDATE SET
                title = EXCLUDED.title,
                poster_file_id = EXCLUDED.poster_file_id,
                caption = EXCLUDED.caption,
                parts_file_ids = EXCLUDED.parts_file_ids,
                genre = EXCLUDED.genre,
                season = EXCLUDED.season,
                quality = EXCLUDED.quality,
                channel_name = EXCLUDED.channel_name,
                dubbed_by = EXCLUDED.dubbed_by,
                total_parts = EXCLUDED.total_parts,
                poster_type = EXCLUDED.poster_type;
        """, code, title, poster_file_id, caption, json.dumps(parts_file_ids), len(parts_file_ids),
           genre, season, quality, channel_name, dubbed_by, total_parts, poster_type)
        await conn.execute("INSERT INTO stats (code) VALUES ($1) ON CONFLICT DO NOTHING", code)


async def get_kino_by_code(code):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT code, title, poster_file_id, caption, parts_file_ids,
                   post_count, channel, message_id, genre, season, quality,
                   channel_name, dubbed_by, total_parts, poster_type
            FROM kino_codes
            WHERE code = $1
        """, code)
        if row:
            data = dict(row)
            data["parts_file_ids"] = json.loads(data["parts_file_ids"]) if data.get("parts_file_ids") else []
            return data
        return None


async def get_all_codes():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM kino_codes")
        result = []
        for row in rows:
            item = dict(row)
            item["parts_file_ids"] = json.loads(item["parts_file_ids"]) if item.get("parts_file_ids") else []
            result.append(item)
        return result


async def delete_kino_code(code):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM stats WHERE code = $1", code)
        result = await conn.execute("DELETE FROM kino_codes WHERE code = $1", code)
        return result.endswith("1")


# === Statistika ===
async def increment_stat(code, field):
    if field not in ("searched", "viewed", "init"):
        return
    pool = await get_conn()
    async with pool.acquire() as conn:
        if field == "init":
            await conn.execute("INSERT INTO stats (code, searched, viewed) VALUES ($1, 0, 0) ON CONFLICT DO NOTHING", code)
        else:
            await conn.execute(f"UPDATE stats SET {field} = {field} + 1 WHERE code = $1", code)


async def get_code_stat(code):
    pool = await get_conn()
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT searched, viewed FROM stats WHERE code = $1", code)


# === Kodni yangilash ===

async def update_anime_code(old_code, new_code=None, new_title=None, **kwargs):
    """
    Anime kodini yangilash.
    
    :param old_code: Yangilanadigan yozuvning joriy kodi (WHERE sharti)
    :param new_code: (ixtiyoriy) Yangi kod qiymati
    :param new_title: (ixtiyoriy) Yangi sarlavha
    :param kwargs: Boshqa yangilanadigan maydonlar (faqat ruxsat etilganlar)
    """
    allowed_fields = {
        'title', 'genre', 'season', 'quality',
        'channel_name', 'dubbed_by', 'total_parts', 'parts_file_ids', 'poster_type'
    }

    # title ni kwargs dan ajratib olish — dublikatni oldini oladi
    title_from_kwargs = kwargs.pop('title', None)

    # new_title berilmagan bo'lsa, lekin kwargs da title bo'lsa, uni ishlat
    if new_title is None:
        new_title = title_from_kwargs
    # Agar ikkalasi ham berilgan bo'lsa — new_title ustunlik qiladi (kwargs dagi title e'tiborsiz qolinadi)

    set_parts = []
    values = []

    # Yangi kod
    if new_code is not None:
        set_parts.append("code = $1")
        values.append(new_code)

    # Sarlavha
    if new_title is not None:
        param_index = len(values) + 1
        set_parts.append(f"title = ${param_index}")
        values.append(new_title)

    # Qolgan maydonlar
    for key, value in kwargs.items():
        if key not in allowed_fields:
            raise ValueError(f"Ruxsat etilmagan maydon: {key}")
        if key == 'parts_file_ids':
            value = json.dumps(value)
        param_index = len(values) + 1
        set_parts.append(f"{key} = ${param_index}")
        values.append(value)

    if not set_parts:
        return  # Yangilanadigan narsa yo'q

    # WHERE uchun old_code qo'shiladi
    where_param_index = len(values) + 1
    query = f"UPDATE kino_codes SET {', '.join(set_parts)} WHERE code = ${where_param_index}"
    values.append(old_code)

    async with db_pool.acquire() as conn:
        await conn.execute(query, *values)

# === Adminlar ===
async def get_all_admins():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
        return {row["user_id"] for row in rows}

async def add_admin(user_id: int):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)

async def remove_admin(user_id: int):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)


# === Foydalanuvchilar ID si ===
async def get_all_user_ids():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [row["user_id"] for row in rows]


# === Qismlar ===
async def add_part_to_anime(code: str, file_id: str):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT parts_file_ids FROM kino_codes WHERE code=$1", code)
        parts = json.loads(row["parts_file_ids"]) if row and row["parts_file_ids"] else []
        parts.append(file_id)
        await conn.execute(
            "UPDATE kino_codes SET parts_file_ids=$1, post_count=$2 WHERE code=$3",
            json.dumps(parts), len(parts), code
        )

async def delete_part_from_anime(code: str, part_number: int):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT parts_file_ids FROM kino_codes WHERE code=$1", code)
        if not row or not row["parts_file_ids"]:
            return False
        parts = json.loads(row["parts_file_ids"])
        if not (1 <= part_number <= len(parts)):
            return False
        parts.pop(part_number - 1)
        await conn.execute(
            "UPDATE kino_codes SET parts_file_ids=$1, post_count=$2 WHERE code=$3",
            json.dumps(parts), len(parts), code
        )
        return True


# === Qidiruv ===
async def search_anime_by_title(query: str):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT code, title FROM kino_codes
            WHERE LOWER(title) LIKE LOWER($1)
            ORDER BY title
            LIMIT 20
        """, f"%{query}%")
        return [{"code": r["code"], "title": r["title"]} for r in rows]


# === ⬇️ Kanallar — SO'ROVLILI TIZIM UCHUN YANGILANGAN ===
async def add_channel(cid: int, link: str, title: str, ctype: str, mode: str = "ochiq"):
    if not ctype:
        raise ValueError("Kanal turi topilmadi")

    ctype = ctype.strip().lower()

    if ctype not in ("sub", "main"):
        raise ValueError(f"Noto‘g‘ri type: {ctype}")

    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO channels (channel_id, link, title, type, mode)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (channel_id, type)
            DO UPDATE SET link = $2, title = $3, mode = $5
        """, cid, link, title, ctype, mode)

async def remove_channel(cid: int, ctype: str = None):
    """Agar ctype berilsa — faqat shu turdagi kanal o'chiriladi.
       Agar berilmasa — ikkala turi ham o'chiriladi."""
    pool = await get_conn()
    async with pool.acquire() as conn:
        if ctype:
            await conn.execute("DELETE FROM channels WHERE channel_id = $1 AND type = $2", cid, ctype)
        else:
            await conn.execute("DELETE FROM channels WHERE channel_id = $1", cid)


async def get_channels(channel_type: str):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT channel_id, title, link, mode FROM channels WHERE type = $1",
            channel_type
        )
        return [
            {
                "cid": r["channel_id"],
                "link": r["link"],
                "title": r["title"],
                "mode": r.get("mode", "ochiq")
            }
            for r in rows
        ]


async def add_join_request(user_id: int, channel_id: int):
    """Foydalanuvchi kanalga so'rov yuborganida chaqiriladi."""
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO join_requests (user_id, channel_id)
            VALUES ($1, $2)
            ON CONFLICT (user_id, channel_id) DO NOTHING
        """, user_id, channel_id)


async def check_user_request(user_id: int, channel_id: int) -> bool:
    """Foydalanuvchi ushbu kanalga so'rov yuborganmi?"""
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM join_requests WHERE user_id = $1 AND channel_id = $2",
            user_id, channel_id
        )
        return row is not None
