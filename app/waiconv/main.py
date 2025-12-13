import asyncio
import json
import os
from typing import Set

from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import redis.asyncio as redis
import asyncpg

app = FastAPI()

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
MESSAGES_KEY = os.getenv("REDIS_MESSAGES_KEY", "chat_messages")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "5"))

# Postgres configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "chat")
POSTGRES_USER = os.getenv("POSTGRES_USER", "chat")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

clients: Set[WebSocket] = set()
redis_client: redis.Redis | None = None
pg_pool: asyncpg.Pool | None = None

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


async def start_redis():
    global redis_client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    try:
        await redis_client.ping()
        print("Redis connected")
    except Exception as e:
        print(f"WARN: Redis ping failed: {e}")


async def start_postgres(max_attempts: int = 10, delay: float = 2.0):
    global pg_pool
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            print(
                f"Connecting to Postgres (attempt {attempt}/{max_attempts}) "
                f"host={POSTGRES_HOST} db={POSTGRES_DB}"
            )
            pg_pool = await asyncpg.create_pool(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                min_size=1,
                max_size=5,
            )
            async with pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chat_messages (
                        id serial PRIMARY KEY,
                        ts timestamptz NOT NULL,
                        user_name text,
                        text text NOT NULL
                    )
                    """
                )
            print("Postgres connected and chat_messages table ready")
            return
        except Exception as e:
            print(f"WARN: Postgres init failed on attempt {attempt}: {e}")
            pg_pool = None
            if attempt < max_attempts:
                await asyncio.sleep(delay)
    print("ERROR: Could not connect to Postgres after retries; continuing without DB")


async def save_message_to_db(text: str, user: str, ts: datetime | str):
    if pg_pool is None:
        return
    try:
        if isinstance(ts, str):
            try:
                ts_value = datetime.fromisoformat(ts)
            except ValueError:
                ts_value = datetime.now(timezone.utc)
        else:
            ts_value = ts

        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO chat_messages (ts, user_name, text) VALUES ($1, $2, $3)",
                ts_value,
                user if user else None,
                text,
            )
    except Exception as e:
        print(f"WARN: failed to insert into Postgres: {e}")


async def store_message_in_redis(msg: dict):
    if redis_client is None:
        return
    data = json.dumps(msg)
    await redis_client.lpush(MESSAGES_KEY, data)
    await redis_client.ltrim(MESSAGES_KEY, 0, MAX_MESSAGES - 1)


async def get_last_messages(limit: int = MAX_MESSAGES):
    if redis_client is None:
        return []
    raw = await redis_client.lrange(MESSAGES_KEY, 0, limit - 1)
    result = []
    for item in raw:
        try:
            result.append(json.loads(item))
        except Exception:
            continue
    return result


async def broadcast_messages():
    if not clients:
        return
    messages = await get_last_messages()
    payload = json.dumps({"type": "messagesUpdate", "messages": messages})
    dead: list[WebSocket] = []
    for ws in list(clients):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


@app.get("/api/messages")
async def get_messages():
    messages = await get_last_messages()
    return {"messages": messages}


@app.post("/api/message")
async def post_message(payload: dict = Body(...)):
    text = str(payload.get("text", "")).strip()
    user = str(payload.get("user", "")).strip()
    if not text:
        return {"status": "error", "message": "empty"}

    ts = datetime.now(timezone.utc).isoformat()

    msg = {
        "text": text,
        "user": user,
        "ts": ts,
    }

    try:
        await save_message_to_db(text=text, user=user, ts=ts)
        await store_message_in_redis(msg)
        await broadcast_messages()
    except Exception as e:
        print(f"Failed to save message: {e}")
        return {"status": "error", "message": "save_failed"}

    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    messages = await get_last_messages()
    await ws.send_text(json.dumps({"type": "messagesUpdate", "messages": messages}))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)


@app.on_event("startup")
async def startup_event():
    await start_redis()
    await start_postgres()
    print("Redis and Postgres started")


@app.on_event("shutdown")
async def shutdown_event():
    global redis_client, pg_pool

    if redis_client:
        await redis_client.aclose()

    if pg_pool is not None:
        await pg_pool.close()
