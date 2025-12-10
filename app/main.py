import asyncio
import json
import os
import threading
from typing import Set

from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaProducer, KafkaConsumer
import redis.asyncio as redis
import asyncpg

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")

KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chat-messages")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "chat-consumers")

KAFKA_SASL_ENABLED = os.getenv("KAFKA_SASL_ENABLED", "false").lower() == "true"
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")

REDIS_HOST = os.getenv("REDIS_HOST", "waichat-redis-master")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

MESSAGES_KEY = os.getenv("REDIS_MESSAGES_KEY", "chat_messages")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "5"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "chat")
POSTGRES_USER = os.getenv("POSTGRES_USER", "chat")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

clients: Set[WebSocket] = set()

producer: KafkaProducer | None = None
consumer: KafkaConsumer | None = None
consumer_thread: threading.Thread | None = None
consumer_running: bool = False
loop: asyncio.AbstractEventLoop | None = None
redis_client: redis.Redis | None = None
pg_pool: asyncpg.Pool | None = None


def build_kafka_config() -> dict:
    servers = (
        KAFKA_BOOTSTRAP_SERVERS.split(",")
        if "," in KAFKA_BOOTSTRAP_SERVERS
        else [KAFKA_BOOTSTRAP_SERVERS]
    )
    cfg: dict[str, object] = {
        "bootstrap_servers": servers,
        "security_protocol": KAFKA_SECURITY_PROTOCOL,
    }
    if KAFKA_SASL_ENABLED:
        cfg["sasl_mechanism"] = KAFKA_SASL_MECHANISM
        cfg["sasl_plain_username"] = KAFKA_SASL_USERNAME
        cfg["sasl_plain_password"] = KAFKA_SASL_PASSWORD
    return cfg


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


def start_producer():
    global producer
    cfg = build_kafka_config()
    producer = KafkaProducer(**cfg)


def start_consumer():
    global consumer
    cfg = build_kafka_config()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        **cfg,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


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


def consumer_loop():
    global consumer_running
    assert consumer is not None
    while consumer_running:
        try:
            msg_pack = consumer.poll(timeout_ms=1000)
        except Exception as e:
            print(f"WARN: Kafka poll error: {e}")
            continue

        if not msg_pack:
            continue

        for _tp, messages in msg_pack.items():
            for msg in messages:
                value_bytes = msg.value
                if value_bytes is None:
                    continue
                try:
                    data = json.loads(value_bytes.decode("utf-8"))
                except Exception:
                    continue
                if loop is not None:
                    asyncio.run_coroutine_threadsafe(handle_message(data), loop)

    consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_thread, consumer_running, loop

    await start_redis()
    await start_postgres()

    if not KAFKA_BOOTSTRAP_SERVERS:
        print("WARN: KAFKA_BOOTSTRAP_SERVERS not set, running without Kafka")
        yield
    else:
        if KAFKA_SASL_ENABLED and (not KAFKA_SASL_USERNAME or not KAFKA_SASL_PASSWORD):
            print(
                "WARN: KAFKA_SASL_ENABLED is true but SASL credentials are missing, "
                "running without Kafka"
            )
            yield
        else:
            start_producer()
            start_consumer()
            loop = asyncio.get_running_loop()
            consumer_running = True
            consumer_thread = threading.Thread(target=consumer_loop, daemon=True)
            consumer_thread.start()
            print("Kafka producer and consumer started")
            try:
                yield
            finally:
                global producer, redis_client, pg_pool
                if consumer_running:
                    consumer_running = False
                    if consumer_thread is not None:
                        consumer_thread.join(timeout=5)
                if producer is not None:
                    try:
                        producer.flush(timeout=5)
                    except Exception:
                        pass
                if redis_client:
                    await redis_client.aclose()
                if pg_pool is not None:
                    await pg_pool.close()


app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/api/messages")
async def get_messages():
    messages = await get_last_messages()
    return {"messages": messages}


@app.post("/api/message")
async def post_message(payload: dict = Body(...)):
    if not producer:
        return {"status": "error", "message": "producer not ready"}

    text = str(payload.get("text", "")).strip()
    user = str(payload.get("user", "")).strip()
    if not text:
        return {"status": "error", "message": "empty"}

    ts = datetime.now(timezone.utc).isoformat()

    event = {
        "type": "chat",
        "text": text,
        "user": user,
        "ts": ts,
    }

    try:
        value = json.dumps(event).encode("utf-8")
        producer.send(KAFKA_TOPIC, value=value)
    except Exception as e:
        print(f"Kafka produce failed: {e}")
        return {"status": "error", "message": "kafka_failed"}

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


async def handle_message(data: dict):
    if data.get("type") != "chat":
        return

    text = str(data.get("text", "")).strip()
    user = str(data.get("user", "")).strip()
    ts = str(data.get("ts", "")).strip()

    if not text:
        return

    msg = {
        "text": text,
        "user": user,
        "ts": ts,
    }

    await save_message_to_db(text=text, user=user, ts=ts)
    await store_message_in_redis(msg)
    await broadcast_messages()
