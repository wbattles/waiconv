import asyncio
import json
import os
import threading
from typing import Set
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from confluent_kafka import Producer, Consumer
import redis.asyncio as redis
import asyncpg

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_ENABLED = os.getenv("KAFKA_SASL_ENABLED", "false").lower() == "true"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chat-messages")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "chat-consumers")

REDIS_HOST = os.getenv("REDIS_HOST", "waiconv-redis-master")
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

producer: Producer | None = None
consumer: Consumer | None = None
consumer_thread: threading.Thread | None = None
consumer_running: bool = False
loop: asyncio.AbstractEventLoop | None = None
redis_client: redis.Redis | None = None
pg_pool: asyncpg.Pool | None = None

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


def build_kafka_config() -> dict:
    cfg: dict[str, str] = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }

    if KAFKA_SECURITY_PROTOCOL:
        cfg["security.protocol"] = KAFKA_SECURITY_PROTOCOL
    else:
        cfg["security.protocol"] = "SASL_SSL" if KAFKA_SASL_ENABLED else "PLAINTEXT"

    if KAFKA_SASL_ENABLED:
        if KAFKA_SASL_MECHANISM:
            cfg["sasl.mechanisms"] = KAFKA_SASL_MECHANISM
        if KAFKA_SASL_USERNAME:
            cfg["sasl.username"] = KAFKA_SASL_USERNAME
        if KAFKA_SASL_PASSWORD:
            cfg["sasl.password"] = KAFKA_SASL_PASSWORD

    print("Kafka config:", cfg)
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
    print("Starting Kafka producer")
    producer = Producer(cfg)


def start_consumer():
    global consumer
    cfg = build_kafka_config()
    cfg["group.id"] = KAFKA_GROUP_ID
    cfg["auto.offset.reset"] = "latest"
    print("Starting Kafka consumer with group", KAFKA_GROUP_ID, "topic", KAFKA_TOPIC)
    consumer = Consumer(cfg)
    consumer.subscribe([KAFKA_TOPIC])


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
        print("WARN: Redis client is None, cannot store message")
        return
    data = json.dumps(msg)
    await redis_client.lpush(MESSAGES_KEY, data)
    await redis_client.ltrim(MESSAGES_KEY, 0, MAX_MESSAGES - 1)
    print("Stored message in Redis:", msg)


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
    print("Broadcasted messages to", len(clients), "clients")


@app.get("/api/messages")
async def get_messages():
    messages = await get_last_messages()
    return {"messages": messages}


@app.post("/api/message")
async def post_message(payload: dict = Body(...)):
    if not producer:
        print("ERROR: Kafka producer not ready")
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
        producer.produce(KAFKA_TOPIC, value=value)
        producer.poll(0)
        print("Produced Kafka message:", event)
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
        print("Ignoring non-chat Kafka message:", data)
        return

    text = str(data.get("text", "")).strip()
    user = str(data.get("user", "")).strip()
    ts = str(data.get("ts", "")).strip()

    if not text:
        print("Ignoring empty chat message from Kafka:", data)
        return

    msg = {
        "text": text,
        "user": user,
        "ts": ts,
    }

    print("Handling Kafka chat message:", msg)

    await save_message_to_db(text=text, user=user, ts=ts)
    await store_message_in_redis(msg)
    await broadcast_messages()


def consumer_loop():
    global consumer_running
    assert consumer is not None
    print("Kafka consumer loop started")
    while consumer_running:
        try:
            msg = consumer.poll(1.0)
        except Exception as e:
            print("Kafka poll exception:", e)
            continue

        if msg is None:
            continue

        if msg.error():
            print("Kafka consumer error:", msg.error())
            continue

        try:
            value_bytes = msg.value()
            if value_bytes is None:
                print("Kafka message has no value")
                continue
            data = json.loads(value_bytes.decode("utf-8"))
            print("Kafka consumer received message:", data)
        except Exception as e:
            print("Failed to decode Kafka message:", e)
            continue

        if loop is not None:
            asyncio.run_coroutine_threadsafe(handle_message(data), loop)
    print("Kafka consumer loop exiting")
    consumer.close()


@app.on_event("startup")
async def startup_event():
    global consumer_thread, consumer_running, loop

    await start_redis()
    await start_postgres()

    if not KAFKA_BOOTSTRAP_SERVERS:
        print("WARN: KAFKA_BOOTSTRAP_SERVERS not set, running without Kafka")
        return

    print("KAFKA_BOOTSTRAP_SERVERS:", KAFKA_BOOTSTRAP_SERVERS)
    print("KAFKA_SECURITY_PROTOCOL:", KAFKA_SECURITY_PROTOCOL)
    print("KAFKA_SASL_ENABLED:", KAFKA_SASL_ENABLED)
    print("KAFKA_SASL_MECHANISM:", KAFKA_SASL_MECHANISM)

    start_producer()
    start_consumer()
    loop = asyncio.get_running_loop()
    consumer_running = True
    consumer_thread = threading.Thread(target=consumer_loop, daemon=True)
    consumer_thread.start()
    print("Kafka producer and consumer started")


@app.on_event("shutdown")
async def shutdown_event():
    global producer, consumer_running, consumer_thread, redis_client, pg_pool

    if consumer_running:
        consumer_running = False
        if consumer_thread is not None:
            consumer_thread.join(timeout=5)

    if producer is not None:
        producer.flush(5.0)

    if redis_client:
        await redis_client.aclose()

    if pg_pool is not None:
        await pg_pool.close()
