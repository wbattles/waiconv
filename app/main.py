import asyncio
import json
import os
import threading
import time
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

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "messages")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "chat-consumers-v1")

KAFKA_SASL_ENABLED = os.getenv("KAFKA_SASL_ENABLED", "false").lower() == "true"
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")

REDIS_HOST = os.getenv("REDIS_HOST", "waiconv-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
MESSAGES_KEY = os.getenv("REDIS_KEY", "messages")
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
    print(
        f"DEBUG: Kafka config: servers={servers} sasl_enabled={KAFKA_SASL_ENABLED} "
        f"topic={KAFKA_TOPIC} group_id={KAFKA_GROUP_ID}"
    )
    return cfg


def start_producer():
    global producer
    cfg = build_kafka_config()
    cfg["acks"] = "all"
    cfg["retries"] = 5
    cfg["request_timeout_ms"] = 10000
    print("DEBUG: creating KafkaProducer")
    producer = KafkaProducer(**cfg)


def start_consumer():
    global consumer
    cfg = build_kafka_config()
    consumer = KafkaConsumer(
        **cfg,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    consumer.subscribe([KAFKA_TOPIC])
    print(f"DEBUG: KafkaConsumer subscribed to {consumer.subscription()}")

async def start_redis():
    global redis_client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    try:
        await redis_client.ping()
        print(f"Redis connected host={REDIS_HOST} port={REDIS_PORT} db={REDIS_DB} key={MESSAGES_KEY}")
    except Exception as e:
        print(f"WARN: Redis ping failed: {e}")


async def start_postgres(max_attempts: int = 10, delay: float = 2.0):
    global pg_pool
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            print(f"Connecting to Postgres (attempt {attempt}/{max_attempts}) host={POSTGRES_HOST} db={POSTGRES_DB}")
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
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS chat_messages (
                        id serial PRIMARY KEY,
                        ts timestamptz NOT NULL,
                        user_name text,
                        text text NOT NULL
                    )
                """)
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
        ts_value = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
    except Exception:
        ts_value = datetime.now(timezone.utc)

    try:
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO chat_messages (ts, user_name, text) VALUES ($1, $2, $3)",
                ts_value, user if user else None, text,
            )
        print(f"DEBUG: inserted into Postgres ts={ts_value} user={user} text={text}")
    except Exception as e:
        print(f"WARN: failed to insert into Postgres: {e}")


async def store_message_in_redis(msg: dict):
    if redis_client is None:
        print("DEBUG: redis_client is None, cannot store message")
        return
    data = json.dumps(msg)
    await redis_client.lpush(MESSAGES_KEY, data)
    await redis_client.ltrim(MESSAGES_KEY, 0, MAX_MESSAGES - 1)
    print(f"DEBUG: stored message in Redis key={MESSAGES_KEY}: {msg}")


async def get_last_messages(limit: int = MAX_MESSAGES):
    if redis_client is None:
        print("DEBUG: redis_client is None in get_last_messages")
        return []
    raw = await redis_client.lrange(MESSAGES_KEY, 0, limit - 1)
    print(f"DEBUG: get_last_messages raw_len={len(raw)}")
    result = []
    for item in raw:
        try:
            result.append(json.loads(item))
        except Exception as e:
            print(f"DEBUG: failed to decode Redis item: {e}")
    print(f"DEBUG: get_last_messages decoded_len={len(result)}")
    return result

def consumer_loop():
    global consumer_running
    assert consumer is not None
    print("DEBUG: consumer_loop started")

    try:
        consumer.poll(timeout_ms=1000)
        print(f"DEBUG: initial assignment={consumer.assignment()}")
    except Exception as e:
        print(f"WARN: initial poll failed: {e}")

    empty_polls = 0
    while consumer_running:
        try:
            msg_pack = consumer.poll(timeout_ms=1000, max_records=50)
        except Exception as e:
            print(f"WARN: Kafka poll error: {e}")
            time.sleep(1)
            continue

        if not msg_pack:
            empty_polls += 1
            if empty_polls % 10 == 0:
                print(f"DEBUG: consumer poll returned no messages (count={empty_polls}) assignment={consumer.assignment()}")
            continue

        for tp, messages in msg_pack.items():
            print(f"DEBUG: received {len(messages)} messages from {tp}")
            for msg in messages:
                if msg.value is None:
                    continue
                try:
                    data = json.loads(msg.value.decode("utf-8"))
                except Exception as e:
                    print(f"DEBUG: failed to decode Kafka message: {e}")
                    continue
                print(f"DEBUG: consumed from Kafka: {data}")
                if loop:
                    asyncio.run_coroutine_threadsafe(handle_message(data), loop)

    print("DEBUG: consumer_loop stopping, closing consumer")
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
            print("WARN: KAFKA_SASL_ENABLED is true but SASL credentials missing; running without Kafka")
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
                if consumer_running:
                    consumer_running = False
                    if consumer_thread:
                        consumer_thread.join(timeout=5)
                if producer:
                    try:
                        producer.flush(timeout=5)
                    except Exception:
                        pass
                if redis_client:
                    await redis_client.aclose()
                if pg_pool:
                    await pg_pool.close()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return FileResponse("static/index.html")

@app.get("/api/messages")
async def get_messages():
    messages = await get_last_messages()
    print(f"DEBUG: /api/messages returning len={len(messages)}")
    return {"messages": messages}

@app.post("/api/message")
async def post_message(payload: dict = Body(...)):
    if not producer:
        print("DEBUG: producer not ready in /api/message")
        return {"status": "error", "message": "producer not ready"}

    text = str(payload.get("text", "")).strip()
    user = str(payload.get("user", "")).strip()
    if not text:
        print("DEBUG: empty text in /api/message")
        return {"status": "error", "message": "empty"}

    ts = datetime.now(timezone.utc).isoformat()
    event = {"type": "chat", "text": text, "user": user, "ts": ts}

    try:
        value = json.dumps(event).encode("utf-8")
        print(f"DEBUG: sending to Kafka topic={KAFKA_TOPIC} event={event}")
        fut = producer.send(KAFKA_TOPIC, value=value)
        record_md = fut.get(timeout=10)
        producer.flush(timeout=5)
        print(f"DEBUG: Kafka delivery topic={record_md.topic} partition={record_md.partition} offset={record_md.offset}")
    except Exception as e:
        print(f"Kafka produce failed: {e}")
        return {"status": "error", "message": "kafka_failed"}

    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    print(f"DEBUG: WebSocket connected, clients={len(clients)}")
    messages = await get_last_messages()
    await ws.send_text(json.dumps({"type": "messagesUpdate", "messages": messages}))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
        print(f"DEBUG: WebSocket disconnected, clients={len(clients)}")
    except Exception as e:
        clients.discard(ws)
        print(f"DEBUG: WebSocket error, clients={len(clients)} err={e}")

async def handle_message(data: dict):
    if data.get("type") != "chat":
        print(f"DEBUG: handle_message ignored non-chat event={data}")
        return

    text = str(data.get("text", "")).strip()
    user = str(data.get("user", "")).strip()
    ts = str(data.get("ts", "")).strip()

    if not text:
        print("DEBUG: handle_message got empty text")
        return

    msg = {"text": text, "user": user, "ts": ts}
    print(f"DEBUG: handle_message processing msg={msg}")
    await save_message_to_db(text=text, user=user, ts=ts)
    await store_message_in_redis(msg)
