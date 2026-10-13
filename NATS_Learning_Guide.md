# Learn NATS, then use it from Django and Flask

This guide is for people who have never used a message broker, and for people who already know HTTP APIs and want a durable, production-shaped NATS setup.

Read it in order the first time. Later, use the contents as a reference.

**What you will be able to do**

- Explain what a subject, a message, and a subscriber are.
- Run a NATS server on your machine.
- Publish and receive messages with Python.
- Choose between plain NATS (fast, fire-and-forget) and JetStream (stored, acknowledged).
- Wire a publisher and a consumer into a Django project and a Flask project without blocking web requests.
- Handle duplicates, crashes, and bad messages.

This repository already uses the pattern taught here. The plant agent publishes with `saf_edge/saf_edge/nats_transport.py`. The Django backend consumes with `backend/ingestion/nats_consumer.py`. The server runs from `backend/docker-compose.dev.yml`. The SAF-specific ingest design is in `SAF_NATS_JetStream_Ingest_Guide.md`. This file teaches the ideas behind that code.

---

## Contents

1. [The problem NATS solves](#1-the-problem-nats-solves)
2. [Words you need](#2-words-you-need)
3. [Run NATS locally](#3-run-nats-locally)
4. [Your first messages](#4-your-first-messages)
5. [Subjects and wildcards](#5-subjects-and-wildcards)
6. [Core NATS in Python](#6-core-nats-in-python)
7. [Queue groups: share the work](#7-queue-groups-share-the-work)
8. [Request and reply](#8-request-and-reply)
9. [Why core NATS is not enough for ingest](#9-why-core-nats-is-not-enough-for-ingest)
10. [JetStream](#10-jetstream)
11. [Django](#11-django)
12. [Flask](#12-flask)
13. [A small shared library both frameworks can use](#13-a-small-shared-library-both-frameworks-can-use)
14. [Reliability habits](#14-reliability-habits)
15. [Security and operations](#15-security-and-operations)
16. [How this maps onto SAF](#16-how-this-maps-onto-saf)
17. [Practice path](#17-practice-path)
18. [Glossary](#18-glossary)

---

## 1. The problem NATS solves

A normal web app talks like a phone call.

```text
Client  --HTTP POST-->  Django or Flask  --write-->  Database
                         (must be up right now)
```

If the server is restarting, the caller gets an error and must retry. If ten services all need the same event ("a reading arrived"), the caller has to know every URL and call each one.

NATS talks like a notice board with named topics.

```text
Publisher  --message on subject "opc.readings"-->  NATS server
                                                      |
                              +-----------------------+------------------+
                              |                       |                  |
                         Django consumer         analytics copy      audit logger
```

The publisher does not know who is listening. Listeners do not know who published. The server routes by **subject** (the topic name).

Two modes matter:

| Mode | Stored on disk? | If nobody is listening | Typical use |
|------|-----------------|------------------------|-------------|
| **Core NATS** | No | Message is dropped | Live updates, chatty signals, request/reply |
| **JetStream** | Yes, in a stream | Message waits until a consumer takes it | Ingest, jobs, anything you cannot lose |

Start with core NATS so the model is clear. Use JetStream when a lost message would lose data.

---

## 2. Words you need

| Word | Meaning |
|------|---------|
| **Server** | The NATS process. Default client port is `4222`. |
| **Client** | Your Python process. It connects, then publishes or subscribes. |
| **Subject** | The address of a message. Dots separate tokens: `orders.created`, `opc.readings`. |
| **Payload** | The bytes in the message. Usually JSON. |
| **Publish** | Send one message to a subject. |
| **Subscribe** | Ask to receive messages on a subject. |
| **Queue group** | Several subscribers share one name. NATS gives each message to only one of them. |
| **Ack** | "I finished this message." JetStream uses this. Core NATS does not. |
| **Stream** | A JetStream log. It stores messages whose subjects match a list you configure. |
| **Consumer** | A JetStream reader with a position in the stream. Durable consumers remember that position after a restart. |

A subject is not a queue by itself. Many subscribers can all receive the same core-NATS message. A queue group, or a JetStream consumer, is what makes "only one worker handles this" true.

---

## 3. Run NATS locally

### Docker (same idea as this repo)

```bash
docker run --name nats -p 4222:4222 -p 8222:8222 nats:2.10-alpine -js -m 8222
```

- `-js` turns on JetStream.
- `-m 8222` turns on the monitoring HTTP port.
- Open `http://localhost:8222/varz` after it starts. You should see JSON about the server.

Stop it with `docker rm -f nats`.

### Official CLI (optional, useful while learning)

Install the `nats` CLI from [https://github.com/nats-io/natscli/releases](https://github.com/nats-io/natscli/releases), then:

```bash
nats server check connection
nats sub "greetings"
# in another terminal:
nats pub greetings "hello"
```

The subscriber terminal prints `hello`.

### Python package

```bash
pip install nats-py
```

The import name is `nats`, not `nats-py`.

---

## 4. Your first messages

Leave the server running. This program subscribes, publishes one message, prints it, and exits.

```python
import asyncio
import nats


async def main() -> None:
    nc = await nats.connect("nats://127.0.0.1:4222")

    async def on_msg(msg):
        print(f"got {msg.subject}: {msg.data.decode()}")

    await nc.subscribe("greetings", cb=on_msg)
    await nc.publish("greetings", b"hello from python")
    await nc.flush()          # wait until the server has the publish
    await asyncio.sleep(0.2)  # give the callback a moment to run
    await nc.drain()          # stop new work, finish in-flight messages, close


asyncio.run(main())
```

What happened:

1. `connect` opens a TCP connection to port 4222.
2. `subscribe` registers interest in `greetings` on that connection.
3. `publish` sends bytes. NATS does not parse JSON for you.
4. `flush` waits until those bytes have been written to the server.
5. `drain` is the polite shutdown. Prefer it to dropping the process.

If you publish **before** anyone is subscribed, core NATS drops the message. That is expected. Section 10 shows how JetStream keeps it.

---

## 5. Subjects and wildcards

Subjects are hierarchies of tokens:

```text
plant.pune.opc.readings
plant.pune.opc.events
plant.mumbai.opc.readings
```

| Pattern | Matches | Does not match |
|---------|---------|----------------|
| `plant.pune.opc.readings` | that exact subject | `plant.pune.opc.events` |
| `plant.*.opc.readings` | one token in place of `*` | `plant.pune.line1.opc.readings` (`*` is exactly one token) |
| `plant.>` | `plant` and anything after it | `other.plant.x` |

`>` is only valid as the last token.

Pick names that read as facts, not as commands aimed at one service:

- Good: `opc.readings`, `lab.samples`, `orders.created`
- Weaker: `django.save_reading`, `call_flask_now`

The publisher should describe what happened. Subscribers decide what to do.

---

## 6. Core NATS in Python

### Publish JSON

```python
import json
import nats

nc = await nats.connect("nats://127.0.0.1:4222")
body = json.dumps({"batch_id": "b-1", "value": 12.5}).encode()
await nc.publish("opc.readings", body)
await nc.flush()
```

### Headers

Headers are string metadata. They are useful for routing and tracing. Keep the business data in the body.

```python
from nats.aio.msg import Msg

await nc.publish(
    "opc.readings",
    body,
    headers={"Nats-Msg-Id": "b-1", "X-Plant": "pune"},
)
```

`Nats-Msg-Id` becomes important with JetStream deduplication (section 14).

### A long-running subscriber

```python
async def main() -> None:
    nc = await nats.connect("nats://127.0.0.1:4222")

    async def on_msg(msg):
        data = json.loads(msg.data.decode())
        print("handle", data)

    await nc.subscribe("opc.readings", cb=on_msg)
    # Stay up until the process is stopped.
    await asyncio.Future()
```

Callbacks run on the client's asyncio loop. If `on_msg` does slow work (database writes), do that work carefully so one slow message does not stall the connection. The Django section shows the usual fix: a thread or `sync_to_async`.

---

## 7. Queue groups: share the work

Two subscribers on the same subject **both** get every message. That is what you want for "save it" and "also count it."

Two workers that must **split** the messages use the same queue-group name:

```python
await nc.subscribe("jobs.resize", queue="image-workers", cb=on_msg)
```

Start that twice. Each message goes to one of the two processes. If one dies, the other keeps getting new messages. Core NATS does not replay messages the dead worker already received and had not finished. JetStream does, if you have not acked them.

---

## 8. Request and reply

Sometimes you want an answer, like HTTP, but over NATS.

Responder:

```python
async def on_req(msg):
    n = int(msg.data.decode())
    await msg.respond(str(n * 2).encode())

await nc.subscribe("math.double", cb=on_req)
```

Caller:

```python
reply = await nc.request("math.double", b"21", timeout=1.0)
print(reply.data.decode())  # 42
```

NATS creates a one-time inbox subject for the reply. Use this for short questions ("is this tag known?"). Use publish plus JetStream for work that must survive a crash.

---

## 9. Why core NATS is not enough for ingest

Core NATS is in memory and interested-party based.

- Publish while the consumer is down: the message is gone.
- Consumer crashes halfway through a database write: the message is gone.
- You restart the process: nothing is waiting for you.

That is fine for a dashboard ping. It is the wrong tool for "these OPC readings must land in Postgres."

JetStream stores the message first, then delivers it, then waits for your ack.

---

## 10. JetStream

Turn JetStream on with the `-js` server flag (the Docker command in section 3 already does this).

### Stream

A stream is the storage bucket. You choose a name and the subjects it captures.

```python
nc = await nats.connect("nats://127.0.0.1:4222")
js = nc.jetstream()

await js.add_stream(
    name="ORDERS",
    subjects=["orders.>"],
)
```

`orders.created` and `orders.paid` both land in `ORDERS`. A second `add_stream` with the same name and same subjects is safe to treat as "ensure it exists." If the stream already exists with a **different** config, the server returns an error. In production, create streams with the CLI or a small bootstrap script, and let applications only publish and consume.

Limits you will actually set:

| Setting | What it means |
|---------|----------------|
| `max_age` | Delete messages older than this (for example 7 days). |
| `max_bytes` / `max_msgs` | Cap disk use. |
| `storage="file"` | Survive a server restart. `memory` is faster and loses data on restart. |
| `duplicate_window` | How long the server remembers `Nats-Msg-Id` values and rejects repeats. Default is 2 minutes. |
| `retention` | `limits` (default) keeps messages until age/size limits. `workqueue` deletes a message after it is acked, so only one consumer type should read it. |

### Publish and wait for a storage ack

```python
ack = await js.publish("orders.created", b'{"id": "o-1"}', timeout=5)
# ack.stream == "ORDERS", ack.seq is the stream sequence number
```

If this call returns, JetStream has the message. If it raises, your code should retry or surface the error. Do not assume a bare `nc.publish` did this. Core publish does not wait for JetStream storage.

### Durable pull consumer

A **durable** name is the bookmark. After a restart, the consumer continues from the last ack.

**Pull** means your code asks for a batch. That is the style to use from Django and Flask workers. You control how many messages you take and how fast you write to the database.

```python
sub = await js.pull_subscribe(
    subject="orders.created",
    durable="django-orders",
    stream="ORDERS",
)

while True:
    try:
        msgs = await sub.fetch(batch=10, timeout=5)
    except asyncio.TimeoutError:
        continue
    for msg in msgs:
        try:
            handle(json.loads(msg.data.decode()))
        except Exception:
            await msg.nak()   # try again later
        else:
            await msg.ack()
```

Ack choices:

| Call | Meaning |
|------|---------|
| `ack()` | Done. Do not send it again. |
| `nak()` | Failed. Send it again (optionally after a delay). |
| `term()` | Poison. Do not retry. Put it on an error path if you have one. |
| `in_progress()` | Still working. Reset the ack timer so a long database write is not treated as a crash. |

If the process dies before `ack()`, JetStream delivers the message again. Your handler **must** be safe to run twice. That is the most important JetStream rule.

### Push consumer (know it, prefer pull for apps)

A push consumer has the server shove messages at you as they arrive. It feels like core NATS. Pull is easier to tune when the database is the slow part, which is the normal Django/Flask case.

### One stream, many consumers

Two durables on the same stream each get every message (unless you set work-queue retention):

- `django-orders` writes to Postgres
- `audit-orders` writes to an audit log

That is the "multiple subscribers later" benefit. The publisher stays unchanged.

---

## 11. Django

Django's request workers (Gunicorn, `runserver`) should stay focused on HTTP. NATS listeners should be a **separate process** that boots Django, then pulls messages.

Reasons:

- Each Gunicorn worker would otherwise open its own consumer and connection.
- A slow database write inside a request worker competes with web traffic.
- You can restart the consumer without dropping the API, and the reverse.

### Layout

```text
backend/
  config/settings.py          # NATS_URL, stream, subjects
  messaging/publisher.py      # called from views, signals, or management commands
  messaging/consumer.py       # long-running process: python -m messaging.consumer
```

### Settings

```python
# config/settings.py
import os

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
NATS_STREAM = os.environ.get("NATS_STREAM", "APP_EVENTS")
NATS_SUBJECT_ORDERS = os.environ.get("NATS_SUBJECT_ORDERS", "orders.created")
```

### Publisher used from a view

`nats-py` is async. Django views are usually sync. A small dedicated thread with one event loop keeps a connection open for the life of the process. Creating a loop per request works for a demo and wastes connections under load.

```python
# messaging/publisher.py
from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

import nats
from django.conf import settings


class JetStreamPublisher:
    def __init__(self, url: str, stream: str, subjects: list[str]) -> None:
        self._url = url
        self._stream = stream
        self._subjects = subjects
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._nc = asyncio.run_coroutine_threadsafe(self._connect(), self._loop).result()
        self._js = self._nc.jetstream()
        asyncio.run_coroutine_threadsafe(self._ensure_stream(), self._loop).result()

    async def _connect(self):
        return await nats.connect(self._url)

    async def _ensure_stream(self) -> None:
        try:
            await self._js.stream_info(self._stream)
        except Exception:
            await self._js.add_stream(name=self._stream, subjects=self._subjects)

    def publish(self, subject: str, payload: dict[str, Any], msg_id: str | None = None) -> None:
        data = json.dumps(payload).encode()
        headers = {"Nats-Msg-Id": msg_id} if msg_id else None
        fut = asyncio.run_coroutine_threadsafe(
            self._js.publish(subject, data, timeout=5, headers=headers),
            self._loop,
        )
        ack = fut.result(timeout=10)
        if ack is None:
            raise RuntimeError(f"no JetStream ack for {subject}")


_publisher: JetStreamPublisher | None = None
_lock = threading.Lock()


def get_publisher() -> JetStreamPublisher:
    global _publisher
    if _publisher is None:
        with _lock:
            if _publisher is None:
                _publisher = JetStreamPublisher(
                    settings.NATS_URL,
                    settings.NATS_STREAM,
                    [settings.NATS_SUBJECT_ORDERS],
                )
    return _publisher
```

View:

```python
# orders/views.py
from django.http import JsonResponse
from django.views.decorators.http import require_POST

from messaging.publisher import get_publisher


@require_POST
def create_order(request):
    order_id = "o-1001"  # replace with the real id you just saved
    get_publisher().publish(
        "orders.created",
        {"order_id": order_id},
        msg_id=order_id,
    )
    return JsonResponse({"status": "queued", "order_id": order_id})
```

Publish **after** the database transaction commits. If you publish inside `transaction.atomic()` and the transaction rolls back, subscribers will see an order that does not exist.

```python
from django.db import transaction

def create_order(request):
    with transaction.atomic():
        order = Order.objects.create(...)
        order_id = str(order.id)
        payload = {"order_id": order_id}
        transaction.on_commit(
            lambda: get_publisher().publish("orders.created", payload, msg_id=order_id)
        )
    return JsonResponse({"status": "queued", "order_id": order_id})
```

### Consumer process

The ORM is synchronous. Pull messages on the asyncio loop, and run the database work in a thread with `sync_to_async`. Call `close_old_connections()` so a long-lived process does not reuse a dead database connection.

```python
# messaging/consumer.py
from __future__ import annotations

import asyncio
import json
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

import nats
from asgiref.sync import sync_to_async
from django.conf import settings
from django.db import close_old_connections

from orders.models import Order  # your app


def apply_order(data: dict) -> None:
    close_old_connections()
    Order.objects.filter(id=data["order_id"]).update(status="processing")


async def run() -> None:
    nc = await nats.connect(settings.NATS_URL)
    js = nc.jetstream()
    sub = await js.pull_subscribe(
        subject=settings.NATS_SUBJECT_ORDERS,
        durable="django-orders",
        stream=settings.NATS_STREAM,
    )
    while True:
        try:
            batch = await sub.fetch(batch=10, timeout=5)
        except asyncio.TimeoutError:
            continue
        for msg in batch:
            try:
                data = json.loads(msg.data.decode())
                await sync_to_async(apply_order)(data)
            except Exception:
                await msg.nak()
            else:
                await msg.ack()


if __name__ == "__main__":
    asyncio.run(run())
```

Run it next to the web server:

```bash
python manage.py runserver
python -m messaging.consumer
```

In Docker Compose, that consumer is its own service, the same way `ingest-consumer` is separate from `api` in this repo.

### What not to do

- Do not `asyncio.run()` inside every request. It fights Django's own async views and opens a connection per call.
- Do not ack before the database commit succeeds.
- Do not skip idempotency. JetStream will redeliver.

### Management command alternative

`python -m messaging.consumer` is enough. A management command (`python manage.py nats_consume`) is the same loop with `django.setup()` already done for you. Pick one entry point and document it.

---

## 12. Flask

Flask has the same split: the web process publishes, a worker process consumes. Flask does not ship an ORM, so the consumer just calls whatever you use (SQLAlchemy, psycopg, an HTTP client).

### App factory and publisher

```python
# app.py
import os
from flask import Flask, jsonify, request

from messaging import JetStreamPublisher

def create_app() -> Flask:
    app = Flask(__name__)
    app.config["NATS_URL"] = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    publisher = JetStreamPublisher(
        app.config["NATS_URL"],
        stream="APP_EVENTS",
        subjects=["orders.created"],
    )

    @app.post("/orders")
    def create_order():
        body = request.get_json(force=True)
        order_id = body["order_id"]
        # Save to the database first, then publish.
        publisher.publish("orders.created", {"order_id": order_id}, msg_id=order_id)
        return jsonify(status="queued", order_id=order_id)

    return app
```

Use a publisher object with one background loop, as in section 11. Construct it once when the app is created. With Gunicorn, that means one connection per worker process, which is acceptable for publishing. Still keep the **listener** out of Gunicorn workers.

### Consumer

```python
# consumer.py
import asyncio
import json

import nats
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])


def apply_order(data: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE orders SET status = 'processing' WHERE id = :id"),
            {"id": data["order_id"]},
        )


async def run() -> None:
    nc = await nats.connect(os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    js = nc.jetstream()
    sub = await js.pull_subscribe(
        "orders.created",
        durable="flask-orders",
        stream="APP_EVENTS",
    )
    while True:
        try:
            batch = await sub.fetch(10, timeout=5)
        except asyncio.TimeoutError:
            continue
        for msg in batch:
            try:
                apply_order(json.loads(msg.data.decode()))
            except Exception:
                await msg.nak()
            else:
                await msg.ack()


if __name__ == "__main__":
    asyncio.run(run())
```

`apply_order` is blocking. For a first version, a batch size of 1 to 10 is fine because `fetch` is the only thing waiting on the network. When the database calls get slow enough to stall heartbeats, run `apply_order` in `asyncio.to_thread(...)`.

### Flask plus a message in the same request

The failure mode to avoid:

```text
insert row  ->  publish fails  ->  user sees 500  ->  row exists, no event
publish      ->  insert fails  ->  event exists, no row
```

Save first, publish second, and make the consumer tolerate a retry of the same `order_id`. If publish fails, return a clear error and have a sweep job that republishes rows stuck in `status='new'`.

---

## 13. A small shared library both frameworks can use

Django and Flask should not fork two copies of the connection code. Put the loop, stream ensure, publish, and pull loop in one module. Pass in a plain function `handle(dict) -> None`.

```text
messaging/
  publisher.py    # JetStreamPublisher
  consumer.py     # async def consume(url, stream, subject, durable, handle)
```

Django's `handle` calls the ORM. Flask's `handle` calls SQLAlchemy. The NATS code stays identical.

Subject names and stream names belong in environment variables, not in scattered string literals.

```text
NATS_URL=nats://127.0.0.1:4222
NATS_STREAM=APP_EVENTS
NATS_SUBJECT_ORDERS=orders.created
NATS_DURABLE=django-orders
```

---

## 14. Reliability habits

### Make handlers idempotent

Redelivery is normal. Use a unique key the publisher already has (`batch_id`, `order_id`).

```text
first delivery:  insert row id=o-1          -> ack
crash before ack
second delivery: row o-1 already exists     -> ack again, do not double-charge
```

In SQL this is `INSERT ... ON CONFLICT DO NOTHING` or an update that sets the same final state.

### Dedupe at the server too

Pass the same id every retry:

```python
await js.publish(subject, data, headers={"Nats-Msg-Id": batch_id})
```

Inside the stream's duplicate window, JetStream stores one copy. The window is not forever. The database unique key is the real guarantee. The header is a helpful extra.

### Bad messages

If JSON is invalid, `nak()` retries forever. After you know the payload can never succeed, `term()` it and log the body. Some teams publish a copy to `orders.created.dead` before `term()` so a human can inspect it.

Track `msg.metadata.num_delivered`. A simple policy: nak below 5 deliveries, term after that.

### Backpressure

If Postgres is slow, stop fetching huge batches. Pull 10, finish them, then fetch again. The stream holds the rest. That is the point of the buffer.

### Timeouts

Set a publish timeout (5–30 seconds). A hung publish inside a web request will hang the user. If you cannot afford that latency, write the outbox row in the request and let another process publish.

### Outbox (advanced, worth knowing)

The strongest web-app pattern:

1. In the same database transaction as the business write, insert a row into `outbox(id, subject, payload, sent_at null)`.
2. A publisher process reads unsent rows, publishes to JetStream, then sets `sent_at`.
3. The consumer is idempotent.

A crash at any step retries cleanly. Use this when "we committed the order but nobody was notified" is unacceptable. The in-request `transaction.on_commit(publish)` path is simpler and good enough for many apps.

---

## 15. Security and operations

Development can listen on `127.0.0.1:4222` with no password. A shared or plant network should not.

Minimum production set:

- Bind the client port on a private interface, or firewall it. In SAF, port 4222 is for the plant VPN, not the public internet.
- Enable accounts or at least a token / username: `nats:2` supports `--user` and `--pass`, and config-file auth is the real setup.
- Use TLS when the path crosses a network you do not fully trust.
- Turn on monitoring (`-m 8222`) on a port that is not public. Watch connections, slow consumers, and JetStream disk usage.
- Put JetStream data on a volume (`-sd /data`), as the Compose file in this repo does. Without a volume, a container recreate deletes the stream.
- Run one NATS process for learning. Clustering (three servers) is for uptime, and it is a later topic: clients use a URL list, JetStream uses clustered streams (R=3).

Health check from another container:

```bash
nats server check connection --server nats://nats:4222
```

Or HTTP: `GET http://nats:8222/healthz`.

---

## 16. How this maps onto SAF

SAF follows the Django pattern in this guide.

```text
saf_edge (publisher, nats-py JetStream)
    subjects like opc readings, lab samples, heartbeats
        ->  NATS with JetStream (Compose service "nats")
        ->  python -m ingestion.nats_consumer
        ->  the same ingest functions the HTTP API uses
        ->  Postgres / AlloyDB
```

Things this code already illustrates:

- A sync wrapper around an asyncio loop for a non-async agent (`NatsPublisher` in `saf_edge/saf_edge/nats_transport.py`).
- `django.setup()` in a standalone consumer, then `sync_to_async` around database work (`backend/ingestion/nats_consumer.py`).
- One stream for the data subjects, and a separate stream for heartbeats.
- HTTP remains available. NATS is a transport switch (`SYNC_TRANSPORT=nats`), not a replacement for the ingest rules.

When you change SAF itself, follow `SAF_NATS_JetStream_Ingest_Guide.md`. Use this file to understand why that design looks the way it does.

---

## 17. Practice path

Do these in order. Each one is small.

1. Start NATS with Docker. Publish and subscribe with the CLI.
2. Run the core-NATS Python sample in section 4.
3. Open two subscribers on the same subject and see both receive the message.
4. Add `queue="workers"` and see only one receive it.
5. Enable JetStream. Publish while the consumer is stopped. Start the consumer and see the old message arrive.
6. Kill the consumer after receive and before `ack()`. Start it again and see the redelivery. Make the handler ignore the second copy.
7. Add a Django **or** Flask app that saves a row, publishes on commit, and updates the row from a consumer process.
8. Only then add auth, TLS, and an outbox.

---

## 18. Glossary

| Term | Short definition |
|------|------------------|
| Core NATS | Pub/sub with no storage. |
| JetStream | The persistence layer: streams, consumers, acks. |
| Subject | Dot-separated message name. |
| Stream | Stored log of messages for a set of subjects. |
| Durable consumer | A named reader that remembers its ack position. |
| Pull consumer | Application asks for batches. Preferred for database workers. |
| Ack | Confirmation that handling finished. |
| Nak | "Failed, redeliver." |
| Term | "Never redeliver." |
| Queue group | Load-balancing group for core NATS subscribers. |
| `Nats-Msg-Id` | Header JetStream uses to drop duplicates inside the duplicate window. |
| At-least-once | JetStream's normal guarantee. You may see a message more than once, so handlers must be idempotent. |
| Drain | Graceful client shutdown. |

---

## Further reading

- NATS concepts: [https://docs.nats.io/nats-concepts/overview](https://docs.nats.io/nats-concepts/overview)
- JetStream: [https://docs.nats.io/nats-concepts/jetstream](https://docs.nats.io/nats-concepts/jetstream)
- Python client: [https://github.com/nats-io/nats.py](https://github.com/nats-io/nats.py)
- This repo's ingest design: `SAF_NATS_JetStream_Ingest_Guide.md`
