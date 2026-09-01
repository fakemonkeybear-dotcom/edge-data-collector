import asyncio, websockets, json, time, os
from datetime import datetime, timezone

SYMBOLS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
FLUSH_EVERY = 200
MAX_RUNTIME_SECONDS = 5.5 * 60 * 60  # stop before GitHub's 6h hard limit

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

async def collect():
    start = time.time()
    buffer = []
    file_idx = 0
    while time.time() - start < MAX_RUNTIME_SECONDS:
        try:
            async with websockets.connect(WS_URL, ping_interval=15) as ws:
                sub_args = []
                for s in SYMBOLS:
                    sub_args.append({"channel": "books5", "instId": s})
                    sub_args.append({"channel": "trades", "instId": s})
                await ws.send(json.dumps({"op": "subscribe", "args": sub_args}))
                print(f"[{datetime.now(timezone.utc)}] connected & subscribed")

                while time.time() - start < MAX_RUNTIME_SECONDS:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    msg = json.loads(raw)
                    if "data" not in msg:
                        continue
                    record = {
                        "recv_ts": datetime.now(timezone.utc).isoformat(),
                        "channel": msg.get("arg", {}).get("channel"),
                        "instId": msg.get("arg", {}).get("instId"),
                        "data": msg["data"]
                    }
                    buffer.append(record)

                    if len(buffer) >= FLUSH_EVERY:
                        fname = f"{DATA_DIR}/batch_{int(time.time())}_{file_idx}.jsonl"
                        with open(fname, "w") as f:
                            for r in buffer:
                                f.write(json.dumps(r) + "\n")
                        print(f"[{datetime.now(timezone.utc)}] flushed {len(buffer)} -> {fname}")
                        buffer = []
                        file_idx += 1

        except Exception as e:
            print(f"[{datetime.now(timezone.utc)}] disconnected: {e} — reconnecting in 5s")
            await asyncio.sleep(5)

    # final partial flush on shutdown
    if buffer:
        fname = f"{DATA_DIR}/batch_{int(time.time())}_final.jsonl"
        with open(fname, "w") as f:
            for r in buffer:
                f.write(json.dumps(r) + "\n")
        print(f"[{datetime.now(timezone.utc)}] final flush -> {fname}")

asyncio.run(collect())
