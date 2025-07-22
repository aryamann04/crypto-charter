import asyncio
import struct
import bisect
import time
import gc
import array
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import motor.motor_asyncio

from memlog import init_log, log_mem_point

MEMLOG = "mem.log"
init_log(MEMLOG)

PORT = 3000
MAX_POINTS = 100
COLLECTION_LIST = ["Asset1", "Asset2"]  
collection_handlers = {}

app = FastAPI()

log_mem_point("FASTAPI Initialized", MEMLOG)

# ---------------- MongoDB Connection ---------------- #
async def mongoconnect(collection_name: str):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://gn-algo2:27017/')
    db = client["Arya"]
    return db[collection_name]
# ---------------------------------------------------- #

# ---------------- Utility Functions ---------------- #
def fast_parse(ts: str) -> int:
    hh, mm, ss = map(int, (ts[:2], ts[3:5], ts[6:8]))
    ms = int(ts[9:12])
    base = (9 * 3600 + 30 * 60) * 1000
    return (hh * 3600 + mm * 60 + ss) * 1000 + ms - base

def linspace(start, stop, num):
    if num == 1:
        yield stop
        return
    step = (stop - start) / (num - 1)
    for i in range(num):
        yield start + i * step

def interp(x, xp, fp):
    def interp1(xi):
        i = bisect.bisect_left(xp, xi)
        if i == 0:
            return fp[0]
        if i == len(xp):
            return fp[-1]
        x0, x1 = xp[i - 1], xp[i]
        f0, f1 = fp[i - 1], fp[i]
        return f0 + (f1 - f0) * (xi - x0) / (x1 - x0)

    return [interp1(xi) for xi in x] if hasattr(x, '__iter__') else interp1(x)
# --------------------------------------------------- #

# ------------------ Handler Class ------------------ #
class CollectionHandler:
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.clients: set[WebSocket] = set()
        self.clients_lock = asyncio.Lock()

        self.DATA_TIMES = array.array('I')
        self.DATA_VALUES = array.array('f')
        self.DATA_VALUE2S = array.array('f')
        self.DATA_VALUE3S = array.array('f')
        self.GLOBAL_MAX = 0
        self.COLLECTION = None

    async def setup(self):
        self.COLLECTION = await mongoconnect(self.collection_name)
        asyncio.create_task(self.read_mongo())

    async def broadcast_global_max(self):
        msg = {"action": "global_max", "globalMax": self.GLOBAL_MAX}
        async with self.clients_lock:
            to_remove = []
            for ws in list(self.clients):
                try:
                    await ws.send_json(msg)
                except WebSocketDisconnect:
                    to_remove.append(ws)
            for ws in to_remove:
                self.clients.discard(ws)

    def get_slice(self, start: int, end: int):
        if len(self.DATA_TIMES) == 0:
            return []

        i0 = bisect.bisect_left(self.DATA_TIMES, start)
        i1 = bisect.bisect_right(self.DATA_TIMES, end)

        xs = self.DATA_TIMES[i0:i1]
        ys1 = self.DATA_VALUES[i0:i1]
        ys2 = self.DATA_VALUE2S[i0:i1]
        ys3 = self.DATA_VALUE3S[i0:i1]

        if len(xs) <= MAX_POINTS:
            return list(zip(xs, ys1, ys2, ys3))

        xs2 = list(linspace(xs[0], xs[-1], MAX_POINTS))
        ys1i = interp(xs2, xs, ys1)
        ys2i = interp(xs2, xs, ys2)
        ys3i = interp(xs2, xs, ys3)
        return list(zip(xs2, ys1i, ys2i, ys3i))

    def pack_points(self, pts):
        buf = bytearray(len(pts) * 16)
        off = 0
        for t, v1, v2, v3 in pts:
            struct.pack_into("<Ifff", buf, off, int(t), v1, v2, v3)
            off += 16
        return buf

    async def read_mongo(self):
        cursor = self.COLLECTION.find({}, {"timestamp": 1, "value": 1, "value2": 1, "value3": 1}).sort("timestamp", 1)
        raw = [(doc["timestamp"], doc["value"], doc.get("value2", 0), doc.get("value3", 0)) async for doc in cursor]

        if raw:
            t0 = fast_parse(raw[0][0])
            self.DATA_TIMES = array.array('I', (fast_parse(t) - t0 for t, _, _, _ in raw))
            self.DATA_VALUES = array.array('f', (float(v) for _, v, _, _ in raw))
            self.DATA_VALUE2S = array.array('f', (float(v2) for _, _, v2, _ in raw))
            self.DATA_VALUE3S = array.array('f', (float(v3) for _, _, _, v3 in raw))
            self.GLOBAL_MAX = self.DATA_TIMES[-1]
        else:
            self.DATA_TIMES = array.array('I')
            self.DATA_VALUES = array.array('f')
            self.DATA_VALUE2S = array.array('f')
            self.DATA_VALUE3S = array.array('f')
            t0 = 0

        last_ping = 0.0
        last_timestamp = raw[-1][0] if raw else None
        del raw, cursor
        gc.collect()
        log_mem_point(f"{self.collection_name}: Initial data loaded", MEMLOG)

        while True:
            query = {"timestamp": {"$gt": last_timestamp}} if last_timestamp else {}
            new_docs_cursor = self.COLLECTION.find(query, {"timestamp": 1, "value": 1, "value2": 1, "value3": 1}).sort("timestamp", 1)
            new_docs = [doc async for doc in new_docs_cursor]

            if new_docs:
                for doc in new_docs:
                    timestamp = doc["timestamp"]
                    value = float(doc["value"])
                    value2 = float(doc.get("value2", 0))
                    value3 = float(doc.get("value3", 0))
                    new_t = fast_parse(timestamp) - t0

                    self.DATA_TIMES.append(int(new_t))
                    self.DATA_VALUES.append(value)
                    self.DATA_VALUE2S.append(value2)
                    self.DATA_VALUE3S.append(value3)

                    last_timestamp = timestamp
                    self.GLOBAL_MAX = new_t

                gc.collect()
                log_mem_point(f"{self.collection_name}: Appended new docs", MEMLOG)
            else:
                # No new docs this loop
                pass

            del new_docs, new_docs_cursor, query
            gc.collect()
            now_time = time.time()
            if now_time - last_ping >= 1.0:
                last_ping = now_time
                await self.broadcast_global_max()

            await asyncio.sleep(0.2)
# ---------------------------------------------------- #

# ---------------- Route Handlers ---------------- #
@app.get("/{c}")
async def serve_index(c: str):
    if c not in collection_handlers:
        return {"error": f"Collection '{c}' not found"}
    return FileResponse("index.html")

@app.websocket("/ws/{c}")
async def collection_ws(ws: WebSocket, c: str):
    if c not in collection_handlers:
        await ws.close()
        return

    handler = collection_handlers[c]
    await ws.accept()
    async with handler.clients_lock:
        handler.clients.add(ws)

    await ws.send_json({"action": "global_max", "globalMax": int(handler.GLOBAL_MAX)})

    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("action") == "manual_fetch":
                t0, t1 = int(msg["t_start"]), int(msg["t_end"])
                pts = handler.pack_points(handler.get_slice(t0, t1))
                await ws.send_bytes(pts)
                del pts
                gc.collect()
    except WebSocketDisconnect:
        pass
    except Exception as e:
        pass
    finally:
        async with handler.clients_lock:
            handler.clients.discard(ws)
# ------------------------------------------------ #

# ---------------- Startup ---------------- #
@app.on_event("startup")
async def startup():
    for c in COLLECTION_LIST:
        handler = CollectionHandler(collection_name=c)
        await handler.setup()
        collection_handlers[c] = handler
# ----------------------------------------- #

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)