import os
import datetime
import websockets
import argparse
import asyncio
import motor.motor_asyncio
import json

_client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI"))
_db = _client["db"]

async def coll(collection: str):
    return _db[collection] 

async def clear_data(): 
    collections = await _db.list_collection_names()
    for c in collections:
        col = await coll(c)
        col.delete_many({})
        print(f"[cleared {c}]")
    print("[all collections cleared]")

async def add_data(sym):
    collection = await coll(sym)
    print(f"[successful connection to {sym}]")
    async with websockets.connect(f"wss://stream.binance.com:9443/ws/{sym}@trade") as ws:
        async for raw in ws:
            t = json.loads(raw)
            data = {
                "price": float(t["p"]),
                "quantity": float(t["q"]),
                "side": "2" if t["m"] else "1",
                "timestamp": t["T"]
            }
            print(data)
            await collection.insert_one(data)

'''
if __name__ == "__main__":
    try:
        asyncio.run(add_data("btcusdt"))
    except KeyboardInterrupt:
        pass
'''