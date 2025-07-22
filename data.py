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

async def clear_data(collection): 
    collection.delete_many({})

async def add_data(sym):
    collection = await coll(sym)
    print(f"[successful connection to {sym}]")
    async with websockets.connect(f"wss://stream.binance.us:9443/ws/{sym}@trade") as ws:
        async for raw in ws:
            t = json.loads(raw)
            data = {
                "price": float(t["p"]),
                "quantity": float(t["q"]),
                "side": "2" if t["m"] else "1",
                "timestamp": datetime.datetime.fromtimestamp(t["T"] / 1000.0)
            }
            await collection.insert_one(data)
            
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets", "-a", nargs="+", 
                    default=["btcusdt", "ethusdt", "solusdt", "dogeusdt"], 
                    help="list of lowercase symbols (e.g. btcusdt)")
    args = ap.parse_args()
    tasks = [asyncio.create_task(add_data(sym)) for sym in args.assets]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
