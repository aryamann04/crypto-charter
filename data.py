import datetime
import websockets
import argparse
import asyncio 
import motor.motor_asyncio
import json 

async def connect(collection_name):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://aryamannagpal04:BdSmL4sH9TVrvmpI@cluster0.2lsafkl.mongodb.net/?retryWrites=true&w=majority')
    db = client["db"]
    collection = db[collection_name]
    return collection 

def clear_data(collection): 
    collection.delete_many({})

async def add_data(sym):
    collection = await connect(sym)
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
