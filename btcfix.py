import asyncio
import json
import random
import string
from datetime import datetime, timezone
import websockets 

SOH = "\x01"
BEGIN_STR = "FIX.4.4"
SENDER = "ME"
TARGET = "YOU"

def utc_ts():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]

def calc_body_length(raw_without_10):
    return len(raw_without_10.encode("ascii"))

def calc_checksum(raw):
    s = sum(raw.encode("ascii")) % 256
    return f"{s:03d}"

def build_fix_message(fields):
    tags = dict(fields)
    header = [
        ("8", tags.get("8", BEGIN_STR)),
        ("9", "000"),  
    ]
    body = [(k, v) for k, v in fields if k not in ("8", "9", "10")]
    pre_checksum = SOH.join(f"{k}={v}" for k, v in header + body) + SOH
    body_len = calc_body_length(pre_checksum.split(SOH, 2)[2])  
    header[1] = ("9", str(body_len))
    pre_checksum = SOH.join(f"{k}={v}" for k, v in header + body) + SOH
    chk = calc_checksum(pre_checksum)
    final = pre_checksum + f"10={chk}" + SOH
    return final

_seq = 1
def next_seq():
    global _seq
    val = _seq
    _seq += 1
    return val

def rand_id(prefix="ID"):
    return prefix + ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def make_exec_report(symbol="BTC/USD", qty=None, price=None, side=None):
    qty = qty or round(random.uniform(0.001, 0.05), 6)
    price = price or round(random.uniform(20000, 80000), 2)
    side = side or random.choice(("1", "2"))  

    now = utc_ts()
    order_id = rand_id("ORD")
    exec_id = rand_id("EXE")

    fields = [
        ("35", "8"),             
        ("34", str(next_seq())), 
        ("49", SENDER),
        ("56", TARGET),
        ("52", now),
        ("37", order_id),       
        ("17", exec_id),       
        ("150", "F"),           
        ("39", "2"),             
        ("55", symbol),         
        ("54", side),         
        ("38", str(qty)),        
        ("14", str(qty)),        
        ("32", str(qty)),        
        ("31", str(price)),      
        ("6", str(price)),       
        ("60", now),             
        ("75", now.split("-")[0]),
    ]
    return build_fix_message(fields)


async def random_fix_stream(rate_per_sec=2):
    while True:
        msg = make_exec_report()
        print(msg, end="")  
        await asyncio.sleep(1 / rate_per_sec)


BINANCE = "wss://stream.binance.us:9443/ws/btcusdt@trade"

async def binance_to_fix():
    async with websockets.connect(BINANCE) as ws:
        async for raw in ws:
            t = json.loads(raw)
            price = float(t["p"])
            qty = float(t["q"])
            side = "2" if t["m"] else "1" 
            msg = make_exec_report(symbol="BTC/USDT", qty=qty, price=price, side=side)
            print(msg, end="")
            print()

def parse_args():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--gen", action="store_true", help="emit random FIX trade messages")
    ap.add_argument("--binance", action="store_true", help="mirror Binance trades as FIX")
    ap.add_argument("--rate", type=float, default=2.0, help="msgs/sec for --gen")
    return ap.parse_args()

async def main():
    args = parse_args()
    tasks = []
    if args.gen:
        tasks.append(asyncio.create_task(random_fix_stream(args.rate)))
    if args.binance:
        tasks.append(asyncio.create_task(binance_to_fix()))
    if not tasks:
        print("Nothing to do. Use --gen and/or --binance.", flush=True)
        return
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
