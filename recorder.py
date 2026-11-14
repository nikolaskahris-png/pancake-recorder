"""pancake-recorder: ζωντανή καταγραφή του PancakeSwap Prediction (BNBUSD) για GitHub Actions.

Τι γράφει (append-only, gzip, ανά ώρα):
  chain:  κάθε log του contract (πονταρίσματα, start/lock/end round) και του Chainlink aggregator,
          με blockNumber, blockTimestamp, transactionIndex, logIndex + ώρα λήψης (wall + monotonic)
  tx:     για κάθε ποντάρισμα: gasPrice, priority fee, nonce, αποστολέας
  head:   κάθε φορά που βλέπουμε νέο block (ώρα λήψης)
  pool:   pools του τρέχοντος γύρου + Chainlink latestRoundData κάθε ~1s στα τελευταία 45s πριν το κλείδωμα
  price:  τιμή BNB από όσες πηγές απαντούν (τελευταία τιμή ανά 200ms όταν αλλάζει)
  health: σύνοψη στο τέλος κάθε run
"""
import argparse, gzip, json, os, queue, random, sys, threading, time, traceback
from datetime import datetime, timezone
import requests

RPCS = ["https://bsc-rpc.publicnode.com", "https://bsc-dataseed.bnbchain.org", "https://bsc.drpc.org"]
CONTRACT = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA"
SEL = {"currentEpoch": "0x76671808", "rounds": "0x8c65c81f", "oracle": "0x7dc0d1d0", "aggregator": "0x245a7bfc", "latestRoundData": "0xfeaf968c"}
BET_TOPICS = {"0x438122d8cff518d18388099a5181f0d17a12b4f1b55faedf6e4a6acee0060c12": "bull",
              "0x0d8c1fe3e67ab767116a81f122b83c2557a8c2564019cb7c4f83de1aeb1f1f0d": "bear"}
PRICE_SOURCES = {
    "binance_vision": {"url": "wss://data-stream.binance.vision/ws/bnbusdt@trade", "sub": None},
    # binance.com / fstream / binance.us: μπλοκαρισμένα ή νεκρά από τους runners των ΗΠΑ (δοκιμή 14/11/2026: 451 / timeouts)
    "okx":            {"url": "wss://ws.okx.com:8443/ws/v5/public", "sub": {"op": "subscribe", "args": [{"channel": "trades", "instId": "BNB-USDT"}]}},
    "bybit":          {"url": "wss://stream.bybit.com/v5/public/spot", "sub": {"op": "subscribe", "args": ["publicTrade.BNBUSDT"]}},
    "gate":           {"url": "wss://api.gateio.ws/ws/v4/", "sub": {"time": 0, "channel": "spot.trades", "event": "subscribe", "payload": ["BNB_USDT"]}},
}
STOP = threading.Event()
H = {"started": datetime.now(timezone.utc).isoformat(), "rpc_errors": 0, "rpc_calls": 0, "logs": 0, "bets": 0, "tx": 0, "heads": 0,
     "pool_snaps": 0, "max_poll_gap_s": 0.0, "max_block_lag": 0, "price": {}, "errors": [], "rounds_started": 0}
HL = threading.Lock()

def now(): return time.time(), time.monotonic()

class Out:
    """Ένα gzip αρχείο ανά (ώρα, είδος). Flush κάθε 10s, ώστε ένα κόψιμο να χάνει το πολύ ~10s."""
    def __init__(self, base, run):
        self.base, self.run, self.f, self.lock, self.last_flush = base, run, {}, threading.Lock(), time.monotonic()
    def write(self, kind, obj):
        hour = datetime.now(timezone.utc).strftime("%Y-%m-%d/%H")
        key = (hour, kind)
        with self.lock:
            if key not in self.f:
                for k in [k for k in self.f if k[1] == kind]:
                    self.f.pop(k).close()
                d = os.path.join(self.base, hour.split("/")[0]); os.makedirs(d, exist_ok=True)
                self.f[key] = gzip.open(os.path.join(d, f"{hour.split('/')[1]}_{self.run}_{kind}.jsonl.gz"), "at", encoding="utf-8")
            self.f[key].write(json.dumps(obj, separators=(",", ":")) + "\n")
            if time.monotonic() - self.last_flush > 10:
                for fh in self.f.values(): fh.flush()
                self.last_flush = time.monotonic()
    def close(self):
        with self.lock:
            for fh in self.f.values(): fh.close()
            self.f = {}

def err(msg):
    with HL:
        H["errors"].append(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {msg}"[:300])
        H["errors"] = H["errors"][-200:]

class RPC:
    def __init__(self):
        self.i = 0; self.s = requests.Session()
    def call(self, method, params, tries=4):
        last = None
        for t in range(tries):
            url = RPCS[self.i % len(RPCS)]
            try:
                with HL: H["rpc_calls"] += 1
                r = self.s.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=6)
                j = r.json()
                if "error" in j: raise RuntimeError(str(j["error"])[:150])
                return j["result"]
            except Exception as e:
                last = e
                with HL: H["rpc_errors"] += 1
                self.i += 1; time.sleep(0.3 * (t + 1))
        raise RuntimeError(f"{method} failed: {last}")

def word(h, k): return int(h[2 + 64*k: 2 + 64*(k+1)], 16)
def sword(h, k):
    v = word(h, k); return v - (1 << 256) if v >= (1 << 255) else v

def chain_loop(rpc, out, aggregator):
    last = int(rpc.call("eth_blockNumber", []), 16)
    txq = queue.Queue()
    def tx_worker():
        r2 = RPC()
        while not STOP.is_set() or not txq.empty():
            try: h = txq.get(timeout=1)
            except queue.Empty: continue
            try:
                tx = r2.call("eth_getTransactionByHash", [h])
                if tx:
                    w, m = now()
                    out.write("tx", {"hash": h, "from": tx.get("from"), "nonce": tx.get("nonce"), "gasPrice": tx.get("gasPrice"),
                                     "maxPriorityFeePerGas": tx.get("maxPriorityFeePerGas"), "maxFeePerGas": tx.get("maxFeePerGas"),
                                     "gas": tx.get("gas"), "type": tx.get("type"), "to": tx.get("to"), "value": tx.get("value"),
                                     "input4": (tx.get("input") or "")[:10], "recv_wall": w})
                    with HL: H["tx"] += 1
            except Exception as e: err(f"tx {h[:10]}: {e}")
    threading.Thread(target=tx_worker, daemon=True).start()
    prev_poll = time.monotonic()
    while not STOP.is_set():
        try:
            bn = int(rpc.call("eth_blockNumber", []), 16)
            w, m = now()
            gap = m - prev_poll; prev_poll = m
            with HL: H["max_poll_gap_s"] = max(H["max_poll_gap_s"], round(gap, 2))
            if bn > last:
                out.write("head", {"n": bn, "recv_wall": w, "recv_mono": m})
                with HL: H["heads"] += 1; H["max_block_lag"] = max(H["max_block_lag"], bn - last)
                frm = last + 1
                while frm <= bn:
                    to = min(bn, frm + 199)
                    logs = rpc.call("eth_getLogs", [{"address": [CONTRACT, aggregator], "fromBlock": hex(frm), "toBlock": hex(to)}])
                    w2, m2 = now()
                    for l in logs:
                        rec = {"addr": l["address"][:6], "block": int(l["blockNumber"], 16),
                               "bts": int(l["blockTimestamp"], 16) if l.get("blockTimestamp") else None,
                               "txi": int(l["transactionIndex"], 16), "li": int(l["logIndex"], 16), "tx": l["transactionHash"],
                               "topics": l["topics"], "data": l["data"], "recv_wall": w2, "recv_mono": m2}
                        side = BET_TOPICS.get(l["topics"][0])
                        if side and l["address"].lower() == CONTRACT.lower():
                            rec["bet"] = {"side": side, "sender": "0x" + l["topics"][1][-40:], "epoch": int(l["topics"][2], 16),
                                          "amount_wei": str(int(l["data"], 16))}
                            txq.put(l["transactionHash"])
                            with HL: H["bets"] += 1
                        out.write("chain", rec)
                        with HL: H["logs"] += 1
                    frm = to + 1
                last = bn
        except Exception as e:
            err(f"chain: {e}"); time.sleep(1)
        time.sleep(0.25)

def pool_loop(rpc, out, proxy):
    """Pools + Chainlink κάθε ~1s στα τελευταία 45s πριν το προγραμματισμένο κλείδωμα (έλεγχος για τα logs)."""
    ep, lock_ts = None, 0
    while not STOP.is_set():
        try:
            e = int(rpc.call("eth_call", [{"to": CONTRACT, "data": SEL["currentEpoch"]}, "latest"]), 16)
            if e != ep:
                h = rpc.call("eth_call", [{"to": CONTRACT, "data": SEL["rounds"] + format(e, "064x")}, "latest"])
                ep, lock_ts = e, word(h, 2)
                with HL: H["rounds_started"] += 1
            left = lock_ts - time.time()
            if -12 < left < 45:
                h = rpc.call("eth_call", [{"to": CONTRACT, "data": SEL["rounds"] + format(ep, "064x")}, "latest"])
                c = rpc.call("eth_call", [{"to": proxy, "data": SEL["latestRoundData"]}, "latest"])
                w, m = now()
                out.write("pool", {"epoch": ep, "sched_lock": lock_ts, "secs_left": round(left, 3), "total": str(word(h, 8)), "bull": str(word(h, 9)),
                                   "bear": str(word(h, 10)), "lockP": sword(h, 4), "cl_round": str(word(c, 0)), "cl_answer": sword(c, 1),
                                   "cl_updatedAt": word(c, 3), "recv_wall": w, "recv_mono": m})
                with HL: H["pool_snaps"] += 1
                time.sleep(0.9)
            else:
                time.sleep(min(5, max(0.5, left - 45)) if left > 45 else 2)
        except Exception as e:
            err(f"pool: {e}"); time.sleep(2)

def price_source(name, cfg, out):
    import websocket  # websocket-client
    st = {"msgs": 0, "connects": 0, "fails": 0, "last_error": None}
    with HL: H["price"][name] = st
    backoff = 1
    while not STOP.is_set():
        bucket, lastp, n, vol = None, None, 0, 0.0
        try:
            ws = websocket.create_connection(cfg["url"], timeout=10)
            st["connects"] += 1; backoff = 1
            if cfg["sub"]: ws.send(json.dumps(cfg["sub"]))
            last_ping = time.monotonic()
            while not STOP.is_set():
                if name == "binance_vision" and time.monotonic() - last_ping > 60: ws.ping(); last_ping = time.monotonic()
                if name in ("okx",) and time.monotonic() - last_ping > 20: ws.send("ping"); last_ping = time.monotonic()
                if name == "bybit" and time.monotonic() - last_ping > 20: ws.send(json.dumps({"op": "ping"})); last_ping = time.monotonic()
                if name == "gate" and time.monotonic() - last_ping > 20: ws.send(json.dumps({"time": int(time.time()), "channel": "spot.ping"})); last_ping = time.monotonic()
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    st["idle"] = st.get("idle", 0) + 1
                    continue  # λίγες συναλλαγές: δεν είναι σφάλμα, στέλνουμε ping στον επόμενο κύκλο
                if raw in ("pong", ""): continue
                msg = json.loads(raw); trades = []
                if name.startswith("binance"):
                    if "p" in msg: trades = [(float(msg["p"]), float(msg["q"]), int(msg.get("T") or msg.get("E")))]
                elif name == "okx":
                    trades = [(float(d["px"]), float(d["sz"]), int(d["ts"])) for d in msg.get("data", [])]
                elif name == "bybit":
                    trades = [(float(d["p"]), float(d["v"]), int(d["T"])) for d in msg.get("data", [])] if isinstance(msg.get("data"), list) else []
                elif name == "gate":
                    r = msg.get("result")
                    if msg.get("event") == "update" and isinstance(r, dict): trades = [(float(r["price"]), float(r["amount"]), int(float(r["create_time_ms"])))]
                w, m = now()
                for p, q, ts in trades:
                    st["msgs"] += 1
                    b = int(w * 5)  # 200ms
                    if bucket is not None and b != bucket and lastp is not None:
                        out.write("price", {"src": name, "bucket_wall": bucket / 5, "last": lastp, "n": n, "vol": round(vol, 6), "ex_ts": last_ts, "recv_wall": w, "recv_mono": m})
                        n, vol = 0, 0.0
                    bucket, lastp, last_ts = b, p, ts; n += 1; vol += q
        except Exception as e:
            st["fails"] += 1; st["last_error"] = str(e)[:150]
            time.sleep(backoff); backoff = min(backoff * 2, 120)

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--minutes", type=float, default=345); ap.add_argument("--out", default="data")
    a = ap.parse_args()
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + f"_{random.randint(0, 9999):04d}"
    out = Out(a.out, run); rpc = RPC()
    proxy = "0x" + rpc.call("eth_call", [{"to": CONTRACT, "data": SEL["oracle"]}, "latest"])[-40:]
    aggregator = "0x" + rpc.call("eth_call", [{"to": proxy, "data": SEL["aggregator"]}, "latest"])[-40:]
    H.update({"run": run, "proxy": proxy, "aggregator": aggregator, "minutes": a.minutes})
    print("run", run, "oracle proxy", proxy, "aggregator", aggregator, flush=True)
    ths = [threading.Thread(target=chain_loop, args=(RPC(), out, aggregator), daemon=True),
           threading.Thread(target=pool_loop, args=(RPC(), out, proxy), daemon=True)]
    ths += [threading.Thread(target=price_source, args=(n, c, out), daemon=True) for n, c in PRICE_SOURCES.items()]
    for t in ths: t.start()
    end = time.monotonic() + a.minutes * 60
    try:
        while time.monotonic() < end:
            time.sleep(30)
            with HL: print(json.dumps({k: H[k] for k in ("heads", "logs", "bets", "tx", "pool_snaps", "rpc_errors", "max_poll_gap_s")}), {k: v["msgs"] for k, v in H["price"].items()}, flush=True)
    finally:
        STOP.set(); time.sleep(3); out.close()
        H["ended"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.join(a.out, "health"), exist_ok=True)
        with open(os.path.join(a.out, "health", f"{run}.json"), "w") as f: json.dump(H, f, indent=1)
        print("health:", json.dumps({k: v for k, v in H.items() if k != "errors"}), flush=True)

if __name__ == "__main__":
    main()
