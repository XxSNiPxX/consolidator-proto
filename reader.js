#!/usr/bin/env node
// reader.js — full self-contained version with watermark support

const fs = require("fs");
const mmap = require("@riaskov/mmap-io"); // npm i @riaskov/mmap-io
const readline = require("readline");
const argv = process.argv.slice(2);

// CLI parsing
let FILE_PATH = "./data/global.mm";
let SHOW = "both";
let PRETTY = false;
let FILTER_EXCHANGE = null;
let FILTER_SYMBOL = null;
let WATERMARK = 0; // default disabled

for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a.startsWith("--show=")) SHOW = a.split("=")[1];
  else if (a === "--pretty") PRETTY = true;
  else if (a.startsWith("--exchange=")) FILTER_EXCHANGE = argv[i].split("=")[1];
  else if (a.startsWith("--symbol=")) FILTER_SYMBOL = argv[i].split("=")[1];
  else if (a.startsWith("--watermark="))
    WATERMARK = parseInt(argv[i].split("=")[1], 10) || 0;
  else if (!a.startsWith("-") && i === argv.length - 1) FILE_PATH = a;
}

const VALID_SHOW = new Set([
  "trades",
  "books",
  "combined",
  "gob",
  "gobjson",
  "both",
  "all",
  "none",
]);
if (!VALID_SHOW.has(SHOW)) {
  console.error(
    "unknown --show value, must be one of:",
    Array.from(VALID_SHOW).join(", "),
  );
  process.exit(2);
}

const HEADER_SIZE = 4096;
const ENVELOPE_SIZE = 32;

const RTYPE = {
  1: "BookSnapshot",
  2: "TradePrint",
  3: "CombinedTick",
  4: "GlobalOrderBook",
  5: "GlobalOrderBookJson",
};

/* --- Binary helpers --- */
function u64(buf, o) {
  return [Number(buf.readBigUInt64LE(o)), o + 8];
}
function i64(buf, o) {
  return [Number(buf.readBigInt64LE(o)), o + 8];
}
function u32(buf, o) {
  return [buf.readUInt32LE(o), o + 4];
}
function u8(buf, o) {
  return [buf.readUInt8(o), o + 1];
}
function f64(buf, o) {
  return [buf.readDoubleLE(o), o + 8];
}

/* Safe bincode-style string reader used by previous JS reader */
/* bincode serializes a u64 length then that many bytes */
function safeReadString(buf, o) {
  let len;
  [len, o] = u64(buf, o);
  if (!Number.isFinite(len) || len < 0 || Number(len) > buf.length - o) {
    throw new Error(`string length OOB: ${len} at offset ${o}`);
  }
  const s = buf.subarray(o, o + Number(len)).toString("utf8");
  return [s, o + Number(len)];
}
function str(buf, o) {
  return safeReadString(buf, o);
}
function assetKind(buf, o) {
  let tag;
  [tag, o] = u32(buf, o);
  return [tag === 0 ? "Spot" : tag === 1 ? "Futures" : `Unknown(${tag})`, o];
}
function tradeSide(buf, o) {
  let tag;
  [tag, o] = u32(buf, o);
  return [tag === 0 ? "Buy" : tag === 1 ? "Sell" : "Unknown", o];
}
function optStr(buf, o) {
  let flg;
  [flg, o] = u8(buf, o);
  if (flg === 0) return [null, o];
  return str(buf, o);
}

/* --- Decoders matching Rust structs --- */
function decodeBookSnapshot(buf) {
  // Rust OrderBookSnapshot: exchange: String, asset: String, kind: AssetKind, bid: f64, ask: f64, mid: f64, ts_ms: i64
  let o = 0;
  let exchange;
  [exchange, o] = str(buf, o);
  let asset;
  [asset, o] = str(buf, o);
  let kind;
  [kind, o] = assetKind(buf, o);
  let bid;
  [bid, o] = f64(buf, o);
  let ask;
  [ask, o] = f64(buf, o);
  let mid;
  [mid, o] = f64(buf, o);
  let ts;
  [ts, o] = i64(buf, o);
  return { exchange, asset, kind, bid, ask, mid, ts_ms: ts };
}

function decodeTradePrint(buf) {
  // Rust TradePrint: exchange: String, asset: String, kind: AssetKind, px: f64, sz: f64, side: TradeSide, ts_ms: i64
  let o = 0;
  let exchange;
  [exchange, o] = str(buf, o);
  let asset;
  [asset, o] = str(buf, o);
  let kind;
  [kind, o] = assetKind(buf, o);
  let px;
  [px, o] = f64(buf, o);
  let sz;
  [sz, o] = f64(buf, o);
  let side;
  [side, o] = tradeSide(buf, o);
  let ts;
  [ts, o] = i64(buf, o);
  return { exchange, asset, kind, px, sz, side, ts_ms: ts };
}

function decodeCombinedTick(buf) {
  // Rust CombinedTick: ts_ms, spot: OrderBookSnapshot, fut: OrderBookSnapshot, spread, stale(bool as u8), stale_reason: Option<String>
  let o = 0;
  let ts_ms;
  [ts_ms, o] = i64(buf, o);

  let s_exchange;
  [s_exchange, o] = str(buf, o);
  let s_asset;
  [s_asset, o] = str(buf, o);
  let s_kind;
  [s_kind, o] = assetKind(buf, o);
  let s_bid;
  [s_bid, o] = f64(buf, o);
  let s_ask;
  [s_ask, o] = f64(buf, o);
  let s_mid;
  [s_mid, o] = f64(buf, o);
  let s_ts;
  [s_ts, o] = i64(buf, o);

  let f_exchange;
  [f_exchange, o] = str(buf, o);
  let f_asset;
  [f_asset, o] = str(buf, o);
  let f_kind;
  [f_kind, o] = assetKind(buf, o);
  let f_bid;
  [f_bid, o] = f64(buf, o);
  let f_ask;
  [f_ask, o] = f64(buf, o);
  let f_mid;
  [f_mid, o] = f64(buf, o);
  let f_ts;
  [f_ts, o] = i64(buf, o);

  let spread;
  [spread, o] = f64(buf, o);
  let stale_u8;
  [stale_u8, o] = u8(buf, o);
  let stale_reason;
  [stale_reason, o] = optStr(buf, o);

  return {
    ts_ms,
    spot: {
      exchange: s_exchange,
      asset: s_asset,
      kind: s_kind,
      bid: s_bid,
      ask: s_ask,
      mid: s_mid,
      ts_ms: s_ts,
    },
    fut: {
      exchange: f_exchange,
      asset: f_asset,
      kind: f_kind,
      bid: f_bid,
      ask: f_ask,
      mid: f_mid,
      ts_ms: f_ts,
    },
    spread,
    stale: !!stale_u8,
    stale_reason,
  };
}

/* Read Envelope */
function readEnvelope(buf, off) {
  const rtype = buf.readUInt16LE(off + 0);
  const flags = buf.readUInt16LE(off + 2);
  const ts_ms = buf.readBigUInt64LE(off + 4);
  const src_id = buf.readUInt32LE(off + 12);
  const inst_id = buf.readUInt32LE(off + 16);
  const len = buf.readUInt32LE(off + 20);
  const pad = buf.readUInt32LE(off + 24);
  return { rtype, flags, ts_ms, src_id, inst_id, len, pad };
}

/* ASCII substring helper for debugging unknown payloads */
function findAsciiSubstrings(payload) {
  const s = payload.toString("latin1");
  const regex = /[ -~]{3,}/g;
  const res = [];
  let m;
  while ((m = regex.exec(s))) {
    res.push(m[0]);
    if (res.length >= 8) break;
  }
  return res;
}

/* JSON resilient parsing helpers (tries to extract book/trade-like objects) */
function tryParseJson(buf) {
  try {
    const s = buf.toString("utf8");
    return JSON.parse(s);
  } catch (e) {
    return null;
  }
}

function normalizeExchangeName(s) {
  if (!s) return null;
  return String(s).trim().toLowerCase();
}
function normalizeSymbol(s) {
  if (!s) return null;
  return String(s)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function tryParseJsonToTrade(buf) {
  const js = tryParseJson(buf);
  if (!js) return null;
  const core =
    js.params && js.params.data ? js.params.data : js.data ? js.data : js;
  let px = null,
    sz = null,
    side = null,
    ts = null,
    exchange = null,
    symbol = null;

  if ("p" in core) px = Number(core.p);
  if ("price" in core) px = Number(core.price);
  if ("q" in core) sz = Number(core.q);
  if ("qty" in core) sz = Number(core.qty);
  if ("size" in core) sz = Number(core.size);
  if ("quantity" in core) sz = Number(core.quantity);

  if ("m" in core && typeof core.m === "boolean")
    side = core.m === false ? "Buy" : "Sell";
  if ("side" in core) side = String(core.side);
  if ("S" in core)
    side =
      core.S === "BUY" ? "Buy" : core.S === "SELL" ? "Sell" : String(core.S);

  if ("E" in core) ts = Number(core.E);
  if ("T" in core) ts = Number(core.T);
  if ("ts" in core) ts = Number(core.ts);
  if ("timestamp" in core) ts = Number(core.timestamp);
  if ("time" in core) ts = Number(core.time);

  if ("s" in core) symbol = core.s;
  if ("symbol" in core) symbol = core.symbol;
  if ("instrument_name" in core) symbol = core.instrument_name;
  if ("exchange" in core) exchange = core.exchange;
  if ("e" in core && !exchange) exchange = core.e;

  if (Number.isFinite(px) && Number.isFinite(sz)) {
    return {
      exchange: exchange ? String(exchange) : null,
      asset: symbol ? String(symbol) : null,
      px,
      sz,
      side: side || null,
      ts_ms: Number.isFinite(ts) ? ts : null,
      raw: core,
    };
  }
  return null;
}

function tryParseJsonToBook(buf) {
  const js = tryParseJson(buf);
  if (!js) return null;
  const core =
    js.params && js.params.data ? js.params.data : js.data ? js.data : js;
  let bid = null,
    ask = null,
    mid = null,
    ts = null,
    exchange = null,
    symbol = null;

  if ("b" in core) bid = Number(core.b);
  if ("a" in core) ask = Number(core.a);

  if (Array.isArray(core.bids) && core.bids.length > 0) {
    const first = core.bids[0];
    if (Array.isArray(first)) bid = Number(first[0]);
    else if (typeof first === "object" && "price" in first)
      bid = Number(first.price);
  }
  if (Array.isArray(core.asks) && core.asks.length > 0) {
    const first = core.asks[0];
    if (Array.isArray(first)) ask = Number(first[0]);
    else if (typeof first === "object" && "price" in first)
      ask = Number(first.price);
  }

  if ("bestBid" in core) bid = Number(core.bestBid);
  if ("bestAsk" in core) ask = Number(core.bestAsk);

  if ("timestamp" in core) ts = Number(core.timestamp);
  if ("E" in core) ts = Number(core.E);
  if ("T" in core) ts = Number(core.T);
  if ("ts" in core) ts = Number(core.ts);

  if ("s" in core) symbol = core.s;
  if ("symbol" in core) symbol = core.symbol;
  if ("instrument_name" in core) symbol = core.instrument_name;
  if ("exchange" in core) exchange = core.exchange;

  if (Number.isFinite(bid) && Number.isFinite(ask)) {
    mid = (bid + ask) / 2.0;
    return {
      exchange: exchange ? String(exchange) : null,
      asset: symbol ? String(symbol) : null,
      bid,
      ask,
      mid,
      ts_ms: Number.isFinite(ts) ? ts : null,
      raw: core,
    };
  }
  return null;
}

/* --- Main loop --- */
(function main() {
  const fd = fs.openSync(FILE_PATH, "r");
  const stat = fs.fstatSync(fd);
  const size = stat.size;
  if (size < HEADER_SIZE + ENVELOPE_SIZE) {
    console.error("File too small:", size);
    process.exit(1);
  }

  const PROT_READ = mmap.PROT_READ;
  const MAP_SHARED = mmap.MAP_SHARED;
  const map = mmap.map(size, PROT_READ, MAP_SHARED, fd, 0);

  let off = HEADER_SIZE;
  console.log(
    `[mmap-reader] mapped ${FILE_PATH} (${size} bytes), starting at ${HEADER_SIZE}`,
  );
  console.log(
    `[mmap-reader] SHOW=${SHOW} PRETTY=${PRETTY} EXCHANGE=${FILTER_EXCHANGE || "<any>"} SYMBOL=${FILTER_SYMBOL || "<any>"} WATERMARK=${WATERMARK}`,
  );

  const normFilterExchange = FILTER_EXCHANGE
    ? normalizeExchangeName(FILTER_EXCHANGE)
    : null;
  const normFilterSymbol = FILTER_SYMBOL
    ? normalizeSymbol(FILTER_SYMBOL)
    : null;

  function shouldPrint(typeKey, decoded) {
    if (SHOW === "none") return false;
    if (SHOW === "all") return true;
    if (SHOW === "both") {
      if (
        typeKey === "trade" ||
        typeKey === "gob" ||
        typeKey === "gobjson" ||
        typeKey === "book"
      )
        return true;
      return false;
    }
    if (SHOW === "trades") return typeKey === "trade";
    if (SHOW === "books") return typeKey === "book";
    if (SHOW === "combined") return typeKey === "combined";
    if (SHOW === "gob") return typeKey === "gob" || typeKey === "gobjson";
    if (SHOW === "gobjson") return typeKey === "gobjson";
    return false;
  }

  function passesFilters(decoded) {
    if (!decoded) return true;

    let ex =
      decoded.exchange ||
      (decoded.spot && decoded.spot.exchange) ||
      (decoded.fut && decoded.fut.exchange) ||
      (decoded.data && decoded.data.exchange) ||
      (decoded.raw && decoded.raw.exchange) ||
      null;

    let asset =
      decoded.asset ||
      decoded.symbol ||
      (decoded.spot && decoded.spot.asset) ||
      (decoded.fut && decoded.fut.asset) ||
      (decoded.data && decoded.data.asset) ||
      (decoded.raw &&
        (decoded.raw.s || decoded.raw.symbol || decoded.raw.instrument_name)) ||
      null;

    if (normFilterExchange) {
      if (ex) {
        if (normalizeExchangeName(ex) !== normFilterExchange) return false;
      } else {
        const joined = JSON.stringify(decoded);
        if (!joined.toLowerCase().includes(normFilterExchange)) return false;
      }
    }

    if (normFilterSymbol) {
      if (asset) {
        if (normalizeSymbol(asset) !== normFilterSymbol) return false;
      } else {
        const joined = JSON.stringify(decoded);
        if (
          !joined
            .toUpperCase()
            .replace(/[^A-Z0-9]/g, "")
            .includes(normFilterSymbol)
        )
          return false;
      }
    }

    return true;
  }

  // dedupe by envelope key (rtype:ts:src:inst:pad)
  let lastSeenKey = null;
  let lastMid = null;
  let maxSeenSeq = 0; // watermark highest seq seen

  function compactLineFor(obj) {
    try {
      const env = obj.envelope || {};
      const data = obj.data || {};
      const exchange =
        data.exchange || (data.spot && data.spot.exchange) || "<unknown>";
      const asset =
        data.asset ||
        (data.spot && data.spot.asset) ||
        (data.raw && (data.raw.s || data.raw.symbol)) ||
        "<unknown>";
      let bid = data.bid,
        ask = data.ask,
        mid = data.mid;
      if (mid == null && typeof bid === "number" && typeof ask === "number")
        mid = (bid + ask) / 2;
      if (mid == null && typeof data.px === "number") mid = data.px;

      const ts =
        env.ts_ms ||
        data.ts_ms ||
        (data.raw && (data.raw.E || data.raw.T || data.raw.ts)) ||
        null;
      const seq = env.pad || (env.pad === 0 ? 0 : null);

      const deltaMid =
        typeof mid === "number" && typeof lastMid === "number"
          ? mid - lastMid
          : null;

      function nf(x) {
        if (x == null) return "-";
        if (Number.isInteger(x)) return String(x);
        return Number.parseFloat(x)
          .toFixed(6)
          .replace(/\.?0+$/, "");
      }

      const kind = env.kind || (obj.__rtype ? obj.__rtype : "");
      const src = `src=${env.src_id || "?"} inst=${env.inst_id || "?"} seq=${seq === null ? "?" : seq}`;
      let pieces = [
        `${exchange}`,
        `${asset}`,
        kind ? `(${kind})` : "",
        `bid:${nf(bid)}`,
        `ask:${nf(ask)}`,
        `mid:${nf(mid)}`,
        `ts:${ts || "-"}`,
        src,
      ];
      if (deltaMid !== null)
        pieces.push(`Δmid:${(deltaMid >= 0 ? "+" : "") + nf(deltaMid)}`);
      return pieces.filter(Boolean).join(" ");
    } catch (e) {
      return "[compact-line-error]";
    }
  }

  function updateSingleLine(obj) {
    const line = compactLineFor(obj);
    readline.clearLine(process.stdout, 0);
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(line);
  }

  function shouldSkipByWatermark(env) {
    if (!env || typeof env.pad !== "number") return false;
    const seq = env.pad;
    if (!seq || seq <= 0) return false;
    if (!WATERMARK || WATERMARK <= 0) return false;
    if (seq <= maxSeenSeq - WATERMARK) return true;
    return false;
  }

  function output(obj, rtype, env) {
    const ts =
      env && env.ts_ms
        ? env.ts_ms.toString()
        : obj && obj.data && obj.data.ts_ms
          ? String(obj.data.ts_ms)
          : null;
    const key = `${rtype || "?"}:${ts || "?"}:${env ? env.src_id : "?"}:${env ? env.inst_id : "?"}:${env ? env.pad : "?"}`;
    if (key === lastSeenKey) return;

    if (shouldSkipByWatermark(env)) return;

    if (obj && obj.data) {
      if (typeof obj.data.mid === "number") lastMid = obj.data.mid;
      else if (typeof obj.data.px === "number") lastMid = obj.data.px;
    }

    if (env && typeof env.pad === "number" && env.pad > maxSeenSeq)
      maxSeenSeq = env.pad;

    lastSeenKey = key;
    updateSingleLine(obj);
  }

  function step() {
    try {
      if (off + ENVELOPE_SIZE > size) {
        off = HEADER_SIZE;
        return setImmediate(step);
      }

      const envA = readEnvelope(map, off);
      const total = ENVELOPE_SIZE + envA.len;

      if (envA.len === 0 || total > size - off) {
        off = HEADER_SIZE;
        return setImmediate(step);
      }

      const payloadStart = off + ENVELOPE_SIZE;
      const payload = map.subarray(payloadStart, payloadStart + envA.len);

      const envB = readEnvelope(map, off);
      const valid = (envB.flags & 0x1) === 0x1;
      const stable =
        envA.rtype === envB.rtype &&
        envA.len === envB.len &&
        envA.ts_ms === envB.ts_ms &&
        envA.src_id === envB.src_id &&
        envA.inst_id === envB.inst_id &&
        envA.pad === envB.pad;

      if (!valid || !stable) return setImmediate(step);

      const rtype = envB.rtype;

      try {
        if (rtype === 2) {
          let decoded;
          try {
            decoded = decodeTradePrint(payload);
          } catch (e) {
            const alt = tryParseJsonToTrade(payload);
            if (alt) decoded = alt;
            else decoded = { decode_error: e.message };
          }
          if (shouldPrint("trade", decoded) && passesFilters(decoded)) {
            const obj = {
              envelope: {
                kind: RTYPE[rtype],
                ts_ms: envB.ts_ms.toString(),
                src_id: envB.src_id,
                inst_id: envB.inst_id,
                len: envB.len,
                pad: envB.pad,
              },
              data: decoded,
            };
            output(obj, "trade", obj.envelope);
          }
        } else if (rtype === 1) {
          let decoded;
          try {
            decoded = decodeBookSnapshot(payload);
          } catch (e) {
            const alt = tryParseJsonToBook(payload);
            if (alt) decoded = alt;
            else decoded = { decode_error: e.message };
          }
          if (shouldPrint("book", decoded) && passesFilters(decoded)) {
            const obj = {
              envelope: {
                kind: RTYPE[rtype],
                ts_ms: envB.ts_ms.toString(),
                src_id: envB.src_id,
                inst_id: envB.inst_id,
                len: envB.len,
                pad: envB.pad,
              },
              data: decoded,
            };
            output(obj, "book", obj.envelope);
          }
        } else if (rtype === 3) {
          let decoded;
          try {
            decoded = decodeCombinedTick(payload);
          } catch (e) {
            decoded = { decode_error: e.message };
          }
          if (shouldPrint("combined", decoded) && passesFilters(decoded)) {
            const obj = {
              envelope: {
                kind: RTYPE[rtype],
                ts_ms: envB.ts_ms.toString(),
                src_id: envB.src_id,
                inst_id: envB.inst_id,
                len: envB.len,
                pad: envB.pad,
              },
              data: decoded,
            };
            output(obj, "combined", obj.envelope);
          }
        } else if (rtype === 5) {
          try {
            const txt = payload.toString("utf8");
            const js = JSON.parse(txt);
            if (shouldPrint("gobjson", js) && passesFilters(js)) {
              const obj = {
                envelope: {
                  kind: "GlobalOrderBookJson",
                  ts_ms: envB.ts_ms.toString(),
                  src_id: envB.src_id,
                  inst_id: envB.inst_id,
                  len: envB.len,
                  pad: envB.pad,
                },
                data: js,
              };
              output(obj, "gobjson", obj.envelope);
            }
          } catch (e) {
            const ascii = findAsciiSubstrings(payload);
            const obj = {
              envelope: {
                kind: "GlobalOrderBookJson",
                ts_ms: envB.ts_ms.toString(),
                src_id: envB.src_id,
                inst_id: envB.inst_id,
                len: envB.len,
                pad: envB.pad,
              },
              ascii_snippets: ascii,
              raw_head_b64: payload.subarray(0, 256).toString("base64"),
            };
            if (shouldPrint("gobjson", obj) && passesFilters(obj))
              output(obj, "gobjson", obj.envelope);
          }
        } else if (rtype === 4) {
          const ascii = findAsciiSubstrings(payload);
          const obj = {
            envelope: {
              kind: "GlobalOrderBook",
              ts_ms: envB.ts_ms.toString(),
              src_id: envB.src_id,
              inst_id: envB.inst_id,
              len: envB.len,
              pad: envB.pad,
            },
            ascii_snippets: ascii,
            raw_head_b64: payload.subarray(0, 256).toString("base64"),
          };
          if (shouldPrint("gob", obj) && passesFilters(obj))
            output(obj, "gob", obj.envelope);
        } else {
          const ascii = findAsciiSubstrings(payload);
          const obj = {
            envelope: {
              kind: RTYPE[rtype] || `Unknown(${rtype})`,
              ts_ms: envB.ts_ms.toString(),
              src_id: envB.src_id,
              inst_id: envB.inst_id,
              len: envB.len,
              pad: envB.pad,
            },
            ascii_snippets: ascii,
            raw_head_b64: payload.subarray(0, 128).toString("base64"),
          };
          if (SHOW !== "none") output(obj, "unknown", obj.envelope);
        }
      } catch (e) {
        console.error("[reader] decode error:", e.stack || e.message);
      }

      // advance
      off += total;
      if (off + ENVELOPE_SIZE > size) off = HEADER_SIZE;
      setImmediate(step);
    } catch (e) {
      console.error("[mmap-reader] unexpected error:", e.stack || e.message);
      setTimeout(step, 10);
    }
  }

  setImmediate(step);
})();
