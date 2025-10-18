// reader.js
// Usage examples:
//   node reader.js ./data/global.mm
//   node reader.js --show=trades ./data/global.mm
//   node reader.js --show=trades --exchange=deribit --symbol=BTC-PERPETUAL --pretty ./data/global.mm
//
// show options: trades | books | combined | gob | gobjson | both | all | none

const fs = require("fs");
const mmap = require("@riaskov/mmap-io"); // npm i @riaskov/mmap-io
const argv = process.argv.slice(2);

// CLI parsing (very small)
let FILE_PATH = "./data/global.mm";
let SHOW = "both";
let PRETTY = false;
let FILTER_EXCHANGE = null;
let FILTER_SYMBOL = null;

for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a.startsWith("--show=")) SHOW = a.split("=")[1];
  else if (a === "--pretty") PRETTY = true;
  else if (a.startsWith("--exchange=")) FILTER_EXCHANGE = a.split("=")[1];
  else if (a.startsWith("--symbol=")) FILTER_SYMBOL = a.split("=")[1];
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

/* binary helpers */
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

function safeReadString(buf, o) {
  // bincode encoded length (u64 little-endian) then UTF-8 bytes
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

/* decoders — UPDATED to match Rust types order (exchange first) */
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

  // spot (OrderBookSnapshot: exchange, asset, kind, bid, ask, mid, ts_ms)
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

  // fut
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

/* main */
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
    `[mmap-reader] SHOW=${SHOW} PRETTY=${PRETTY} EXCHANGE=${FILTER_EXCHANGE || "<any>"} SYMBOL=${FILTER_SYMBOL || "<any>"}`,
  );

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
    if (FILTER_EXCHANGE) {
      const ex =
        decoded.exchange ||
        (decoded.spot && decoded.spot.exchange) ||
        (decoded.fut && decoded.fut.exchange) ||
        (decoded.data && decoded.data.exchange) ||
        null;
      if (ex && ex !== FILTER_EXCHANGE) return false;
      if (!ex) {
        const joined = JSON.stringify(decoded);
        if (!joined.includes(FILTER_EXCHANGE)) return false;
      }
    }
    if (FILTER_SYMBOL) {
      const asset =
        decoded.asset ||
        decoded.symbol ||
        (decoded.spot && decoded.spot.asset) ||
        (decoded.fut && decoded.fut.asset) ||
        (decoded.data && decoded.data.asset);
      if (asset && asset !== FILTER_SYMBOL) return false;
      if (!asset) {
        const joined = JSON.stringify(decoded);
        if (!joined.includes(FILTER_SYMBOL)) return false;
      }
    }
    return true;
  }

  function output(obj) {
    if (PRETTY) console.log(JSON.stringify(obj, null, 2));
    else console.log(JSON.stringify(obj));
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
        envA.inst_id === envB.inst_id;

      if (!valid || !stable) return setImmediate(step);

      const rtype = envB.rtype;
      const kind = RTYPE[rtype] || `Unknown(${rtype})`;

      try {
        if (rtype === 2) {
          let decoded;
          try {
            decoded = decodeTradePrint(payload);
          } catch (e) {
            decoded = { decode_error: e.message };
          }
          if (shouldPrint("trade", decoded) && passesFilters(decoded)) {
            output({
              envelope: {
                kind,
                ts_ms: envB.ts_ms.toString(),
                src_id: envB.src_id,
                inst_id: envB.inst_id,
                len: envB.len,
              },
              data: decoded,
            });
          }
        } else if (rtype === 1) {
          let decoded;
          try {
            decoded = decodeBookSnapshot(payload);
          } catch (e) {
            decoded = { decode_error: e.message };
          }
          if (shouldPrint("book", decoded) && passesFilters(decoded)) {
            output({
              envelope: {
                kind,
                ts_ms: envB.ts_ms.toString(),
                src_id: envB.src_id,
                inst_id: envB.inst_id,
                len: envB.len,
              },
              data: decoded,
            });
          }
        } else if (rtype === 3) {
          let decoded;
          try {
            decoded = decodeCombinedTick(payload);
          } catch (e) {
            decoded = { decode_error: e.message };
          }
          if (shouldPrint("combined", decoded) && passesFilters(decoded)) {
            output({
              envelope: {
                kind,
                ts_ms: envB.ts_ms.toString(),
                src_id: envB.src_id,
                inst_id: envB.inst_id,
                len: envB.len,
              },
              data: decoded,
            });
          }
        } else if (rtype === 5) {
          try {
            const txt = payload.toString("utf8");
            const js = JSON.parse(txt);
            if (shouldPrint("gobjson", js) && passesFilters(js)) {
              output({
                envelope: {
                  kind: "GlobalOrderBookJson",
                  ts_ms: envB.ts_ms.toString(),
                  src_id: envB.src_id,
                  inst_id: envB.inst_id,
                  len: envB.len,
                },
                data: js,
              });
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
              },
              ascii_snippets: ascii,
              raw_head_b64: payload.subarray(0, 256).toString("base64"),
            };
            if (shouldPrint("gobjson", obj) && passesFilters(obj)) output(obj);
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
            },
            ascii_snippets: ascii,
            raw_head_b64: payload.subarray(0, 256).toString("base64"),
          };
          if (shouldPrint("gob", obj) && passesFilters(obj)) output(obj);
        } else {
          const ascii = findAsciiSubstrings(payload);
          const obj = {
            envelope: {
              kind,
              ts_ms: envB.ts_ms.toString(),
              src_id: envB.src_id,
              inst_id: envB.inst_id,
              len: envB.len,
            },
            ascii_snippets: ascii,
            raw_head_b64: payload.subarray(0, 128).toString("base64"),
          };
          if (SHOW !== "none") output(obj);
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
