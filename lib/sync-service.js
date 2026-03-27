const { CUSTOMER_SNAPSHOT_FIELDS } = require("./customer-fields");
const { CUSTOMER_SPEC_LABELS_JA } = require("./customer-spec-labels");
const { postAcquisition } = require("./digitaleyes");
const { getDb } = require("./sync-db");
const {
  mergeRemoteAndOverrides,
  rowArgsForUpsert,
  denormFromMerged,
} = require("./customer-row");
const fs = require("fs");
const path = require("path");
const { DEFAULT_VISIT_STATUSES } = require("./visit-status-defaults");
const FALLBACK_VISIT_STATUS = "資料請求・エントリー";
const PREFS = [
  "北海道","青森県","岩手県","宮城県","秋田県","山形県","福島県","茨城県","栃木県","群馬県","埼玉県","千葉県","東京都","神奈川県","新潟県","富山県","石川県","福井県","山梨県","長野県","岐阜県","静岡県","愛知県","三重県","滋賀県","京都府","大阪府","兵庫県","奈良県","和歌山県","鳥取県","島根県","岡山県","広島県","山口県","徳島県","香川県","愛媛県","高知県","福岡県","佐賀県","長崎県","熊本県","大分県","宮崎県","鹿児島県","沖縄県",
];
const DEFAULT_NINZU_OPTIONS = ["1", "2", "3", "4", "5", "6以上"];
const DEFAULT_YOSAN_OPTIONS = ["～3000万円","～3500万円","～4000万円","～4500万円","～5000万円","～5500万円","～6000万円","～6500万円","～7000万円","～7500万円","～8000万円"];
const DEFAULT_JIKOSIKIN_OPTIONS = ["〜100万円","〜200万円","〜300万円","〜400万円","〜500万円","〜600万円","〜700万円","〜800万円","〜900万円","〜1000万円","〜1200万円","〜1500万円","〜2000万円"];
const DEFAULT_MADORI_OPTIONS = ["1LDK","2LDK","3LDK","4LDK","5LDK"];
const DEFAULT_MENSEKI_OPTIONS = ["40㎡未満","40㎡～","50㎡～","60㎡～","65㎡～","70㎡～","75㎡～","80㎡～","90㎡～","100㎡～"];

/** `diag=1` のとき NDJSON を追記（PII なし） */
const CURSOR_CUSTOMER_LIST_DEBUG_LOG = path.join(
  __dirname,
  "..",
  ".cursor",
  "debug-202a69.log"
);
/** `.cursor` が書けない環境向け（リポジトリ内・取り込みしやすい） */
const DATA_CUSTOMER_LIST_DIAG_LOG = path.join(
  __dirname,
  "..",
  "data",
  "customer-list-diag.ndjson"
);

function debugFieldStrLen(v) {
  if (v == null) return 0;
  const s = String(v).trim();
  if (!s || s === "null" || s === "undefined") return 0;
  return s.length;
}

const UPSERT_SQL = `INSERT INTO customer_snapshot (
  property_id, customer_id, name, kana, mail, tel, state, city, baitai, status,
  ninzu, yosan, jikosikin, questionnaire23, questionnaire24, date_entry, upd_date,
  payload, remote_payload, local_overrides
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(property_id, customer_id) DO UPDATE SET
  name=excluded.name, kana=excluded.kana, mail=excluded.mail, state=excluded.state,
  tel=excluded.tel, city=excluded.city, baitai=excluded.baitai, status=excluded.status,
  ninzu=excluded.ninzu, yosan=excluded.yosan, jikosikin=excluded.jikosikin,
  questionnaire23=excluded.questionnaire23, questionnaire24=excluded.questionnaire24,
  date_entry=excluded.date_entry, upd_date=excluded.upd_date,
  payload=excluded.payload, remote_payload=excluded.remote_payload,
  local_overrides=excluded.local_overrides`;

function escapeLike(s) {
  return String(s).replace(/[%_\\]/g, "\\$&");
}

/** 顧客一覧 SELECT の列順（execute().columns が空・欠ける環境のフォールバック） */
const CUSTOMER_LIST_SELECT_FALLBACK = [
  "customer_id",
  "mail",
  "upd_date",
  "payload",
  "local_overrides",
  "remote_payload",
  "visit_status",
];

/** 下記 queryLocalCustomers の SELECT 列順と厳密に一致（名前解決失敗時のインデックスフォールバック） */
const LOCAL_CUSTOMERS_LIST_COL = {
  customer_id: 0,
  mail: 1,
  upd_date: 2,
  payload: 3,
  local_overrides: 4,
  remote_payload: 5,
  visit_status: 6,
};

function trimStr(v) {
  if (v == null) return "";
  return String(v).trim();
}

function toStatusDateKey(v) {
  const s = trimStr(v);
  if (!s) return new Date().toISOString().slice(0, 10);
  const m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (m) {
    const y = m[1];
    const mo = String(m[2]).padStart(2, "0");
    const d = String(m[3]).padStart(2, "0");
    return `${y}-${mo}-${d}`;
  }
  const t = Date.parse(s.replace(/\//g, "-"));
  if (!Number.isNaN(t)) return new Date(t).toISOString().slice(0, 10);
  return new Date().toISOString().slice(0, 10);
}

function toStatusDateTimeText(v) {
  const s = trimStr(v);
  if (!s) return new Date().toISOString().slice(0, 19).replace("T", " ");
  const m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?/);
  if (m) {
    const y = m[1];
    const mo = String(m[2]).padStart(2, "0");
    const d = String(m[3]).padStart(2, "0");
    const hh = String(m[4] || "00").padStart(2, "0");
    const mm = String(m[5] || "00").padStart(2, "0");
    const ss = String(m[6] || "00").padStart(2, "0");
    return `${y}-${mo}-${d} ${hh}:${mm}:${ss}`;
  }
  const t = Date.parse(s.replace(/\//g, "-"));
  if (!Number.isNaN(t)) return new Date(t).toISOString().slice(0, 19).replace("T", " ");
  return new Date().toISOString().slice(0, 19).replace("T", " ");
}

function stateFilterCandidates(raw) {
  const s = trimStr(raw);
  if (!s) return [];
  const out = new Set([s]);
  const addNumericVariants = (n) => {
    if (!Number.isFinite(n) || n < 1 || n > PREFS.length) return;
    const i = Math.trunc(n);
    out.add(String(i));
    out.add(String(i).padStart(2, "0"));
    out.add(`${i}.0`);
    out.add(PREFS[i - 1]);
  };
  const idx = PREFS.indexOf(s);
  if (idx >= 0) {
    addNumericVariants(idx + 1);
    return [...out];
  }
  const n = Number(s);
  if (Number.isFinite(n)) addNumericVariants(n);
  return [...out];
}

function normalizeFilterToken(s) {
  return String(s || "")
    .trim()
    .normalize("NFKC")
    .replace(/\s+/g, " ");
}

function parseJpAmountUpperBound(raw) {
  const s = trimStr(raw).replace(/[,\s]/g, "");
  if (!s) return Number.NaN;
  if (s.includes("未満")) {
    // 「40㎡未満」等の lower-bound 系は別用途。ここでは対象外。
    return Number.NaN;
  }
  if (s.includes("以上")) {
    const v = s.replace("以上", "");
    const n = parseJpAmountUpperBound(v);
    return Number.isFinite(n) ? n + 0.5 : Number.NaN;
  }
  let total = 0;
  const oku = s.match(/(\d+(?:\.\d+)?)億/);
  const man = s.match(/(\d+(?:\.\d+)?)万/);
  if (oku) total += Number(oku[1]) * 100000000;
  if (man) total += Number(man[1]) * 10000;
  if (!oku && !man) {
    const onlyNum = s.match(/^\d+(?:\.\d+)?$/);
    if (onlyNum) total += Number(onlyNum[0]);
  }
  return total > 0 ? total : Number.NaN;
}

function listRowVisitStatusRaw(row, parsed) {
  const a = trimStr(parsed.visit_status ?? parsed.status);
  if (a) return a;
  const byIdx = trimStr(row[LOCAL_CUSTOMERS_LIST_COL.visit_status]);
  if (byIdx) return byIdx;
  if (row && typeof row === "object" && !Array.isArray(row)) {
    for (const key of ["visit_status", "VisitStatus", "VISIT_STATUS", "status"]) {
      if (!Object.prototype.hasOwnProperty.call(row, key)) continue;
      const t = trimStr(row[key]);
      if (t && t.length < 400 && !t.startsWith("{") && !t.startsWith("[")) return t;
    }
  }
  return "";
}

/** payload / remote_payload が二重 JSON 文字列のときにオブジェクトへ展開 */
function parseJsonObjectLoose(maybeJson) {
  if (maybeJson == null) return {};
  if (typeof maybeJson === "object" && !Array.isArray(maybeJson)) return { ...maybeJson };
  let s = typeof maybeJson === "string" ? maybeJson : String(maybeJson);
  for (let d = 0; d < 3; d++) {
    if (!String(s).trim()) return {};
    try {
      const p = JSON.parse(s);
      if (p && typeof p === "object" && !Array.isArray(p)) return { ...p };
      if (typeof p === "string") {
        s = p;
        continue;
      }
      return {};
    } catch (_) {
      return {};
    }
  }
  return {};
}

function listRowFieldRaw(row, parsed, colName, idx) {
  const v = parsed[colName];
  if (v !== undefined && v !== null && !(typeof v === "string" && v.length === 0)) return v;
  const fb = row[idx];
  if (fb !== undefined && fb !== null) return fb;
  return v;
}

function normalizeListQueryColumnNames(rawColumns) {
  const raw = Array.isArray(rawColumns) ? rawColumns : [];
  const n = Math.max(raw.length, CUSTOMER_LIST_SELECT_FALLBACK.length);
  const out = [];
  for (let i = 0; i < n; i++) {
    const c = raw[i];
    const s = c != null ? String(c).trim() : "";
    out.push(s !== "" ? s : CUSTOMER_LIST_SELECT_FALLBACK[i] || "");
  }
  return out;
}

/** Hrana / libsql の Row を一覧用フィールドへ（名前を添字より優先：インデックスずれ対策） */
function parseCustomerListRow(row, columnNames) {
  const cols = normalizeListQueryColumnNames(columnNames);
  const o = {};
  for (let i = 0; i < cols.length; i++) {
    const key = cols[i];
    if (!key) continue;
    const byIndex = row[i];
    const byName = row[key];
    if (byName !== undefined) o[key] = byName;
    else if (byIndex !== undefined) o[key] = byIndex;
  }
  if (o.visit_status == null && o.status != null) o.visit_status = o.status;
  return o;
}

/**
 * GET /local/customers 専用: 列数を SELECT と同じ 7 本に固定する。
 * max(raw.length,…) で余分列があると空キーで continue し row[i] が捨てられインデックスがずれるのを防ぐ。
 */
function normalizeLocalCustomersListColumnNames(rawColumns) {
  const raw = Array.isArray(rawColumns) ? rawColumns : [];
  const n = CUSTOMER_LIST_SELECT_FALLBACK.length;
  const out = [];
  for (let i = 0; i < n; i++) {
    const c = raw[i];
    const s = c != null ? String(c).trim() : "";
    out.push(s !== "" ? s : CUSTOMER_LIST_SELECT_FALLBACK[i] || "");
  }
  return out;
}

function parseCustomerListRowForLocalQuery(row, columnNames) {
  const cols = normalizeLocalCustomersListColumnNames(columnNames);
  const o = {};
  for (let i = 0; i < cols.length; i++) {
    const key = cols[i];
    if (!key) continue;
    const byIndex = row[i];
    const byName = row[key];
    if (byName !== undefined) o[key] = byName;
    else if (byIndex !== undefined) o[key] = byIndex;
  }
  if (o.visit_status == null && o.status != null) o.visit_status = o.status;
  return o;
}

/** upsertBatch 既存行 SELECT の列順（columns 欠損時のフォールバック） */
const SNAPSHOT_UPSERT_EXISTING_FALLBACK = [
  "customer_id",
  "local_overrides",
  "remote_payload",
  "baitai",
  "status",
  "payload",
];

function normalizeUpsertExistingColumns(rawColumns) {
  const raw = Array.isArray(rawColumns) ? rawColumns : [];
  const n = Math.max(raw.length, SNAPSHOT_UPSERT_EXISTING_FALLBACK.length);
  const out = [];
  for (let i = 0; i < n; i++) {
    const c = raw[i];
    const s = c != null ? String(c).trim() : "";
    out.push(s !== "" ? s : SNAPSHOT_UPSERT_EXISTING_FALLBACK[i] || "");
  }
  return out;
}

function parseUpsertExistingRow(row, columnNames) {
  const cols = normalizeUpsertExistingColumns(columnNames);
  const o = {};
  for (let i = 0; i < cols.length; i++) {
    const key = cols[i];
    if (!key) continue;
    const byIndex = row[i];
    const byName = row[key];
    if (byName !== undefined) o[key] = byName;
    else if (byIndex !== undefined) o[key] = byIndex;
  }
  return o;
}

/** Digitaleyes がオブジェクト・配列で返すフィールドから表示用文字列を再帰的に抽出 */
function collectVisitFieldStrings(v, depth = 0) {
  if (depth > 4) return [];
  if (v == null) return [];
  if (typeof v !== "object") {
    const s = String(v).trim();
    if (!s || s === "[object Object]") return [];
    return [s];
  }
  if (Array.isArray(v)) {
    return v.flatMap((x) => collectVisitFieldStrings(x, depth + 1));
  }
  const o = v;
  const preferred = [];
  for (const k of ["label", "name", "text", "value", "title", "display"]) {
    if (o[k] != null && typeof o[k] !== "object") {
      const s = String(o[k]).trim();
      if (s) preferred.push(s);
    }
  }
  if (preferred.length) return preferred;
  return Object.values(o).flatMap((x) => collectVisitFieldStrings(x, depth + 1));
}

/** payload / API 行で c.* を優先し、空なら仕様の日本語ラベル列（Digitaleyes が論理名キーで返す場合） */
function pickPayloadLocalized(rowObj, cKey) {
  if (!rowObj || typeof rowObj !== "object") return "";
  const fromKey = collectVisitFieldStrings(rowObj[cKey], 0)[0] || "";
  if (fromKey) return fromKey;
  const ja = CUSTOMER_SPEC_LABELS_JA[cKey];
  if (ja) {
    const t = collectVisitFieldStrings(rowObj[ja], 0)[0] || "";
    if (t) return t;
  }
  return "";
}

/** 物件ごとの来場ステータス一覧の重複判定（表記ゆれ吸収） */
function visitStatusDedupKey(s) {
  return String(s || "")
    .trim()
    .normalize("NFKC")
    .replace(/\s+/g, " ");
}

/** local_overrides の空の c.status は未指定扱い（マージでデジタライズの値を潰さない） */
function omitEmptyLocalStatusOverride(lo) {
  if (!lo || typeof lo !== "object" || Array.isArray(lo)) return lo;
  const o = { ...lo };
  if (!Object.prototype.hasOwnProperty.call(o, "c.status")) return o;
  const v = o["c.status"];
  const t = v == null ? "" : trimStr(String(v));
  if (!t || t === "null" || t === "undefined") delete o["c.status"];
  return o;
}

/** キー名がステータス列っぽい項目から値を採る（API が論理名以外で返す場合のフォールバック） */
function pickStatusByKeyHeuristic(obj) {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return "";
  for (const [k, v] of Object.entries(obj)) {
    const ks = String(k);
    if (
      ks === "status" ||
      ks.includes("ステータス") ||
      /(^|_)status$/i.test(ks) ||
      /Status$/.test(ks)
    ) {
      const s = collectVisitFieldStrings(v, 0)[0] || "";
      if (s) return s;
    }
  }
  return "";
}

/** acquisition 行・remote_payload から API の来場ステータス文字列 */
function acquisitionVisitStatusRaw(row) {
  if (!row || typeof row !== "object") return "";
  const fromC = pickPayloadLocalized(row, "c.status");
  if (fromC) return fromC;
  const fromBare = collectVisitFieldStrings(row.status, 0)[0] || "";
  if (fromBare) return fromBare;
  const sit = pickPayloadLocalized(row, "c.situation");
  if (sit) return sit;
  const q51 = pickPayloadLocalized(row, "c.questionnaire51");
  if (q51) return q51;
  const q54 = pickPayloadLocalized(row, "c.questionnaire54");
  if (q54) return q54;
  const q41 = pickPayloadLocalized(row, "c.questionnaire41");
  if (q41) return q41;
  return pickStatusByKeyHeuristic(row) || "";
}

/** 同期直前に 1 行を正規化（論理名キー → c.* へ寄せ、一覧・ダッシュボードで参照しやすくする） */
function normalizeAcquisitionRow(row) {
  if (!row || typeof row !== "object") return row;
  const o = { ...row };
  const vis = acquisitionVisitStatusRaw(o);
  if (vis) o["c.status"] = vis;
  const sit = pickPayloadLocalized(o, "c.situation");
  if (sit && !String(o["c.situation"] ?? "").trim()) o["c.situation"] = sit;
  const q51 = pickPayloadLocalized(o, "c.questionnaire51");
  if (q51 && !String(o["c.questionnaire51"] ?? "").trim()) o["c.questionnaire51"] = q51;
  const q54 = pickPayloadLocalized(o, "c.questionnaire54");
  if (q54 && !String(o["c.questionnaire54"] ?? "").trim()) o["c.questionnaire54"] = q54;
  const q41 = pickPayloadLocalized(o, "c.questionnaire41");
  if (q41 && !String(o["c.questionnaire41"] ?? "").trim()) o["c.questionnaire41"] = q41;
  return o;
}

/** upsert 時: 既存キャッシュの status 列またはマージ済み payload の c.status（local_overrides より前のベース） */
function visitStatusFromPrev(prev) {
  if (!prev) return "";
  const col = String(prev.status ?? "").trim();
  if (col) return col;
  try {
    const p = JSON.parse(prev.payload || "{}");
    return (
      pickPayloadLocalized(p, "c.status") ||
      String(p["status"] ?? "").trim()
    );
  } catch (_) {
    return "";
  }
}

/** 更新日時の大小（文字列比較で足りない形式は Date で比較） */
function updAfter(rowUpd, watermark) {
  if (!watermark) return true;
  if (!rowUpd) return false;
  const a = Date.parse(String(rowUpd).replace(/\//g, "-"));
  const b = Date.parse(String(watermark).replace(/\//g, "-"));
  if (!Number.isNaN(a) && !Number.isNaN(b)) return a > b;
  return String(rowUpd).localeCompare(String(watermark)) > 0;
}

async function upsertBatch(db, propertyId, rows) {
  if (!rows.length) return;
  rows = rows.map((r) => normalizeAcquisitionRow(r));
  const ids = rows.map((r) => String(r["c.id"] ?? ""));
  const ph = ids.map(() => "?").join(",");
  const existing = await db.execute({
    sql: `SELECT customer_id, local_overrides, remote_payload, baitai, status, payload
          FROM customer_snapshot WHERE property_id = ? AND customer_id IN (${ph})`,
    args: [propertyId, ...ids],
  });
  const existingColNames = existing.columns || existing.columnNames || [];
  const overrideMap = new Map();
  const prevMap = new Map();
  for (const row of existing.rows || []) {
    const f = parseUpsertExistingRow(row, existingColNames);
    const cid = String(f.customer_id ?? "").trim();
    if (!cid) continue;
    const lo = f.local_overrides;
    const loStr =
      typeof lo === "string"
        ? lo || "{}"
        : lo != null && typeof lo === "object"
          ? JSON.stringify(lo)
          : "{}";
    overrideMap.set(cid, loStr);
    prevMap.set(cid, {
      remotePayload: f.remote_payload != null ? String(f.remote_payload) : "",
      baitai: f.baitai != null ? String(f.baitai) : "",
      status: f.status != null ? String(f.status) : "",
      payload: f.payload != null ? String(f.payload) : "",
    });
  }

  // 変更履歴（baitai/status の変化）と日次ステータスを積む
  const historyStmts = [];
  const statusDailyStmts = [];
  for (const remoteRow of rows) {
    const cid = String(remoteRow["c.id"] ?? "");
    const prev = prevMap.get(cid);
    const nextBaitai = String(remoteRow["c.baitai"] ?? "");
    const snapshot = JSON.stringify(remoteRow);
    if (!prev) {
      historyStmts.push({
        sql: `INSERT INTO customer_history(property_id, customer_id, kind, field, from_value, to_value, snapshot_payload, created_at)
              VALUES(?, ?, 'created', NULL, NULL, NULL, ?, datetime('now'))`,
        args: [propertyId, cid, snapshot],
      });
      continue;
    }
    const prevBaitai = String(prev.baitai ?? "");
    if (prevBaitai && nextBaitai && prevBaitai !== nextBaitai) {
      historyStmts.push({
        sql: `INSERT INTO customer_history(property_id, customer_id, kind, field, from_value, to_value, snapshot_payload, created_at)
              VALUES(?, ?, 'baitai_changed', 'baitai', ?, ?, ?, datetime('now'))`,
        args: [propertyId, cid, prevBaitai, nextBaitai, snapshot],
      });
    }
  }
  const stmts = rows.map((remoteRow) => {
    const cid = String(remoteRow["c.id"] ?? "");
    let loStr = overrideMap.get(cid) || "{}";
    let lo = {};
    try {
      lo = JSON.parse(loStr);
    } catch (_) {
      lo = {};
    }
    const prev = prevMap.get(cid);
    const sanitized = { ...remoteRow };
    const remoteStatusRaw = acquisitionVisitStatusRaw(remoteRow);
    const remoteStatusOrDefault = trimStr(remoteStatusRaw) || FALLBACK_VISIT_STATUS;
    if (trimStr(remoteStatusRaw)) {
      sanitized["c.status"] = remoteStatusRaw;
    } else if (prev) {
      sanitized["c.status"] = visitStatusFromPrev(prev) || remoteStatusOrDefault;
    } else {
      sanitized["c.status"] = remoteStatusOrDefault;
    }
    if (prev) {
      let prevPayload = {};
      try {
        prevPayload = JSON.parse(prev.payload || "{}");
      } catch (_) {}
      sanitized["c.status_changed_at"] = prevPayload["c.status_changed_at"] ?? "";
    } else {
      sanitized["c.status_changed_at"] = "";
    }
    const merged = mergeRemoteAndOverrides(sanitized, omitEmptyLocalStatusOverride(lo));
    return {
      sql: UPSERT_SQL,
      args: rowArgsForUpsert(propertyId, remoteRow, merged, loStr),
    };
  });
  rows.forEach((remoteRow, idx) => {
    const cid = String(remoteRow["c.id"] ?? "");
    const prev = prevMap.get(cid);
    const stmt = stmts[idx];
    if (!stmt || !stmt.args) return;
    let merged = {};
    try {
      merged = JSON.parse(stmt.args[16] || "{}");
    } catch (_) {
      merged = {};
    }
    const nextStatus = trimStr(String(merged["c.status"] ?? ""));
    if (!nextStatus) return;
    const changedAt = toStatusDateTimeText(remoteRow["c.upd_date"] || remoteRow["c.ins_date"] || merged["c.upd_date"] || "");
    const statusDate = toStatusDateKey(changedAt);
    if (prev && nextStatus !== trimStr(visitStatusFromPrev(prev))) {
      historyStmts.push({
        sql: `INSERT INTO customer_history(property_id, customer_id, kind, field, from_value, to_value, snapshot_payload, created_at)
              VALUES(?, ?, 'status_changed', 'status', ?, ?, ?, datetime('now'))`,
        args: [propertyId, cid, trimStr(visitStatusFromPrev(prev)), nextStatus, JSON.stringify(merged)],
      });
    }
    statusDailyStmts.push({
      sql: `INSERT INTO customer_status_daily(property_id, customer_id, status_date, status, changed_at, source)
            VALUES(?, ?, ?, ?, ?, 'sync')
            ON CONFLICT(property_id, customer_id, status_date) DO UPDATE SET
              status = CASE WHEN coalesce(excluded.changed_at, '') >= coalesce(customer_status_daily.changed_at, '') THEN excluded.status ELSE customer_status_daily.status END,
              changed_at = CASE WHEN coalesce(excluded.changed_at, '') >= coalesce(customer_status_daily.changed_at, '') THEN excluded.changed_at ELSE customer_status_daily.changed_at END,
              source = CASE WHEN coalesce(excluded.changed_at, '') >= coalesce(customer_status_daily.changed_at, '') THEN 'sync' ELSE customer_status_daily.source END`,
      args: [propertyId, cid, statusDate, nextStatus, changedAt],
    });
  });
  await db.batch([...historyStmts, ...statusDailyStmts, ...stmts]);

  const mergedStatuses = [];
  for (const s of stmts) {
    if (!s || s.sql !== UPSERT_SQL || !s.args) continue;
    try {
      const mergedRow = JSON.parse(s.args[16]);
      const st = mergedRow["c.status"];
      if (st != null && trimStr(st)) mergedStatuses.push(String(st).trim());
    } catch (_) {}
  }
  if (mergedStatuses.length) {
    try {
      await appendMissingVisitStatuses(propertyId, mergedStatuses);
    } catch (e) {
      console.warn("[appendMissingVisitStatuses]", e.message || e);
    }
  }
}

async function getWatermark(db, propertyId) {
  const r = await db.execute({
    sql: "SELECT last_max_upd FROM sync_cursor WHERE property_id = ?",
    args: [propertyId],
  });
  const row = r.rows[0];
  if (!row) return null;
  return row.last_max_upd ?? row[0] ?? null;
}

async function updateCursor(db, propertyId, mode) {
  const maxR = await db.execute({
    sql: "SELECT MAX(upd_date) AS m FROM customer_snapshot WHERE property_id = ?",
    args: [propertyId],
  });
  const row0 = maxR.rows[0];
  const m = row0 ? row0.m ?? row0[0] : null;
  const cntR = await db.execute({
    sql: "SELECT COUNT(*) AS n FROM customer_snapshot WHERE property_id = ?",
    args: [propertyId],
  });
  const row1 = cntR.rows[0];
  const n = Number(row1?.n ?? row1?.[0] ?? 0);

  if (mode === "full") {
    await db.execute({
      sql: `INSERT INTO sync_cursor (property_id, last_max_upd, last_full_at, last_incremental_at, rows_total)
            VALUES (?, ?, datetime('now'), datetime('now'), ?)
            ON CONFLICT(property_id) DO UPDATE SET
              last_max_upd = excluded.last_max_upd,
              last_full_at = datetime('now'),
              last_incremental_at = datetime('now'),
              rows_total = excluded.rows_total`,
      args: [propertyId, m, n],
    });
  } else {
    await db.execute({
      sql: `INSERT INTO sync_cursor (property_id, last_max_upd, last_incremental_at, rows_total)
            VALUES (?, ?, datetime('now'), ?)
            ON CONFLICT(property_id) DO UPDATE SET
              last_max_upd = excluded.last_max_upd,
              last_incremental_at = datetime('now'),
              rows_total = excluded.rows_total`,
      args: [propertyId, m, n],
    });
  }
}

/**
 * 条件付き差分取得。APIが conditions を拒否したら page===1 で status false → rows: null でフォールバックへ。
 */
async function tryIncrementalByCondition(prop, secrets, watermark) {
  const base = {
    limit: 500,
    order: JSON.stringify(["c.upd_date asc"]),
    conditions: JSON.stringify([{ "c.upd_date >": watermark }]),
  };
  let page = 1;
  const all = [];
  for (;;) {
    const resp = await postAcquisition(prop, secrets, { ...base, page });
    if (!resp.status) {
      if (page === 1) return { error: resp.message || "APIエラー", rows: null };
      break;
    }
    const chunk = resp.data?.result || [];
    if (!chunk.length) break;
    all.push(...chunk);
    if (chunk.length < 500) break;
    page++;
    if (page > 400) break;
  }
  return { error: null, rows: all, usedCondition: true };
}

/**
 * 更新日降順でページングし、watermark より新しい行だけ保存（通信量削減・条件未対応時のフォールバック）
 */
async function incrementalByDescPages(prop, secrets, watermark) {
  const pageSize = 300;
  let page = 1;
  const collected = [];
  for (;;) {
    const resp = await postAcquisition(prop, secrets, {
      limit: pageSize,
      page,
      order: JSON.stringify(["c.upd_date desc"]),
    });
    if (!resp.status) throw new Error(resp.message || "APIエラー");
    const rows = resp.data?.result || [];
    if (!rows.length) break;
    for (const row of rows) {
      if (updAfter(row["c.upd_date"], watermark)) collected.push(row);
    }
    const oldest = rows[rows.length - 1]["c.upd_date"];
    if (rows.length < pageSize) break;
    if (!updAfter(oldest, watermark)) break;
    page++;
    if (page > 400) break;
  }
  return collected;
}

async function fullSyncAllPages(prop, secrets) {
  const pageSize = 500;
  let page = 1;
  const all = [];
  for (;;) {
    const resp = await postAcquisition(prop, secrets, {
      limit: pageSize,
      page,
      order: JSON.stringify(["c.upd_date asc"]),
    });
    if (!resp.status) throw new Error(resp.message || "APIエラー");
    const rows = resp.data?.result || [];
    if (!rows.length) break;
    all.push(...rows);
    if (rows.length < pageSize) break;
    page++;
    if (page > 2000) break;
  }
  return all;
}

/**
 * @param {object} prop 物件
 * @param {object} secrets { SECRET_ID, SECRET_PASSWORD }
 * @param {'full'|'incremental'} mode
 */
async function runCustomerSync(prop, secrets, mode) {
  const db = await getDb();
  let effectiveMode = mode;
  let watermark = await getWatermark(db, prop.id);
  if (effectiveMode === "incremental" && !watermark) {
    effectiveMode = "full";
  }

  let rows = [];
  let strategy = "";

  if (effectiveMode === "full") {
    rows = await fullSyncAllPages(prop, secrets);
    strategy = "full_pages_asc";
  } else {
    const wm = watermark || "1970-01-01 00:00:00";
    const tryC = await tryIncrementalByCondition(prop, secrets, wm);
    if (Array.isArray(tryC.rows)) {
      rows = tryC.rows;
      strategy = "incremental_condition";
    } else {
      rows = await incrementalByDescPages(prop, secrets, wm);
      strategy = "incremental_desc_fallback";
    }
  }

  await upsertBatch(db, prop.id, rows);
  await updateCursor(db, prop.id, effectiveMode);

  return {
    ok: true,
    mode: effectiveMode,
    strategy,
    fetched: rows.length,
    upserted: rows.length,
    message:
      effectiveMode === "incremental"
        ? `差分同期（${strategy}）: ${rows.length}件`
        : `全件同期: ${rows.length}件`,
  };
}

/**
 * ローカルDBから顧客一覧（フィルタ・ページ）
 */
async function queryLocalCustomers(propertyId, query) {
  const db = await getDb();
  const page = Math.max(1, parseInt(query.page, 10) || 1);
  const limit = Math.min(200, Math.max(1, parseInt(query.limit, 10) || 20));
  const offset = (page - 1) * limit;

  const where = ["property_id = ?"];
  const args = [propertyId];

  if (query.keyword) {
    const kw = String(query.keyword || "").trim();
    const q = "%" + escapeLike(kw) + "%";
    const qCompact = "%" + escapeLike(kw.replace(/[\s\-ー－]/g, "")) + "%";
    // キーワード検索: 氏名・フリガナに加えてメール/電話も対象。
    // 電話は payload/remote_payload の JSON 文字列を使い、区切り記号を除去した比較も行う。
    where.push(
      "(name LIKE ? ESCAPE '\\' OR kana LIKE ? ESCAPE '\\' OR mail LIKE ? ESCAPE '\\' OR tel LIKE ? ESCAPE '\\' OR payload LIKE ? ESCAPE '\\' OR remote_payload LIKE ? ESCAPE '\\' OR replace(replace(replace(replace(tel,'-',''),'ー',''),'－',''),' ','') LIKE ? ESCAPE '\\' OR replace(replace(replace(replace(payload,'-',''),'ー',''),'－',''),' ','') LIKE ? ESCAPE '\\' OR replace(replace(replace(replace(remote_payload,'-',''),'ー',''),'－',''),' ','') LIKE ? ESCAPE '\\')"
    );
    args.push(q, q, q, q, q, q, qCompact, qCompact, qCompact);
  }
  if (query.state) {
    const cands = stateFilterCandidates(query.state);
    const n = Number(trimStr(query.state));
    const numericStateSql = Number.isFinite(n)
      ? " OR (trim(coalesce(state,'')) GLOB '[0-9]*.[0-9]*' AND CAST(state AS REAL) = ?)"
      : "";
    if (cands.length <= 1) {
      where.push(`(state = ?${numericStateSql})`);
      args.push(cands[0] || query.state);
      if (Number.isFinite(n)) args.push(n);
    } else {
      where.push(`(state IN (${cands.map(() => "?").join(",")})${numericStateSql})`);
      args.push(...cands);
      if (Number.isFinite(n)) args.push(n);
    }
  }
  if (query.baitai) {
    const b = normalizeFilterToken(query.baitai);
    where.push(
      "(trim(coalesce(baitai,'')) = ? OR trim(coalesce(baitai,'')) = ?)"
    );
    args.push(query.baitai, b);
  }
  if (query.status) {
    const s = normalizeFilterToken(query.status);
    // status 列だけでなく payload/remote_payload の c.status も見る（同期途中の不整合吸収）
    where.push(
      "(trim(coalesce(status,'')) = ? OR trim(coalesce(status,'')) = ? OR trim(coalesce(cast(json_extract(payload,'$.\"c.status\"') as text),'')) = ? OR trim(coalesce(cast(json_extract(payload,'$.\"c.status\"') as text),'')) = ? OR trim(coalesce(cast(json_extract(remote_payload,'$.\"c.status\"') as text),'')) = ? OR trim(coalesce(cast(json_extract(remote_payload,'$.\"c.status\"') as text),'')) = ?)"
    );
    args.push(query.status, s, query.status, s, query.status, s);
  }
  const reachedFrom = trimStr(query.statusReachedFrom || "");
  const reachedTo = trimStr(query.statusReachedTo || "");
  if (reachedFrom || reachedTo) {
    const fromDate = reachedFrom || "0001-01-01";
    const toDate = reachedTo || "9999-12-31";
    if (query.status) {
      const s = normalizeFilterToken(query.status);
      where.push(
        `EXISTS (
           SELECT 1
           FROM customer_status_daily sd
           WHERE sd.property_id = customer_snapshot.property_id
             AND sd.customer_id = customer_snapshot.customer_id
             AND sd.status_date >= ?
             AND sd.status_date <= ?
             AND (trim(coalesce(sd.status,'')) = ? OR trim(coalesce(sd.status,'')) = ?)
         )`
      );
      args.push(fromDate, toDate, query.status, s);
    } else {
      where.push(
        `EXISTS (
           SELECT 1
           FROM customer_status_daily sd
           WHERE sd.property_id = customer_snapshot.property_id
             AND sd.customer_id = customer_snapshot.customer_id
             AND sd.status_date >= ?
             AND sd.status_date <= ?
         )`
      );
      args.push(fromDate, toDate);
    }
  }
  if (query.ninzu) {
    where.push("ninzu = ?");
    args.push(query.ninzu);
  }
  if (query.yosan) {
    where.push("yosan = ?");
    args.push(query.yosan);
  }
  if (query.jikosikin) {
    where.push("jikosikin = ?");
    args.push(query.jikosikin);
  }
  if (query.madori) {
    where.push("questionnaire23 = ?");
    args.push(query.madori);
  }
  if (query.menseki) {
    where.push("questionnaire24 = ?");
    args.push(query.menseki);
  }

  const whereSql = where.join(" AND ");

  const dataR = await db.execute({
    sql: `SELECT customer_id, mail, upd_date, payload, local_overrides, remote_payload, status AS visit_status
          FROM customer_snapshot WHERE ${whereSql} ORDER BY upd_date DESC`,
    args,
  });
  const listColNames = dataR.columns || dataR.columnNames || [];
  const rows = (dataR.rows || []).map((row) => {
    const f = parseCustomerListRowForLocalQuery(row, listColNames);
    const payloadRaw = listRowFieldRaw(row, f, "payload", LOCAL_CUSTOMERS_LIST_COL.payload);
    const payload = parseJsonObjectLoose(payloadRaw);
    const st = listRowVisitStatusRaw(row, f);
    const stPayload = pickPayloadLocalized(payload, "c.status");
    const rpRaw = listRowFieldRaw(row, f, "remote_payload", LOCAL_CUSTOMERS_LIST_COL.remote_payload);
    const remote = parseJsonObjectLoose(rpRaw);
    let combined = trimStr(st) || trimStr(stPayload) || "";
    if (!combined) combined = trimStr(acquisitionVisitStatusRaw(remote)) || "";
    payload["c.status"] = combined;
    return {
      customerId: String(f.customer_id ?? payload?.["c.id"] ?? ""),
      mail: String(f.mail ?? payload?.["c.mail"] ?? "").trim().toLowerCase(),
      updDate: String(f.upd_date ?? payload?.["c.upd_date"] ?? ""),
      payload,
    };
  });
  const groups = new Map();
  for (const r of rows) {
    const gk = r.mail || `__id:${r.customerId}`;
    if (!groups.has(gk)) {
      groups.set(gk, {
        merged: { ...r.payload },
        mergedIds: [r.customerId],
        latestUpd: r.updDate || "",
        latestId: r.customerId,
      });
      continue;
    }
    const g = groups.get(gk);
    g.mergedIds.push(r.customerId);
    Object.entries(r.payload || {}).forEach(([k, v]) => {
      if (k === "c.status") {
        const cur = g.merged[k];
        const curEmpty = cur == null || String(cur).trim() === "";
        const nextStr = v == null ? "" : String(v).trim();
        if (curEmpty && nextStr) g.merged[k] = nextStr;
        return;
      }
      if (g.merged[k] == null || g.merged[k] === "") {
        g.merged[k] = v;
      }
    });
  }
  const mergedRows = [...groups.values()]
    .sort((a, b) => String(b.latestUpd || "").localeCompare(String(a.latestUpd || "")))
    .map((g) => {
      const out = { ...g.merged };
      out["c.id"] = out["c.id"] || g.latestId;
      out["c.merged_ids"] = g.mergedIds;
      out["c.merged_count"] = g.mergedIds.length;
      // JSON.stringify は undefined のキーを落とすため、一覧 API では常に文字列で返す（未設定は ""）
      {
        const s = out["c.status"];
        out["c.status"] = s == null ? "" : trimStr(String(s));
      }
      return out;
    });
  const total = mergedRows.length;
  const result = mergedRows.slice(offset, offset + limit);

  const wantDiag = String(query.diag || "").trim() === "1";
  const diagPayload = wantDiag
    ? {
        page,
        limit,
        filterStatus: query.status ? String(query.status).slice(0, 80) : "",
        sqlRowCount: rows.length,
        mergedTotal: total,
        pageResultLen: result.length,
        firstStatusLen: result[0] ? String(result[0]["c.status"] ?? "").length : 0,
        pageNonEmptyStatusCount: result.filter((r) => String(r["c.status"] ?? "").trim() !== "").length,
        firstRowStatusLikeLens: result[0]
          ? {
              c_status: debugFieldStrLen(result[0]["c.status"]),
              q51: debugFieldStrLen(result[0]["c.questionnaire51"]),
              q41: debugFieldStrLen(result[0]["c.questionnaire41"]),
            }
          : null,
      }
    : null;
  if (diagPayload) {
    const line = `${JSON.stringify({
      location: "sync-service.js:queryLocalCustomers",
      ...diagPayload,
      timestamp: Date.now(),
    })}\n`;
    for (const logPath of [DATA_CUSTOMER_LIST_DIAG_LOG, CURSOR_CUSTOMER_LIST_DEBUG_LOG]) {
      try {
        fs.mkdirSync(path.dirname(logPath), { recursive: true });
        fs.appendFileSync(logPath, line);
      } catch (_) {}
    }
  }

  return {
    status: true,
    data: {
      result,
      count: String(total),
      total_count: String(total),
      ...(diagPayload ? { _diag: diagPayload } : {}),
    },
  };
}

async function listCustomerReactions(propertyId, customerId, limit = 100) {
  const db = await getDb();
  const meR = await db.execute({
    sql: `SELECT mail FROM customer_snapshot WHERE property_id = ? AND customer_id = ? LIMIT 1`,
    args: [propertyId, customerId],
  });
  const mail = String(meR.rows?.[0]?.mail ?? "").trim().toLowerCase();
  let r;
  if (mail) {
    r = await db.execute({
      sql: `SELECT customer_id, date_entry, upd_date, payload
            FROM customer_snapshot
            WHERE property_id = ? AND lower(trim(mail)) = ?
            ORDER BY upd_date DESC
            LIMIT ?`,
      args: [propertyId, mail, Math.max(1, Math.min(300, Number(limit) || 100))],
    });
  } else {
    r = await db.execute({
      sql: `SELECT customer_id, date_entry, upd_date, payload
            FROM customer_snapshot
            WHERE property_id = ? AND customer_id = ?
            ORDER BY upd_date DESC
            LIMIT ?`,
      args: [propertyId, customerId, Math.max(1, Math.min(300, Number(limit) || 100))],
    });
  }
  return (r.rows || []).map((x) => {
    const payloadRaw = x.payload ?? x[3];
    let payload = {};
    try { payload = typeof payloadRaw === "string" ? JSON.parse(payloadRaw) : (payloadRaw || {}); } catch (_) {}
    return {
      customerId: String(x.customer_id ?? x[0] ?? ""),
      dateEntry: String(x.date_entry ?? x[1] ?? ""),
      updDate: String(x.upd_date ?? x[2] ?? ""),
      payload,
    };
  });
}

async function getSyncStatus(propertyId) {
  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT last_max_upd, last_full_at, last_incremental_at, rows_total FROM sync_cursor WHERE property_id = ?",
    args: [propertyId],
  });
  const row = r.rows[0];
  if (!row) {
    return { hasData: false, rows_total: 0 };
  }
  return {
    hasData: (row.rows_total ?? row[3] ?? 0) > 0,
    last_max_upd: row.last_max_upd ?? row[0],
    last_full_at: row.last_full_at ?? row[1],
    last_incremental_at: row.last_incremental_at ?? row[2],
    rows_total: Number(row.rows_total ?? row[3] ?? 0),
  };
}

async function getVisitStatuses(propertyId) {
  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT statuses_json FROM property_visit_statuses WHERE property_id = ?",
    args: [propertyId],
  });
  const raw = r.rows[0]?.statuses_json ?? r.rows[0]?.[0];
  if (raw) {
    try {
      const arr = JSON.parse(raw);
      if (Array.isArray(arr) && arr.length) return arr;
    } catch (_) {}
  }
  return [...DEFAULT_VISIT_STATUSES];
}

function stateLabelFromRaw(raw) {
  const s = trimStr(raw);
  if (!s) return "";
  const idx = PREFS.indexOf(s);
  if (idx >= 0) return s;
  const n = Number(s);
  if (Number.isFinite(n)) {
    const i = Math.trunc(n);
    if (i >= 1 && i <= PREFS.length) return PREFS[i - 1];
  }
  return s;
}

async function distinctNonEmptyValues(db, propertyId, colName, limit = 80) {
  const allow = new Set([
    "state",
    "baitai",
    "status",
    "ninzu",
    "yosan",
    "jikosikin",
    "questionnaire23",
    "questionnaire24",
  ]);
  if (!allow.has(colName)) throw new Error(`unsupported column: ${colName}`);
  const r = await db.execute({
    sql: `SELECT ${colName} AS v, COUNT(*) AS n
          FROM customer_snapshot
          WHERE property_id = ? AND trim(coalesce(${colName}, '')) <> ''
          GROUP BY ${colName}
          ORDER BY n DESC, v ASC
          LIMIT ?`,
    args: [propertyId, Math.max(1, Math.min(500, Number(limit) || 80))],
  });
  return (r.rows || [])
    .map((x) => trimStr(x.v ?? x[0]))
    .filter((x) => x && x !== "null" && x !== "undefined");
}

async function getCustomerFilterOptions(propertyId) {
  const db = await getDb();
  const [statesRaw, baitai, ninzu, yosan, jikosikin, madori, menseki, statusRaw, visitStatuses] = await Promise.all([
    distinctNonEmptyValues(db, propertyId, "state", 100),
    distinctNonEmptyValues(db, propertyId, "baitai", 40),
    distinctNonEmptyValues(db, propertyId, "ninzu", 40),
    distinctNonEmptyValues(db, propertyId, "yosan", 80),
    distinctNonEmptyValues(db, propertyId, "jikosikin", 80),
    distinctNonEmptyValues(db, propertyId, "questionnaire23", 60),
    distinctNonEmptyValues(db, propertyId, "questionnaire24", 80),
    distinctNonEmptyValues(db, propertyId, "status", 80),
    getVisitStatuses(propertyId),
  ]);

  const statesSet = new Set(statesRaw.map((x) => stateLabelFromRaw(x)).filter(Boolean));
  const states = [...statesSet];
  states.sort((a, b) => a.localeCompare(b, "ja"));

  const sortByAmountAsc = (arr) =>
    [...arr].sort((a, b) => {
      const na = parseJpAmountUpperBound(a);
      const nb = parseJpAmountUpperBound(b);
      const fa = Number.isFinite(na);
      const fb = Number.isFinite(nb);
      if (fa && fb) return na - nb;
      if (fa) return -1;
      if (fb) return 1;
      return String(a).localeCompare(String(b), "ja");
    });

  const statusSet = new Set();
  for (const s of visitStatuses || []) {
    const t = trimStr(s);
    if (t) statusSet.add(t);
  }
  for (const s of statusRaw || []) {
    const t = trimStr(s);
    if (t) statusSet.add(t);
  }

  return {
    state: states,
    baitai: baitai.length ? baitai : ["公式HP", "SUUMO"],
    status: [...statusSet],
    ninzu: ninzu.length ? ninzu : [...DEFAULT_NINZU_OPTIONS],
    yosan: yosan.length ? sortByAmountAsc(yosan) : sortByAmountAsc([...DEFAULT_YOSAN_OPTIONS]),
    jikosikin: jikosikin.length ? sortByAmountAsc(jikosikin) : sortByAmountAsc([...DEFAULT_JIKOSIKIN_OPTIONS]),
    madori: madori.length ? madori : [...DEFAULT_MADORI_OPTIONS],
    menseki: menseki.length ? menseki : [...DEFAULT_MENSEKI_OPTIONS],
  };
}

async function saveVisitStatuses(propertyId, statuses) {
  if (!Array.isArray(statuses)) {
    throw new Error("statuses は配列で指定してください");
  }
  const clean = [
    ...new Set(statuses.map((s) => String(s).trim()).filter(Boolean)),
  ].slice(0, 60);
  if (!clean.length) throw new Error("1件以上のステータス名が必要です");
  const db = await getDb();
  await db.execute({
    sql: `INSERT INTO property_visit_statuses (property_id, statuses_json, updated_at)
          VALUES (?, ?, datetime('now'))
          ON CONFLICT(property_id) DO UPDATE SET
            statuses_json = excluded.statuses_json,
            updated_at = datetime('now')`,
    args: [propertyId, JSON.stringify(clean)],
  });
  return clean;
}

const MAX_VISIT_STATUSES = 60;

/**
 * 同期・保存で出てきたステータス文言が、物件の来場ステータス一覧に無ければ末尾に追記（最大60件、重複・表記ゆれのみ除外）。
 */
async function appendMissingVisitStatuses(propertyId, candidates) {
  if (!propertyId || !Array.isArray(candidates) || !candidates.length) return;
  const current = await getVisitStatuses(propertyId);
  const seen = new Set(current.map((s) => visitStatusDedupKey(s)));
  const out = [...current];
  let changed = false;
  for (const c of candidates) {
    const t = trimStr(c);
    if (!t || t === "null" || t === "undefined") continue;
    const k = visitStatusDedupKey(t);
    if (!k || seen.has(k)) continue;
    if (out.length >= MAX_VISIT_STATUSES) break;
    seen.add(k);
    out.push(t);
    changed = true;
  }
  if (!changed) return;
  await saveVisitStatuses(propertyId, out);
}

/**
 * キャッシュ上の顧客を更新（デジタライズには書き込まない）。local_overrides に反映。
 */
async function patchLocalCustomer(propertyId, customerId, patch) {
  const allowed = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (!String(k).startsWith("c.")) continue;
    allowed[k] = v;
  }
  if (!Object.keys(allowed).length) {
    throw new Error("c. で始まるフィールドのみ更新できます（例: c.status）");
  }

  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT remote_payload, local_overrides, payload FROM customer_snapshot WHERE property_id = ? AND customer_id = ?",
    args: [propertyId, customerId],
  });
  const row0 = r.rows[0];
  if (!row0) throw new Error("顧客が見つかりません");

  let remoteStr = row0.remote_payload ?? row0[0];
  const loStr0 = row0.local_overrides ?? row0[1] ?? "{}";
  const payStr = row0.payload ?? row0[2];
  if (!remoteStr) remoteStr = payStr;

  let remote = {};
  try {
    remote = JSON.parse(remoteStr || "{}");
  } catch (_) {
    remote = {};
  }
  let lo = {};
  try {
    lo = JSON.parse(loStr0 || "{}");
  } catch (_) {
    lo = {};
  }
  const currentMerged = mergeRemoteAndOverrides(remote, omitEmptyLocalStatusOverride(lo));
  const beforeStatus = String(currentMerged["c.status"] ?? "");
  const nextLo = { ...lo, ...allowed };
  for (const [k, v] of Object.entries(allowed)) {
    if (v === null) delete nextLo[k];
  }
  if (Object.prototype.hasOwnProperty.call(allowed, "c.status")) {
    const afterStatus = allowed["c.status"] == null ? "" : String(allowed["c.status"]);
    if (afterStatus !== beforeStatus) {
      nextLo["c.status_changed_at"] = new Date().toISOString().slice(0, 19).replace("T", " ");
    }
  }
  const merged = mergeRemoteAndOverrides(remote, omitEmptyLocalStatusOverride(nextLo));
  const loJson = JSON.stringify(nextLo);
  const d = denormFromMerged(merged);

  // local override 履歴（例: 来場ステータス手動変更）を積む
  const historyStmts = [];
  for (const [k, v] of Object.entries(allowed)) {
    if (k === "c.status") {
      const toVal = v == null ? "" : String(v);
      historyStmts.push({
        sql: `INSERT INTO customer_history(property_id, customer_id, kind, field, from_value, to_value, snapshot_payload, created_at)
              VALUES(?, ?, 'status_changed', 'status', ?, ?, ?, datetime('now'))`,
        args: [propertyId, customerId, beforeStatus, toVal, JSON.stringify(merged)],
      });
      continue;
    }
    historyStmts.push({
      sql: `INSERT INTO customer_history(property_id, customer_id, kind, field, from_value, to_value, snapshot_payload, created_at)
            VALUES(?, ?, 'local_override', ?, NULL, ?, ?, datetime('now'))`,
      args: [propertyId, customerId, String(k), v == null ? "" : String(v), JSON.stringify(merged)],
    });
  }

  await db.execute({
    sql: `UPDATE customer_snapshot SET
      name=?, kana=?, mail=?, tel=?, state=?, city=?, baitai=?, status=?,
      ninzu=?, yosan=?, jikosikin=?, questionnaire23=?, questionnaire24=?,
      date_entry=?, upd_date=?, payload=?, local_overrides=?
      WHERE property_id=? AND customer_id=?`,
    args: [
      d.name,
      d.kana,
      d.mail,
      d.tel,
      d.state,
      d.city,
      d.baitai,
      d.status,
      d.ninzu,
      d.yosan,
      d.jikosikin,
      d.questionnaire23,
      d.questionnaire24,
      d.date_entry,
      d.upd_date,
      JSON.stringify(merged),
      loJson,
      propertyId,
      customerId,
    ],
  });
  if (Object.prototype.hasOwnProperty.call(allowed, "c.status")) {
    const toVal = trimStr(String(allowed["c.status"] == null ? "" : allowed["c.status"]));
    if (toVal) {
      const changedAt = toStatusDateTimeText(nextLo["c.status_changed_at"] || "");
      const statusDate = toStatusDateKey(changedAt);
      await db.execute({
        sql: `INSERT INTO customer_status_daily(property_id, customer_id, status_date, status, changed_at, source)
              VALUES(?, ?, ?, ?, ?, 'manual')
              ON CONFLICT(property_id, customer_id, status_date) DO UPDATE SET
                status = CASE WHEN coalesce(excluded.changed_at, '') >= coalesce(customer_status_daily.changed_at, '') THEN excluded.status ELSE customer_status_daily.status END,
                changed_at = CASE WHEN coalesce(excluded.changed_at, '') >= coalesce(customer_status_daily.changed_at, '') THEN excluded.changed_at ELSE customer_status_daily.changed_at END,
                source = CASE WHEN coalesce(excluded.changed_at, '') >= coalesce(customer_status_daily.changed_at, '') THEN 'manual' ELSE customer_status_daily.source END`,
        args: [propertyId, customerId, statusDate, toVal, changedAt],
      });
    }
  }
  if (historyStmts.length) await db.batch(historyStmts);
  try {
    const st = merged["c.status"];
    if (st != null && trimStr(st)) await appendMissingVisitStatuses(propertyId, [String(st).trim()]);
  } catch (_) {}
  return { ok: true, customer: merged };
}

async function listCustomerHistory(propertyId, customerId, limit = 50) {
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, kind, field, from_value, to_value, snapshot_payload, created_at
          FROM customer_history
          WHERE property_id = ? AND customer_id = ?
          ORDER BY id DESC
          LIMIT ?`,
    args: [propertyId, customerId, Math.max(1, Math.min(200, Number(limit) || 50))],
  });
  return (r.rows || []).map((x) => ({
    id: String(x.id ?? x[0]),
    kind: x.kind ?? x[1],
    field: x.field ?? x[2] ?? "",
    fromValue: x.from_value ?? x[3] ?? "",
    toValue: x.to_value ?? x[4] ?? "",
    snapshotPayload: x.snapshot_payload ?? x[5] ?? "",
    createdAt: x.created_at ?? x[6] ?? "",
  }));
}

async function listSalesHistory(propertyId, customerId, limit = 100) {
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, action_date, staff_name, action_type, action_detail, result, next_action_date, memo, created_at
          FROM customer_sales_history
          WHERE property_id = ? AND customer_id = ?
          ORDER BY id DESC
          LIMIT ?`,
    args: [propertyId, customerId, Math.max(1, Math.min(300, Number(limit) || 100))],
  });
  return (r.rows || []).map((x) => ({
    id: String(x.id ?? x[0]),
    actionDate: x.action_date ?? x[1] ?? "",
    staffName: x.staff_name ?? x[2] ?? "",
    actionType: x.action_type ?? x[3] ?? "",
    actionDetail: x.action_detail ?? x[4] ?? "",
    result: x.result ?? x[5] ?? "",
    nextActionDate: x.next_action_date ?? x[6] ?? "",
    memo: x.memo ?? x[7] ?? "",
    createdAt: x.created_at ?? x[8] ?? "",
  }));
}

async function addSalesHistory(propertyId, customerId, payload) {
  const db = await getDb();
  const actionDate = String(payload.actionDate || "").trim();
  const staffName = String(payload.staffName || "").trim();
  const actionType = String(payload.actionType || "").trim();
  const actionDetail = String(payload.actionDetail || "").trim();
  const result = String(payload.result || "").trim();
  const nextActionDate = String(payload.nextActionDate || "").trim();
  const memo = String(payload.memo || "").trim();
  await db.execute({
    sql: `INSERT INTO customer_sales_history
          (property_id, customer_id, action_date, staff_name, action_type, action_detail, result, next_action_date, memo, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
    args: [propertyId, customerId, actionDate, staffName, actionType, actionDetail, result, nextActionDate, memo],
  });
  return { ok: true };
}

async function updateSalesHistory(propertyId, customerId, salesId, payload) {
  const db = await getDb();
  const sid = Number(salesId);
  if (!Number.isFinite(sid) || sid <= 0) throw new Error("営業履歴IDが不正です");
  const actionDate = String(payload.actionDate || "").trim();
  const staffName = String(payload.staffName || "").trim();
  const actionType = String(payload.actionType || "").trim();
  const actionDetail = String(payload.actionDetail || "").trim();
  const result = String(payload.result || "").trim();
  const nextActionDate = String(payload.nextActionDate || "").trim();
  const memo = String(payload.memo || "").trim();
  const r = await db.execute({
    sql: `UPDATE customer_sales_history
          SET action_date = ?, staff_name = ?, action_type = ?, action_detail = ?, result = ?, next_action_date = ?, memo = ?
          WHERE id = ? AND property_id = ? AND customer_id = ?`,
    args: [actionDate, staffName, actionType, actionDetail, result, nextActionDate, memo, sid, propertyId, customerId],
  });
  const changed = Number(r.rowsAffected || 0);
  if (!changed) throw new Error("更新対象の営業履歴が見つかりません");
  return { ok: true };
}

async function deleteSalesHistory(propertyId, customerId, salesId) {
  const db = await getDb();
  const sid = Number(salesId);
  if (!Number.isFinite(sid) || sid <= 0) throw new Error("営業履歴IDが不正です");
  const r = await db.execute({
    sql: `DELETE FROM customer_sales_history
          WHERE id = ? AND property_id = ? AND customer_id = ?`,
    args: [sid, propertyId, customerId],
  });
  const changed = Number(r.rowsAffected || 0);
  if (!changed) throw new Error("削除対象の営業履歴が見つかりません");
  return { ok: true };
}

async function listCustomerFiles(propertyId, customerId, limit = 100) {
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, filename, mime_type, file_data, created_at
          FROM customer_files
          WHERE property_id = ? AND customer_id = ?
          ORDER BY created_at DESC
          LIMIT ?`,
    args: [propertyId, customerId, Math.max(1, Math.min(300, Number(limit) || 100))],
  });
  return (r.rows || []).map((x) => ({
    id: String(x.id ?? x[0]),
    filename: x.filename ?? x[1] ?? "",
    mimeType: x.mime_type ?? x[2] ?? "",
    fileData: x.file_data ?? x[3] ?? "",
    createdAt: x.created_at ?? x[4] ?? "",
  }));
}

async function addCustomerFile(propertyId, customerId, payload) {
  const filename = String(payload.filename || "").trim();
  const mimeType = String(payload.mimeType || "").trim();
  const fileData = String(payload.fileData || "");
  if (!filename) throw new Error("ファイル名が不正です");
  const allow = ["application/pdf", "image/jpeg", "image/png"];
  if (!allow.includes(mimeType)) throw new Error("対応形式は pdf / jpg / png のみです");
  if (!fileData.startsWith("data:")) throw new Error("ファイルデータ形式が不正です");
  if (fileData.length > 5 * 1024 * 1024) throw new Error("ファイルサイズが大きすぎます（5MB以下）");
  const db = await getDb();
  const id = `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
  await db.execute({
    sql: `INSERT INTO customer_files(id, property_id, customer_id, filename, mime_type, file_data, created_at)
          VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`,
    args: [id, propertyId, customerId, filename, mimeType, fileData],
  });
  return { id };
}

async function deleteCustomerFile(propertyId, customerId, fileId) {
  const db = await getDb();
  await db.execute({
    sql: `DELETE FROM customer_files WHERE id = ? AND property_id = ? AND customer_id = ?`,
    args: [String(fileId), propertyId, customerId],
  });
  return { ok: true };
}

const DEFAULT_CUSTOMER_COLUMNS = [
  { key: "c.id", width: 100 },
  { key: "c.name", width: 220 },
  { key: "c.status", width: 140 },
  { key: "c.mail", width: 220 },
  { key: "c.state", width: 120 },
  { key: "c.city", width: 140 },
  { key: "c.baitai", width: 140 },
  { key: "c.date_entry", width: 120 },
  { key: "c.upd_date", width: 120 },
];

const KNOWN_CUSTOMER_FIELD_LABELS = {
  "c.id": "顧客ID",
  "c.name": "顧客名",
  "c.kana": "フリガナ",
  "c.status": "ステータス",
  "c.mail": "E-mail",
  "c.tel": "電話",
  "c.zip": "郵便番号",
  "c.state": "都道府県",
  "c.city": "市区町村",
  "c.town": "町域",
  "c.address": "番地",
  "c.tatemono": "建物名",
  "c.moyori": "最寄り駅",
  "c.sex": "性別",
  "c.birth": "生年月日",
  "c.work": "職業",
  "c.baitai": "反響メディア",
  "c.date_entry": "反響日",
  "c.ins_date": "登録日時",
  "c.upd_date": "更新日",
  "c.ninzu": "家族人数",
  "c.yosan": "予算",
  "c.jikosikin": "自己資金",
  "c.questionnaire23": "希望間取り",
  "c.questionnaire24": "希望面積",
  "c.questionnaire30": "年収（税込）",
  "c.questionnaire41": "見込み度",
  "c.questionnaire49": "ネック",
  "c.questionnaire50": "フック",
  "c.questionnaire51": "状況",
  "c.questionnaire52": "検討部屋",
  "c.qr": "QR",
};

function defaultLabelOfField(key) {
  const k = String(key || "");
  if (CUSTOMER_SPEC_LABELS_JA[k]) return CUSTOMER_SPEC_LABELS_JA[k];
  if (KNOWN_CUSTOMER_FIELD_LABELS[k]) return KNOWN_CUSTOMER_FIELD_LABELS[k];
  const m = k.match(/^c\.questionnaire(\d+)$/);
  if (m) return `アンケート項目${m[1]}`;
  return k.replace(/^c\./, "");
}

/** DB に保存された列設定に c.status が無いとき補う（表示項目の誤保存で列ごと消えるのを防ぐ） */
function normalizeCustomerListColKey(key) {
  return String(key || "")
    .trim()
    .replace(/\uFF0E/g, ".")
    .replace(/\u3002/g, ".")
    .normalize("NFKC");
}

function dedupeCustomerListColumnsByKey(list) {
  const seen = new Set();
  return list.filter((c) => {
    const k = c.key;
    if (!k || seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function ensureListColumnsIncludeStatus(columns) {
  let list = Array.isArray(columns)
    ? columns.map((c) => ({ ...c, key: normalizeCustomerListColKey(c.key) }))
    : [];
  list = dedupeCustomerListColumnsByKey(list.filter((c) => c.key));
  if (!list.length) return list;
  if (list.some((c) => c.key === "c.status")) return list;
  const def = DEFAULT_CUSTOMER_COLUMNS.find((c) => c.key === "c.status") || { key: "c.status", width: 140 };
  const nameIdx = list.findIndex((c) => c.key === "c.name");
  const idx = nameIdx >= 0 ? nameIdx + 1 : Math.min(2, list.length);
  list.splice(idx, 0, {
    key: "c.status",
    width: Math.max(80, Math.min(600, Number(def.width || 140))),
    label: defaultLabelOfField("c.status"),
  });
  return list;
}

/** 仕様一覧（API・columns.specFields 用） */
function getCustomerSpecFieldDefs() {
  return CUSTOMER_SNAPSHOT_FIELDS.map((key) => ({ key, defaultLabel: defaultLabelOfField(key) }));
}

const DEFAULT_CUSTOMER_DETAIL_FIELDS = [
  { key: "c.name", label: "顧客名", display: 1, print: 1, oneLine: 0, width: 240 },
  { key: "c.kana", label: "フリガナ", display: 1, print: 1, oneLine: 0, width: 220 },
  { key: "c.sex", label: "性別", display: 1, print: 1, oneLine: 1, width: 120 },
  { key: "c.baitai", label: "反響メディア", display: 1, print: 1, oneLine: 1, width: 180 },
  { key: "c.status", label: "ステータス", display: 1, print: 1, oneLine: 1, width: 180 },
  { key: "c.birth", label: "生年月日", display: 1, print: 1, oneLine: 1, width: 160 },
  { key: "c.ninzu", label: "ご家族構成", display: 1, print: 1, oneLine: 1, width: 180 },
  { key: "c.mail", label: "メール", display: 1, print: 1, oneLine: 0, width: 260 },
  { key: "c.tel", label: "電話番号", display: 1, print: 1, oneLine: 0, width: 180 },
  { key: "c.state", label: "都道府県", display: 1, print: 1, oneLine: 1, width: 140 },
  { key: "c.city", label: "市区町村", display: 1, print: 1, oneLine: 1, width: 160 },
  { key: "c.address", label: "住所", display: 1, print: 1, oneLine: 0, width: 260 },
  { key: "c.yosan", label: "予算", display: 1, print: 1, oneLine: 1, width: 140 },
  { key: "c.jikosikin", label: "自己資金", display: 1, print: 1, oneLine: 1, width: 140 },
];

async function getCustomerListColumns(tenantId, userId) {
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT columns_json FROM customer_list_columns WHERE tenant_id = ? AND user_id = ? LIMIT 1`,
    args: [String(tenantId), String(userId)],
  });
  const raw = r.rows?.[0]?.columns_json;
  if (!raw) {
    return DEFAULT_CUSTOMER_COLUMNS;
  }
  try {
    const arr = JSON.parse(raw);
    if (Array.isArray(arr) && arr.length) {
      return ensureListColumnsIncludeStatus(arr);
    }
  } catch (_) {}
  return DEFAULT_CUSTOMER_COLUMNS;
}

async function saveCustomerListColumns(tenantId, userId, columns) {
  if (!Array.isArray(columns) || !columns.length) throw new Error("columns が不正です");
  let clean = columns
    .map((c) => ({
      key: normalizeCustomerListColKey(c.key),
      label: String(c.label || "").trim(),
      width: Math.max(80, Math.min(600, Number(c.width || 120))),
    }))
    .filter((c) => c.key)
    .map((c) => ({ ...c, label: c.label || defaultLabelOfField(c.key) }));
  clean = dedupeCustomerListColumnsByKey(clean);
  clean = ensureListColumnsIncludeStatus(clean);
  if (!clean.length) throw new Error("列が1件以上必要です");
  const db = await getDb();
  await db.execute({
    sql: `INSERT INTO customer_list_columns(tenant_id, user_id, columns_json, updated_at)
          VALUES (?, ?, ?, datetime('now'))
          ON CONFLICT(tenant_id, user_id) DO UPDATE SET
            columns_json = excluded.columns_json,
            updated_at = datetime('now')`,
    args: [String(tenantId), String(userId), JSON.stringify(clean)],
  });
  return clean;
}

/** 一覧の表示候補に出す UI 専用列（API の acquisition フィールド外） */
const LIST_COLUMN_UI_EXTRA_KEYS = ["c.qr"];

async function listCustomerAvailableFields(propertyId, limitRows = 300, options = {}) {
  const keys = new Set();
  CUSTOMER_SNAPSHOT_FIELDS.forEach((k) => keys.add(String(k)));
  LIST_COLUMN_UI_EXTRA_KEYS.forEach((k) => keys.add(String(k)));
  DEFAULT_CUSTOMER_COLUMNS.forEach((c) => keys.add(String(c.key)));

  const remoteProp = options && options.prop;
  const remoteSecrets = options && options.secrets;
  if (remoteProp && remoteSecrets) {
    try {
      const pageSize = 200;
      let page = 1;
      let stableCount = 0;
      while (page <= 5) {
        const before = keys.size;
        const resp = await postAcquisition(remoteProp, remoteSecrets, {
          limit: pageSize,
          page,
          order: JSON.stringify(["c.upd_date desc"]),
        });
        if (!resp || !resp.status) break;
        const rows = Array.isArray(resp?.data?.result) ? resp.data.result : [];
        rows.forEach((row) => {
          if (!row || typeof row !== "object") return;
          Object.keys(row).forEach((k) => {
            if (String(k).startsWith("c.")) keys.add(String(k));
          });
        });
        if (!rows.length || rows.length < pageSize) break;
        if (keys.size === before) {
          stableCount += 1;
          if (stableCount >= 2) break;
        } else {
          stableCount = 0;
        }
        page += 1;
      }
    } catch (_) {}
  }

  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT payload FROM customer_snapshot
          WHERE property_id = ?
          ORDER BY upd_date DESC
          LIMIT ?`,
    args: [propertyId, Math.max(50, Math.min(1000, Number(limitRows) || 300))],
  });
  (r.rows || []).forEach((row) => {
    const p = row.payload ?? row[0];
    if (!p) return;
    try {
      const obj = typeof p === "string" ? JSON.parse(p) : p;
      Object.keys(obj || {}).forEach((k) => {
        if (String(k).startsWith("c.")) keys.add(String(k));
      });
    } catch (_) {}
  });
  const finalKeys = [...keys].sort((a, b) => a.localeCompare(b, "ja"));

  return finalKeys
    .map((k) => ({ key: k, defaultLabel: defaultLabelOfField(k) }));
}

async function getCustomerDetailFieldConfig(tenantId, userId) {
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT fields_json FROM customer_detail_field_config WHERE tenant_id = ? AND user_id = ? LIMIT 1`,
    args: [String(tenantId), String(userId)],
  });
  const raw = r.rows?.[0]?.fields_json;
  if (!raw) return DEFAULT_CUSTOMER_DETAIL_FIELDS;
  try {
    const arr = JSON.parse(raw);
    if (Array.isArray(arr) && arr.length) return arr;
  } catch (_) {}
  return DEFAULT_CUSTOMER_DETAIL_FIELDS;
}

async function saveCustomerDetailFieldConfig(tenantId, userId, fields) {
  if (!Array.isArray(fields) || !fields.length) throw new Error("fields が不正です");
  const clean = fields.map((f) => ({
    key: String(f.key || "").trim(),
    label: String(f.label || "").trim(),
    display: Number(f.display) === 0 ? 0 : 1,
    print: Number(f.print) === 0 ? 0 : 1,
    oneLine: Number(f.oneLine) === 1 ? 1 : 0,
    width: Math.max(80, Math.min(600, Number(f.width || 160))),
  })).filter((f) => f.key);
  const db = await getDb();
  await db.execute({
    sql: `INSERT INTO customer_detail_field_config(tenant_id, user_id, fields_json, updated_at)
          VALUES (?, ?, ?, datetime('now'))
          ON CONFLICT(tenant_id, user_id) DO UPDATE SET
            fields_json = excluded.fields_json,
            updated_at = datetime('now')`,
    args: [String(tenantId), String(userId), JSON.stringify(clean)],
  });
  return clean;
}

async function getPropertySalesMetrics(propertyId, totalUnitsRaw) {
  const db = await getDb();
  const visitStatuses = await getVisitStatuses(propertyId);
  const contractStatuses = (visitStatuses || [])
    .map((s) => trimStr(s))
    .filter((s) => s && /契約/.test(s));
  const contractSet = new Set(contractStatuses.length ? contractStatuses : ["契約"]);
  const r = await db.execute({
    sql: `SELECT status, payload
          FROM customer_snapshot
          WHERE property_id = ?`,
    args: [propertyId],
  });
  const unitSet = new Set();
  let contractedCustomerCount = 0;
  for (const row of r.rows || []) {
    const st = trimStr(row.status ?? row[0]);
    if (!st || !contractSet.has(st)) continue;
    contractedCustomerCount += 1;
    let p = {};
    try {
      p = JSON.parse(row.payload ?? row[1] ?? "{}");
    } catch (_) {
      p = {};
    }
    const roomNo = trimStr(p["c.questionnaire18"] ?? p["申込部屋番号"] ?? "");
    if (roomNo) unitSet.add(roomNo);
  }
  const totalUnitsNum = Number(totalUnitsRaw);
  const totalUnits = Number.isFinite(totalUnitsNum) && totalUnitsNum >= 0 ? Math.trunc(totalUnitsNum) : null;
  const contractedUnits = unitSet.size;
  const remainingUnits = totalUnits == null ? null : Math.max(0, totalUnits - contractedUnits);
  return {
    contractStatuses: [...contractSet],
    contractedCustomerCount,
    contractedUnits,
    remainingUnits,
    totalUnits,
  };
}

module.exports = {
  runCustomerSync,
  queryLocalCustomers,
  getSyncStatus,
  getVisitStatuses,
  getCustomerFilterOptions,
  saveVisitStatuses,
  patchLocalCustomer,
  listCustomerHistory,
  listCustomerReactions,
  listSalesHistory,
  addSalesHistory,
  updateSalesHistory,
  deleteSalesHistory,
  listCustomerFiles,
  addCustomerFile,
  deleteCustomerFile,
  getCustomerListColumns,
  saveCustomerListColumns,
  DEFAULT_CUSTOMER_COLUMNS,
  listCustomerAvailableFields,
  defaultLabelOfField,
  getCustomerSpecFieldDefs,
  getCustomerDetailFieldConfig,
  saveCustomerDetailFieldConfig,
  getPropertySalesMetrics,
  DEFAULT_CUSTOMER_DETAIL_FIELDS,
  getDb,
  DEFAULT_VISIT_STATUSES,
};
