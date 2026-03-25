const { CUSTOMER_SNAPSHOT_FIELDS } = require("./customer-fields");
const { postAcquisition } = require("./digitaleyes");
const { getDb } = require("./sync-db");
const {
  mergeRemoteAndOverrides,
  rowArgsForUpsert,
  denormFromMerged,
} = require("./customer-row");
const { DEFAULT_VISIT_STATUSES } = require("./visit-status-defaults");

const FIELDS_JSON = JSON.stringify(CUSTOMER_SNAPSHOT_FIELDS);

const UPSERT_SQL = `INSERT INTO customer_snapshot (
  property_id, customer_id, name, kana, mail, state, city, baitai, status,
  ninzu, yosan, jikosikin, questionnaire23, questionnaire24, date_entry, upd_date,
  payload, remote_payload, local_overrides
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(property_id, customer_id) DO UPDATE SET
  name=excluded.name, kana=excluded.kana, mail=excluded.mail, state=excluded.state,
  city=excluded.city, baitai=excluded.baitai, status=excluded.status,
  ninzu=excluded.ninzu, yosan=excluded.yosan, jikosikin=excluded.jikosikin,
  questionnaire23=excluded.questionnaire23, questionnaire24=excluded.questionnaire24,
  date_entry=excluded.date_entry, upd_date=excluded.upd_date,
  payload=excluded.payload, remote_payload=excluded.remote_payload,
  local_overrides=excluded.local_overrides`;

function escapeLike(s) {
  return String(s).replace(/[%_\\]/g, "\\$&");
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
  const ids = rows.map((r) => String(r["c.id"] ?? ""));
  const ph = ids.map(() => "?").join(",");
  const existing = await db.execute({
    sql: `SELECT customer_id, local_overrides FROM customer_snapshot WHERE property_id = ? AND customer_id IN (${ph})`,
    args: [propertyId, ...ids],
  });
  const overrideMap = new Map();
  for (const row of existing.rows || []) {
    const cid = row.customer_id ?? row[0];
    const lo = row.local_overrides ?? row[1] ?? "{}";
    overrideMap.set(String(cid), typeof lo === "string" ? lo : "{}");
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
    const merged = mergeRemoteAndOverrides(remoteRow, lo);
    return {
      sql: UPSERT_SQL,
      args: rowArgsForUpsert(propertyId, remoteRow, merged, loStr),
    };
  });
  await db.batch(stmts);
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
    fields: FIELDS_JSON,
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
      fields: FIELDS_JSON,
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
      fields: FIELDS_JSON,
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
    const q = "%" + escapeLike(query.keyword) + "%";
    where.push("(name LIKE ? ESCAPE '\\' OR kana LIKE ? ESCAPE '\\')");
    args.push(q, q);
  }
  if (query.state) {
    where.push("state = ?");
    args.push(query.state);
  }
  if (query.baitai) {
    where.push("baitai = ?");
    args.push(query.baitai);
  }
  if (query.status) {
    where.push("status = ?");
    args.push(query.status);
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

  const cntR = await db.execute({
    sql: `SELECT COUNT(*) AS n FROM customer_snapshot WHERE ${whereSql}`,
    args,
  });
  const total = Number(cntR.rows[0]?.n ?? cntR.rows[0]?.[0] ?? 0);

  const dataR = await db.execute({
    sql: `SELECT payload FROM customer_snapshot WHERE ${whereSql} ORDER BY upd_date DESC LIMIT ? OFFSET ?`,
    args: [...args, limit, offset],
  });

  const result = (dataR.rows || []).map((row) => {
    const p = row.payload ?? row[0];
    return typeof p === "string" ? JSON.parse(p) : p;
  });

  return {
    status: true,
    data: {
      result,
      count: String(total),
      total_count: String(total),
    },
  };
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
  const nextLo = { ...lo, ...allowed };
  for (const [k, v] of Object.entries(allowed)) {
    if (v === null) delete nextLo[k];
  }
  const merged = mergeRemoteAndOverrides(remote, nextLo);
  const loJson = JSON.stringify(nextLo);
  const d = denormFromMerged(merged);

  await db.execute({
    sql: `UPDATE customer_snapshot SET
      name=?, kana=?, mail=?, state=?, city=?, baitai=?, status=?,
      ninzu=?, yosan=?, jikosikin=?, questionnaire23=?, questionnaire24=?,
      date_entry=?, upd_date=?, payload=?, local_overrides=?
      WHERE property_id=? AND customer_id=?`,
    args: [
      d.name,
      d.kana,
      d.mail,
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
  return { ok: true, customer: merged };
}

module.exports = {
  runCustomerSync,
  queryLocalCustomers,
  getSyncStatus,
  getVisitStatuses,
  saveVisitStatuses,
  patchLocalCustomer,
  getDb,
  DEFAULT_VISIT_STATUSES,
};
