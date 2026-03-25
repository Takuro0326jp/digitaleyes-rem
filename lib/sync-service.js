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
    sql: `SELECT customer_id, local_overrides, remote_payload, baitai, status, payload
          FROM customer_snapshot WHERE property_id = ? AND customer_id IN (${ph})`,
    args: [propertyId, ...ids],
  });
  const overrideMap = new Map();
  const prevMap = new Map();
  for (const row of existing.rows || []) {
    const cid = row.customer_id ?? row[0];
    const lo = row.local_overrides ?? row[1] ?? "{}";
    overrideMap.set(String(cid), typeof lo === "string" ? lo : "{}");
    prevMap.set(String(cid), {
      remotePayload: row.remote_payload ?? row[2] ?? "",
      baitai: row.baitai ?? row[3] ?? "",
      status: row.status ?? row[4] ?? "",
      payload: row.payload ?? row[5] ?? "",
    });
  }

  // 変更履歴（baitai の変化）を積む
  const historyStmts = [];
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
    // status はローカル単独管理（デジタライズ値で上書きしない）
    if (prev) {
      sanitized["c.status"] = prev.status ?? "";
      let prevPayload = {};
      try { prevPayload = JSON.parse(prev.payload || "{}"); } catch (_) {}
      sanitized["c.status_changed_at"] = prevPayload["c.status_changed_at"] ?? "";
    } else {
      sanitized["c.status"] = "";
      sanitized["c.status_changed_at"] = "";
    }
    const merged = mergeRemoteAndOverrides(sanitized, lo);
    return {
      sql: UPSERT_SQL,
      args: rowArgsForUpsert(propertyId, sanitized, merged, loStr),
    };
  });
  await db.batch([...historyStmts, ...stmts]);
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

  const dataR = await db.execute({
    sql: `SELECT customer_id, mail, upd_date, payload FROM customer_snapshot WHERE ${whereSql} ORDER BY upd_date DESC`,
    args,
  });
  const rows = (dataR.rows || []).map((row) => {
    const payloadRaw = row.payload ?? row[3] ?? row[0];
    const payload = typeof payloadRaw === "string" ? JSON.parse(payloadRaw) : payloadRaw;
    return {
      customerId: String(row.customer_id ?? row[0] ?? payload?.["c.id"] ?? ""),
      mail: String(row.mail ?? row[1] ?? payload?.["c.mail"] ?? "").trim().toLowerCase(),
      updDate: String(row.upd_date ?? row[2] ?? payload?.["c.upd_date"] ?? ""),
      payload: payload || {},
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
      return out;
    });
  const total = mergedRows.length;
  const result = mergedRows.slice(offset, offset + limit);

  return {
    status: true,
    data: {
      result,
      count: String(total),
      total_count: String(total),
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
  const currentMerged = mergeRemoteAndOverrides(remote, lo);
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
  const merged = mergeRemoteAndOverrides(remote, nextLo);
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
  if (historyStmts.length) await db.batch(historyStmts);
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
  "c.state": "都道府県",
  "c.city": "市区町村",
  "c.baitai": "反響メディア",
  "c.date_entry": "反響日",
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
};

function defaultLabelOfField(key) {
  const k = String(key || "");
  if (KNOWN_CUSTOMER_FIELD_LABELS[k]) return KNOWN_CUSTOMER_FIELD_LABELS[k];
  const m = k.match(/^c\.questionnaire(\d+)$/);
  if (m) return `アンケート項目${m[1]}`;
  return k.replace(/^c\./, "");
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
  if (!raw) return DEFAULT_CUSTOMER_COLUMNS;
  try {
    const arr = JSON.parse(raw);
    if (Array.isArray(arr) && arr.length) return arr;
  } catch (_) {}
  return DEFAULT_CUSTOMER_COLUMNS;
}

async function saveCustomerListColumns(tenantId, userId, columns) {
  if (!Array.isArray(columns) || !columns.length) throw new Error("columns が不正です");
  const clean = columns
    .map((c) => ({
      key: String(c.key || "").trim(),
      label: String(c.label || "").trim(),
      width: Math.max(80, Math.min(600, Number(c.width || 120))),
    }))
    .filter((c) => c.key)
    .map((c) => ({ ...c, label: c.label || defaultLabelOfField(c.key) }));
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

async function listCustomerAvailableFields(propertyId, limitRows = 300) {
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT payload FROM customer_snapshot
          WHERE property_id = ?
          ORDER BY upd_date DESC
          LIMIT ?`,
    args: [propertyId, Math.max(50, Math.min(1000, Number(limitRows) || 300))],
  });
  const keys = new Set();
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
  DEFAULT_CUSTOMER_COLUMNS.forEach((c) => keys.add(String(c.key)));
  return [...keys]
    .sort((a, b) => a.localeCompare(b, "ja"))
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

module.exports = {
  runCustomerSync,
  queryLocalCustomers,
  getSyncStatus,
  getVisitStatuses,
  saveVisitStatuses,
  patchLocalCustomer,
  listCustomerHistory,
  listCustomerReactions,
  listSalesHistory,
  addSalesHistory,
  listCustomerFiles,
  addCustomerFile,
  deleteCustomerFile,
  getCustomerListColumns,
  saveCustomerListColumns,
  DEFAULT_CUSTOMER_COLUMNS,
  listCustomerAvailableFields,
  defaultLabelOfField,
  getCustomerDetailFieldConfig,
  saveCustomerDetailFieldConfig,
  DEFAULT_CUSTOMER_DETAIL_FIELDS,
  getDb,
  DEFAULT_VISIT_STATUSES,
};
