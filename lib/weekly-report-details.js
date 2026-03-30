const { getDb } = require("./sync-db");

function roomsFromPayload(payloadStr) {
  let p = {};
  try {
    p = JSON.parse(payloadStr || "{}");
  } catch (_) {
    return [""];
  }
  const raw = String(p["c.questionnaire18"] ?? p["申込部屋番号"] ?? p.questionnaire18 ?? "").trim();
  if (!raw) return [""];
  const parts = raw.split(/[,、／/]/).map((s) => s.trim()).filter(Boolean);
  return parts.length ? parts : [""];
}

function displayName(nameCol, payloadStr) {
  const n = String(nameCol || "").trim();
  if (n) return n;
  try {
    const p = JSON.parse(payloadStr || "{}");
    return String(p["c.name"] ?? p.name ?? "").trim();
  } catch (_) {
    return "";
  }
}

/**
 * 週内に to_value が該当ステータスへ変わった履歴を列挙し、部屋番号が複数なら同名で行を分割する。
 */
async function loadExpandedStatusChangeRows(propertyId, weekStart, weekEnd, toStatusList, maxOut) {
  if (!toStatusList || !toStatusList.length) return [];
  const placeholders = toStatusList.map(() => "?").join(",");
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT substr(h.created_at, 1, 10) AS d, c.name AS cname, c.payload
          FROM customer_history h
          JOIN customer_snapshot c ON c.property_id = h.property_id AND c.customer_id = h.customer_id
          WHERE h.property_id = ?
            AND h.kind = 'status_changed'
            AND substr(h.created_at, 1, 10) >= ? AND substr(h.created_at, 1, 10) <= ?
            AND h.to_value IN (${placeholders})
          ORDER BY h.created_at ASC, h.id ASC`,
    args: [propertyId, weekStart, weekEnd, ...toStatusList],
  });
  const out = [];
  for (const row of r.rows || []) {
    const d = String(row.d ?? row[0] ?? "");
    const name = displayName(row.cname ?? row[1], row.payload ?? row[2]);
    const rooms = roomsFromPayload(row.payload ?? row[2]);
    for (const room of rooms) {
      if (out.length >= maxOut) return out;
      out.push({ roomNumber: room, name, dateStr: d });
    }
  }
  return out;
}

function extractPayloadFields(payloadStr) {
  let p = {};
  try {
    p = JSON.parse(payloadStr || "{}");
  } catch (_) {
    p = {};
  }
  const get = (...keys) => {
    for (const k of keys) {
      const v = p[k];
      if (v != null && String(v).trim() !== "") return String(v).trim();
    }
    return "";
  };
  return {
    price: get("販売価格", "金額", "c.questionnaire17"),
    deposit: get("手付金", "c.questionnaireDeposit"),
    staff: get("担当", "c.tantouName", "c.staff"),
    withholding: get("反響元", "c.baitai"),
    contractDate: get("契約日", "c.keiyaku_date"),
    applicationDate: get("申込日", "c.moushikomi_date"),
    cancelDate: get("申込キャンセル日", "キャンセル日"),
    cancellationDate: get("解約日"),
    notes: get("備考", "c.memo", "memo"),
  };
}

function statusInList(value, list) {
  const v = String(value ?? "");
  return (list || []).some((s) => String(s) === v);
}

/**
 * 週報明細・Excel 用: 週内の来場アクション＋ステータス変更をフラットな顧客行にまとめる。
 */
async function loadWeekCustomersForReport(propertyId, weekStart, weekEnd, applyStatuses, contractStatuses, cancelStatuses) {
  const db = await getDb();
  const out = [];

  const hist = await db.execute({
    sql: `SELECT substr(h.created_at, 1, 10) AS d, h.from_value, h.to_value, c.name AS cname, c.payload, c.baitai
          FROM customer_history h
          JOIN customer_snapshot c ON c.property_id = h.property_id AND c.customer_id = h.customer_id
          WHERE h.property_id = ?
            AND h.kind = 'status_changed'
            AND substr(h.created_at, 1, 10) >= ? AND substr(h.created_at, 1, 10) <= ?
          ORDER BY h.created_at ASC, h.id ASC`,
    args: [propertyId, weekStart, weekEnd],
  });

  for (const row of hist.rows || []) {
    const d = String(row.d ?? "");
    const fromV = row.from_value;
    const toV = row.to_value;
    const payload = row.payload ?? "";
    const name = displayName(row.cname, payload);
    const fx = extractPayloadFields(payload);
    const rooms = roomsFromPayload(payload);
    const room0 = rooms[0] || "";

    let statusLabel = null;
    if (statusInList(toV, cancelStatuses) && statusInList(fromV, applyStatuses)) statusLabel = "申込キャンセル";
    else if (statusInList(toV, applyStatuses)) statusLabel = "申込";
    else if (statusInList(toV, contractStatuses)) statusLabel = "契約";
    else if (statusInList(toV, cancelStatuses)) statusLabel = "解約";

    if (!statusLabel) continue;

    const rec = {
      status: statusLabel,
      roomNumber: room0,
      price: fx.price,
      name,
      applicationDate: "",
      contractDate: "",
      cancelDate: "",
      cancellationDate: "",
      visitDate: "",
      deposit: fx.deposit,
      withholding: fx.withholding || String(row.baitai ?? "").trim(),
      staff: fx.staff,
      notes: fx.notes,
    };

    if (statusLabel === "申込") {
      rec.applicationDate = d;
      if (fx.contractDate) rec.contractDate = fx.contractDate;
    } else if (statusLabel === "申込キャンセル") {
      rec.applicationDate = fx.applicationDate || d;
      rec.cancelDate = d;
    } else if (statusLabel === "契約") {
      rec.applicationDate = fx.applicationDate || "";
      rec.contractDate = d;
    } else if (statusLabel === "解約") {
      rec.contractDate = fx.contractDate || "";
      rec.cancellationDate = d;
    }

    out.push(rec);
  }

  const visitTypes = new Set(["新規来場", "再来場", "再々来場"]);
  const sales = await db.execute({
    sql: `SELECT substr(s.action_date, 1, 10) AS d, s.action_type, c.name AS cname, c.payload, c.baitai
          FROM customer_sales_history s
          JOIN customer_snapshot c ON c.property_id = s.property_id AND c.customer_id = s.customer_id
          WHERE s.property_id = ?
            AND substr(s.action_date, 1, 10) >= ? AND substr(s.action_date, 1, 10) <= ?
          ORDER BY s.action_date ASC, s.id ASC`,
    args: [propertyId, weekStart, weekEnd],
  });

  for (const row of sales.rows || []) {
    const at = String(row.action_type ?? "");
    if (!visitTypes.has(at)) continue;
    const payload = row.payload ?? "";
    const fx = extractPayloadFields(payload);
    const rooms = roomsFromPayload(payload);
    out.push({
      status: at,
      roomNumber: rooms[0] || "",
      price: fx.price,
      name: displayName(row.cname, payload),
      applicationDate: "",
      contractDate: "",
      cancelDate: "",
      cancellationDate: "",
      visitDate: String(row.d ?? ""),
      deposit: fx.deposit,
      withholding: fx.withholding || String(row.baitai ?? "").trim(),
      staff: fx.staff,
      notes: fx.notes,
    });
  }

  return out;
}

module.exports = {
  loadExpandedStatusChangeRows,
  loadWeekCustomersForReport,
  roomsFromPayload,
};
