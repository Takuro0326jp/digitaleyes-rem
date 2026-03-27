const https = require("https");

const API_HOST = "api.digital-eyes.jp";
const ACQUISITION_PATH = "/api/v1/database/acquisition.json";

function extractMissingColumns(message) {
  const s = String(message || "");
  if (!s) return [];
  // 例: 指定されたカラム名が存在しません（c.q1, c.questionnaire35, c.system_point）
  const m = s.match(/存在しません[（(]\s*([^()）]+)\s*[)）]/);
  if (m && m[1]) {
    return m[1]
      .split(",")
      .map((x) => String(x || "").trim())
      .filter((x) => x.startsWith("c."));
  }
  return [];
}

function removeMissingColumnsFromOrderAndConditions(params, missingCols) {
  if (!params || !Array.isArray(missingCols) || !missingCols.length) return false;
  const missing = new Set(missingCols.map((x) => String(x)));
  let changed = false;

  const parseMaybeJson = (v) => {
    if (Array.isArray(v) || (v && typeof v === "object")) return v;
    if (typeof v !== "string") return null;
    try { return JSON.parse(v); } catch (_) { return null; }
  };

  const orderRaw = parseMaybeJson(params.order);
  if (Array.isArray(orderRaw)) {
    const next = orderRaw.filter((entry) => {
      const s = String(entry || "").trim();
      if (!s) return false;
      // "c.upd_date desc" -> "c.upd_date"
      const col = s.split(/\s+/)[0];
      return !missing.has(col);
    });
    if (next.length !== orderRaw.length) {
      params.order = JSON.stringify(next);
      changed = true;
    }
  }

  const condRaw = parseMaybeJson(params.conditions);
  if (Array.isArray(condRaw)) {
    const next = condRaw.filter((obj) => {
      if (!obj || typeof obj !== "object") return false;
      const ks = Object.keys(obj);
      if (!ks.length) return false;
      // { "c.upd_date >": "..." } -> "c.upd_date"
      const lhs = String(ks[0]).trim().split(/\s+/)[0];
      return !missing.has(lhs);
    });
    if (next.length !== condRaw.length) {
      params.conditions = JSON.stringify(next);
      changed = true;
    }
  }

  return changed;
}

function requestAcquisitionRaw(payload) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: API_HOST,
        path: ACQUISITION_PATH,
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
          "Content-Length": Buffer.byteLength(payload),
        },
      },
      (pres) => {
        let raw = "";
        pres.on("data", (c) => (raw += c));
        pres.on("end", () => {
          try {
            resolve(JSON.parse(raw));
          } catch (_) {
            reject(new Error("デジタライズAPIの応答がJSONではありません"));
          }
        });
      }
    );
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

/**
 * デジタライズ acquisition.json を POST
 * @param {object} prop 物件 { databaseId, databasePassword, tableName }
 * @param {object} secrets { SECRET_ID, SECRET_PASSWORD }
 * @param {object} paramsObj limit, page, order, fields, conditions など（table_name は内部で付与）
 */
async function postAcquisition(prop, secrets, paramsObj) {
  const params = { ...(paramsObj || {}) };
  // 仕様差分のあるテーブルでも失敗しにくいよう fields 固定指定は常に送らない。
  delete params.fields;
  for (let attempt = 0; attempt < 3; attempt++) {
    const body = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      body.append(k, typeof v === "object" ? JSON.stringify(v) : String(v));
    });
    body.set("X_secret_id", secrets.SECRET_ID || "");
    body.set("X_secret_password", secrets.SECRET_PASSWORD || "");
    body.set("X_database_id", prop.databaseId);
    body.set("X_database_password", prop.databasePassword || "");
    body.set("table_name", prop.tableName);

    const resp = await requestAcquisitionRaw(body.toString());
    if (resp?.status) return resp;

    const missing = extractMissingColumns(resp?.message);
    if (!missing.length) return resp;
    if (!removeMissingColumnsFromOrderAndConditions(params, missing)) return resp;
  }
  return { status: false, message: "APIエラー" };
}

module.exports = { postAcquisition, API_HOST, ACQUISITION_PATH };
