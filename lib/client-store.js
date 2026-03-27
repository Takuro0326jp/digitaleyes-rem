const crypto = require("crypto");
const { getDb } = require("./sync-db");

let _migrated = false;

async function migrate() {
  if (_migrated) return;
  const db = await getDb();
  await db.execute(`CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    name_kana TEXT NOT NULL,
    email TEXT NOT NULL,
    site_type INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  )`);
  await db.execute("CREATE INDEX IF NOT EXISTS idx_clients_tenant ON clients(tenant_id, updated_at DESC)");

  await db.execute(`CREATE TABLE IF NOT EXISTS client_s3_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 0,
    aws_key TEXT,
    aws_secret_key TEXT,
    bucket_name TEXT,
    region TEXT,
    sync_hour INTEGER,
    sync_minute INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(tenant_id, client_id)
  )`);
  await db.execute("CREATE INDEX IF NOT EXISTS idx_s3_client ON client_s3_settings(tenant_id, client_id)");
  _migrated = true;
}

function validEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || ""));
}

function normalizeSiteType(v) {
  const n = Number(v);
  return n === 1 ? 1 : 0;
}

async function listClients(tenantId) {
  await migrate();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, name, name_kana, email, site_type, created_at, updated_at
          FROM clients WHERE tenant_id = ?
          ORDER BY updated_at DESC, id DESC`,
    args: [String(tenantId)],
  });
  return (r.rows || []).map((x) => ({
    id: Number(x.id),
    name: x.name,
    nameKana: x.name_kana,
    email: x.email,
    siteType: Number(x.site_type || 0),
    createdAt: x.created_at,
    updatedAt: x.updated_at,
  }));
}

async function getClient(tenantId, id) {
  await migrate();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, name, name_kana, email, site_type, created_at, updated_at
          FROM clients WHERE tenant_id = ? AND id = ? LIMIT 1`,
    args: [String(tenantId), Number(id)],
  });
  const x = r.rows?.[0];
  if (!x) return null;
  return {
    id: Number(x.id),
    name: x.name,
    nameKana: x.name_kana,
    email: x.email,
    siteType: Number(x.site_type || 0),
    createdAt: x.created_at,
    updatedAt: x.updated_at,
  };
}

/** 同一テナント内で企業名（前後空白除去後の完全一致）が既に使われているか */
async function clientNameTaken(tenantId, name, excludeClientId = null) {
  await migrate();
  const n = String(name || "").trim();
  if (!n) return false;
  const db = await getDb();
  const sql =
    excludeClientId == null
      ? `SELECT 1 FROM clients WHERE tenant_id = ? AND name = ? LIMIT 1`
      : `SELECT 1 FROM clients WHERE tenant_id = ? AND name = ? AND id != ? LIMIT 1`;
  const args =
    excludeClientId == null
      ? [String(tenantId), n]
      : [String(tenantId), n, Number(excludeClientId)];
  const r = await db.execute({ sql, args });
  return (r.rows || []).length > 0;
}

async function createClient(tenantId, payload) {
  await migrate();
  const name = String(payload.name || "").trim();
  const nameKana = String(payload.nameKana || "").trim();
  const email = String(payload.email || "").trim();
  const siteType = normalizeSiteType(payload.siteType);
  if (!name) throw new Error("企業名は必須です");
  if (!nameKana) throw new Error("フリガナは必須です");
  if (!email || !validEmail(email)) throw new Error("E-mail が不正です");
  if (await clientNameTaken(tenantId, name)) {
    throw new Error("同じ企業名のクライアントが既に登録されています");
  }
  const db = await getDb();
  const ins = await db.execute({
    sql: `INSERT INTO clients(tenant_id, name, name_kana, email, site_type, created_at, updated_at)
          VALUES(?, ?, ?, ?, ?, datetime('now'), datetime('now'))
          RETURNING id`,
    args: [String(tenantId), name, nameKana, email, siteType],
  });
  const row0 = ins.rows?.[0];
  const id = Number(
    row0?.id ?? row0?.[0] ?? 0
  );
  if (!id) throw new Error("登録後の ID を取得できませんでした");
  return getClient(tenantId, id);
}

async function updateClient(tenantId, id, payload) {
  await migrate();
  const name = String(payload.name || "").trim();
  const nameKana = String(payload.nameKana || "").trim();
  const email = String(payload.email || "").trim();
  const current = await getClient(tenantId, id);
  if (!current) throw new Error("クライアントが見つかりません");
  const siteType = payload.siteType == null ? Number(current.siteType || 0) : normalizeSiteType(payload.siteType);
  if (!name) throw new Error("企業名は必須です");
  if (!nameKana) throw new Error("フリガナは必須です");
  if (!email || !validEmail(email)) throw new Error("E-mail が不正です");
  if (await clientNameTaken(tenantId, name, id)) {
    throw new Error("同じ企業名のクライアントが既に登録されています");
  }
  const db = await getDb();
  await db.execute({
    sql: `UPDATE clients SET name=?, name_kana=?, email=?, site_type=?, updated_at=datetime('now')
          WHERE tenant_id=? AND id=?`,
    args: [name, nameKana, email, siteType, String(tenantId), Number(id)],
  });
  return getClient(tenantId, id);
}

async function deleteClients(tenantId, ids) {
  await migrate();
  const clean = [...new Set((ids || []).map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0))];
  if (!clean.length) return { deleted: 0 };
  const db = await getDb();
  const ph = clean.map(() => "?").join(",");
  await db.execute({
    sql: `DELETE FROM client_s3_settings WHERE tenant_id = ? AND client_id IN (${ph})`,
    args: [String(tenantId), ...clean],
  });
  const r = await db.execute({
    sql: `DELETE FROM clients WHERE tenant_id = ? AND id IN (${ph})`,
    args: [String(tenantId), ...clean],
  });
  return { deleted: Number(r.rowsAffected || 0) };
}

async function getClientS3Settings(tenantId, clientId) {
  await migrate();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, is_enabled, aws_key, aws_secret_key, bucket_name, region, sync_hour, sync_minute, created_at, updated_at
          FROM client_s3_settings WHERE tenant_id = ? AND client_id = ? LIMIT 1`,
    args: [String(tenantId), Number(clientId)],
  });
  const x = r.rows?.[0];
  if (!x) {
    return {
      isEnabled: 0,
      awsKey: "",
      awsSecretKey: "",
      bucketName: "",
      region: "",
      syncHour: 0,
      syncMinute: 0,
    };
  }
  return {
    id: Number(x.id),
    isEnabled: Number(x.is_enabled || 0),
    awsKey: x.aws_key || "",
    awsSecretKey: x.aws_secret_key || "",
    bucketName: x.bucket_name || "",
    region: x.region || "",
    syncHour: x.sync_hour == null ? 0 : Number(x.sync_hour),
    syncMinute: x.sync_minute == null ? 0 : Number(x.sync_minute),
    createdAt: x.created_at,
    updatedAt: x.updated_at,
  };
}

async function saveClientS3Settings(tenantId, clientId, payload) {
  await migrate();
  const isEnabled = Number(payload.isEnabled) === 1 ? 1 : 0;
  const awsKey = String(payload.awsKey || "").trim();
  const awsSecretKey = String(payload.awsSecretKey || "").trim();
  const bucketName = String(payload.bucketName || "").trim();
  const region = String(payload.region || "").trim();
  const syncHour = Number(payload.syncHour ?? 0);
  const syncMinute = Number(payload.syncMinute ?? 0);
  const db = await getDb();
  await db.execute({
    sql: `INSERT INTO client_s3_settings
          (tenant_id, client_id, is_enabled, aws_key, aws_secret_key, bucket_name, region, sync_hour, sync_minute, created_at, updated_at)
          VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
          ON CONFLICT(tenant_id, client_id) DO UPDATE SET
            is_enabled=excluded.is_enabled,
            aws_key=excluded.aws_key,
            aws_secret_key=excluded.aws_secret_key,
            bucket_name=excluded.bucket_name,
            region=excluded.region,
            sync_hour=excluded.sync_hour,
            sync_minute=excluded.sync_minute,
            updated_at=datetime('now')`,
    args: [
      String(tenantId),
      Number(clientId),
      isEnabled,
      awsKey,
      awsSecretKey,
      bucketName,
      region,
      Number.isFinite(syncHour) ? syncHour : 0,
      Number.isFinite(syncMinute) ? syncMinute : 0,
    ],
  });
  return getClientS3Settings(tenantId, clientId);
}

module.exports = {
  listClients,
  getClient,
  createClient,
  updateClient,
  deleteClients,
  getClientS3Settings,
  saveClientS3Settings,
};

