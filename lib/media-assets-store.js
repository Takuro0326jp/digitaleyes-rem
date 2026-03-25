const { getDb } = require("./sync-db");

let _migrated = false;

const ASSET_TYPES = new Set([
  "property_photo",
  "floor_plan",
  "banner",
  "logo_brand",
  "other",
]);
const STATUSES = new Set(["active", "archived"]);

function parseJsonArray(raw) {
  try {
    const v = JSON.parse(raw || "[]");
    return Array.isArray(v) ? v : [];
  } catch (_) {
    return [];
  }
}

function normalizeAssetType(v) {
  const s = String(v || "").trim();
  if (!s) return "other";
  return s.slice(0, 50);
}

function normalizeStatus(v) {
  const s = String(v || "").trim();
  return STATUSES.has(s) ? s : "active";
}

function toInt(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.floor(n) : fallback;
}

function asAssetRow(x) {
  return {
    id: x.id,
    propertyId: x.property_id || null,
    name: x.name || "",
    filePath: x.file_path || "",
    fileKey: x.file_key || "",
    fileUrl: x.file_url || "",
    thumbnailUrl: x.thumbnail_url || "",
    mimeType: x.mime_type || "",
    fileSize: Number(x.file_size || 0),
    assetType: x.asset_type || "other",
    status: x.status || "active",
    tags: parseJsonArray(x.tags_json),
    memo: x.memo || "",
    uploadedBy: x.uploaded_by || "",
    createdAt: x.created_at || "",
    updatedAt: x.updated_at || "",
  };
}

async function migrate() {
  if (_migrated) return;
  const db = await getDb();
  await db.execute(`CREATE TABLE IF NOT EXISTS media_assets (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    property_id TEXT,
    name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_key TEXT DEFAULT '',
    file_url TEXT DEFAULT '',
    thumbnail_url TEXT,
    mime_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    tags_json TEXT DEFAULT '[]',
    memo TEXT,
    uploaded_by TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  )`);
  await db.execute("CREATE INDEX IF NOT EXISTS idx_media_assets_tenant ON media_assets(tenant_id, created_at DESC)");
  await db.execute("CREATE INDEX IF NOT EXISTS idx_media_assets_property ON media_assets(property_id)");
  await db.execute("CREATE INDEX IF NOT EXISTS idx_media_assets_type ON media_assets(asset_type)");
  await db.execute("CREATE INDEX IF NOT EXISTS idx_media_assets_status ON media_assets(status)");
  await db.execute(`CREATE TABLE IF NOT EXISTS media_asset_types (
    tenant_id TEXT NOT NULL,
    type_key TEXT NOT NULL,
    label TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (tenant_id, type_key)
  )`);
  await db.execute("CREATE INDEX IF NOT EXISTS idx_media_asset_types_tenant ON media_asset_types(tenant_id, sort_order, type_key)");
  try { await db.execute("ALTER TABLE media_assets ADD COLUMN file_key TEXT DEFAULT ''"); } catch (_) {}
  try { await db.execute("ALTER TABLE media_assets ADD COLUMN file_url TEXT DEFAULT ''"); } catch (_) {}
  await db.execute("UPDATE media_assets SET file_key = file_path WHERE (file_key IS NULL OR file_key = '') AND file_path LIKE 'assets/%'");
  _migrated = true;
}

async function ensureDefaultAssetTypes(tenantId) {
  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT COUNT(*) AS n FROM media_asset_types WHERE tenant_id = ?",
    args: [String(tenantId)],
  });
  const n = Number(r.rows?.[0]?.n || 0);
  if (n > 0) return;
  const defs = [
    ["property_photo", "物件写真", 10],
    ["floor_plan", "間取り図・図面", 20],
    ["banner", "広告・バナー", 30],
    ["logo_brand", "ロゴ・ブランド素材", 40],
    ["other", "その他", 50],
  ];
  await db.batch(defs.map(([typeKey, label, sortOrder]) => ({
    sql: `INSERT INTO media_asset_types(tenant_id, type_key, label, sort_order, created_at, updated_at)
          VALUES(?, ?, ?, ?, datetime('now'), datetime('now'))`,
    args: [String(tenantId), String(typeKey), String(label), Number(sortOrder)],
  })));
}

async function listMediaAssetTypes(tenantId) {
  await migrate();
  await ensureDefaultAssetTypes(tenantId);
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT type_key, label, sort_order
          FROM media_asset_types
          WHERE tenant_id = ?
          ORDER BY sort_order ASC, type_key ASC`,
    args: [String(tenantId)],
  });
  return (r.rows || []).map((x) => ({
    typeKey: String(x.type_key || ""),
    label: String(x.label || ""),
    sortOrder: Number(x.sort_order || 0),
  }));
}

async function addMediaAssetType(tenantId, payload = {}) {
  await migrate();
  const db = await getDb();
  const typeKeyRaw = String(payload.typeKey || "").trim().toLowerCase();
  const label = String(payload.label || "").trim();
  if (!typeKeyRaw) throw new Error("種別キーは必須です");
  if (!/^[a-z0-9_]+$/.test(typeKeyRaw)) throw new Error("種別キーは英小文字・数字・アンダースコアのみ使用できます");
  if (!label) throw new Error("表示名は必須です");
  const sortOrder = Number(payload.sortOrder || 999);
  await db.execute({
    sql: `INSERT INTO media_asset_types(tenant_id, type_key, label, sort_order, created_at, updated_at)
          VALUES(?, ?, ?, ?, datetime('now'), datetime('now'))`,
    args: [String(tenantId), typeKeyRaw, label, Number.isFinite(sortOrder) ? sortOrder : 999],
  });
  return { typeKey: typeKeyRaw, label, sortOrder: Number.isFinite(sortOrder) ? sortOrder : 999 };
}

async function deleteMediaAssetType(tenantId, typeKey) {
  await migrate();
  const tk = String(typeKey || "").trim();
  if (!tk) throw new Error("種別キーが不正です");
  const db = await getDb();
  const use = await db.execute({
    sql: `SELECT COUNT(*) AS n FROM media_assets WHERE tenant_id = ? AND asset_type = ?`,
    args: [String(tenantId), tk],
  });
  if (Number(use.rows?.[0]?.n || 0) > 0) throw new Error("この種別は素材で使用中のため削除できません");
  await db.execute({
    sql: `DELETE FROM media_asset_types WHERE tenant_id = ? AND type_key = ?`,
    args: [String(tenantId), tk],
  });
}

async function listMediaAssets(tenantId, query = {}) {
  await migrate();
  const db = await getDb();
  const page = Math.max(1, toInt(query.page, 1));
  const limit = Math.max(1, Math.min(200, toInt(query.limit, 40)));
  const offset = (page - 1) * limit;

  const where = ["tenant_id = ?"];
  const args = [String(tenantId)];

  if (query.propertyId === "common") where.push("property_id IS NULL");
  else if (query.propertyId) {
    where.push("property_id = ?");
    args.push(String(query.propertyId));
  }
  if (query.assetType && ASSET_TYPES.has(query.assetType)) {
    where.push("asset_type = ?");
    args.push(String(query.assetType));
  }
  if (query.status && STATUSES.has(query.status)) {
    where.push("status = ?");
    args.push(String(query.status));
  }
  if (query.search) {
    where.push("(name LIKE ? OR tags_json LIKE ?)");
    const q = `%${String(query.search)}%`;
    args.push(q, q);
  }

  const whereSql = `WHERE ${where.join(" AND ")}`;
  const countRes = await db.execute({
    sql: `SELECT COUNT(*) AS n FROM media_assets ${whereSql}`,
    args,
  });
  const total = Number(countRes.rows?.[0]?.n || 0);

  const rows = await db.execute({
    sql: `SELECT id, property_id, name, file_path, file_key, file_url, thumbnail_url, mime_type, file_size,
                 asset_type, status, tags_json, memo, uploaded_by, created_at, updated_at
          FROM media_assets
          ${whereSql}
          ORDER BY created_at DESC
          LIMIT ? OFFSET ?`,
    args: [...args, limit, offset],
  });

  return {
    data: (rows.rows || []).map(asAssetRow),
    total,
    page,
    limit,
  };
}

async function getMediaAssetById(tenantId, id) {
  await migrate();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, property_id, name, file_path, file_key, file_url, thumbnail_url, mime_type, file_size,
                 asset_type, status, tags_json, memo, uploaded_by, created_at, updated_at
          FROM media_assets
          WHERE tenant_id = ? AND id = ?
          LIMIT 1`,
    args: [String(tenantId), String(id)],
  });
  const x = r.rows?.[0];
  return x ? asAssetRow(x) : null;
}

async function createMediaAssetRecord(tenantId, userId, payload = {}) {
  await migrate();
  const db = await getDb();
  const propertyId = payload.propertyId ? String(payload.propertyId) : null;
  const assetType = normalizeAssetType(payload.assetType);
  const tags = Array.isArray(payload.tags)
    ? payload.tags.map((x) => String(x || "").trim()).filter(Boolean)
    : String(payload.tags || "").split(",").map((x) => x.trim()).filter(Boolean);
  const memo = String(payload.memo || "");

  const id = String(payload.id || "");
  const name = String(payload.name || "file");
  const mimeType = String(payload.mimeType || "");
  const fileSize = Math.max(0, Number(payload.fileSize || 0));
  const fileKey = String(payload.fileKey || "");
  const filePath = fileKey || String(payload.filePath || "");
  const thumbnailUrl = String(payload.thumbnailUrl || "");
  if (!id) throw new Error("id は必須です");
  if (!mimeType) throw new Error("mimeType は必須です");
  if (!filePath) throw new Error("fileKey は必須です");
  await db.execute({
    sql: `INSERT INTO media_assets
          (id, tenant_id, property_id, name, file_path, file_key, file_url, thumbnail_url, mime_type, file_size,
           asset_type, status, tags_json, memo, uploaded_by, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, 'active', ?, ?, ?, datetime('now'), datetime('now'))`,
    args: [
      id,
      String(tenantId),
      propertyId,
      name,
      filePath,
      fileKey,
      thumbnailUrl,
      mimeType,
      fileSize,
      assetType,
      JSON.stringify(tags),
      memo,
      String(userId || ""),
    ],
  });
  return getMediaAssetById(tenantId, id);
}

async function patchMediaAsset(tenantId, id, patch = {}) {
  await migrate();
  const curr = await getMediaAssetById(tenantId, id);
  if (!curr) return null;
  const db = await getDb();
  const nextName = patch.name == null ? curr.name : String(patch.name || "").trim();
  const nextStatus = patch.status == null ? curr.status : normalizeStatus(patch.status);
  const nextMemo = patch.memo == null ? curr.memo : String(patch.memo || "");
  const nextTags = patch.tags == null
    ? curr.tags
    : (Array.isArray(patch.tags)
      ? patch.tags.map((x) => String(x || "").trim()).filter(Boolean)
      : String(patch.tags || "").split(",").map((x) => x.trim()).filter(Boolean));

  await db.execute({
    sql: `UPDATE media_assets
          SET name = ?, status = ?, tags_json = ?, memo = ?, updated_at = datetime('now')
          WHERE tenant_id = ? AND id = ?`,
    args: [nextName || curr.name, nextStatus, JSON.stringify(nextTags), nextMemo, String(tenantId), String(id)],
  });
  return getMediaAssetById(tenantId, id);
}

async function deleteMediaAsset(tenantId, id) {
  await migrate();
  const db = await getDb();
  await db.execute({
    sql: `DELETE FROM media_assets WHERE tenant_id = ? AND id = ?`,
    args: [String(tenantId), String(id)],
  });
}

module.exports = {
  listMediaAssets,
  getMediaAssetById,
  createMediaAssetRecord,
  patchMediaAsset,
  deleteMediaAsset,
  listMediaAssetTypes,
  addMediaAssetType,
  deleteMediaAssetType,
  ASSET_TYPES: [...ASSET_TYPES],
  STATUSES: [...STATUSES],
};

