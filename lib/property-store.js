const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { getDb } = require("./sync-db");

let _migrated = false;

function dataPath(file) {
  return path.join(__dirname, "..", "data", file);
}

function readJSON(file) {
  try {
    return JSON.parse(fs.readFileSync(dataPath(file), "utf-8"));
  } catch {
    return [];
  }
}

function writeJSON(file, data) {
  fs.writeFileSync(dataPath(file), JSON.stringify(data, null, 2), "utf-8");
}

async function migratePropertiesTable() {
  if (_migrated) return;
  const db = await getDb();
  await db.execute(`CREATE TABLE IF NOT EXISTS property_master (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    database_id TEXT NOT NULL,
    database_password TEXT DEFAULT '',
    table_name TEXT NOT NULL,
    extra_json TEXT DEFAULT '{}',
    created_at TEXT,
    updated_at TEXT
  )`);
  await db.execute(`CREATE TABLE IF NOT EXISTS property_update_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    actor_name TEXT,
    change_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  )`);
  await db.execute(`CREATE TABLE IF NOT EXISTS property_images (
    id TEXT PRIMARY KEY,
    property_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    image_data TEXT NOT NULL,
    created_at TEXT
  )`);
  await db.execute("CREATE INDEX IF NOT EXISTS idx_property_images_prop ON property_images(property_id, created_at DESC)");
  const cnt = await db.execute("SELECT COUNT(*) AS n FROM property_master");
  const n = Number(cnt.rows?.[0]?.n ?? 0);
  if (n === 0) {
    const legacy = readJSON("properties.json");
    if (legacy.length) {
      await db.batch(
        legacy.map((p) => ({
          sql: `INSERT INTO property_master
                (id, user_id, name, database_id, database_password, table_name, extra_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, '{}', datetime('now'), datetime('now'))`,
          args: [
            p.id || crypto.randomUUID(),
            p.userId,
            p.name || "",
            p.databaseId || "",
            p.databasePassword || "",
            p.tableName || "",
          ],
        }))
      );
    }
  }
  try {
    await db.execute("ALTER TABLE property_master ADD COLUMN extra_json TEXT DEFAULT '{}'");
  } catch (_) {}
  _migrated = true;
}

function parseExtra(raw) {
  try {
    const x = JSON.parse(raw || "{}");
    return x && typeof x === "object" ? x : {};
  } catch {
    return {};
  }
}

async function listPropertiesByUser(userId) {
  await migratePropertiesTable();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT
            pm.id,
            pm.user_id,
            pm.name,
            pm.database_id,
            pm.database_password,
            pm.table_name,
            pm.extra_json,
            pm.created_at,
            pm.updated_at,
            (
              SELECT pi.image_data
              FROM property_images pi
              WHERE pi.property_id = pm.id AND pi.user_id = pm.user_id
              ORDER BY pi.created_at DESC
              LIMIT 1
            ) AS thumbnail_url
          FROM property_master pm
          WHERE pm.user_id = ?
          ORDER BY pm.updated_at DESC`,
    args: [userId],
  });
  return (r.rows || []).map((x) => {
    const extra = parseExtra(x.extra_json);
    return {
      id: x.id,
      userId: x.user_id,
      name: x.name,
      databaseId: x.database_id,
      databasePassword: x.database_password || "",
      tableName: x.table_name,
      createdAt: x.created_at || "",
      updatedAt: x.updated_at || "",
      thumbnailUrl: x.thumbnail_url || "",
      ...extra,
    };
  });
}

async function getPropertyByIdForUser(id, userId) {
  await migratePropertiesTable();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT
            pm.id,
            pm.user_id,
            pm.name,
            pm.database_id,
            pm.database_password,
            pm.table_name,
            pm.extra_json,
            pm.created_at,
            pm.updated_at,
            (
              SELECT pi.image_data
              FROM property_images pi
              WHERE pi.property_id = pm.id AND pi.user_id = pm.user_id
              ORDER BY pi.created_at DESC
              LIMIT 1
            ) AS thumbnail_url
          FROM property_master pm
          WHERE pm.id = ? AND pm.user_id = ?
          LIMIT 1`,
    args: [id, userId],
  });
  const x = r.rows?.[0];
  if (!x) return null;
  const extra = parseExtra(x.extra_json);
  return {
    id: x.id,
    userId: x.user_id,
    name: x.name,
    databaseId: x.database_id,
    databasePassword: x.database_password || "",
    tableName: x.table_name,
    createdAt: x.created_at || "",
    updatedAt: x.updated_at || "",
    thumbnailUrl: x.thumbnail_url || "",
    ...extra,
  };
}

async function createProperty(userId, payload) {
  await migratePropertiesTable();
  const db = await getDb();
  const id = crypto.randomUUID();
  await db.execute({
    sql: `INSERT INTO property_master
          (id, user_id, name, database_id, database_password, table_name, extra_json, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, '{}', datetime('now'), datetime('now'))`,
    args: [id, userId, payload.name, payload.databaseId, payload.databasePassword || "", payload.tableName],
  });
  return getPropertyByIdForUser(id, userId);
}

async function updateProperty(id, userId, payload) {
  await migratePropertiesTable();
  const db = await getDb();
  await db.execute({
    sql: `UPDATE property_master
          SET name = ?, database_id = ?, database_password = ?, table_name = ?, updated_at = datetime('now')
          WHERE id = ? AND user_id = ?`,
    args: [payload.name, payload.databaseId, payload.databasePassword || "", payload.tableName, id, userId],
  });
  await db.execute({
    sql: `INSERT INTO property_update_history(property_id, user_id, actor_name, change_json, created_at)
          VALUES (?, ?, ?, ?, datetime('now'))`,
    args: [id, userId, payload.actorName || "", JSON.stringify({ type: "property_core_update" })],
  });
  return getPropertyByIdForUser(id, userId);
}

async function updatePropertyMeta(id, userId, patch, actorName = "") {
  await migratePropertiesTable();
  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT extra_json FROM property_master WHERE id = ? AND user_id = ? LIMIT 1",
    args: [id, userId],
  });
  const row = r.rows?.[0];
  if (!row) return null;
  const curr = parseExtra(row.extra_json);
  const next = { ...curr, ...(patch || {}) };
  await db.execute({
    sql: `UPDATE property_master SET extra_json = ?, updated_at = datetime('now')
          WHERE id = ? AND user_id = ?`,
    args: [JSON.stringify(next), id, userId],
  });
  await db.execute({
    sql: `INSERT INTO property_update_history(property_id, user_id, actor_name, change_json, created_at)
          VALUES (?, ?, ?, ?, datetime('now'))`,
    args: [id, userId, actorName || "", JSON.stringify({ type: "property_meta_update", patch: patch || {} })],
  });
  return getPropertyByIdForUser(id, userId);
}

async function listPropertyUpdateHistory(id, userId, limit = 50) {
  await migratePropertiesTable();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, actor_name, change_json, created_at
          FROM property_update_history
          WHERE property_id = ? AND user_id = ?
          ORDER BY id DESC
          LIMIT ?`,
    args: [id, userId, Math.max(1, Math.min(300, Number(limit) || 50))],
  });
  return (r.rows || []).map((x) => ({
    id: Number(x.id),
    actorName: x.actor_name || "",
    changeJson: x.change_json || "{}",
    createdAt: x.created_at || "",
  }));
}

async function deleteProperty(id, userId) {
  await migratePropertiesTable();
  const db = await getDb();
  await db.execute({
    sql: "DELETE FROM property_images WHERE property_id = ? AND user_id = ?",
    args: [id, userId],
  });
  await db.execute({
    sql: "DELETE FROM property_master WHERE id = ? AND user_id = ?",
    args: [id, userId],
  });
}

async function listPropertyImages(propertyId, userId) {
  await migratePropertiesTable();
  const db = await getDb();
  const r = await db.execute({
    sql: `SELECT id, filename, mime_type, image_data, created_at
          FROM property_images
          WHERE property_id = ? AND user_id = ?
          ORDER BY created_at DESC`,
    args: [propertyId, userId],
  });
  return (r.rows || []).map((x) => ({
    id: x.id,
    filename: x.filename,
    mimeType: x.mime_type,
    imageData: x.image_data,
    createdAt: x.created_at,
  }));
}

async function addPropertyImage(propertyId, userId, payload) {
  await migratePropertiesTable();
  const filename = String(payload.filename || "image");
  const mimeType = String(payload.mimeType || "");
  const imageData = String(payload.imageData || "");
  if (!mimeType.startsWith("image/")) throw new Error("画像形式が不正です");
  if (!imageData.startsWith("data:image/")) throw new Error("画像データ形式が不正です");
  if (imageData.length > 3 * 1024 * 1024) throw new Error("画像サイズが大きすぎます（3MB以下）");
  const db = await getDb();
  const id = crypto.randomUUID();
  await db.execute({
    sql: `INSERT INTO property_images
          (id, property_id, user_id, filename, mime_type, image_data, created_at)
          VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`,
    args: [id, propertyId, userId, filename, mimeType, imageData],
  });
  return { id };
}

async function deletePropertyImage(propertyId, userId, imageId) {
  await migratePropertiesTable();
  const db = await getDb();
  await db.execute({
    sql: "DELETE FROM property_images WHERE id = ? AND property_id = ? AND user_id = ?",
    args: [imageId, propertyId, userId],
  });
}

async function setActiveProperty(userId, propertyId) {
  const userStore = require("./user-store");
  const users = await userStore.listUsers();
  const u = users.find((x) => x.id === userId);
  if (!u) return;
  u.activePropertyId = propertyId || null;
  await userStore.updateUser(u);
}

module.exports = {
  listPropertiesByUser,
  getPropertyByIdForUser,
  createProperty,
  updateProperty,
  deleteProperty,
  updatePropertyMeta,
  listPropertyUpdateHistory,
  listPropertyImages,
  addPropertyImage,
  deletePropertyImage,
  setActiveProperty,
};
