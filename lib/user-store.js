/**
 * アカウント（users.json 相当）を LibSQL に永続化。
 * Vercel では data/ への書き込みができないため、handler / proxy はここ経由で保存する。
 */
const fs = require("fs");
const path = require("path");
const { getDb } = require("./sync-db");

function dataPath(file) {
  return path.join(__dirname, "..", "data", file);
}

function readJSONFile(file) {
  try {
    return JSON.parse(fs.readFileSync(dataPath(file), "utf-8"));
  } catch {
    return [];
  }
}

let _migrated = false;

async function migrateUsers() {
  if (_migrated) return;
  const db = await getDb();
  await db.execute(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    active_property_id TEXT,
    role INTEGER NOT NULL DEFAULT 2,
    tenant_id TEXT NOT NULL DEFAULT '1',
    client TEXT DEFAULT '',
    property_ids_json TEXT DEFAULT '[]'
  )`);
  await db.execute("CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)");

  const cnt = await db.execute("SELECT COUNT(*) AS n FROM users");
  const row0 = cnt.rows?.[0];
  const n = Number(row0?.n ?? row0?.[0] ?? 0);
  if (n === 0) {
    const legacy = readJSONFile("users.json");
    if (legacy.length) {
      const stmts = legacy.map((u) => ({
        sql: `INSERT INTO users (id, email, password, name, active_property_id, role, tenant_id, client, property_ids_json)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          String(u.id),
          String(u.email || "").toLowerCase().trim(),
          String(u.password || ""),
          String(u.name || ""),
          u.activePropertyId || null,
          Number(u.role ?? 2),
          String(u.tenantId || "1"),
          String(u.client || ""),
          JSON.stringify(Array.isArray(u.propertyIds) ? u.propertyIds : []),
        ],
      }));
      await db.batch(stmts);
    }
  }
  _migrated = true;
}

function rowToUser(row) {
  const r = row;
  const pid = r.property_ids_json ?? r[8];
  let propertyIds = [];
  try {
    propertyIds =
      typeof pid === "string" ? JSON.parse(pid || "[]") : Array.isArray(pid) ? pid : [];
  } catch (_) {
    propertyIds = [];
  }
  return {
    id: String(r.id ?? r[0]),
    email: String(r.email ?? r[1]),
    password: String(r.password ?? r[2]),
    name: String(r.name ?? r[3] ?? ""),
    activePropertyId: r.active_property_id ?? r[4] ?? null,
    role: Number(r.role ?? r[5] ?? 2),
    tenantId: String(r.tenant_id ?? r[6] ?? "1"),
    client: String(r.client ?? r[7] ?? ""),
    propertyIds,
  };
}

async function listUsers() {
  await migrateUsers();
  const db = await getDb();
  const r = await db.execute(
    "SELECT id, email, password, name, active_property_id, role, tenant_id, client, property_ids_json FROM users ORDER BY id"
  );
  return (r.rows || []).map(rowToUser);
}

async function insertUser(u) {
  await migrateUsers();
  const db = await getDb();
  await db.execute({
    sql: `INSERT INTO users (id, email, password, name, active_property_id, role, tenant_id, client, property_ids_json)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    args: [
      String(u.id),
      String(u.email || "").toLowerCase().trim(),
      String(u.password || ""),
      String(u.name || ""),
      u.activePropertyId || null,
      Number(u.role ?? 2),
      String(u.tenantId || "1"),
      String(u.client || ""),
      JSON.stringify(Array.isArray(u.propertyIds) ? u.propertyIds : []),
    ],
  });
}

async function updateUser(u) {
  await migrateUsers();
  const db = await getDb();
  await db.execute({
    sql: `UPDATE users SET email=?, password=?, name=?, active_property_id=?, role=?, tenant_id=?, client=?, property_ids_json=?
          WHERE id=?`,
    args: [
      String(u.email || "").toLowerCase().trim(),
      String(u.password || ""),
      String(u.name || ""),
      u.activePropertyId || null,
      Number(u.role ?? 2),
      String(u.tenantId || "1"),
      String(u.client || ""),
      JSON.stringify(Array.isArray(u.propertyIds) ? u.propertyIds : []),
      String(u.id),
    ],
  });
}

async function deleteUserById(id) {
  await migrateUsers();
  const db = await getDb();
  await db.execute({ sql: "DELETE FROM users WHERE id = ?", args: [String(id)] });
}

module.exports = {
  listUsers,
  insertUser,
  updateUser,
  deleteUserById,
};
