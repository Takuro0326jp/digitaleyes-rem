/**
 * アカウント（users.json 相当）を LibSQL に永続化。
 * Vercel では data/ への書き込みができないため、handler / proxy はここ経由で保存する。
 */
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { getDb } = require("./sync-db");
const { ROLE } = require("./roles");

/** listUsers の SELECT 列順（Hrana 等で columnNames に空文字が混ざるときのフォールバック） */
const USER_LIST_SELECT_COLUMNS = [
  "id",
  "email",
  "password",
  "name",
  "active_property_id",
  "login_default_property_id",
  "role",
  "tenant_id",
  "client",
  "property_ids_json",
];

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
  try {
    await db.execute("ALTER TABLE users ADD COLUMN login_default_property_id TEXT");
  } catch (_) {}

  const cnt = await db.execute("SELECT COUNT(*) AS n FROM users");
  const row0 = cnt.rows?.[0];
  const n = Number(row0?.n ?? row0?.[0] ?? 0);
  if (n === 0) {
    const legacy = readJSONFile("users.json");
    if (legacy.length) {
      const stmts = legacy.map((u) => ({
        sql: `INSERT INTO users (id, email, password, name, active_property_id, login_default_property_id, role, tenant_id, client, property_ids_json)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          String(u.id),
          String(u.email || "").toLowerCase().trim(),
          String(u.password || ""),
          String(u.name || ""),
          u.activePropertyId || null,
          null,
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

let _bootstrapMasterDone = false;

/** REM_BOOTSTRAP_MASTER=1 のとき 1 回だけマスターアカウントを作成または role/パスワードを更新 */
async function maybeBootstrapMaster() {
  if (_bootstrapMasterDone) return;
  _bootstrapMasterDone = true;
  if (String(process.env.REM_BOOTSTRAP_MASTER || "").trim() !== "1") return;
  const email = String(process.env.REM_MASTER_EMAIL || "").toLowerCase().trim();
  const plain = String(process.env.REM_MASTER_PASSWORD || "");
  if (!email || !plain) {
    console.warn(
      "[user-store] REM_BOOTSTRAP_MASTER=1 ですが REM_MASTER_EMAIL または REM_MASTER_PASSWORD が未設定です"
    );
    return;
  }
  const hash = crypto.createHash("sha256").update(plain).digest("hex");
  const db = await getDb();
  const r = await db.execute({
    sql: "SELECT id, email, password, name, active_property_id, login_default_property_id, role, tenant_id, client, property_ids_json FROM users WHERE lower(trim(email)) = ?",
    args: [email],
  });
  const cols = normalizeUserListColumnNames(r.columns || []);
  const rows = r.rows || [];
  const name = String(process.env.REM_MASTER_NAME || "マスター").trim() || "マスター";
  const client = String(process.env.REM_MASTER_CLIENT || "株式会社ワールド・エステート").trim();
  const tenantId = String(process.env.REM_MASTER_TENANT_ID || "1").trim() || "1";

  if (rows.length) {
    const u = rowToUser(rows[0], cols);
    await updateUser({
      ...u,
      password: hash,
      role: ROLE.MASTER,
      name: name || u.name,
    });
    console.log("[user-store] マスターアカウントを更新しました:", email);
  } else {
    await insertUser({
      id: crypto.randomUUID(),
      email,
      password: hash,
      name,
      activePropertyId: null,
      loginDefaultPropertyId: null,
      role: ROLE.MASTER,
      tenantId,
      client,
      propertyIds: [],
    });
    console.log("[user-store] マスターアカウントを作成しました:", email);
  }
}

/** execute().columns が空・欠け・空文字のときも listUsers の SELECT 順で添字マッピングする */
function normalizeUserListColumnNames(rawColumns) {
  const raw = Array.isArray(rawColumns) ? rawColumns : [];
  const n = Math.max(raw.length, USER_LIST_SELECT_COLUMNS.length);
  const out = [];
  for (let i = 0; i < n; i++) {
    const c = raw[i];
    const s = c != null ? String(c).trim() : "";
    out.push(s !== "" ? s : USER_LIST_SELECT_COLUMNS[i] || "");
  }
  return out;
}

/**
 * LibSQL の Row は row.name と row[i] の両方が使えるが、環境によって名前アクセスが効かず
 * login_default_property_id だけ常に undefined になることがある。columns + 添字で確実に読む。
 */
function rowToObject(row, columnNames) {
  if (!row || typeof row !== "object") return {};
  if (!Array.isArray(columnNames) || columnNames.length === 0) return row;
  const o = {};
  const n = columnNames.length;
  for (let i = 0; i < n; i++) {
    const raw = columnNames[i];
    const fallback = USER_LIST_SELECT_COLUMNS[i];
    const key =
      raw != null && String(raw).trim() !== "" ? String(raw) : fallback || "";
    if (!key) continue;
    const byIndex = row[i];
    const byName = row[key];
    if (byIndex !== undefined) o[key] = byIndex;
    else if (byName !== undefined) o[key] = byName;
  }
  return o;
}

function rowToUser(row, columnNames) {
  const r = rowToObject(row, columnNames);
  const pid = r.property_ids_json;
  let propertyIds = [];
  try {
    propertyIds =
      typeof pid === "string" ? JSON.parse(pid || "[]") : Array.isArray(pid) ? pid : [];
  } catch (_) {
    propertyIds = [];
  }
  const ld = r.login_default_property_id ?? r.loginDefaultPropertyId;
  return {
    id: String(r.id),
    email: String(r.email),
    password: String(r.password),
    name: String(r.name ?? ""),
    activePropertyId: r.active_property_id ?? null,
    loginDefaultPropertyId:
      ld != null && String(ld).trim() !== "" ? String(ld) : null,
    role: Number(r.role ?? 2),
    tenantId: String(r.tenant_id ?? "1"),
    client: String(r.client ?? ""),
    propertyIds,
  };
}

async function listUsers() {
  await migrateUsers();
  await maybeBootstrapMaster();
  const db = await getDb();
  const r = await db.execute(
    "SELECT id, email, password, name, active_property_id, login_default_property_id, role, tenant_id, client, property_ids_json FROM users ORDER BY id"
  );
  const cols = normalizeUserListColumnNames(r.columns || []);
  const users = (r.rows || []).map((row) => rowToUser(row, cols));
  return users;
}

async function insertUser(u) {
  await migrateUsers();
  const db = await getDb();
  await db.execute({
    sql: `INSERT INTO users (id, email, password, name, active_property_id, login_default_property_id, role, tenant_id, client, property_ids_json)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    args: [
      String(u.id),
      String(u.email || "").toLowerCase().trim(),
      String(u.password || ""),
      String(u.name || ""),
      u.activePropertyId || null,
      u.loginDefaultPropertyId || null,
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
    sql: `UPDATE users SET email=?, password=?, name=?, active_property_id=?, login_default_property_id=?, role=?, tenant_id=?, client=?, property_ids_json=?
          WHERE id=?`,
    args: [
      String(u.email || "").toLowerCase().trim(),
      String(u.password || ""),
      String(u.name || ""),
      u.activePropertyId || null,
      u.loginDefaultPropertyId != null && u.loginDefaultPropertyId !== ""
        ? String(u.loginDefaultPropertyId)
        : null,
      Number(u.role ?? 2),
      String(u.tenantId || "1"),
      String(u.client || ""),
      JSON.stringify(Array.isArray(u.propertyIds) ? u.propertyIds : []),
      String(u.id),
    ],
  });
}

async function clearLoginDefaultPropertyId(propertyId) {
  await migrateUsers();
  const db = await getDb();
  await db.execute({
    sql: "UPDATE users SET login_default_property_id = NULL WHERE login_default_property_id = ?",
    args: [String(propertyId)],
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
  clearLoginDefaultPropertyId,
};
