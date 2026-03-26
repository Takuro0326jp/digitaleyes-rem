const path = require("path");
const { createClient } = require("@libsql/client");

let _client = null;

/** リポジトリの data/（proxy-server の readJSON と同じ基準。cwd 違いで 500 にならないよう __dirname 基準） */
function defaultDataDir() {
  return path.join(__dirname, "..", "data");
}

function resolveDatabaseUrl() {
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;
  if (process.env.LIBSQL_URL) return process.env.LIBSQL_URL;
  if (process.env.TURSO_DATABASE_URL) return process.env.TURSO_DATABASE_URL;
  // Vercel でリモート DB 未設定時: デプロイに同梱された data/properties.json から物件を読み込めるよう
  // メモリ上の LibSQL にフォールバック（ログイン可能。顧客キャッシュ等はインスタンス単位で非永続）。
  if (process.env.VERCEL) {
    return "file::memory:?cache=shared";
  }
  const dir = process.env.REM_DATA_DIR || defaultDataDir();
  const fp = path.join(dir, "sync.db").replace(/\\/g, "/");
  return `file:${fp}`;
}

async function getDb() {
  if (_client) return _client;
  const url = resolveDatabaseUrl();
  const authToken =
    process.env.TURSO_AUTH_TOKEN ||
    process.env.TURSO_API_TOKEN ||
    process.env.DATABASE_AUTH_TOKEN ||
    process.env.LIBSQL_AUTH_TOKEN ||
    undefined;
  _client = createClient({ url, authToken });
  await migrate(_client);
  return _client;
}

async function tableHasColumn(c, table, colName) {
  const info = await c.execute({ sql: `PRAGMA table_info(${table})` });
  for (const row of info.rows || []) {
    const n = row.name ?? row[1];
    if (n === colName) return true;
  }
  return false;
}

async function migrate(c) {
  const ddl = [
    `CREATE TABLE IF NOT EXISTS customer_snapshot (
      property_id TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      name TEXT,
      kana TEXT,
      mail TEXT,
      state TEXT,
      city TEXT,
      baitai TEXT,
      status TEXT,
      ninzu TEXT,
      yosan TEXT,
      jikosikin TEXT,
      questionnaire23 TEXT,
      questionnaire24 TEXT,
      date_entry TEXT,
      upd_date TEXT,
      payload TEXT NOT NULL,
      remote_payload TEXT,
      local_overrides TEXT DEFAULT '{}',
      PRIMARY KEY (property_id, customer_id)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_cs_prop_upd ON customer_snapshot(property_id, upd_date)`,
    `CREATE INDEX IF NOT EXISTS idx_cs_prop_state ON customer_snapshot(property_id, state)`,
    `CREATE TABLE IF NOT EXISTS sync_cursor (
      property_id TEXT PRIMARY KEY,
      last_max_upd TEXT,
      last_full_at TEXT,
      last_incremental_at TEXT,
      rows_total INTEGER DEFAULT 0
    )`,
    `CREATE TABLE IF NOT EXISTS property_visit_statuses (
      property_id TEXT PRIMARY KEY,
      statuses_json TEXT NOT NULL,
      updated_at TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS customer_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      property_id TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      kind TEXT NOT NULL,              -- baitai_changed / status_changed / created / local_override
      field TEXT,                      -- baitai / status / c.status など
      from_value TEXT,
      to_value TEXT,
      snapshot_payload TEXT,           -- その時点の remote row（JSON）
      created_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_ch_prop_cust ON customer_history(property_id, customer_id, created_at)`,
    `CREATE TABLE IF NOT EXISTS customer_sales_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      property_id TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      action_date TEXT,
      staff_name TEXT,
      action_type TEXT,
      action_detail TEXT,
      result TEXT,
      next_action_date TEXT,
      memo TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_csh_prop_cust ON customer_sales_history(property_id, customer_id, created_at)`,
    `CREATE TABLE IF NOT EXISTS customer_files (
      id TEXT PRIMARY KEY,
      property_id TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      filename TEXT NOT NULL,
      mime_type TEXT NOT NULL,
      file_data TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    )`,
    `CREATE INDEX IF NOT EXISTS idx_cf_prop_cust ON customer_files(property_id, customer_id, created_at)`,
    `CREATE TABLE IF NOT EXISTS customer_list_columns (
      tenant_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      columns_json TEXT NOT NULL,
      updated_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (tenant_id, user_id)
    )`,
    `CREATE TABLE IF NOT EXISTS customer_detail_field_config (
      tenant_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      fields_json TEXT NOT NULL,
      updated_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (tenant_id, user_id)
    )`,
  ];
  for (const sql of ddl) {
    await c.execute(sql);
  }

  if (!(await tableHasColumn(c, "customer_snapshot", "remote_payload"))) {
    await c.execute(
      "ALTER TABLE customer_snapshot ADD COLUMN remote_payload TEXT"
    );
  }
  if (!(await tableHasColumn(c, "customer_snapshot", "local_overrides"))) {
    await c.execute(
      "ALTER TABLE customer_snapshot ADD COLUMN local_overrides TEXT DEFAULT '{}'"
    );
  }
  await c.execute(
    `UPDATE customer_snapshot SET remote_payload = payload WHERE remote_payload IS NULL OR remote_payload = ''`
  );
  await c.execute(
    `UPDATE customer_snapshot SET local_overrides = '{}' WHERE local_overrides IS NULL OR local_overrides = ''`
  );
}

function resetClient() {
  _client = null;
}

module.exports = { getDb, migrate, resetClient, resolveDatabaseUrl };
