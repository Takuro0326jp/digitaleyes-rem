#!/usr/bin/env node
/**
 * Turso / ローカル LibSQL 接続確認（.env を process.env に読み込んでから getDb）
 * 使い方: node scripts/verify-db.js
 */
const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const envPath = path.join(root, ".env");
try {
  fs.readFileSync(envPath, "utf8")
    .split("\n")
    .forEach((line) => {
      line = line.trim();
      if (!line || line.startsWith("#")) return;
      const i = line.indexOf("=");
      if (i === -1) return;
      const k = line.slice(0, i).trim();
      const v = line.slice(i + 1).trim();
      if (k && process.env[k] === undefined) process.env[k] = v;
    });
} catch (e) {
  console.error(".env が読めません:", e.message);
  process.exit(2);
}

const { getDb, resolveDatabaseUrl } = require("../lib/sync-db");
const url = resolveDatabaseUrl();
const mode = url.startsWith("libsql://")
  ? "Turso/LibSQL（リモート）"
  : url.includes("memory")
    ? "メモリ（非永続）"
    : "ローカル file";
console.log("接続先:", mode);
console.log("トークン:", process.env.TURSO_AUTH_TOKEN || process.env.TURSO_API_TOKEN ? "設定あり" : "未設定（リモートでは必須）");

getDb()
  .then(async (db) => {
    const r = await db.execute("SELECT 1 AS ok");
    const row = r.rows && r.rows[0];
    const ok = row && (row.ok !== undefined ? row.ok : row[0]);
    console.log("SELECT 1:", ok === 1 ? "OK" : JSON.stringify(row));
    const t = await db.execute(
      "SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table'"
    );
    const tr = t.rows && t.rows[0];
    const n = tr && (tr.n !== undefined ? tr.n : tr[0]);
    console.log("テーブル数:", n);
    process.exit(0);
  })
  .catch((e) => {
    console.error("接続失敗:", e.message || e);
    process.exit(1);
  });
