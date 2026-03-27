/**
 * 管理者ユーザーを LibSQL に登録・更新する（1回限りの運用用）
 *
 * 使い方:
 *   REM_BOOTSTRAP_PASSWORD='平文パスワード' node scripts/upsert-admin.js you@example.com
 *
 * または（非推奨・シェル履歴に残る）:
 *   node scripts/upsert-admin.js you@example.com '平文パスワード'
 */
const crypto = require("crypto");
const userStore = require("../lib/user-store");

function sha256(str) {
  return crypto.createHash("sha256").update(String(str), "utf8").digest("hex");
}

async function main() {
  const emailArg = String(process.argv[2] || "").trim().toLowerCase();
  const pwdEnv = String(process.env.REM_BOOTSTRAP_PASSWORD || "").trim();
  const pwdArg = String(process.argv[3] || "").trim();
  const password = pwdEnv || pwdArg;
  if (!emailArg || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(emailArg)) {
    console.error("メールアドレスを指定してください。例: node scripts/upsert-admin.js admin@example.com");
    process.exit(1);
  }
  if (!password || password.length < 6) {
    console.error("パスワードは REM_BOOTSTRAP_PASSWORD または第2引数で指定してください（6文字以上）。");
    process.exit(1);
  }

  const hash = sha256(password);
  const users = await userStore.listUsers();
  const existing = users.find((u) => String(u.email || "").toLowerCase() === emailArg);

  if (existing) {
    await userStore.updateUser({
      ...existing,
      email: emailArg,
      password: hash,
      role: 2,
      name: existing.name || "管理者",
    });
    console.log("更新しました:", emailArg, "role=2");
    return;
  }

  const maxId = users.reduce((m, u) => {
    const n = Number.parseInt(String(u.id), 10);
    return Number.isFinite(n) && n > m ? n : m;
  }, 0);
  const id = String(maxId + 1 || 1);
  await userStore.insertUser({
    id,
    email: emailArg,
    password: hash,
    name: "管理者",
    activePropertyId: null,
    role: 2,
    tenantId: "1",
    client: "",
    propertyIds: [],
  });
  console.log("登録しました:", emailArg, "id=", id, "role=2");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
