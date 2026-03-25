const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = 3001;
const API_HOST = "api.digital-eyes.jp";
const DATA_DIR = path.join(__dirname, "data");

// ── .env 読み込み ──────────────────────────────
function loadEnv() {
  try {
    return fs.readFileSync(path.join(__dirname, ".env"), "utf-8")
      .split("\n").reduce((acc, line) => {
        line = line.trim();
        if (!line || line.startsWith("#")) return acc;
        const [k, ...v] = line.split("=");
        acc[k.trim()] = v.join("=").trim();
        return acc;
      }, {});
  } catch { return {}; }
}
const ENV = loadEnv();
console.log(`✅ SECRET_ID: ${ENV.SECRET_ID ? ENV.SECRET_ID.slice(0,6)+"***" : "⚠️ 未設定"}`);

// ── データ管理 ─────────────────────────────────
function readJSON(file) {
  try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), "utf-8")); }
  catch { return []; }
}
function writeJSON(file, data) {
  fs.writeFileSync(path.join(DATA_DIR, file), JSON.stringify(data, null, 2), "utf-8");
}

// ── セッション管理（メモリ） ───────────────────
const sessions = new Map(); // token -> { userId, createdAt }

function createSession(userId) {
  const token = crypto.randomBytes(32).toString("hex");
  sessions.set(token, { userId, createdAt: Date.now() });
  return token;
}
function getSession(token) {
  const s = sessions.get(token);
  if (!s) return null;
  if (Date.now() - s.createdAt > 24 * 60 * 60 * 1000) { sessions.delete(token); return null; }
  return s;
}
function deleteSession(token) { sessions.delete(token); }

function sha256(str) { return crypto.createHash("sha256").update(str).digest("hex"); }

// ── ルーティングヘルパー ────────────────────────
function getToken(req) {
  const auth = req.headers["authorization"] || "";
  return auth.replace("Bearer ", "").trim();
}
function json(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data));
}
function readBody(req) {
  return new Promise(resolve => {
    let body = "";
    req.on("data", c => body += c);
    req.on("end", () => {
      try { resolve(JSON.parse(body)); } catch { resolve({}); }
    });
  });
}

// ── MIME ───────────────────────────────────────
const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json",
  ".ico": "image/x-icon",
  ".png": "image/png",
  ".jpg": "image/jpeg",
};

/** 拡張子なしURL → .html（例: /settings → settings.html） */
const HTML_PAGE_ALIASES = {
  "/login": "/login.html",
  "/dashboard": "/dashboard.html",
  "/settings": "/settings.html",
  "/customer": "/customer.html",
  "/property": "/property.html",
  "/client": "/client.html",
  "/schedule": "/schedule.html",
  "/ad": "/ad.html",
};

// ── サーバー ───────────────────────────────────
const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const urlPath = req.url.split("?")[0];

  // ══ POST /auth/login ═══════════════════════════
  if (urlPath === "/auth/login" && req.method === "POST") {
    const { email, password } = await readBody(req);
    const users = readJSON("users.json");
    const user = users.find(u => u.email === email && u.password === sha256(password));
    if (!user) { json(res, 401, { ok: false, message: "メールアドレスまたはパスワードが正しくありません" }); return; }
    const token = createSession(user.id);
    const props = readJSON("properties.json").filter(p => p.userId === user.id);
    json(res, 200, { ok: true, token, user: { id: user.id, name: user.name, email: user.email, activePropertyId: user.activePropertyId }, properties: props });
    return;
  }

  // ══ POST /auth/logout ══════════════════════════
  if (urlPath === "/auth/logout" && req.method === "POST") {
    deleteSession(getToken(req));
    json(res, 200, { ok: true });
    return;
  }

  // ══ GET /auth/me ═══════════════════════════════
  if (urlPath === "/auth/me" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const users = readJSON("users.json");
    const user = users.find(u => u.id === session.userId);
    if (!user) { json(res, 401, { ok: false }); return; }
    const props = readJSON("properties.json").filter(p => p.userId === user.id);
    json(res, 200, { ok: true, user: { id: user.id, name: user.name, email: user.email, activePropertyId: user.activePropertyId }, properties: props });
    return;
  }

  // ══ GET /user/properties ═══════════════════════
  if (urlPath === "/user/properties" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const props = readJSON("properties.json").filter(p => p.userId === session.userId);
    json(res, 200, { ok: true, properties: props });
    return;
  }

  // ══ POST /user/properties ══════════════════════
  if (urlPath === "/user/properties" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const { name, databaseId, databasePassword, tableName } = await readBody(req);
    if (!name || !databaseId || !tableName) { json(res, 400, { ok: false, message: "必須項目が不足しています" }); return; }
    const props = readJSON("properties.json");
    const newProp = { id: crypto.randomUUID(), userId: session.userId, name, databaseId, databasePassword: databasePassword||"", tableName };
    props.push(newProp);
    writeJSON("properties.json", props);
    // 初回追加時はアクティブに設定
    const users = readJSON("users.json");
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui >= 0 && !users[ui].activePropertyId) { users[ui].activePropertyId = newProp.id; writeJSON("users.json", users); }
    json(res, 200, { ok: true, property: newProp });
    return;
  }

  // ══ PUT /user/properties/:id ═══════════════════
  if (urlPath.startsWith("/user/properties/") && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const id = urlPath.replace("/user/properties/", "");
    const { name, databaseId, databasePassword, tableName } = await readBody(req);
    const props = readJSON("properties.json");
    const pi = props.findIndex(p => p.id === id && p.userId === session.userId);
    if (pi < 0) { json(res, 404, { ok: false }); return; }
    props[pi] = { ...props[pi], name, databaseId, databasePassword, tableName };
    writeJSON("properties.json", props);
    json(res, 200, { ok: true, property: props[pi] });
    return;
  }

  // ══ DELETE /user/properties/:id ════════════════
  if (urlPath.startsWith("/user/properties/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const id = urlPath.replace("/user/properties/", "");
    let props = readJSON("properties.json");
    props = props.filter(p => !(p.id === id && p.userId === session.userId));
    writeJSON("properties.json", props);
    // アクティブ物件が削除されたらリセット
    const users = readJSON("users.json");
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui >= 0 && users[ui].activePropertyId === id) {
      const remaining = props.filter(p => p.userId === session.userId);
      users[ui].activePropertyId = remaining.length ? remaining[0].id : null;
      writeJSON("users.json", users);
    }
    json(res, 200, { ok: true });
    return;
  }

  // ══ POST /user/select-property ════════════════
  if (urlPath === "/user/select-property" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const { propertyId } = await readBody(req);
    const users = readJSON("users.json");
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui >= 0) { users[ui].activePropertyId = propertyId; writeJSON("users.json", users); }
    json(res, 200, { ok: true });
    return;
  }

  // ══ POST /user/change-password ═════════════════
  if (urlPath === "/user/change-password" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const { currentPassword, newPassword } = await readBody(req);
    const users = readJSON("users.json");
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui < 0 || users[ui].password !== sha256(currentPassword)) {
      json(res, 400, { ok: false, message: "現在のパスワードが正しくありません" }); return;
    }
    users[ui].password = sha256(newPassword);
    writeJSON("users.json", users);
    json(res, 200, { ok: true });
    return;
  }

  // ══ POST /api/... → デジタライズAPIへプロキシ ══
  if (urlPath.startsWith("/api/")) {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }

    // アクティブ物件の認証情報を取得
    const users = readJSON("users.json");
    const user = users.find(u => u.id === session.userId);
    const props = readJSON("properties.json");
    const prop = props.find(p => p.id === user?.activePropertyId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。設定ページから物件を追加してください。" }); return; }

    let body = "";
    req.on("data", c => body += c);
    req.on("end", () => {
      const params = new URLSearchParams(body);
      params.set("X_secret_id", ENV.SECRET_ID || "");
      params.set("X_secret_password", ENV.SECRET_PASSWORD || "");
      params.set("X_database_id", prop.databaseId);
      params.set("X_database_password", prop.databasePassword);
      params.set("table_name", prop.tableName);
      const newBody = params.toString();

      console.log(`[API] ${urlPath} → ${prop.name}`);
      const options = {
        hostname: API_HOST, path: urlPath, method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8", "Content-Length": Buffer.byteLength(newBody) },
      };
      const pr = https.request(options, pres => {
        res.writeHead(pres.statusCode, { "Content-Type": "application/json" });
        pres.pipe(res);
      });
      pr.on("error", err => { res.writeHead(500); res.end(JSON.stringify({ status: false, message: err.message })); });
      pr.write(newBody); pr.end();
    });
    return;
  }

  // ══ 静的ファイル配信 ════════════════════════════
  let filePath =
    urlPath === "/" ? "/login.html" : HTML_PAGE_ALIASES[urlPath] || urlPath;
  filePath = path.join(__dirname, filePath);
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end("Not found"); return; }
    const ext = path.extname(filePath);
    res.writeHead(200, { "Content-Type": MIME[ext] || "text/plain" });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`\n✅ サーバー起動中: http://localhost:${PORT}`);
  console.log(`   初期ログイン: admin@example.com / admin`);
  console.log(`   停止するには Ctrl+C\n`);
});
