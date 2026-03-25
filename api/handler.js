/**
 * Vercel Serverless: auth / user / デジタライズ API プロキシ
 * ルートは vercel.json の rewrite で __r クエリに渡す
 */
const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const API_HOST = "api.digital-eyes.jp";
const DATA_DIR = path.join(process.cwd(), "data");

function loadEnv() {
  const out = { ...process.env };
  try {
    const p = path.join(process.cwd(), ".env");
    if (fs.existsSync(p)) {
      fs.readFileSync(p, "utf-8")
        .split("\n")
        .forEach((line) => {
          line = line.trim();
          if (!line || line.startsWith("#")) return;
          const i = line.indexOf("=");
          if (i < 0) return;
          const k = line.slice(0, i).trim();
          out[k] = line.slice(i + 1).trim();
        });
    }
  } catch (_) {}
  return out;
}

const ENV = () => loadEnv();

function readJSON(file) {
  try {
    return JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), "utf-8"));
  } catch {
    return [];
  }
}

function writeJSON(file, data) {
  const full = path.join(DATA_DIR, file);
  fs.writeFileSync(full, JSON.stringify(data, null, 2), "utf-8");
}

function sha256(str) {
  return crypto.createHash("sha256").update(str).digest("hex");
}

const JWT_SECRET = () =>
  process.env.JWT_SECRET || process.env.REM_SESSION_SECRET || "change-me-in-vercel-env";

function signToken(userId) {
  const exp = Math.floor(Date.now() / 1000) + 86400;
  const payload = Buffer.from(JSON.stringify({ sub: userId, exp })).toString(
    "base64url"
  );
  const sig = crypto
    .createHmac("sha256", JWT_SECRET())
    .update(payload)
    .digest("base64url");
  return `${payload}.${sig}`;
}

function verifyToken(token) {
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length !== 2) return null;
  const [payload, sig] = parts;
  const expected = crypto
    .createHmac("sha256", JWT_SECRET())
    .update(payload)
    .digest("base64url");
  if (expected !== sig) return null;
  try {
    const data = JSON.parse(Buffer.from(payload, "base64url").toString());
    if (data.exp < Math.floor(Date.now() / 1000)) return null;
    return { userId: data.sub };
  } catch {
    return null;
  }
}

function getToken(req) {
  const auth = req.headers.authorization || "";
  return auth.replace(/^Bearer\s+/i, "").trim();
}

function readJsonBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try {
        resolve(JSON.parse(body || "{}"));
      } catch {
        resolve({});
      }
    });
  });
}

function readRawBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () =>
      resolve(Buffer.concat(chunks).toString("utf8"))
    );
  });
}

function json(res, status, data) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(data));
}

function persistOrFail(res, fn) {
  try {
    fn();
    return true;
  } catch (e) {
    if (process.env.VERCEL) {
      json(res, 503, {
        ok: false,
        message:
          "Vercel 上では data への書き込みができません。物件の追加・編集・パスワード変更はローカルで proxy-server を実行するか、リポジトリの data/*.json を編集して再デプロイしてください。",
      });
      return false;
    }
    throw e;
  }
}

module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    res.statusCode = 204;
    res.end();
    return;
  }

  const host = req.headers.host || "localhost";
  const url = new URL(req.url || "/", `http://${host}`);
  let routePath = url.searchParams.get("__r");
  if (Array.isArray(routePath)) routePath = routePath[0];
  if (!routePath || typeof routePath !== "string") {
    json(res, 400, { ok: false, message: "Invalid route" });
    return;
  }
  if (!routePath.startsWith("/")) routePath = "/" + routePath;

  const env = ENV();
  const secretId = env.SECRET_ID || "";
  const secretPassword = env.SECRET_PASSWORD || "";

  // ── POST /auth/login ──
  if (routePath === "/auth/login" && req.method === "POST") {
    const { email, password } = await readJsonBody(req);
    const users = readJSON("users.json");
    const user = users.find(
      (u) => u.email === email && u.password === sha256(password)
    );
    if (!user) {
      json(res, 401, { ok: false, message: "メールアドレスまたはパスワードが正しくありません" });
      return;
    }
    const token = signToken(user.id);
    const props = readJSON("properties.json").filter((p) => p.userId === user.id);
    json(res, 200, {
      ok: true,
      token,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        activePropertyId: user.activePropertyId,
      },
      properties: props,
    });
    return;
  }

  // ── POST /auth/logout ──
  if (routePath === "/auth/logout" && req.method === "POST") {
    json(res, 200, { ok: true });
    return;
  }

  // ── GET /auth/me ──
  if (routePath === "/auth/me" && req.method === "GET") {
    const session = verifyToken(getToken(req));
    if (!session) {
      json(res, 401, { ok: false });
      return;
    }
    const users = readJSON("users.json");
    const user = users.find((u) => u.id === session.userId);
    if (!user) {
      json(res, 401, { ok: false });
      return;
    }
    const props = readJSON("properties.json").filter((p) => p.userId === user.id);
    json(res, 200, {
      ok: true,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        activePropertyId: user.activePropertyId,
      },
      properties: props,
    });
    return;
  }

  const session = verifyToken(getToken(req));
  if (!session) {
    json(res, 401, { ok: false, message: "認証が必要です" });
    return;
  }

  // ── GET /user/properties ──
  if (routePath === "/user/properties" && req.method === "GET") {
    const props = readJSON("properties.json").filter(
      (p) => p.userId === session.userId
    );
    json(res, 200, { ok: true, properties: props });
    return;
  }

  // ── POST /user/properties ──
  if (routePath === "/user/properties" && req.method === "POST") {
    const { name, databaseId, databasePassword, tableName } = await readJsonBody(req);
    if (!name || !databaseId || !tableName) {
      json(res, 400, { ok: false, message: "必須項目が不足しています" });
      return;
    }
    const props = readJSON("properties.json");
    const newProp = {
      id: crypto.randomUUID(),
      userId: session.userId,
      name,
      databaseId,
      databasePassword: databasePassword || "",
      tableName,
    };
    const ok = persistOrFail(res, () => {
      props.push(newProp);
      writeJSON("properties.json", props);
      const users = readJSON("users.json");
      const ui = users.findIndex((u) => u.id === session.userId);
      if (ui >= 0 && !users[ui].activePropertyId) {
        users[ui].activePropertyId = newProp.id;
        writeJSON("users.json", users);
      }
    });
    if (!ok) return;
    json(res, 200, { ok: true, property: newProp });
    return;
  }

  // ── PUT /user/properties/:id ──
  if (routePath.startsWith("/user/properties/") && req.method === "PUT") {
    const id = routePath.replace("/user/properties/", "");
    const { name, databaseId, databasePassword, tableName } = await readJsonBody(req);
    const props = readJSON("properties.json");
    const pi = props.findIndex((p) => p.id === id && p.userId === session.userId);
    if (pi < 0) {
      json(res, 404, { ok: false });
      return;
    }
    const ok = persistOrFail(res, () => {
      props[pi] = { ...props[pi], name, databaseId, databasePassword, tableName };
      writeJSON("properties.json", props);
    });
    if (!ok) return;
    json(res, 200, { ok: true, property: props[pi] });
    return;
  }

  // ── DELETE /user/properties/:id ──
  if (routePath.startsWith("/user/properties/") && req.method === "DELETE") {
    const id = routePath.replace("/user/properties/", "");
    let props = readJSON("properties.json");
    const ok = persistOrFail(res, () => {
      props = props.filter((p) => !(p.id === id && p.userId === session.userId));
      writeJSON("properties.json", props);
      const users = readJSON("users.json");
      const ui = users.findIndex((u) => u.id === session.userId);
      if (ui >= 0 && users[ui].activePropertyId === id) {
        const remaining = props.filter((p) => p.userId === session.userId);
        users[ui].activePropertyId = remaining.length ? remaining[0].id : null;
        writeJSON("users.json", users);
      }
    });
    if (!ok) return;
    json(res, 200, { ok: true });
    return;
  }

  // ── POST /user/select-property ──
  if (routePath === "/user/select-property" && req.method === "POST") {
    const { propertyId } = await readJsonBody(req);
    const ok = persistOrFail(res, () => {
      const users = readJSON("users.json");
      const ui = users.findIndex((u) => u.id === session.userId);
      if (ui >= 0) {
        users[ui].activePropertyId = propertyId;
        writeJSON("users.json", users);
      }
    });
    if (!ok) return;
    json(res, 200, { ok: true });
    return;
  }

  // ── POST /user/change-password ──
  if (routePath === "/user/change-password" && req.method === "POST") {
    const { currentPassword, newPassword } = await readJsonBody(req);
    const users = readJSON("users.json");
    const ui = users.findIndex((u) => u.id === session.userId);
    if (ui < 0 || users[ui].password !== sha256(currentPassword)) {
      json(res, 400, { ok: false, message: "現在のパスワードが正しくありません" });
      return;
    }
    const ok = persistOrFail(res, () => {
      users[ui].password = sha256(newPassword);
      writeJSON("users.json", users);
    });
    if (!ok) return;
    json(res, 200, { ok: true });
    return;
  }

  // ── POST /api/v1/... → デジタライズ API ──
  if (routePath.startsWith("/api/") && req.method === "POST") {
    const users = readJSON("users.json");
    const user = users.find((u) => u.id === session.userId);
    const props = readJSON("properties.json");
    const prop = props.find((p) => p.id === user?.activePropertyId);
    if (!prop) {
      json(res, 400, {
        ok: false,
        message: "物件が選択されていません。設定ページから物件を追加してください。",
      });
      return;
    }

    const body = await readRawBody(req);
    const params = new URLSearchParams(body);
    params.set("X_secret_id", secretId);
    params.set("X_secret_password", secretPassword);
    params.set("X_database_id", prop.databaseId);
    params.set("X_database_password", prop.databasePassword);
    params.set("table_name", prop.tableName);
    const newBody = params.toString();

    await new Promise((resolve, reject) => {
      const options = {
        hostname: API_HOST,
        path: routePath,
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
          "Content-Length": Buffer.byteLength(newBody),
        },
      };
      const pr = https.request(options, (pres) => {
        res.statusCode = pres.statusCode || 500;
        res.setHeader("Access-Control-Allow-Origin", "*");
        const ct = pres.headers["content-type"] || "application/json";
        res.setHeader("Content-Type", ct);
        pres.pipe(res);
        pres.on("end", resolve);
      });
      pr.on("error", (err) => {
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ status: false, message: err.message }));
        resolve();
      });
      pr.write(newBody);
      pr.end();
    });
    return;
  }

  json(res, 404, { ok: false, message: "Not found" });
};
