const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const {
  runCustomerSync,
  queryLocalCustomers,
  getSyncStatus,
  getVisitStatuses,
  getCustomerFilterOptions,
  saveVisitStatuses,
  patchLocalCustomer,
  listCustomerHistory,
  listCustomerReactions,
  listSalesHistory,
  addSalesHistory,
  updateSalesHistory,
  deleteSalesHistory,
  listCustomerFiles,
  addCustomerFile,
  deleteCustomerFile,
  getCustomerListColumns,
  saveCustomerListColumns,
  DEFAULT_CUSTOMER_COLUMNS,
  listCustomerAvailableFields,
  getCustomerSpecFieldDefs,
  getCustomerDetailFieldConfig,
  saveCustomerDetailFieldConfig,
  getPropertySalesMetrics,
  DEFAULT_CUSTOMER_DETAIL_FIELDS,
  DEFAULT_VISIT_STATUSES,
} = require("./lib/sync-service");
const { CUSTOMER_SNAPSHOT_FIELDS } = require("./lib/customer-fields");
const {
  listPropertiesByUser,
  getPropertyByIdForUser,
  createProperty,
  updateProperty,
  deleteProperty,
  updatePropertyMeta,
  syncPropertiesClientName,
  listPropertyUpdateHistory,
  listPropertyImages,
  addPropertyImage,
  deletePropertyImage,
} = require("./lib/property-store");
const {
  listRooms,
  listSchedules,
  createSchedule,
  updateSchedule,
  deleteSchedule,
  getScheduleSettings,
  saveScheduleSettings,
} = require("./lib/schedule-store");
const {
  listClients,
  getClient,
  createClient,
  updateClient,
  deleteClients,
  getClientS3Settings,
  saveClientS3Settings,
} = require("./lib/client-store");
const {
  listMediaAssets,
  getMediaAssetById,
  createMediaAssetRecord,
  patchMediaAsset,
  deleteMediaAsset,
  listMediaAssetTypes,
  addMediaAssetType,
  deleteMediaAssetType,
} = require("./lib/media-assets-store");
const {
  buildAssetKey,
  createUploadPresignedUrl,
  ensureBucketCors,
  createReadSignedUrl,
  uploadObjectDirect,
  removeObject,
} = require("./lib/s3-media");
const { s3ConfigFromProperty } = require("./lib/media-s3");
const { S3Client, HeadBucketCommand } = require("@aws-sdk/client-s3");
const {
  sendAccountInviteMail,
  sendSmtpTestMail,
  sendLoginOtpMail,
  sendPasswordResetMail,
  hasSmtpEnv,
} = require("./lib/mailer");
const {
  buildWeeklyReportBuffer,
  computeDashboardWeekStats,
  ymdUtcWeekday,
} = require("./lib/weekly-report");
const { buildWeeklyReportTemplateBuffer } = require("./lib/weekly-report-template");
const { buildCustomerAnalysisReport } = require("./lib/customer-analysis");
const { buildCustomerKarteExcelBuffer } = require("./lib/customer-karte-export");
const { buildLabelWorkbookWithOpenpyxl } = require("./lib/label-print-openpyxl");
const userStore = require("./lib/user-store");
const { applyLoginDefaultToUser } = require("./lib/login-default");
const {
  ROLE,
  normalizeRole,
  isPropertyScopedRole,
  isAdminLike,
  isMaster,
  canUsePropertyMediaRoles,
} = require("./lib/roles");

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
// lib/sync-db.js 等は process.env を参照するため、.env を未設定キーに反映する
for (const [k, v] of Object.entries(ENV)) {
  if (typeof v === "string" && v !== "" && process.env[k] === undefined) process.env[k] = v;
}
const PORT = Number(process.env.PORT) || 3001;
console.log(`✅ SECRET_ID: ${ENV.SECRET_ID ? ENV.SECRET_ID.slice(0,6)+"***" : "⚠️ 未設定"}`);
const { resolveDatabaseUrl } = require("./lib/sync-db");
const _resolvedSync = resolveDatabaseUrl();
const _syncLabel = _resolvedSync.startsWith("libsql://")
  ? "Turso/LibSQL（リモート・REM_USE_REMOTE_SYNC_DB 有効）"
  : _resolvedSync.includes("memory")
    ? "メモリ（非永続）"
    : "ローカル data/sync.db（既定）";
console.log(`✅ LibSQL 同期DB: ${_syncLabel}`);

// ── データ管理 ─────────────────────────────────
function readJSON(file) {
  try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), "utf-8")); }
  catch { return []; }
}
function writeJSON(file, data) {
  fs.writeFileSync(path.join(DATA_DIR, file), JSON.stringify(data, null, 2), "utf-8");
}

function normalizeUser(u) {
  const roleRaw = u.role ?? u.roleId ?? 2;
  const tenantId = u.tenantId ?? u.clientId ?? "1";
  const client = u.client ?? "株式会社ワールド・エステート";
  const propertyIds = Array.isArray(u.propertyIds) ? u.propertyIds.map(String) : [];
  const ld = u.loginDefaultPropertyId;
  return {
    ...u,
    role: normalizeRole(roleRaw),
    tenantId: String(tenantId),
    client: String(client),
    propertyIds,
    name: u.name || [u.lastName, u.firstName].filter(Boolean).join(" ") || "",
    loginDefaultPropertyId:
      ld != null && String(ld).trim() !== "" ? String(ld) : null,
  };
}

/** 物件管理者: propertyIds は文字列化済み。物件 id は数値のことがあるため String で突き合わせる */
function visiblePropsForUser(user, propsAll) {
  if (!user || !isPropertyScopedRole(user.role)) return propsAll;
  const idSet = new Set((user.propertyIds || []).map(String));
  return propsAll.filter((p) => idSet.has(String(p.id)));
}

function role3MayAccessPropertyId(user, propertyId) {
  if (!user || !isPropertyScopedRole(user.role)) return true;
  if (!Array.isArray(user.propertyIds)) return false;
  return new Set(user.propertyIds.map(String)).has(String(propertyId));
}

async function loadUsers() {
  const raw = await userStore.listUsers();
  return raw.map(normalizeUser);
}

async function saveUsers(users) {
  const current = await userStore.listUsers();
  const curIds = new Set(current.map((u) => String(u.id)));
  const nextIds = new Set(users.map((u) => String(u.id)));
  for (const id of curIds) {
    if (!nextIds.has(id)) {
      const victim = current.find((u) => String(u.id) === id);
      if (victim && isMaster(victim.role)) continue;
      await userStore.deleteUserById(id);
    }
  }
  for (const u of users) {
    const id = String(u.id);
    if (curIds.has(id)) await userStore.updateUser(u);
    else await userStore.insertUser(u);
  }
}

async function currentUserFromSession(session) {
  const sid = String(session.userId ?? "");
  const users = await loadUsers();
  return users.find((u) => String(u.id) === sid) || null;
}

function publicUser(u) {
  return {
    id: u.id,
    name: u.name,
    email: u.email,
    role: u.role,
    tenantId: u.tenantId,
    client: u.client,
    propertyIds: u.propertyIds || [],
    activePropertyId: u.activePropertyId || null,
    loginDefaultPropertyId: u.loginDefaultPropertyId || null,
    mustChangePassword: !!u.mustChangePassword,
  };
}

function canManageAccounts(me) {
  return me && isAdminLike(me.role);
}

function canManageClients(me) {
  return me && isAdminLike(me.role);
}

function assertClientRecordAccess(me, clientRow) {
  if (!me || !clientRow) return { ok: false, message: "権限がありません" };
  if (isMaster(me.role)) return { ok: true };
  if (!isAdminLike(me.role)) return { ok: false, message: "権限がありません" };
  if (String(me.client || "").trim() !== String(clientRow.name || "").trim()) {
    return { ok: false, message: "このクライアントを操作する権限がありません" };
  }
  return { ok: true };
}

// ── セッション管理（メモリ） ───────────────────
const sessions = new Map(); // token -> { userId, createdAt }
const loginChallenges = new Map(); // challengeToken -> { userId, codeHash, exp }

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
function generateOtpCode() { return String(Math.floor(100000 + Math.random() * 900000)); }
function createLoginChallenge(userId, code) {
  const token = crypto.randomBytes(24).toString("hex");
  loginChallenges.set(token, { userId: String(userId), codeHash: sha256(String(code)), exp: Date.now() + 10 * 60 * 1000 });
  return token;
}
function verifyLoginChallenge(token, code) {
  const row = loginChallenges.get(String(token || ""));
  if (!row) return null;
  if (Date.now() > Number(row.exp || 0)) { loginChallenges.delete(String(token || "")); return null; }
  if (sha256(String(code || "").trim()) !== row.codeHash) return null;
  loginChallenges.delete(String(token || ""));
  return row;
}
function randomTempPassword(len = 12) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789";
  const buf = crypto.randomBytes(len);
  let s = "";
  for (let i = 0; i < len; i++) s += chars[buf[i] % chars.length];
  return s;
}

/** パスワード再設定ページ URL（api/handler の resolvePasswordResetBaseUrl と同規則） */
function resolvePasswordResetBaseUrlProxy(req, body) {
  let login = String(body.loginUrl || body.resetBaseUrl || ENV.APP_LOGIN_URL || "").trim();
  if (login) {
    try {
      const x = new URL(login);
      let path = String(x.pathname || "").replace(/\/login\.html?$/i, "/reset-password.html");
      if (!/\/reset-password\.html$/i.test(path)) path = "/reset-password.html";
      return `${x.origin}${path}`;
    } catch (_) {}
  }
  const origin = String(req.headers.origin || "").trim();
  if (origin) {
    try {
      return `${new URL(origin).origin}/reset-password.html`;
    } catch (_) {}
  }
  const ref = String(req.headers.referer || "").trim();
  if (ref) {
    try {
      return `${new URL(ref).origin}/reset-password.html`;
    } catch (_) {}
  }
  const og = String(body.origin || "").trim();
  if (og) {
    try {
      const base = /^https?:\/\//i.test(og) ? og : `https://${og}`;
      return `${new URL(base).origin}/reset-password.html`;
    } catch (_) {}
  }
  return "";
}

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

async function getActivePropertyForUser(userId) {
  const users = await loadUsers();
  const user = users.find((u) => u.id === userId);
  if (!user) return null;
  const propsAll = await listPropertiesByUser(user.tenantId);
  const props = visiblePropsForUser(user, propsAll);
  return props.find((p) => String(p.id) === String(user.activePropertyId)) || null;
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
  "/forgot-password": "/forgot-password.html",
  "/reset-password": "/reset-password.html",
  "/dashboard": "/dashboard.html",
  "/settings": "/settings.html",
  "/customer": "/customer.html",
  "/customer/map": "/customer-mapping.html",
  "/analysis": "/analysis.html",
  "/property": "/property.html",
  "/property-detail": "/property-detail.html",
  "/account": "/account.html",
  "/schedule": "/schedule.html",
  "/ad": "/ad.html",
  "/client": "/client.html",
  "/client/create": "/client-create.html",
  "/gallery": "/gallery.html",
};

// ── サーバー ───────────────────────────────────
const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  let urlPath = req.url.split("?")[0] || "/";
  if (!urlPath.startsWith("/")) urlPath = "/" + urlPath;
  urlPath = urlPath.replace(/\/+$/, "") || "/";
  const fullUrl = new URL(req.url || "/", "http://127.0.0.1");

  // ══ POST /auth/login ═══════════════════════════
  if (urlPath === "/auth/login" && req.method === "POST") {
    const { email, password } = await readBody(req);
    const users = await loadUsers();
    const em = String(email || "").toLowerCase().trim();
    const user = users.find(
      (u) => String(u.email || "").toLowerCase() === em && u.password === sha256(password)
    );
    if (!user) { json(res, 401, { ok: false, message: "メールアドレスまたはパスワードが正しくありません" }); return; }
    if (String(process.env.REM_SKIP_2FA || "").trim() === "1") {
      let u = user;
      u = await applyLoginDefaultToUser(u, {
        listPropertiesByUser,
        updateUser: (x) => userStore.updateUser(x),
      });
      const token = createSession(u.id);
      const propsAll = await listPropertiesByUser(u.tenantId);
      const props = visiblePropsForUser(u, propsAll);
      json(res, 200, { ok: true, token, user: publicUser(u), properties: props });
      return;
    }
    if (!hasSmtpEnv(process.env)) {
      json(res, 400, { ok: false, message: "2段階認証に必要なSMTPが未設定です。管理者にお問い合わせください。" });
      return;
    }
    let code = "";
    try {
      code = generateOtpCode();
      await sendLoginOtpMail(process.env, { email: user.email, name: user.name || "", code });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || "認証コード送信に失敗しました" });
      return;
    }
    const challengeToken = createLoginChallenge(user.id, code);
    json(res, 200, { ok: true, requires2fa: true, challengeToken, message: "認証コードをメール送信しました。10分以内に入力してください。" });
    return;
  }

  if (urlPath === "/auth/verify-2fa" && req.method === "POST") {
    const { challengeToken, code } = await readBody(req);
    const ch = verifyLoginChallenge(challengeToken, code);
    if (!ch) { json(res, 401, { ok: false, message: "認証コードが不正、または有効期限切れです" }); return; }
    const users = await loadUsers();
    let user = users.find((u) => String(u.id) === String(ch.userId));
    if (!user) { json(res, 401, { ok: false, message: "認証対象ユーザーが見つかりません" }); return; }
    user = await applyLoginDefaultToUser(user, {
      listPropertiesByUser,
      updateUser: (x) => userStore.updateUser(x),
    });
    const token = createSession(user.id);
    const propsAll = await listPropertiesByUser(user.tenantId);
    const props = visiblePropsForUser(user, propsAll);
    json(res, 200, { ok: true, token, user: publicUser(user), properties: props });
    return;
  }

  if (urlPath === "/auth/forgot-password" && req.method === "POST") {
    const body = await readBody(req);
    const email = String(body.email || "").toLowerCase().trim();
    const genericOk = {
      ok: true,
      message:
        "該当するメールアドレスが登録されている場合、パスワード再設定用の案内を送信しました。受信トレイをご確認ください。",
    };
    if (!email) {
      json(res, 200, genericOk);
      return;
    }
    if (!hasSmtpEnv(process.env)) {
      json(res, 503, {
        ok: false,
        message: "パスワード再設定メールを送信できません。SMTP が未設定です。管理者にお問い合わせください。",
      });
      return;
    }
    const users = await loadUsers();
    const user = users.find((u) => String(u.email || "").toLowerCase() === email);
    if (!user) {
      json(res, 200, genericOk);
      return;
    }
    const base = resolvePasswordResetBaseUrlProxy(req, body);
    if (!base) {
      json(res, 500, {
        ok: false,
        message:
          "再設定ページのURLを決定できませんでした。ブラウザのURLから操作するか、環境変数 APP_LOGIN_URL を設定してください。",
      });
      return;
    }
    const token = crypto.randomBytes(32).toString("hex");
    const expiresMs = Date.now() + 60 * 60 * 1000;
    await userStore.setPasswordResetTokenByEmail(email, token, expiresMs);
    const resetUrl = `${base}?token=${encodeURIComponent(token)}`;
    try {
      await sendPasswordResetMail(process.env, {
        email: user.email,
        name: user.name || "",
        resetUrl,
      });
    } catch (e) {
      await userStore.clearPasswordResetByEmail(email);
      json(res, 500, { ok: false, message: e.message || "メール送信に失敗しました" });
      return;
    }
    json(res, 200, genericOk);
    return;
  }

  if (urlPath === "/auth/reset-password" && req.method === "POST") {
    const body = await readBody(req);
    const token = String(body.token || "").trim();
    const newPassword = String(body.newPassword || "");
    const newPasswordConfirm = String(body.newPasswordConfirm || "");
    if (!token) {
      json(res, 400, { ok: false, message: "再設定用のトークンがありません。" });
      return;
    }
    if (newPassword.length < 8) {
      json(res, 400, { ok: false, message: "パスワードは8文字以上にしてください" });
      return;
    }
    if (newPassword !== newPasswordConfirm) {
      json(res, 400, { ok: false, message: "パスワード（確認）が一致しません" });
      return;
    }
    const result = await userStore.consumePasswordReset(token, sha256(newPassword));
    if (!result.ok) {
      json(res, 400, { ok: false, message: result.message });
      return;
    }
    json(res, 200, {
      ok: true,
      message: "パスワードを更新しました。ログイン画面からログインしてください。",
    });
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
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const users = await loadUsers();
    const user = users.find((u) => String(u.id) === String(session.userId));
    if (!user) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const propsAll = await listPropertiesByUser(user.tenantId);
    const props = visiblePropsForUser(user, propsAll);
    json(res, 200, { ok: true, user: publicUser(user), properties: props });
    return;
  }

  // ══ GET /client/detail/:id /client/edit/:id (HTML) ═════════
  if ((urlPath.startsWith("/client/detail/") || urlPath.startsWith("/client/edit/")) && req.method === "GET") {
    const file = urlPath.startsWith("/client/detail/") ? "/client-detail.html" : "/client-edit.html";
    const filePath = path.join(__dirname, file);
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end("Not found"); return; }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(data);
    });
    return;
  }
  if (urlPath.startsWith("/gallery/") && req.method === "GET") {
    const filePath = path.join(__dirname, "/gallery.html");
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end("Not found"); return; }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(data);
    });
    return;
  }

  // ══ GET /client/api/list ／ GET /client?api=1（一覧 JSON）══════════════════
  if (
    (urlPath === "/client/api/list" ||
      (urlPath === "/client" && fullUrl.searchParams.get("api") === "1")) &&
    req.method === "GET"
  ) {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const clients = await listClients(me.tenantId);
    json(res, 200, { ok: true, clients });
    return;
  }
  if (urlPath === "/client/master" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const clients = await listClients(me.tenantId);
    json(res, 200, { ok: true, clients });
    return;
  }

  // ══ POST /client/create ═════════════════════════
  if (urlPath === "/client/create" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readBody(req);
      const c = await createClient(me.tenantId, body || {});
      json(res, 200, { ok: true, client: c });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET /client/api/detail?id= ══════════════════
  if (urlPath === "/client/api/detail" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = fullUrl.searchParams.get("id");
    const c = await getClient(me.tenantId, id);
    if (!c) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
    const ac = assertClientRecordAccess(me, c);
    if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
    const s3 = await getClientS3Settings(me.tenantId, c.id);
    json(res, 200, { ok: true, client: c, s3 });
    return;
  }

  if (urlPath === "/client/api/linked-properties" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = fullUrl.searchParams.get("id");
    const c = await getClient(me.tenantId, id);
    if (!c) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
    const ac = assertClientRecordAccess(me, c);
    if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
    const clientName = String(c.name || "").trim();
    const all = await listPropertiesByUser(me.tenantId);
    const properties = all.map((p) => {
      const cur = String(p.clientName || "").trim();
      return {
        id: p.id,
        name: p.name || "",
        clientName: cur,
        linked: cur === clientName,
      };
    });
    json(res, 200, { ok: true, client: { id: c.id, name: c.name }, properties });
    return;
  }

  if (urlPath === "/client/api/linked-properties" && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readBody(req);
      const c = await getClient(me.tenantId, body.clientId);
      if (!c) { json(res, 404, { ok: false, message: "クライアントが見つかりません" }); return; }
      const ac = assertClientRecordAccess(me, c);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      const propertyIds = Array.isArray(body.propertyIds) ? body.propertyIds : [];
      await syncPropertiesClientName(me.tenantId, c.name, propertyIds, me.name || "");
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /client/edit/:id ═══════════════════════
  if (urlPath.startsWith("/client/edit/") && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(urlPath.slice("/client/edit/".length));
    try {
      const existing = await getClient(me.tenantId, id);
      if (!existing) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
      const ac = assertClientRecordAccess(me, existing);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      const body = await readBody(req);
      const c = await updateClient(me.tenantId, id, body || {});
      json(res, 200, { ok: true, client: c });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /client/delete ═════════════════════════
  if (urlPath === "/client/delete" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const body = await readBody(req);
    const ids = body.ids || [];
    for (const cid of ids) {
      const row = await getClient(me.tenantId, cid);
      if (row) {
        const ac = assertClientRecordAccess(me, row);
        if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      }
    }
    const r = await deleteClients(me.tenantId, ids);
    json(res, 200, { ok: true, ...r });
    return;
  }

  // ══ POST /client/s3/save/:id ════════════════════
  if (urlPath.startsWith("/client/s3/save/") && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(urlPath.slice("/client/s3/save/".length));
    try {
      const row = await getClient(me.tenantId, id);
      if (!row) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
      const ac = assertClientRecordAccess(me, row);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      const body = await readBody(req);
      const saved = await saveClientS3Settings(me.tenantId, id, body || {});
      json(res, 200, { ok: true, s3: saved });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /client/s3/test/:id ════════════════════
  if (urlPath.startsWith("/client/s3/test/") && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageClients(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(urlPath.slice("/client/s3/test/".length));
    const testRow = await getClient(me.tenantId, id);
    if (testRow) {
      const ac = assertClientRecordAccess(me, testRow);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
    }
    try {
      const body = await readBody(req);
      const region = String(body.region || "");
      const bucket = String(body.bucketName || "");
      const awsKey = String(body.awsKey || "");
      const awsSecretKey = String(body.awsSecretKey || "");
      if (!region || !bucket || !awsKey || !awsSecretKey) throw new Error("S3設定が不足しています");
      const s3 = new S3Client({ region, credentials: { accessKeyId: awsKey, secretAccessKey: awsSecretKey } });
      await s3.send(new HeadBucketCommand({ Bucket: bucket }));
      json(res, 200, { ok: true, message: "接続に成功しました" });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "接続に失敗しました" });
    }
    return;
  }

  // ══ GET /user/properties ═══════════════════════
  if (urlPath === "/user/properties" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const propsAll = await listPropertiesByUser(me.tenantId);
    const props = visiblePropsForUser(me, propsAll);
    json(res, 200, { ok: true, properties: props });
    return;
  }

  // ══ POST /user/properties ══════════════════════
  if (urlPath === "/user/properties" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const { name, databaseId, databasePassword, tableName } = await readBody(req);
    if (!name || !databaseId || !tableName) { json(res, 400, { ok: false, message: "必須項目が不足しています" }); return; }
    const createPayload = {
      name,
      databaseId,
      databasePassword: databasePassword || "",
      tableName,
    };
    if (normalizeRole(me.role) === ROLE.CLIENT_ADMIN) {
      const cn = String(me.client || "").trim();
      if (cn) createPayload.clientName = cn;
    }
    const newProp = await createProperty(me.tenantId, createPayload);
    // 初回追加時はアクティブに設定
    const users = await loadUsers();
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui >= 0 && !users[ui].activePropertyId) {
      users[ui].activePropertyId = newProp.id;
      await userStore.updateUser(users[ui]);
    }
    json(res, 200, { ok: true, property: newProp });
    return;
  }

  // ══ PUT /user/properties/:id ═══════════════════
  if (urlPath.startsWith("/user/properties/") && urlPath.endsWith("/images") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const id = urlPath.slice("/user/properties/".length, -"/images".length);
    const prop = await getPropertyByIdForUser(id, me.tenantId);
    if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
    const images = await listPropertyImages(id, me.tenantId);
    json(res, 200, { ok: true, images });
    return;
  }
  if (urlPath.startsWith("/user/properties/") && urlPath.endsWith("/history") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const id = urlPath.slice("/user/properties/".length, -"/history".length);
    const prop = await getPropertyByIdForUser(id, me.tenantId);
    if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
    const items = await listPropertyUpdateHistory(id, me.tenantId, fullUrl.searchParams.get("limit") || "50");
    json(res, 200, { ok: true, history: items });
    return;
  }
  if (urlPath.startsWith("/user/properties/") && urlPath.endsWith("/meta") && req.method === "PATCH") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = urlPath.slice("/user/properties/".length, -"/meta".length);
    try {
      const body = await readBody(req);
      const updated = await updatePropertyMeta(id, me.tenantId, body.patch || {}, me.name || "");
      if (!updated) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      json(res, 200, { ok: true, property: updated });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath.startsWith("/user/properties/") && urlPath.endsWith("/images") && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = urlPath.slice("/user/properties/".length, -"/images".length);
    const prop = await getPropertyByIdForUser(id, me.tenantId);
    if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
    try {
      const body = await readBody(req);
      const r = await addPropertyImage(id, me.tenantId, body || {}, me.name || "");
      json(res, 200, { ok: true, imageId: r.id });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath.startsWith("/user/properties/") && urlPath.includes("/images/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const tail = urlPath.slice("/user/properties/".length);
    const i = tail.indexOf("/images/");
    const pid = tail.slice(0, i);
    const iid = tail.slice(i + "/images/".length);
    try {
      await deletePropertyImage(pid, me.tenantId, decodeURIComponent(iid), me.name || "");
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ PUT /user/properties/:id ═══════════════════
  if (urlPath.startsWith("/user/properties/") && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const id = urlPath.replace("/user/properties/", "");
    const { name, databaseId, databasePassword, tableName } = await readBody(req);
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const existing = await getPropertyByIdForUser(id, me.tenantId);
    if (!existing) { json(res, 404, { ok: false }); return; }
    const updated = await updateProperty(id, me.tenantId, { name, databaseId, databasePassword, tableName });
    json(res, 200, { ok: true, property: updated });
    return;
  }

  // ══ DELETE /user/properties/:id ════════════════
  if (urlPath.startsWith("/user/properties/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const id = urlPath.replace("/user/properties/", "");
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    await deleteProperty(id, me.tenantId);
    await userStore.clearLoginDefaultPropertyId(id);
    // アクティブ物件が削除されたらリセット
    const users = await loadUsers();
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui >= 0 && users[ui].activePropertyId === id) {
      const remainingAll = await listPropertiesByUser(me.tenantId);
      const remaining = visiblePropsForUser(me, remainingAll);
      users[ui].activePropertyId = remaining.length ? remaining[0].id : null;
      await userStore.updateUser(users[ui]);
    }
    json(res, 200, { ok: true });
    return;
  }

  // ══ POST /user/select-property ════════════════
  if (urlPath === "/user/select-property" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const { propertyId } = await readBody(req);
    const users = await loadUsers();
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui >= 0) {
      users[ui].activePropertyId = propertyId;
      await userStore.updateUser(users[ui]);
    }
    json(res, 200, { ok: true });
    return;
  }

  // ══ POST /user/login-default-property（ログイン時に表示する物件） ══
  if (urlPath === "/user/login-default-property" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const body = await readBody(req);
      const raw = body.propertyId;
      const pid = raw == null || raw === "" ? null : String(raw);
      const propsAll = await listPropertiesByUser(me.tenantId);
      const visible = visiblePropsForUser(me, propsAll);
      if (pid && !visible.some((p) => String(p.id) === String(pid))) {
        json(res, 400, { ok: false, message: "指定の物件にアクセスできません" });
        return;
      }
      const users = await loadUsers();
      const mid = String(me.id);
      const ui = users.findIndex((u) => String(u.id) === mid);
      if (ui < 0) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
      users[ui].loginDefaultPropertyId = pid;
      if (pid) users[ui].activePropertyId = pid;
      await userStore.updateUser(users[ui]);
      json(res, 200, { ok: true, user: publicUser(users[ui]) });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /user/change-password ═════════════════
  if (urlPath === "/user/change-password" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const { currentPassword, newPassword } = await readBody(req);
    const users = await loadUsers();
    const ui = users.findIndex(u => u.id === session.userId);
    if (ui < 0 || users[ui].password !== sha256(currentPassword)) {
      json(res, 400, { ok: false, message: "現在のパスワードが正しくありません" }); return;
    }
    users[ui].password = sha256(newPassword);
    users[ui].mustChangePassword = false;
    await userStore.updateUser(users[ui]);
    json(res, 200, { ok: true, user: publicUser(users[ui]) });
    return;
  }

  // ══ GET/POST /user/accounts ════════════════════
  if (urlPath === "/user/accounts" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const users = (await loadUsers()).filter(
      (u) => String(u.tenantId) === String(me.tenantId)
    );
    let visible;
    if (isPropertyScopedRole(me.role)) {
      visible = users.filter((u) => String(u.id) === String(me.id));
    } else if (isMaster(me.role)) {
      visible = users;
    } else if (normalizeRole(me.role) === ROLE.CLIENT_ADMIN) {
      const mc = String(me.client || "").trim();
      visible = mc
        ? users.filter((u) => String(u.client || "").trim() === mc)
        : users.filter((u) => String(u.id) === String(me.id));
    } else {
      visible = users;
    }
    json(res, 200, { ok: true, accounts: visible.map(publicUser) });
    return;
  }

  if (urlPath === "/user/mail-status" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    json(res, 200, { ok: true, smtpConfigured: hasSmtpEnv(process.env) });
    return;
  }

  if (urlPath === "/user/mail-test" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    if (!hasSmtpEnv(process.env)) { json(res, 400, { ok: false, message: "SMTP設定が未登録です" }); return; }
    const to = String(me.email || "").trim();
    if (!to) { json(res, 400, { ok: false, message: "ログイン中のアカウントにメールアドレスがありません" }); return; }
    try {
      const out = await sendSmtpTestMail(process.env, { to });
      json(res, 200, { ok: true, sent: out.sent, messageId: out.messageId || "" });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || "テストメール送信に失敗しました" });
    }
    return;
  }

  if (urlPath === "/user/accounts" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageAccounts(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const body = await readBody(req);
    const err = (m) => json(res, 400, { ok: false, message: m });

    const name = String(body.name || "").trim();
    const email = String(body.email || "").trim().toLowerCase();
    const role = Number(body.role);
    const client = String(body.client || "").trim();
    const password = String(body.password || "");
    const passwordConfirm = String(body.passwordConfirm || "");
    const propertyIds = Array.isArray(body.propertyIds) ? [...new Set(body.propertyIds.map(String))] : [];

    if (!name) return err("担当者名は必須です");
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return err("E-mail が不正です");
    const users = await loadUsers();
    if (users.some((u) => u.email.toLowerCase() === email)) return err("このE-mailは既に登録されています");
    if (![ROLE.MASTER, ROLE.CLIENT_ADMIN, ROLE.PROPERTY_MANAGER].includes(role))
      return err("権限を選択してください");
    if (role === ROLE.MASTER && !isMaster(me.role)) {
      json(res, 403, { ok: false, message: "マスター権限の付与はマスターアカウントのみが行えます" });
      return;
    }
    if (!client) return err("クライアントは必須です");
    const pwdTrim = String(password || "").trim();
    const pwd2Trim = String(passwordConfirm || "").trim();
    let passwordHash;
    if (!pwdTrim) {
      passwordHash = sha256(randomTempPassword(12));
    } else {
      if (pwdTrim.length < 8) return err("パスワードは8文字以上にしてください");
      if (pwdTrim !== pwd2Trim) return err("パスワード（確認）が一致しません");
      passwordHash = sha256(pwdTrim);
    }
    if (isPropertyScopedRole(role) && propertyIds.length === 0)
      return err("物件管理者は担当物件を1件以上選択してください");
    if (!isMaster(me.role) && normalizeRole(me.role) === ROLE.CLIENT_ADMIN) {
      const mc = String(me.client || "").trim();
      if (!mc || String(client).trim() !== mc) {
        json(res, 403, { ok: false, message: "自分のクライアントに属するアカウントのみ作成できます" });
        return;
      }
    }

    const newUser = normalizeUser({
      id: crypto.randomUUID(),
      tenantId: me.tenantId,
      client,
      name,
      email,
      role,
      propertyIds: isPropertyScopedRole(role) ? propertyIds : [],
      password: passwordHash,
      activePropertyId: isPropertyScopedRole(role) ? propertyIds[0] || null : null,
      mustChangePassword: !pwdTrim,
    });
    users.push(newUser);
    await saveUsers(users);
    json(res, 200, { ok: true, account: publicUser(newUser) });
    return;
  }

  // ══ GET /api/properties/:propertyId/analysis ═══════════════════
  if (urlPath.startsWith("/api/properties/") && urlPath.endsWith("/analysis") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const head = "/api/properties/";
      const tail = "/analysis";
      const propertyId = decodeURIComponent(urlPath.slice(head.length, -tail.length));
      const prop = await getPropertyByIdForUser(propertyId, me.tenantId);
      if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      if (!role3MayAccessPropertyId(me, propertyId)) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
      const weekStart = String(fullUrl.searchParams.get("weekStart") || "").trim();
      const weekEnd = String(fullUrl.searchParams.get("weekEnd") || "").trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(weekStart) || !/^\d{4}-\d{2}-\d{2}$/.test(weekEnd)) {
        json(res, 400, { ok: false, message: "weekStart/weekEnd は YYYY-MM-DD で指定してください" });
        return;
      }
      if (weekStart > weekEnd) {
        json(res, 400, { ok: false, message: "weekStart は weekEnd 以下にしてください" });
        return;
      }
      const report = await buildCustomerAnalysisReport({
        propertyId,
        property: prop,
        weekStart,
        weekEnd,
      });
      json(res, 200, { ok: true, ...report });
    } catch (e) {
      console.error("[analysis]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || "分析データの取得に失敗しました" });
    }
    return;
  }

  // ══ GET /api/properties/:propertyId/weekly-report/download ═════
  if (urlPath.startsWith("/api/properties/") && urlPath.endsWith("/weekly-report/download") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const head = "/api/properties/";
      const tail = "/weekly-report/download";
      const propertyId = decodeURIComponent(urlPath.slice(head.length, -tail.length));
      const prop = await getPropertyByIdForUser(propertyId, me.tenantId);
      if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      if (!role3MayAccessPropertyId(me, propertyId)) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }

      const startDate = String(fullUrl.searchParams.get("startDate") || "").trim();
      const endDate = String(fullUrl.searchParams.get("endDate") || "").trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate) || !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
        json(res, 400, { ok: false, message: "startDate/endDate は YYYY-MM-DD 形式で指定してください" });
        return;
      }
      if (startDate > endDate) {
        json(res, 400, { ok: false, message: "startDate は endDate 以下にしてください" });
        return;
      }
      const ms = new Date(`${endDate}T00:00:00`).getTime() - new Date(`${startDate}T00:00:00`).getTime();
      if (ms > 180 * 24 * 60 * 60 * 1000) {
        json(res, 400, { ok: false, message: "期間は最大180日以内で指定してください" });
        return;
      }

      const buf = await buildWeeklyReportBuffer({ property: prop, startDate, endDate });
      const name = `${String(prop.name || "物件").replace(/[\\/:*?"<>|]/g, "_")}_週間報告書_${startDate.replace(/-/g, "")}_${endDate.replace(/-/g, "")}.xlsx`;
      res.writeHead(200, {
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition": `attachment; filename*=UTF-8''${encodeURIComponent(name)}`,
      });
      res.end(Buffer.from(buf));
    } catch (e) {
      console.error("[weekly-report download]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || "週報生成に失敗しました" });
    }
    return;
  }

  // ══ PUT/DELETE /user/accounts/:id ═══════════════
  if (urlPath.startsWith("/user/accounts/") && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const id = decodeURIComponent(urlPath.slice("/user/accounts/".length));
    const body = await readBody(req);
    const users = await loadUsers();
    const idx = users.findIndex((u) => u.id === id && u.tenantId === me.tenantId);
    if (idx < 0) { json(res, 404, { ok: false }); return; }
    if (isPropertyScopedRole(me.role) && id !== me.id) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    if (!isMaster(me.role) && normalizeRole(me.role) === ROLE.CLIENT_ADMIN) {
      const mc = String(me.client || "").trim();
      if (!mc || String(users[idx].client || "").trim() !== mc) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
      if (String(client).trim() !== mc) {
        json(res, 403, { ok: false, message: "クライアント（企業）の変更はできません" });
        return;
      }
    }
    if (isMaster(users[idx].role) && !isMaster(me.role)) {
      json(res, 403, { ok: false, message: "マスターアカウントの編集はマスターのみが行えます" });
      return;
    }
    const prevRole = normalizeRole(users[idx].role || 2);

    const next = { ...users[idx] };
    const name = String(body.name ?? next.name).trim();
    const email = String(body.email ?? next.email).trim().toLowerCase();
    const role = body.role != null ? Number(body.role) : next.role;
    const client = String(body.client ?? next.client).trim();
    const propertyIds = Array.isArray(body.propertyIds) ? [...new Set(body.propertyIds.map(String))] : next.propertyIds;
    const password = String(body.password || "");
    const passwordConfirm = String(body.passwordConfirm || "");

    if (!name) { json(res, 400, { ok: false, message: "担当者名は必須です" }); return; }
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) { json(res, 400, { ok: false, message: "E-mail が不正です" }); return; }
    if (users.some((u) => u.id !== id && u.email.toLowerCase() === email)) { json(res, 400, { ok: false, message: "このE-mailは既に登録されています" }); return; }
    if (![ROLE.MASTER, ROLE.CLIENT_ADMIN, ROLE.PROPERTY_MANAGER].includes(role)) {
      json(res, 400, { ok: false, message: "権限を選択してください" });
      return;
    }
    if (role === ROLE.MASTER && !isMaster(me.role)) {
      json(res, 403, { ok: false, message: "マスター権限の付与はマスターアカウントのみが行えます" });
      return;
    }
    if (!isMaster(me.role) && isMaster(users[idx].role) && role !== ROLE.MASTER) {
      json(res, 403, { ok: false, message: "マスター権限の変更はマスターのみが行えます" });
      return;
    }
    if (!client) { json(res, 400, { ok: false, message: "クライアントは必須です" }); return; }
    if (isPropertyScopedRole(role) && (!propertyIds || propertyIds.length === 0)) {
      json(res, 400, { ok: false, message: "物件管理者は担当物件を1件以上選択してください" });
      return;
    }
    if (password) {
      if (password.length < 8) { json(res, 400, { ok: false, message: "パスワードは8文字以上にしてください" }); return; }
      if (password !== passwordConfirm) { json(res, 400, { ok: false, message: "パスワード（確認）が一致しません" }); return; }
      next.password = sha256(password);
      next.mustChangePassword = false;
    }
    next.name = name;
    next.email = email;
    next.role = role;
    next.client = client;
    next.propertyIds = isPropertyScopedRole(role) ? propertyIds : [];
    if (isPropertyScopedRole(prevRole) && !isPropertyScopedRole(role)) next.activePropertyId = null;
    if (isPropertyScopedRole(next.role) && next.activePropertyId) {
      const allowed = new Set((next.propertyIds || []).map(String));
      if (!allowed.has(String(next.activePropertyId))) {
        next.activePropertyId = next.propertyIds[0] || null;
      }
    }

    users[idx] = normalizeUser(next);
    await saveUsers(users);
    json(res, 200, { ok: true, account: publicUser(users[idx]) });
    return;
  }

  if (urlPath.startsWith("/user/accounts/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageAccounts(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(urlPath.slice("/user/accounts/".length));
    if (id === me.id) { json(res, 400, { ok: false, message: "自分自身は削除できません" }); return; }
    const users = await loadUsers();
    const victim = users.find((u) => u.id === id && u.tenantId === me.tenantId);
    if (!victim) { json(res, 404, { ok: false, message: "対象が見つかりません" }); return; }
    if (!isMaster(me.role) && normalizeRole(me.role) === ROLE.CLIENT_ADMIN) {
      const mc = String(me.client || "").trim();
      if (!mc || String(victim.client || "").trim() !== mc) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
    }
    if (isMaster(victim.role)) {
      json(res, 400, { ok: false, message: "マスター権限のユーザーは削除できません" });
      return;
    }
    await saveUsers(users.filter((u) => !(u.id === id && u.tenantId === me.tenantId)));
    json(res, 200, { ok: true });
    return;
  }

  if (urlPath.startsWith("/user/accounts/") && urlPath.endsWith("/invite") && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!canManageAccounts(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(urlPath.slice("/user/accounts/".length, -"/invite".length));
    const users = await loadUsers();
    const target = users.find((u) => u.id === id && String(u.tenantId) === String(me.tenantId));
    if (!target) { json(res, 404, { ok: false, message: "対象アカウントが見つかりません" }); return; }
    if (!isMaster(me.role) && normalizeRole(me.role) === ROLE.CLIENT_ADMIN) {
      const mc = String(me.client || "").trim();
      if (!mc || String(target.client || "").trim() !== mc) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
    }
    if (!target.email) { json(res, 400, { ok: false, message: "メールアドレスが未設定です" }); return; }
    if (!hasSmtpEnv(process.env)) { json(res, 400, { ok: false, message: "SMTP設定が未登録です" }); return; }
    const body = await readBody(req);
    const loginUrl = String(body.loginUrl || ENV.APP_LOGIN_URL || `${body.origin || ""}/login.html`).trim();
    let tempPassword = String(body.tempPassword || "").trim();
    const idxInvite = users.findIndex((u) => u.id === target.id);
    if (!tempPassword) {
      tempPassword = randomTempPassword(12);
      if (idxInvite >= 0) {
        users[idxInvite] = normalizeUser({
          ...users[idxInvite],
          password: sha256(tempPassword),
          mustChangePassword: true,
        });
        await saveUsers(users);
      }
    } else if (idxInvite >= 0) {
      users[idxInvite] = normalizeUser({
        ...users[idxInvite],
        password: sha256(tempPassword),
        mustChangePassword: true,
      });
      await saveUsers(users);
    }
    try {
      const out = await sendAccountInviteMail(process.env, {
        name: target.name || "",
        email: target.email,
        loginUrl,
        tempPassword,
      });
      json(res, 200, { ok: true, sent: out.sent, messageId: out.messageId || "" });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || "招待メール送信に失敗しました" });
    }
    return;
  }

  // ══ GET /media-assets ═══════════════════════════
  if (urlPath === "/media-assets" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    try {
      const result = await listMediaAssets(me.tenantId, {
        assetType: fullUrl.searchParams.get("assetType") || "",
        search: fullUrl.searchParams.get("search") || "",
        page: fullUrl.searchParams.get("page") || "1",
        limit: fullUrl.searchParams.get("limit") || "40",
      });
      const props = await listPropertiesByUser(me.tenantId);
      const pMap = new Map((props || []).map((p) => [p.id, p]));
      const activeProp = await getActivePropertyForUser(session.userId);
      const data = await Promise.all((result.data || []).map(async (x) => {
        const prop = (x.propertyId && pMap.get(x.propertyId)) || activeProp;
        const cfg = s3ConfigFromProperty(prop, process.env);
        const fileUrl = (cfg && x.fileKey)
          ? await createReadSignedUrl({ bucketName: cfg.bucketName, key: x.fileKey, expiresIn: 3600, region: cfg.region }, process.env)
          : (x.fileUrl || "");
        const thumbnailUrl = String(x.mimeType || "").startsWith("image/") ? fileUrl : "";
        return {
          ...x,
          fileUrl,
          thumbnailUrl,
          property: x.propertyId ? { id: x.propertyId, name: pMap.get(x.propertyId)?.name || `物件(${x.propertyId})` } : null,
        };
      }));
      json(res, 200, { ok: true, data, total: result.total, page: result.page, limit: result.limit });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || "素材取得に失敗しました" });
    }
    return;
  }
  if (urlPath === "/media-asset-types" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const types = await listMediaAssetTypes(me.tenantId);
    json(res, 200, { ok: true, types });
    return;
  }
  if (urlPath === "/media-asset-types" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readBody(req);
      const created = await addMediaAssetType(me.tenantId, body || {});
      json(res, 200, { ok: true, type: created });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "種別追加に失敗しました" });
    }
    return;
  }
  if (urlPath.startsWith("/media-asset-types/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const typeKey = decodeURIComponent(urlPath.slice("/media-asset-types/".length));
      await deleteMediaAssetType(me.tenantId, typeKey);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "種別削除に失敗しました" });
    }
    return;
  }
  if (urlPath === "/media-assets/presigned-url" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readBody(req);
      const prop = await getActivePropertyForUser(session.userId);
      const cfg = s3ConfigFromProperty(prop, process.env);
      if (!cfg) {
        json(res, 400, {
          ok: false,
          message: "アクティブ物件が無いか、この物件に S3 バケットが未設定です。物件詳細でバケットを登録し、ヘッダーで物件を選択してください。",
        });
        return;
      }
      const fileName = String(body.fileName || "").trim();
      const mimeType = String(body.mimeType || "").trim();
      const fileSize = Number(body.fileSize || 0);
      if (!fileName || !mimeType || !fileSize) { json(res, 400, { ok: false, message: "fileName/mimeType/fileSize は必須です" }); return; }
      const key = buildAssetKey(prop.id, fileName);
      await ensureBucketCors({
        bucketName: cfg.bucketName,
        region: cfg.region,
        requestOrigin: req.headers.origin || "",
      }, process.env);
      const signed = await createUploadPresignedUrl({
        bucketName: cfg.bucketName,
        region: cfg.region,
        key,
        mimeType,
        fileSize,
        userId: me.id,
        accountId: me.id,
      }, process.env);
      json(res, 200, { ok: true, presignedUrl: signed.presignedUrl, key });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "署名付きURLの発行に失敗しました" });
    }
    return;
  }
  if (urlPath === "/media-assets/confirm-upload" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readBody(req);
      const key = String(body.key || "").trim();
      if (!key) { json(res, 400, { ok: false, message: "key は必須です" }); return; }
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop || !prop.id) { json(res, 400, { ok: false, message: "アクティブ物件が選択されていません。ヘッダーで物件を選んでください。" }); return; }
      const created = await createMediaAssetRecord(me.tenantId, me.id, {
        id: crypto.randomUUID(),
        name: body.fileName || "file",
        mimeType: body.mimeType || "application/octet-stream",
        fileSize: Number(body.fileSize || 0),
        fileKey: key,
        filePath: key,
        thumbnailUrl: "",
        propertyId: prop.id,
        assetType: body.assetType || "other",
        tags: body.tags || [],
        memo: body.memo || "",
      });
      json(res, 200, { ok: true, asset: created });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "アップロード完了処理に失敗しました" });
    }
    return;
  }
  if (urlPath === "/media-assets/upload-direct" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readBody(req);
      const prop = await getActivePropertyForUser(session.userId);
      const cfg = s3ConfigFromProperty(prop, process.env);
      if (!cfg) { json(res, 400, { ok: false, message: "S3設定が見つかりません。物件詳細をご確認ください。" }); return; }
      const fileName = String(body.fileName || "").trim();
      const mimeType = String(body.mimeType || "application/octet-stream").trim();
      const fileSize = Number(body.fileSize || 0);
      const dataBase64 = String(body.dataBase64 || "");
      if (!fileName || !fileSize || !dataBase64) { json(res, 400, { ok: false, message: "fileName/fileSize/dataBase64 は必須です" }); return; }
      const bin = Buffer.from(dataBase64, "base64");
      if (!bin.length) { json(res, 400, { ok: false, message: "ファイルデータが空です" }); return; }
      const key = buildAssetKey(prop.id, fileName);
      await uploadObjectDirect({ bucketName: cfg.bucketName, key, mimeType, body: bin, region: cfg.region }, process.env);
      const created = await createMediaAssetRecord(me.tenantId, me.id, {
        id: crypto.randomUUID(),
        name: fileName,
        mimeType,
        fileSize,
        fileKey: key,
        filePath: key,
        thumbnailUrl: "",
        propertyId: prop.id,
        assetType: body.assetType || "other",
        tags: body.tags || [],
        memo: body.memo || "",
      });
      json(res, 200, { ok: true, asset: created, key });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "サーバー経由アップロードに失敗しました" });
    }
    return;
  }
  if (urlPath.startsWith("/media-assets/") && req.method === "PATCH") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const id = decodeURIComponent(urlPath.slice("/media-assets/".length));
      const body = await readBody(req);
      const updated = await patchMediaAsset(me.tenantId, id, body);
      if (!updated) { json(res, 404, { ok: false, message: "素材が見つかりません" }); return; }
      json(res, 200, { ok: true, asset: updated });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "更新に失敗しました" });
    }
    return;
  }
  if (urlPath.startsWith("/media-assets/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(urlPath.slice("/media-assets/".length));
    const target = await getMediaAssetById(me.tenantId, id);
    if (!target) { json(res, 404, { ok: false, message: "素材が見つかりません" }); return; }
    try {
      let delProp = target.propertyId ? await getPropertyByIdForUser(String(target.propertyId), me.tenantId) : null;
      if (!delProp) delProp = await getActivePropertyForUser(session.userId);
      const cfg = s3ConfigFromProperty(delProp, process.env);
      if (cfg && target.fileKey) {
        await removeObject({ bucketName: cfg.bucketName, key: target.fileKey, region: cfg.region }, process.env);
      }
    } catch (_) {}
    await deleteMediaAsset(me.tenantId, id);
    json(res, 200, { ok: true });
    return;
  }

  // ══ GET /schedule/rooms ═════════════════════════
  if (urlPath === "/schedule/rooms" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const rooms = await listRooms(prop.id);
    json(res, 200, { ok: true, rooms });
    return;
  }

  // ══ GET/PUT /schedule/settings ══════════════════
  if (urlPath === "/schedule/settings" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const settings = await getScheduleSettings(prop.id);
    json(res, 200, { ok: true, settings });
    return;
  }
  if (urlPath === "/schedule/settings" && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const body = await readBody(req);
      const settings = await saveScheduleSettings(prop.id, body || {});
      json(res, 200, { ok: true, settings });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET /schedule/events ════════════════════════
  if (urlPath === "/schedule/events" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    const prop = await getActivePropertyForUser(session.userId);
    if (!me || !prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const u = new URL(req.url || "/", "http://127.0.0.1");
      const f = {
        start: u.searchParams.get("start"),
        end: u.searchParams.get("end"),
        roomId: u.searchParams.get("roomId"),
        staffId: u.searchParams.get("staffId"),
        keyword: u.searchParams.get("keyword"),
      };
      const vf = u.searchParams.get("vf");
      if (vf === "pending") f.onlyPending = true;
      if (vf === "confirmed") f.onlyConfirmed = true;
      const statusValues = (u.searchParams.get("statusValues") || "").split(",").map((v) => v.trim()).filter(Boolean);
      if (statusValues.length) f.statusValues = statusValues;

      let rows = await listSchedules(prop.id, f);
      // 担当者は「自分担当」または「pending」のみ
      if (isPropertyScopedRole(me.role)) rows = rows.filter((r) => r.status === "pending" || String(r.staffId || "") === String(me.id));
      json(res, 200, { ok: true, events: rows });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /schedule/events（手動登録） ═══════════
  if (urlPath === "/schedule/events" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const body = await readBody(req);
      const out = await createSchedule(prop.id, { ...body, source: body.source || "manual" });
      json(res, 200, { ok: true, warning: out.warning || "" });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ PUT /schedule/events/:id（更新/確定） ════════
  if (urlPath.startsWith("/schedule/events/") && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    const prop = await getActivePropertyForUser(session.userId);
    if (!me || !prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const id = decodeURIComponent(urlPath.slice("/schedule/events/".length));
    try {
      const body = await readBody(req);
      // 担当者は確定操作時に自分を担当者としてセット
      if (isPropertyScopedRole(me.role) && body.status === "confirmed" && !body.staffId) body.staffId = me.id;
      await updateSchedule(prop.id, id, body);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ DELETE /schedule/events/:id ═════════════════
  if (urlPath.startsWith("/schedule/events/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const id = decodeURIComponent(urlPath.slice("/schedule/events/".length));
    try {
      await deleteSchedule(prop.id, id);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /schedule/intake（フォーム自動連携） ═══
  if (urlPath === "/schedule/intake" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const body = await readBody(req);
      const settings = await getScheduleSettings(prop.id);
      const slotMin = Number(body.autoSlotMinutes || settings.autoSlotMinutes || 60);
      const start = String(body.startTime || "10:00");
      const [h, m] = start.split(":").map(Number);
      const d = new Date(2000, 0, 1, h, m, 0);
      d.setMinutes(d.getMinutes() + slotMin);
      const end = `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
      const out = await createSchedule(prop.id, {
        date: body.date,
        startTime: start,
        endTime: end,
        status: "pending",
        source: "form",
        roomId: null,
        staffId: null,
        participants: body.participants || 1,
        customerNameSei: body.customerNameSei || "",
        customerNameMei: body.customerNameMei || "",
        customerKanaSei: body.customerKanaSei || "",
        customerKanaMei: body.customerKanaMei || "",
        customerTel: body.customerTel || "",
        customerEmail: body.customerEmail || "",
        customerStatus: body.customerStatus ?? "",
      });
      json(res, 200, { ok: true, warning: out.warning || "" });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /sync/customers（デジタライズ → ローカルDB） ══
  if (urlPath === "/sync/customers" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const me = await currentUserFromSession(session);
      if (!me) { json(res, 401, { ok: false, message: "認証が必要です。" }); return; }
      const body = await readBody(req);
      const mode = body.mode === "full" ? "full" : "incremental";
      if (mode === "full" && !isMaster(me.role)) {
        json(res, 403, { ok: false, message: "全件同期はマスター権限のみ実行できます。" });
        return;
      }
      const result = await runCustomerSync(prop, ENV, mode);
      json(res, 200, result);
    } catch (e) {
      console.error("[sync/customers]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET /local/customers（DBキャッシュ参照・通信なし） ══
  if (urlPath === "/local/customers" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const u = new URL(req.url || "/", "http://127.0.0.1");
      const qp = {};
      u.searchParams.forEach((v, k) => { qp[k] = v; });
      const pack = await queryLocalCustomers(prop.id, qp);
      if (String(qp.diag || "").trim() === "1" && pack.data && pack.data._diag) {
        console.info("[REM customer-list diag]", JSON.stringify(pack.data._diag));
      }
      json(res, 200, { ok: true, data: pack.data });
    } catch (e) {
      console.error("[local/customers]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET /local/sync-status ═══════════════════════
  if (urlPath === "/local/sync-status" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 200, { ok: true, rows_total: 0, hasData: false }); return; }
      const st = await getSyncStatus(prop.id);
      json(res, 200, { ok: true, ...st });
    } catch (e) {
      console.error("[local/sync-status]", e.stack || e);
      json(res, 200, {
        ok: true,
        hasData: false,
        rows_total: 0,
        dbQueryError: e.message || String(e),
      });
    }
    return;
  }

  // ══ GET/PUT /local/visit-statuses（案件＝アクティブ物件ごと） ══
  if (urlPath === "/local/visit-statuses" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 200, { ok: true, statuses: DEFAULT_VISIT_STATUSES }); return; }
      const arr = await getVisitStatuses(prop.id);
      json(res, 200, { ok: true, statuses: arr });
    } catch (e) {
      console.error("[local/visit-statuses GET]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath === "/local/customers/filter-options" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 200, { ok: true, options: {} }); return; }
      const options = await getCustomerFilterOptions(prop.id);
      json(res, 200, { ok: true, options });
    } catch (e) {
      console.error("[local/customers/filter-options]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  if (urlPath === "/local/property-sales-metrics" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const totalUnitsRaw = fullUrl.searchParams.get("totalUnits") || "";
      const metrics = await getPropertySalesMetrics(prop.id, totalUnitsRaw);
      json(res, 200, { ok: true, metrics });
    } catch (e) {
      console.error("[local/property-sales-metrics]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  if (urlPath === "/local/weekly-stats" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const propertyId = String(fullUrl.searchParams.get("propertyId") || "").trim();
      const start = String(fullUrl.searchParams.get("start") || "").trim();
      const end = String(fullUrl.searchParams.get("end") || "").trim();
      if (!propertyId || !start || !end) {
        json(res, 400, { ok: false, message: "propertyId, start, end は必須です" });
        return;
      }
      if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
        json(res, 400, { ok: false, message: "start/end は YYYY-MM-DD で指定してください" });
        return;
      }
      if (start > end) {
        json(res, 400, { ok: false, message: "start は end 以下にしてください" });
        return;
      }
      const weekStartDayQ = fullUrl.searchParams.get("weekStartDay");
      if (weekStartDayQ != null && weekStartDayQ !== "") {
        const exp = parseInt(weekStartDayQ, 10);
        if (exp === 0 || exp === 1) {
          const dow = ymdUtcWeekday(start);
          if (dow !== null && dow !== exp) {
            json(res, 400, {
              ok: false,
              message: `週の開始日が一致しません（${start} は${["日", "月", "火", "水", "木", "金", "土"][dow]}曜。週の開始は ${exp === 1 ? "月曜" : "日曜"} に合わせてください）`,
            });
            return;
          }
        }
      }
      const prop = await getPropertyByIdForUser(propertyId, me.tenantId);
      if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      if (!role3MayAccessPropertyId(me, propertyId)) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
      const data = await computeDashboardWeekStats(prop, start, end);
      json(res, 200, data);
    } catch (e) {
      console.error("[local/weekly-stats]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || "集計に失敗しました" });
    }
    return;
  }

  if (urlPath === "/local/weekly-report-template/download" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const body = await readBody(req);
      const propertyId = String(body.propertyId || "").trim();
      const weekStart = String(body.weekStart || "").trim();
      const weekEnd = String(body.weekEnd || "").trim();
      if (!propertyId || !weekStart || !weekEnd) {
        json(res, 400, { ok: false, message: "propertyId, weekStart, weekEnd は必須です" });
        return;
      }
      const prop = await getPropertyByIdForUser(propertyId, me.tenantId);
      if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      if (!role3MayAccessPropertyId(me, propertyId)) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
      const buf = await buildWeeklyReportTemplateBuffer({
        property: prop,
        weekStart,
        weekEnd,
        detailExcelStatuses:
          body.detailExcelStatuses !== undefined && Array.isArray(body.detailExcelStatuses)
            ? body.detailExcelStatuses
            : null,
        texts: {
          adLastWeek: body.adLastWeek,
          salesLastWeek: body.salesLastWeek,
          adThisWeek: body.adThisWeek,
          salesThisWeek: body.salesThisWeek,
          request: body.request,
          notes: body.notes,
        },
      });
      const name = `${String(prop.name || "物件").replace(/[\\/:*?"<>|]/g, "_")}_週間報告書_${weekStart.replace(/-/g, "")}.xlsx`;
      res.writeHead(200, {
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition": `attachment; filename*=UTF-8''${encodeURIComponent(name)}`,
      });
      res.end(Buffer.from(buf));
    } catch (e) {
      console.error("[local/weekly-report-template/download]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || "Excel の生成に失敗しました" });
    }
    return;
  }

  if (urlPath === "/local/customers/label-print" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const body = await readBody(req);
      const customers = Array.isArray(body?.customers) ? body.customers : [];
      if (!customers.length) {
        json(res, 400, { ok: false, message: "customers が空です" });
        return;
      }
      const templatePath = path.join(__dirname, "assets", "template.xlsx");
      const out = await buildLabelWorkbookWithOpenpyxl(templatePath, customers);
      const filename = `label_print_${new Date().toISOString().slice(0, 10)}.xlsx`;
      res.writeHead(200, {
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition": `attachment; filename="${filename}"`,
      });
      res.end(out);
    } catch (e) {
      console.error("[local/customers/label-print]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath === "/local/visit-statuses" && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const body = await readBody(req);
      const saved = await saveVisitStatuses(prop.id, body.statuses);
      json(res, 200, { ok: true, statuses: saved });
    } catch (e) {
      console.error(e);
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET /local/customers/field-spec-keys ═══════
  if (urlPath === "/local/customers/field-spec-keys" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    json(res, 200, {
      ok: true,
      keys: CUSTOMER_SNAPSHOT_FIELDS,
      fields: getCustomerSpecFieldDefs(),
    });
    return;
  }

  // ══ GET/PUT /local/customers/columns ═══════════
  if (urlPath === "/local/customers/columns" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const targetUserId = fullUrl.searchParams.get("userId") || me.id;
    if (!isAdminLike(me.role) && targetUserId !== me.id) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const columns = await getCustomerListColumns(me.tenantId, targetUserId);
    const prop = await getActivePropertyForUser(session.userId);
    const availableFields = prop
      ? await listCustomerAvailableFields(prop.id, 500, { prop, secrets: ENV })
      : [];
    json(res, 200, {
      ok: true,
      columns,
      defaults: DEFAULT_CUSTOMER_COLUMNS,
      availableFields,
      specFieldKeys: CUSTOMER_SNAPSHOT_FIELDS,
      specFields: getCustomerSpecFieldDefs(),
    });
    return;
  }
  if (urlPath === "/local/customers/columns" && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const body = await readBody(req);
    const targetUserId = String(body.userId || me.id);
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "管理者のみ設定できます" }); return; }
    try {
      const saved = await saveCustomerListColumns(me.tenantId, targetUserId, body.columns || []);
      json(res, 200, { ok: true, columns: saved });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET/PUT /local/customers/detail-fields ═════
  if (urlPath === "/local/customers/detail-fields" && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    const targetUserId = fullUrl.searchParams.get("userId") || me.id;
    if (!isAdminLike(me.role) && targetUserId !== me.id) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const fields = await getCustomerDetailFieldConfig(me.tenantId, targetUserId);
    json(res, 200, { ok: true, fields, defaults: DEFAULT_CUSTOMER_DETAIL_FIELDS });
    return;
  }
  if (urlPath === "/local/customers/detail-fields" && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "管理者のみ設定できます" }); return; }
    const body = await readBody(req);
    const targetUserId = String(body.userId || me.id);
    try {
      const saved = await saveCustomerDetailFieldConfig(me.tenantId, targetUserId, body.fields || []);
      json(res, 200, { ok: true, fields: saved });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /local/customers/export-karte-excel（ExcelJS・スタイル付き顧客カルテ） ══
  if (urlPath === "/local/customers/export-karte-excel" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const body = await readBody(req);
      const rows = Array.isArray(body.rows) ? body.rows : [];
      if (rows.length > 800) { json(res, 400, { ok: false, message: "出力項目が多すぎます（800件以内）" }); return; }
      const safeRows = rows.map((x) => ({
        label: String(x.label == null ? "" : x.label).slice(0, 500),
        value: String(x.value == null ? "" : x.value).slice(0, 32000),
      }));
      const buf = await buildCustomerKarteExcelBuffer({
        title: body.title,
        jaDate: body.jaDate,
        rows: safeRows,
      });
      const rawId = String(body.customerId || "detail").replace(/[\\/:*?"<>|]/g, "_");
      const ts = String(body.ts || "").replace(/[^\d]/g, "").slice(0, 20) || String(Date.now());
      const name = `customer_export_${rawId.slice(0, 64)}_${ts}.xlsx`;
      res.writeHead(200, {
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition": `attachment; filename*=UTF-8''${encodeURIComponent(name)}`,
      });
      res.end(Buffer.from(buf));
    } catch (e) {
      console.error("[export-karte-excel]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || "Excel生成に失敗しました" });
    }
    return;
  }

  // ══ POST /local/media-asset-upload（S3へサーバー経由・アプリ内API） ══
  if (urlPath === "/local/media-asset-upload" && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    const me = await currentUserFromSession(session);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readBody(req);
      const prop = await getActivePropertyForUser(session.userId);
      const cfg = s3ConfigFromProperty(prop, process.env);
      if (!cfg) {
        json(res, 400, { ok: false, message: "S3設定が見つかりません。物件詳細でバケットを登録し、ヘッダーで物件を選択してください。" });
        return;
      }
      const fileName = String(body.fileName || "").trim();
      const mimeType = String(body.mimeType || "application/octet-stream").trim();
      const fileSize = Number(body.fileSize || 0);
      const dataBase64 = String(body.dataBase64 || "");
      if (!fileName || !fileSize || !dataBase64) { json(res, 400, { ok: false, message: "fileName/fileSize/dataBase64 は必須です" }); return; }
      const bin = Buffer.from(dataBase64, "base64");
      if (!bin.length) { json(res, 400, { ok: false, message: "ファイルデータが空です" }); return; }
      const key = buildAssetKey(prop.id, fileName);
      await uploadObjectDirect({ bucketName: cfg.bucketName, key, mimeType, body: bin, region: cfg.region }, process.env);
      const created = await createMediaAssetRecord(me.tenantId, me.id, {
        id: crypto.randomUUID(),
        name: fileName,
        mimeType,
        fileSize,
        fileKey: key,
        filePath: key,
        thumbnailUrl: "",
        propertyId: prop.id,
        assetType: body.assetType || "other",
        tags: body.tags || [],
        memo: body.memo || "",
      });
      json(res, 200, { ok: true, asset: created, key });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "サーバー経由アップロードに失敗しました" });
    }
    return;
  }

  // ══ PATCH /local/customers/:id（キャッシュ上書き） ══
  if (urlPath.startsWith("/local/customers/") && req.method === "PATCH") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const customerId = decodeURIComponent(urlPath.slice("/local/customers/".length));
      if (!customerId) { json(res, 400, { ok: false, message: "顧客IDが不正です" }); return; }
      const body = await readBody(req);
      const result = await patchLocalCustomer(prop.id, customerId, body);
      json(res, 200, result);
    } catch (e) {
      console.error(e);
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET /local/customers/:id/history ════════════
  if (urlPath.startsWith("/local/customers/") && urlPath.endsWith("/history") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const base = urlPath.slice("/local/customers/".length, -"/history".length);
      const customerId = decodeURIComponent(base);
      const u = new URL(req.url || "/", "http://127.0.0.1");
      const limit = u.searchParams.get("limit") || "50";
      const items = await listCustomerHistory(prop.id, customerId, limit);
      json(res, 200, { ok: true, history: items });
    } catch (e) {
      console.error(e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  if (urlPath.startsWith("/local/customers/") && urlPath.endsWith("/reactions") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const base = urlPath.slice("/local/customers/".length, -"/reactions".length);
      const customerId = decodeURIComponent(base);
      const u = new URL(req.url || "/", "http://127.0.0.1");
      const limit = u.searchParams.get("limit") || "100";
      const items = await listCustomerReactions(prop.id, customerId, limit);
      json(res, 200, { ok: true, items });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET/POST /local/customers/:id/sales-history ═════════════
  if (urlPath.startsWith("/local/customers/") && urlPath.endsWith("/sales-history") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const base = urlPath.slice("/local/customers/".length, -"/sales-history".length);
      const customerId = decodeURIComponent(base);
      const u = new URL(req.url || "/", "http://127.0.0.1");
      const limit = u.searchParams.get("limit") || "100";
      const items = await listSalesHistory(prop.id, customerId, limit);
      json(res, 200, { ok: true, items });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath.startsWith("/local/customers/") && urlPath.endsWith("/sales-history") && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const base = urlPath.slice("/local/customers/".length, -"/sales-history".length);
      const customerId = decodeURIComponent(base);
      const body = await readBody(req);
      await addSalesHistory(prop.id, customerId, body || {});
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath.startsWith("/local/customers/") && urlPath.includes("/sales-history/") && req.method === "PUT") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const tail = urlPath.slice("/local/customers/".length);
      const i = tail.indexOf("/sales-history/");
      const customerId = decodeURIComponent(tail.slice(0, i));
      const salesId = decodeURIComponent(tail.slice(i + "/sales-history/".length));
      const body = await readBody(req);
      await updateSalesHistory(prop.id, customerId, salesId, body || {});
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath.startsWith("/local/customers/") && urlPath.includes("/sales-history/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const tail = urlPath.slice("/local/customers/".length);
      const i = tail.indexOf("/sales-history/");
      const customerId = decodeURIComponent(tail.slice(0, i));
      const salesId = decodeURIComponent(tail.slice(i + "/sales-history/".length));
      await deleteSalesHistory(prop.id, customerId, salesId);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ GET/POST/DELETE /local/customers/:id/files ═══════════════
  if (urlPath.startsWith("/local/customers/") && urlPath.endsWith("/files") && req.method === "GET") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const base = urlPath.slice("/local/customers/".length, -"/files".length);
      const customerId = decodeURIComponent(base);
      const u = new URL(req.url || "/", "http://127.0.0.1");
      const limit = u.searchParams.get("limit") || "100";
      const files = await listCustomerFiles(prop.id, customerId, limit);
      json(res, 200, { ok: true, files });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath.startsWith("/local/customers/") && urlPath.endsWith("/files") && req.method === "POST") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const base = urlPath.slice("/local/customers/".length, -"/files".length);
      const customerId = decodeURIComponent(base);
      const body = await readBody(req);
      const out = await addCustomerFile(prop.id, customerId, body || {});
      json(res, 200, { ok: true, ...out });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (urlPath.startsWith("/local/customers/") && urlPath.includes("/files/") && req.method === "DELETE") {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false }); return; }
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const tail = urlPath.slice("/local/customers/".length);
      const i = tail.indexOf("/files/");
      const customerId = decodeURIComponent(tail.slice(0, i));
      const fileId = decodeURIComponent(tail.slice(i + "/files/".length));
      await deleteCustomerFile(prop.id, customerId, fileId);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ══ POST /api/... → デジタライズAPIへプロキシ ══
  if (urlPath.startsWith("/api/")) {
    const session = getSession(getToken(req));
    if (!session) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }

    // アクティブ物件の認証情報を取得
    const prop = await getActivePropertyForUser(session.userId);
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
  console.log(`   停止するには Ctrl+C\n`);
});
