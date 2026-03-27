/**
 * Vercel Serverless: auth / user / デジタライズ API プロキシ
 * ルートは vercel.json の rewrite で __r クエリに渡す
 */
const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const {
  runCustomerSync,
  queryLocalCustomers,
  getSyncStatus,
  getVisitStatuses,
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
  getCustomerDetailFieldConfig,
  saveCustomerDetailFieldConfig,
  DEFAULT_CUSTOMER_DETAIL_FIELDS,
  DEFAULT_VISIT_STATUSES,
} = require("../lib/sync-service");
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
} = require("../lib/property-store");
const {
  listRooms,
  listSchedules,
  createSchedule,
  updateSchedule,
  deleteSchedule,
  getScheduleSettings,
  saveScheduleSettings,
} = require("../lib/schedule-store");
const {
  listClients,
  getClient,
  createClient,
  updateClient,
  deleteClients,
  getClientS3Settings,
  saveClientS3Settings,
} = require("../lib/client-store");
const {
  listMediaAssets,
  getMediaAssetById,
  createMediaAssetRecord,
  patchMediaAsset,
  deleteMediaAsset,
  listMediaAssetTypes,
  addMediaAssetType,
  deleteMediaAssetType,
} = require("../lib/media-assets-store");
const {
  buildAssetKey,
  createUploadPresignedUrl,
  ensureBucketCors,
  createReadSignedUrl,
  uploadObjectDirect,
  removeObject,
} = require("../lib/s3-media");
const { s3ConfigFromProperty } = require("../lib/media-s3");
const { S3Client, HeadBucketCommand } = require("@aws-sdk/client-s3");
const { sendAccountInviteMail, sendSmtpTestMail, sendLoginOtpMail, hasSmtpEnv } = require("../lib/mailer");
const { buildWeeklyReportBuffer } = require("../lib/weekly-report");
const { buildCustomerAnalysisReport } = require("../lib/customer-analysis");
const { buildCustomerKarteExcelBuffer } = require("../lib/customer-karte-export");
const { resolveDatabaseUrl, getDb } = require("../lib/sync-db");
const userStore = require("../lib/user-store");
const { applyLoginDefaultToUser } = require("../lib/login-default");
const {
  ROLE,
  normalizeRole,
  isPropertyScopedRole,
  isAdminLike,
  isMaster,
  canUsePropertyMediaRoles,
} = require("../lib/roles");

const API_HOST = "api.digital-eyes.jp";

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

function sha256(str) {
  return crypto.createHash("sha256").update(str).digest("hex");
}

function randomTempPassword(len = 12) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789";
  const buf = crypto.randomBytes(len);
  let s = "";
  for (let i = 0; i < len; i++) s += chars[buf[i] % chars.length];
  return s;
}

function publicUser(u) {
  if (!u) return null;
  return {
    id: u.id,
    name: u.name,
    email: u.email,
    role: normalizeRole(u.role ?? 2),
    tenantId: String(u.tenantId || "1"),
    client: u.client || "",
    propertyIds: Array.isArray(u.propertyIds) ? u.propertyIds.map(String) : [],
    activePropertyId: u.activePropertyId || null,
    loginDefaultPropertyId: u.loginDefaultPropertyId || null,
  };
}

async function sessionMe(sessionUserId) {
  const sid = String(sessionUserId ?? "");
  const users = await userStore.listUsers();
  return users.find((u) => String(u.id) === sid) || null;
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

function canManageAccounts(me) {
  return me && isAdminLike(me.role);
}

/** 招待メール用ログインURL（クライアントの body.loginUrl → APP_LOGIN_URL → Origin/Referer/body.origin） */
function resolveInviteLoginUrl(req, body, env) {
  let u = String(body.loginUrl || env.APP_LOGIN_URL || "").trim();
  if (u) return u;
  const origin = String(req.headers.origin || "").trim();
  if (origin) {
    try {
      return `${new URL(origin).origin}/login.html`;
    } catch (_) {}
  }
  const ref = String(req.headers.referer || "").trim();
  if (ref) {
    try {
      return `${new URL(ref).origin}/login.html`;
    } catch (_) {}
  }
  const og = String(body.origin || "").trim();
  if (og) {
    try {
      const base = /^https?:\/\//i.test(og) ? og : `https://${og}`;
      return `${new URL(base).origin}/login.html`;
    } catch (_) {}
  }
  return "";
}

/** マスターは全クライアント。クライアント管理者は user.client と企業名が一致する行のみ。 */
function assertClientRecordAccess(me, clientRow) {
  if (!me || !clientRow) return { ok: false, message: "権限がありません" };
  if (isMaster(me.role)) return { ok: true };
  if (!isAdminLike(me.role)) return { ok: false, message: "権限がありません" };
  if (String(me.client || "").trim() !== String(clientRow.name || "").trim()) {
    return { ok: false, message: "このクライアントを操作する権限がありません" };
  }
  return { ok: true };
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

function generateOtpCode() {
  return String(Math.floor(100000 + Math.random() * 900000));
}
function signOtpChallenge(userId, code, email) {
  const exp = Date.now() + 10 * 60 * 1000;
  const payload = Buffer.from(
    JSON.stringify({ sub: String(userId), exp, ch: sha256(code), em: String(email || "") })
  ).toString("base64url");
  const sig = crypto.createHmac("sha256", JWT_SECRET()).update(payload).digest("base64url");
  return `${payload}.${sig}`;
}
function verifyOtpChallenge(token, code) {
  if (!token || !code) return null;
  const [payload, sig] = String(token).split(".");
  if (!payload || !sig) return null;
  const expected = crypto.createHmac("sha256", JWT_SECRET()).update(payload).digest("base64url");
  if (sig !== expected) return null;
  let obj = null;
  try { obj = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")); } catch (_) { return null; }
  if (!obj || !obj.sub || !obj.exp || !obj.ch) return null;
  if (Date.now() > Number(obj.exp)) return null;
  if (sha256(String(code).trim()) !== obj.ch) return null;
  return { userId: String(obj.sub), email: String(obj.em || "") };
}

function getToken(req) {
  const auth = req.headers.authorization || "";
  return auth.replace(/^Bearer\s+/i, "").trim();
}

function classifyDbUrl(raw) {
  const s = String(raw || "");
  if (s.startsWith("file:") && s.includes("memory")) return "memory";
  if (s.startsWith("libsql://")) return "libsql_remote";
  if (s.startsWith("file:")) return "file_local";
  return "other";
}

/** Vercel かつメモリ DB のときのみ（同期データが保持されない） */
function dbPersistenceWarningPayload() {
  if (!process.env.VERCEL) return null;
  try {
    if (classifyDbUrl(resolveDatabaseUrl()) !== "memory") return null;
  } catch (_) {
    return null;
  }
  return "Vercel 本番で DATABASE_URL（Turso の libsql://…）と TURSO_AUTH_TOKEN が未設定のため、メモリ内 DB にフォールバックしています。同期した顧客データはインスタンス間で共有されず、冷えたあと消えます。Environment Variables に設定し Redeploy してください。";
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

function sendHtml(res, filePath) {
  try {
    const fromApi = path.join(__dirname, "..", filePath);
    const fromCwd = path.join(process.cwd(), filePath);
    const full = fs.existsSync(fromApi) ? fromApi : fromCwd;
    const html = fs.readFileSync(full);
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.statusCode = 200;
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(html);
  } catch (e) {
    res.statusCode = 404;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.end("Not found");
  }
}

async function visiblePropertiesForUser(user) {
  if (!user) return [];
  const tk = String(user.tenantId || "1");
  const propsAll = await listPropertiesByUser(tk);
  if (!isPropertyScopedRole(user.role)) return propsAll;
  if (!Array.isArray(user.propertyIds)) return [];
  const idSet = new Set(user.propertyIds.map(String));
  return propsAll.filter((p) => idSet.has(String(p.id)));
}

async function getActivePropertyForUser(userId) {
  const users = await userStore.listUsers();
  const user = users.find((u) => u.id === userId);
  if (!user) return null;
  const props = await visiblePropertiesForUser(user);
  return props.find((p) => String(p.id) === String(user.activePropertyId)) || null;
}

module.exports = async (req, res) => {
  try {
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
    if (routePath.length > 1 && routePath.endsWith("/")) routePath = routePath.replace(/\/+$/, "");

    // ── GET /local/db-diag（トークン・URL は返さない。本番の DB 接続状況確認用）──
    if (routePath === "/local/db-diag" && req.method === "GET") {
      let raw = "";
      try {
        raw = resolveDatabaseUrl();
      } catch (e) {
        json(res, 200, {
          ok: false,
          vercel: Boolean(process.env.VERCEL),
          dbMode: "error",
          resolveError: String(e && e.message ? e.message : e),
        });
        return;
      }
      const hasDatabaseUrl = Boolean(
        process.env.DATABASE_URL || process.env.TURSO_DATABASE_URL || process.env.LIBSQL_URL
      );
      const hasTursoToken = Boolean(
        process.env.TURSO_AUTH_TOKEN || process.env.TURSO_API_TOKEN
      );
      let pingOk = false;
      let pingError = "";
      try {
        const db = await getDb();
        await db.execute("SELECT 1 AS ok");
        pingOk = true;
      } catch (e) {
        pingError = String(e && e.message ? e.message : e);
      }
      json(res, 200, {
        ok: true,
        vercel: Boolean(process.env.VERCEL),
        dbMode: classifyDbUrl(raw),
        hasDatabaseUrl,
        hasTursoToken,
        pingOk,
        pingError: pingError || undefined,
        jwtFromEnv: Boolean(process.env.JWT_SECRET || process.env.REM_SESSION_SECRET),
      });
      return;
    }

    const env = ENV();
    const secretId = env.SECRET_ID || "";
    const secretPassword = env.SECRET_PASSWORD || "";

  // ── POST /auth/login ──
  if (routePath === "/auth/login" && req.method === "POST") {
    const { email, password } = await readJsonBody(req);
    const users = await userStore.listUsers();
    const em = String(email || "").toLowerCase().trim();
    const user = users.find(
      (u) => String(u.email || "").toLowerCase() === em && u.password === sha256(password)
    );
    if (!user) {
      json(res, 401, { ok: false, message: "メールアドレスまたはパスワードが正しくありません" });
      return;
    }
    if (String(process.env.REM_SKIP_2FA || "").trim() === "1") {
      let u = normalizeUser(user);
      u = await applyLoginDefaultToUser(u, {
        listPropertiesByUser,
        updateUser: (x) => userStore.updateUser(x),
      });
      const token = signToken(u.id);
      const props = await visiblePropertiesForUser(u);
      json(res, 200, { ok: true, token, user: publicUser(u), properties: props });
      return;
    }
    const env2 = ENV();
    if (!hasSmtpEnv(env2)) {
      json(res, 400, { ok: false, message: "2段階認証に必要なSMTPが未設定です。管理者にお問い合わせください。" });
      return;
    }
    let code = "";
    try {
      code = generateOtpCode();
      await sendLoginOtpMail(env2, { email: user.email, name: user.name || "", code });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || "認証コード送信に失敗しました" });
      return;
    }
    const challengeToken = signOtpChallenge(user.id, code, user.email);
    json(res, 200, {
      ok: true,
      requires2fa: true,
      challengeToken,
      message: "認証コードをメール送信しました。10分以内に入力してください。",
    });
    return;
  }

  if (routePath === "/auth/verify-2fa" && req.method === "POST") {
    const body = await readJsonBody(req);
    const challengeToken = String(body.challengeToken || "");
    const code = String(body.code || "").trim();
    const verified = verifyOtpChallenge(challengeToken, code);
    if (!verified) {
      json(res, 401, { ok: false, message: "認証コードが不正、または有効期限切れです" });
      return;
    }
    const users = await userStore.listUsers();
    let user = users.find((u) => String(u.id) === String(verified.userId));
    if (!user) { json(res, 401, { ok: false, message: "認証対象ユーザーが見つかりません" }); return; }
    user = normalizeUser(user);
    user = await applyLoginDefaultToUser(user, {
      listPropertiesByUser,
      updateUser: (x) => userStore.updateUser(x),
    });
    const token = signToken(user.id);
    const props = await visiblePropertiesForUser(user);
    json(res, 200, { ok: true, token, user: publicUser(user), properties: props });
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
      json(res, 401, { ok: false, message: "認証が必要です" });
      return;
    }
    const users = await userStore.listUsers();
    const user = users.find((u) => String(u.id) === String(session.userId));
    if (!user) {
      json(res, 401, { ok: false, message: "認証が必要です" });
      return;
    }
    const props = await visiblePropertiesForUser(user);
    json(res, 200, {
      ok: true,
      user: publicUser(user),
      properties: props,
    });
    return;
  }

  // ── GET /client pages (HTML) ──
  if (routePath === "/client" && req.method === "GET" && url.searchParams.get("api") !== "1") {
    sendHtml(res, "client.html");
    return;
  }
  if (routePath === "/customer/map" && req.method === "GET") {
    sendHtml(res, "customer-mapping.html");
    return;
  }
  if (routePath === "/analysis" && req.method === "GET") {
    sendHtml(res, "analysis.html");
    return;
  }
  if (routePath === "/gallery" && req.method === "GET") {
    sendHtml(res, "gallery.html");
    return;
  }
  if (routePath.startsWith("/gallery/") && req.method === "GET") {
    sendHtml(res, "gallery.html");
    return;
  }
  if (routePath === "/client/create" && req.method === "GET") {
    sendHtml(res, "client-create.html");
    return;
  }
  if (routePath.startsWith("/client/detail/") && req.method === "GET") {
    sendHtml(res, "client-detail.html");
    return;
  }
  if (routePath.startsWith("/client/edit/") && req.method === "GET") {
    sendHtml(res, "client-edit.html");
    return;
  }

  const session = verifyToken(getToken(req));
  if (!session) {
    json(res, 401, { ok: false, message: "認証が必要です" });
    return;
  }

  // ── GET /user/properties ──
  if (routePath === "/user/properties" && req.method === "GET") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const props = await visiblePropertiesForUser(me);
    json(res, 200, { ok: true, properties: props });
    return;
  }

  // ── GET /user/accounts（schedule用: 担当者一覧） ──
  if (routePath === "/user/accounts" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const myTenant = String(me.tenantId || "1");
    const base = users
      .filter((u) => String(u.tenantId || "1") === myTenant)
      .map((u) => publicUser(normalizeUser(u)));
    const accounts = isPropertyScopedRole(me.role) ? base.filter((u) => u.id === me.id) : base;
    json(res, 200, { ok: true, accounts });
    return;
  }

  // ── GET /user/mail-status（招待メール用 SMTP 設定の有無。管理者のみ）──
  if (routePath === "/user/mail-status" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    json(res, 200, { ok: true, smtpConfigured: hasSmtpEnv(ENV()) });
    return;
  }

  // ── POST /user/mail-test（SMTP 疎通。管理者のみ・ログイン中ユーザーのメール宛）──
  if (routePath === "/user/mail-test" && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const env = ENV();
    if (!hasSmtpEnv(env)) { json(res, 400, { ok: false, message: "SMTP設定が未登録です" }); return; }
    const to = String(me.email || "").trim();
    if (!to) { json(res, 400, { ok: false, message: "ログイン中のアカウントにメールアドレスがありません" }); return; }
    try {
      const out = await sendSmtpTestMail(env, { to });
      json(res, 200, { ok: true, sent: out.sent, messageId: out.messageId || "" });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || "テストメール送信に失敗しました" });
    }
    return;
  }

  // ── POST /user/accounts ──
  if (routePath === "/user/accounts" && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canManageAccounts(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const body = await readJsonBody(req);
    const err = (status, m) => json(res, status, { ok: false, message: m });
    const name = String(body.name || "").trim();
    const email = String(body.email || "").trim().toLowerCase();
    const role = Number(body.role);
    const client = String(body.client || "").trim();
    const password = String(body.password || "");
    const passwordConfirm = String(body.passwordConfirm || "");
    const propertyIds = Array.isArray(body.propertyIds) ? [...new Set(body.propertyIds.map(String))] : [];

    if (!name) return err(400, "担当者名は必須です");
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return err(400, "E-mail が不正です");
    if (users.some((u) => u.email.toLowerCase() === email)) return err(400, "このE-mailは既に登録されています");
    if (![ROLE.MASTER, ROLE.CLIENT_ADMIN, ROLE.PROPERTY_MANAGER].includes(role))
      return err(400, "権限を選択してください");
    if (role === ROLE.MASTER && !isMaster(me.role))
      return err(403, "マスター権限の付与はマスターアカウントのみが行えます");
    if (!client) return err(400, "クライアントは必須です");
    const pwdTrim = password.trim();
    const pwd2Trim = passwordConfirm.trim();
    let passwordHash;
    if (!pwdTrim) {
      passwordHash = sha256(randomTempPassword(12));
    } else {
      if (pwdTrim.length < 8) return err(400, "パスワードは8文字以上にしてください");
      if (pwdTrim !== pwd2Trim) return err(400, "パスワード（確認）が一致しません");
      passwordHash = sha256(pwdTrim);
    }
    if (isPropertyScopedRole(role) && propertyIds.length === 0)
      return err(400, "物件管理者は担当物件を1件以上選択してください");

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
    });

    try {
      await userStore.insertUser(newUser);
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
      return;
    }
    json(res, 200, { ok: true, account: publicUser(newUser) });
    return;
  }

  // ── PUT /user/accounts/:id ──
  if (routePath.startsWith("/user/accounts/") && !routePath.includes("/invite") && req.method === "PUT") {
    const id = decodeURIComponent(routePath.slice("/user/accounts/".length));
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const idx = users.findIndex((u) => u.id === id && String(u.tenantId || "1") === String(me.tenantId || "1"));
    if (idx < 0) { json(res, 404, { ok: false, message: "対象が見つかりません" }); return; }
    if (isPropertyScopedRole(me.role) && id !== me.id) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const prevRole = normalizeRole(users[idx].role || 2);
    if (isMaster(users[idx].role) && !isMaster(me.role)) {
      json(res, 403, { ok: false, message: "マスターアカウントの編集はマスターのみが行えます" });
      return;
    }

    const body = await readJsonBody(req);
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

    const normalized = normalizeUser(next);
    try {
      await userStore.updateUser(normalized);
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
      return;
    }
    json(res, 200, { ok: true, account: publicUser(normalized) });
    return;
  }

  // ── DELETE /user/accounts/:id ──
  if (routePath.startsWith("/user/accounts/") && !routePath.includes("/invite") && req.method === "DELETE") {
    const id = decodeURIComponent(routePath.slice("/user/accounts/".length));
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canManageAccounts(me)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    if (id === me.id) { json(res, 400, { ok: false, message: "自分自身は削除できません" }); return; }
    const victim = users.find(
      (u) => u.id === id && String(u.tenantId || "1") === String(me.tenantId || "1")
    );
    if (!victim) { json(res, 404, { ok: false, message: "対象が見つかりません" }); return; }
    if (isMaster(victim.role)) {
      json(res, 400, { ok: false, message: "マスター権限のユーザーは削除できません" });
      return;
    }
    try {
      await userStore.deleteUserById(id);
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
      return;
    }
    json(res, 200, { ok: true });
    return;
  }

  if (routePath.startsWith("/user/accounts/") && routePath.endsWith("/invite") && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(routePath.slice("/user/accounts/".length, -"/invite".length));
    const target = users.find((u) => u.id === id && String(u.tenantId || "1") === String(me.tenantId || "1"));
    if (!target) { json(res, 404, { ok: false, message: "対象アカウントが見つかりません" }); return; }
    if (!target.email) { json(res, 400, { ok: false, message: "メールアドレスが未設定です" }); return; }
    const env = ENV();
    if (!hasSmtpEnv(env)) { json(res, 400, { ok: false, message: "SMTP設定が未登録です" }); return; }
    const body = await readJsonBody(req);
    const loginUrl = resolveInviteLoginUrl(req, body, env);
    let tempPassword = String(body.tempPassword || "").trim();
    if (!tempPassword) {
      tempPassword = randomTempPassword(12);
      const updated = normalizeUser({
        ...target,
        password: sha256(tempPassword),
      });
      await userStore.updateUser(updated);
    }
    try {
      const out = await sendAccountInviteMail(env, {
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

  if (routePath === "/media-assets" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    try {
      const result = await listMediaAssets(String(me.tenantId || "1"), {
        assetType: url.searchParams.get("assetType") || "",
        search: url.searchParams.get("search") || "",
        page: url.searchParams.get("page") || "1",
        limit: url.searchParams.get("limit") || "40",
      });
      const props = await listPropertiesByUser(String(me.tenantId || "1"));
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
  if (routePath === "/media-asset-types" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const types = await listMediaAssetTypes(String(me.tenantId || "1"));
    json(res, 200, { ok: true, types });
    return;
  }
  if (routePath === "/media-asset-types" && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readJsonBody(req);
      const created = await addMediaAssetType(String(me.tenantId || "1"), body || {});
      json(res, 200, { ok: true, type: created });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "種別追加に失敗しました" });
    }
    return;
  }
  if (routePath.startsWith("/media-asset-types/") && req.method === "DELETE") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const typeKey = decodeURIComponent(routePath.slice("/media-asset-types/".length));
      await deleteMediaAssetType(String(me.tenantId || "1"), typeKey);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "種別削除に失敗しました" });
    }
    return;
  }
  if (routePath === "/media-assets/presigned-url" && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readJsonBody(req);
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
  if (routePath === "/media-assets/confirm-upload" && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readJsonBody(req);
      const key = String(body.key || "").trim();
      if (!key) { json(res, 400, { ok: false, message: "key は必須です" }); return; }
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop || !prop.id) { json(res, 400, { ok: false, message: "アクティブ物件が選択されていません。ヘッダーで物件を選んでください。" }); return; }
      const created = await createMediaAssetRecord(String(me.tenantId || "1"), me.id, {
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
  if (routePath === "/media-assets/upload-direct" && req.method === "POST") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readJsonBody(req);
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
      const created = await createMediaAssetRecord(String(me.tenantId || "1"), me.id, {
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
  if (routePath.startsWith("/media-assets/") && req.method === "PATCH") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const id = decodeURIComponent(routePath.slice("/media-assets/".length));
      const body = await readJsonBody(req);
      const updated = await patchMediaAsset(String(me.tenantId || "1"), id, body || {});
      if (!updated) { json(res, 404, { ok: false, message: "素材が見つかりません" }); return; }
      json(res, 200, { ok: true, asset: updated });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || "更新に失敗しました" });
    }
    return;
  }
  if (routePath.startsWith("/media-assets/") && req.method === "DELETE") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(routePath.slice("/media-assets/".length));
    const target = await getMediaAssetById(String(me.tenantId || "1"), id);
    if (!target) { json(res, 404, { ok: false, message: "素材が見つかりません" }); return; }
    try {
      const tk = String(me.tenantId || "1");
      let delProp = target.propertyId ? await getPropertyByIdForUser(String(target.propertyId), tk) : null;
      if (!delProp) delProp = await getActivePropertyForUser(session.userId);
      const cfg = s3ConfigFromProperty(delProp, process.env);
      if (cfg && target.fileKey) {
        await removeObject({ bucketName: cfg.bucketName, key: target.fileKey, region: cfg.region }, process.env);
      }
    } catch (_) {}
    await deleteMediaAsset(String(me.tenantId || "1"), id);
    json(res, 200, { ok: true });
    return;
  }

  // ── POST /user/properties ──
  if (routePath === "/user/properties" && req.method === "POST") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const { name, databaseId, databasePassword, tableName } = await readJsonBody(req);
    if (!name || !databaseId || !tableName) {
      json(res, 400, { ok: false, message: "必須項目が不足しています" });
      return;
    }
    const tk = String(me.tenantId || "1");
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
    const newProp = await createProperty(tk, createPayload);
    const users = await userStore.listUsers();
    const ui = users.findIndex((u) => u.id === session.userId);
    if (ui >= 0 && !users[ui].activePropertyId) {
      users[ui].activePropertyId = newProp.id;
      await userStore.updateUser(users[ui]);
    }
    json(res, 200, { ok: true, property: newProp });
    return;
  }

  // ── GET /api/properties/:id/analysis ──
  if (routePath.startsWith("/api/properties/") && routePath.endsWith("/analysis") && req.method === "GET") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const head = "/api/properties/";
      const tail = "/analysis";
      const propertyId = decodeURIComponent(routePath.slice(head.length, -tail.length));
      const tk = String(me.tenantId || "1");
      const prop = await getPropertyByIdForUser(propertyId, tk);
      if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      if (
        isPropertyScopedRole(me.role) &&
        Array.isArray(me.propertyIds) &&
        !new Set(me.propertyIds.map(String)).has(String(propertyId))
      ) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
      const weekStart = String(url.searchParams.get("weekStart") || "").trim();
      const weekEnd = String(url.searchParams.get("weekEnd") || "").trim();
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

  // ── GET /api/properties/:id/weekly-report/download ──
  if (routePath.startsWith("/api/properties/") && routePath.endsWith("/weekly-report/download") && req.method === "GET") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const head = "/api/properties/";
      const tail = "/weekly-report/download";
      const propertyId = decodeURIComponent(routePath.slice(head.length, -tail.length));
      const tk = String(me.tenantId || "1");
      const prop = await getPropertyByIdForUser(propertyId, tk);
      if (!prop) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      if (
        isPropertyScopedRole(me.role) &&
        Array.isArray(me.propertyIds) &&
        !new Set(me.propertyIds.map(String)).has(String(propertyId))
      ) {
        json(res, 403, { ok: false, message: "権限がありません" });
        return;
      }
      const startDate = String(url.searchParams.get("startDate") || "").trim();
      const endDate = String(url.searchParams.get("endDate") || "").trim();
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
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
      res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(name)}`);
      res.end(Buffer.from(buf));
    } catch (e) {
      console.error("[weekly-report download]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || "週報生成に失敗しました" });
    }
    return;
  }

  // ── PUT /user/properties/:id ──
  if (routePath.startsWith("/user/properties/") && routePath.endsWith("/images") && req.method === "GET") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const id = routePath.slice("/user/properties/".length, -"/images".length);
    const tk = String(me.tenantId || "1");
    const existing = await getPropertyByIdForUser(id, tk);
    if (!existing) {
      json(res, 404, { ok: false, message: "物件が見つかりません" });
      return;
    }
    const images = await listPropertyImages(id, tk);
    json(res, 200, { ok: true, images });
    return;
  }
  if (routePath.startsWith("/user/properties/") && routePath.endsWith("/history") && req.method === "GET") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const id = routePath.slice("/user/properties/".length, -"/history".length);
    const tk = String(me.tenantId || "1");
    const existing = await getPropertyByIdForUser(id, tk);
    if (!existing) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
    const items = await listPropertyUpdateHistory(id, tk, url.searchParams.get("limit") || "50");
    json(res, 200, { ok: true, history: items });
    return;
  }
  if (routePath.startsWith("/user/properties/") && routePath.endsWith("/meta") && req.method === "PATCH") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = routePath.slice("/user/properties/".length, -"/meta".length);
    const tk = String(me.tenantId || "1");
    try {
      const body = await readJsonBody(req);
      const updated = await updatePropertyMeta(id, tk, body.patch || {}, String(me.name || ""));
      if (!updated) { json(res, 404, { ok: false, message: "物件が見つかりません" }); return; }
      json(res, 200, { ok: true, property: updated });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/user/properties/") && routePath.endsWith("/images") && req.method === "POST") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = routePath.slice("/user/properties/".length, -"/images".length);
    const tk = String(me.tenantId || "1");
    const existing = await getPropertyByIdForUser(id, tk);
    if (!existing) {
      json(res, 404, { ok: false, message: "物件が見つかりません" });
      return;
    }
    try {
      const body = await readJsonBody(req);
      const r = await addPropertyImage(id, tk, body || {});
      json(res, 200, { ok: true, imageId: r.id });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/user/properties/") && routePath.includes("/images/") && req.method === "DELETE") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const tail = routePath.slice("/user/properties/".length);
    const i = tail.indexOf("/images/");
    const pid = tail.slice(0, i);
    const iid = decodeURIComponent(tail.slice(i + "/images/".length));
    const tk = String(me.tenantId || "1");
    try {
      await deletePropertyImage(pid, tk, iid);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── PUT /user/properties/:id ──
  if (routePath.startsWith("/user/properties/") && req.method === "PUT") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = routePath.replace("/user/properties/", "");
    const tk = String(me.tenantId || "1");
    const { name, databaseId, databasePassword, tableName } = await readJsonBody(req);
    const existing = await getPropertyByIdForUser(id, tk);
    if (!existing) {
      json(res, 404, { ok: false });
      return;
    }
    const updated = await updateProperty(id, tk, {
      name,
      databaseId,
      databasePassword,
      tableName,
    });
    json(res, 200, { ok: true, property: updated });
    return;
  }

  // ── DELETE /user/properties/:id ──
  if (routePath.startsWith("/user/properties/") && req.method === "DELETE") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = routePath.replace("/user/properties/", "");
    const tk = String(me.tenantId || "1");
    await deleteProperty(id, tk);
    await userStore.clearLoginDefaultPropertyId(id);
    const users = await userStore.listUsers();
    const ui = users.findIndex((u) => u.id === session.userId);
    if (ui >= 0 && users[ui].activePropertyId === id) {
      const remainingAll = await listPropertiesByUser(tk);
      const remaining =
        isPropertyScopedRole(me.role) && Array.isArray(me.propertyIds)
          ? remainingAll.filter((p) => new Set(me.propertyIds.map(String)).has(String(p.id)))
          : remainingAll;
      users[ui].activePropertyId = remaining.length ? remaining[0].id : null;
      await userStore.updateUser(users[ui]);
    }
    json(res, 200, { ok: true });
    return;
  }

  // ── POST /user/select-property ──
  if (routePath === "/user/select-property" && req.method === "POST") {
    const { propertyId } = await readJsonBody(req);
    try {
      const users = await userStore.listUsers();
      const ui = users.findIndex((u) => u.id === session.userId);
      if (ui >= 0) {
        users[ui].activePropertyId = propertyId;
        await userStore.updateUser(normalizeUser(users[ui]));
      }
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
      return;
    }
    json(res, 200, { ok: true });
    return;
  }

  // ── POST /user/login-default-property（ログイン時に表示する物件） ──
  if (routePath === "/user/login-default-property" && req.method === "POST") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false, message: "認証が必要です" }); return; }
    try {
      const body = await readJsonBody(req);
      const raw = body.propertyId;
      const pid = raw == null || raw === "" ? null : String(raw);
      const tk = String(me.tenantId || "1");
      const propsAll = await listPropertiesByUser(tk);
      const visible =
        isPropertyScopedRole(me.role)
          ? propsAll.filter(
              (p) => Array.isArray(me.propertyIds) && me.propertyIds.map(String).includes(String(p.id))
            )
          : propsAll;
      if (pid && !visible.some((p) => String(p.id) === String(pid))) {
        json(res, 400, { ok: false, message: "指定の物件にアクセスできません" });
        return;
      }
      const users = await userStore.listUsers();
      const mid = String(me.id);
      const ui = users.findIndex((u) => String(u.id) === mid);
      if (ui < 0) {
        json(res, 401, { ok: false, message: "認証が必要です" });
        return;
      }
      users[ui].loginDefaultPropertyId = pid;
      if (pid) users[ui].activePropertyId = pid;
      await userStore.updateUser(normalizeUser(users[ui]));
      let fresh = normalizeUser(
        (await userStore.listUsers()).find((u) => String(u.id) === mid)
      );
      if (!fresh) {
        fresh = normalizeUser(users[ui]);
      }
      json(res, 200, { ok: true, user: publicUser(fresh) });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── POST /user/change-password ──
  if (routePath === "/user/change-password" && req.method === "POST") {
    const { currentPassword, newPassword } = await readJsonBody(req);
    const users = await userStore.listUsers();
    const ui = users.findIndex((u) => u.id === session.userId);
    if (ui < 0 || users[ui].password !== sha256(currentPassword)) {
      json(res, 400, { ok: false, message: "現在のパスワードが正しくありません" });
      return;
    }
    try {
      users[ui].password = sha256(newPassword);
      await userStore.updateUser(users[ui]);
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
      return;
    }
    json(res, 200, { ok: true });
    return;
  }

  // ── schedule routes ──
  if (routePath === "/schedule/rooms" && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const rooms = await listRooms(prop.id);
    json(res, 200, { ok: true, rooms });
    return;
  }
  if (routePath === "/schedule/settings" && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const settings = await getScheduleSettings(prop.id);
    json(res, 200, { ok: true, settings });
    return;
  }
  if (routePath === "/schedule/settings" && req.method === "PUT") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const body = await readJsonBody(req);
    const settings = await saveScheduleSettings(prop.id, body || {});
    json(res, 200, { ok: true, settings });
    return;
  }
  if (routePath === "/schedule/events" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    const prop = await getActivePropertyForUser(session.userId);
    if (!me || !prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const f = {
      start: url.searchParams.get("start"),
      end: url.searchParams.get("end"),
      roomId: url.searchParams.get("roomId"),
      staffId: url.searchParams.get("staffId"),
      keyword: url.searchParams.get("keyword"),
    };
    const vf = url.searchParams.get("vf");
    if (vf === "pending") f.onlyPending = true;
    if (vf === "confirmed") f.onlyConfirmed = true;
    const statusValues = (url.searchParams.get("statusValues") || "").split(",").map((v) => v.trim()).filter(Boolean);
    if (statusValues.length) f.statusValues = statusValues;
    let rows = await listSchedules(prop.id, f);
    if (isPropertyScopedRole(me.role)) rows = rows.filter((r) => r.status === "pending" || String(r.staffId || "") === String(me.id));
    json(res, 200, { ok: true, events: rows });
    return;
  }
  if (routePath === "/schedule/events" && req.method === "POST") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const body = await readJsonBody(req);
      const out = await createSchedule(prop.id, { ...body, source: body.source || "manual" });
      json(res, 200, { ok: true, warning: out.warning || "" });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/schedule/events/") && req.method === "PUT") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    const prop = await getActivePropertyForUser(session.userId);
    if (!me || !prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const id = routePath.replace("/schedule/events/", "");
    try {
      const body = await readJsonBody(req);
      if (isPropertyScopedRole(me.role) && body.status === "confirmed" && !body.staffId) body.staffId = me.id;
      await updateSchedule(prop.id, id, body);
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/schedule/events/") && req.method === "DELETE") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    const id = routePath.replace("/schedule/events/", "");
    await deleteSchedule(prop.id, id);
    json(res, 200, { ok: true });
    return;
  }
  if (routePath === "/schedule/intake" && req.method === "POST") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const body = await readJsonBody(req);
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

  // ── client routes ──
  // JSON 一覧はパスで分離（/client?api=1 はリライトでクエリが落ちると HTML が返り、フロントの res.json() が落ちてスピナーが止まらない）
  if (
    (routePath === "/client/api/list" ||
      (routePath === "/client" && url.searchParams.get("api") === "1")) &&
    req.method === "GET"
  ) {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const clients = await listClients(String(me.tenantId || "1"));
    json(res, 200, { ok: true, clients });
    return;
  }
  if (routePath === "/client/master" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const clients = await listClients(String(me.tenantId || "1"));
    json(res, 200, { ok: true, clients });
    return;
  }
  if (routePath === "/client/create" && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readJsonBody(req);
      const c = await createClient(String(me.tenantId || "1"), body || {});
      json(res, 200, { ok: true, client: c });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath === "/client/api/detail" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = url.searchParams.get("id");
    const c = await getClient(String(me.tenantId || "1"), id);
    if (!c) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
    const ac = assertClientRecordAccess(me, c);
    if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
    const s3 = await getClientS3Settings(String(me.tenantId || "1"), c.id);
    json(res, 200, { ok: true, client: c, s3 });
    return;
  }
  if (routePath === "/client/api/linked-properties" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const tk = String(me.tenantId || "1");
    const id = url.searchParams.get("id");
    const c = await getClient(tk, id);
    if (!c) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
    const ac = assertClientRecordAccess(me, c);
    if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
    const clientName = String(c.name || "").trim();
    const all = await listPropertiesByUser(tk);
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
  if (routePath === "/client/api/linked-properties" && req.method === "PUT") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const tk = String(me.tenantId || "1");
    try {
      const body = await readJsonBody(req);
      const c = await getClient(tk, body.clientId);
      if (!c) { json(res, 404, { ok: false, message: "クライアントが見つかりません" }); return; }
      const ac = assertClientRecordAccess(me, c);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      const propertyIds = Array.isArray(body.propertyIds) ? body.propertyIds : [];
      await syncPropertiesClientName(tk, c.name, propertyIds, me.name || "");
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/client/edit/") && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(routePath.slice("/client/edit/".length));
    try {
      const existing = await getClient(String(me.tenantId || "1"), id);
      if (!existing) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
      const ac = assertClientRecordAccess(me, existing);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      const body = await readJsonBody(req);
      const c = await updateClient(String(me.tenantId || "1"), id, body || {});
      json(res, 200, { ok: true, client: c });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath === "/client/delete" && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const body = await readJsonBody(req);
    const tk = String(me.tenantId || "1");
    const ids = body.ids || [];
    for (const cid of ids) {
      const row = await getClient(tk, cid);
      if (row) {
        const ac = assertClientRecordAccess(me, row);
        if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      }
    }
    const r = await deleteClients(tk, ids);
    json(res, 200, { ok: true, ...r });
    return;
  }
  if (routePath.startsWith("/client/s3/save/") && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const id = decodeURIComponent(routePath.slice("/client/s3/save/".length));
    try {
      const row = await getClient(String(me.tenantId || "1"), id);
      if (!row) { json(res, 404, { ok: false, message: "見つかりません" }); return; }
      const ac = assertClientRecordAccess(me, row);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
      const body = await readJsonBody(req);
      const saved = await saveClientS3Settings(String(me.tenantId || "1"), id, body || {});
      json(res, 200, { ok: true, s3: saved });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/client/s3/test/") && req.method === "POST") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me || !isAdminLike(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const testCid = decodeURIComponent(routePath.slice("/client/s3/test/".length));
    const testRow = await getClient(String(me.tenantId || "1"), testCid);
    if (testRow) {
      const ac = assertClientRecordAccess(me, testRow);
      if (!ac.ok) { json(res, 403, { ok: false, message: ac.message }); return; }
    }
    try {
      const body = await readJsonBody(req);
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

  // ── POST /sync/customers ──
  if (routePath === "/sync/customers" && req.method === "POST") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) {
      json(res, 400, { ok: false, message: "物件が選択されていません。" });
      return;
    }
    try {
      const users = await userStore.listUsers();
      const me = users.find((u) => u.id === session.userId);
      if (!me) {
        json(res, 401, { ok: false, message: "認証が必要です。" });
        return;
      }
      const body = await readJsonBody(req);
      const mode = body.mode === "full" ? "full" : "incremental";
      if (mode === "full" && !isMaster(me.role)) {
        json(res, 403, { ok: false, message: "全件同期はマスター権限のみ実行できます。" });
        return;
      }
      const result = await runCustomerSync(prop, env, mode);
      json(res, 200, result);
    } catch (e) {
      console.error(e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── POST /local/customers/export-karte-excel ──
  if (routePath === "/local/customers/export-karte-excel" && req.method === "POST") {
    try {
      const prop = await getActivePropertyForUser(session.userId);
      if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
      const body = await readJsonBody(req);
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
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
      res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(name)}`);
      res.end(Buffer.from(buf));
    } catch (e) {
      console.error("[export-karte-excel]", e.stack || e);
      json(res, 500, { ok: false, message: e.message || "Excel生成に失敗しました" });
    }
    return;
  }

  // ── GET /local/customers ──
  if (routePath === "/local/customers" && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) {
      json(res, 400, { ok: false, message: "物件が選択されていません。" });
      return;
    }
    try {
      const fullUrl = new URL(req.url || "/", `http://${host}`);
      const qp = {};
      fullUrl.searchParams.forEach((v, k) => {
        if (k === "__r") return;
        qp[k] = v;
      });
      const pack = await queryLocalCustomers(prop.id, qp);
      json(res, 200, { ok: true, data: pack.data });
    } catch (e) {
      console.error(e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── GET /local/sync-status ──
  if (routePath === "/local/sync-status" && req.method === "GET") {
    const persistWarn = dbPersistenceWarningPayload();
    const persistExtra = persistWarn
      ? { dbEphemeral: true, dbPersistenceWarning: persistWarn }
      : {};
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) {
      json(res, 200, { ok: true, hasData: false, rows_total: 0, ...persistExtra });
      return;
    }
    try {
      const st = await getSyncStatus(prop.id);
      json(res, 200, { ok: true, ...st, ...persistExtra });
    } catch (e) {
      console.error("[local/sync-status]", e.stack || e);
      // 500 にするとフロントが ok を見ず握りつぶすため、接続エラーでも 200 + dbQueryError で返す
      json(res, 200, {
        ok: true,
        hasData: false,
        rows_total: 0,
        dbQueryError: e.message || String(e),
        ...persistExtra,
      });
    }
    return;
  }

  // ── GET/PUT /local/visit-statuses ──
  if (routePath === "/local/visit-statuses" && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) {
      json(res, 200, { ok: true, statuses: DEFAULT_VISIT_STATUSES });
      return;
    }
    try {
      const arr = await getVisitStatuses(prop.id);
      json(res, 200, { ok: true, statuses: arr });
    } catch (e) {
      console.error(e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath === "/local/visit-statuses" && req.method === "PUT") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) {
      json(res, 400, { ok: false, message: "物件が選択されていません。" });
      return;
    }
    try {
      const body = await readJsonBody(req);
      const saved = await saveVisitStatuses(prop.id, body.statuses);
      json(res, 200, { ok: true, statuses: saved });
    } catch (e) {
      console.error(e);
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── GET/PUT /local/customers/columns ──
  if (routePath === "/local/customers/columns" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const targetUserId = url.searchParams.get("userId") || me.id;
    if (!isAdminLike(me.role) && targetUserId !== me.id) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const columns = await getCustomerListColumns(String(me.tenantId || "1"), targetUserId);
    const prop = await getActivePropertyForUser(session.userId);
    const availableFields = prop ? await listCustomerAvailableFields(prop.id, 500) : [];
    json(res, 200, { ok: true, columns, defaults: DEFAULT_CUSTOMER_COLUMNS, availableFields });
    return;
  }
  if (routePath === "/local/customers/columns" && req.method === "PUT") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "管理者のみ設定できます" }); return; }
    try {
      const body = await readJsonBody(req);
      const targetUserId = String(body.userId || me.id);
      const saved = await saveCustomerListColumns(String(me.tenantId || "1"), targetUserId, body.columns || []);
      json(res, 200, { ok: true, columns: saved });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── GET/PUT /local/customers/detail-fields ──
  if (routePath === "/local/customers/detail-fields" && req.method === "GET") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    const targetUserId = url.searchParams.get("userId") || me.id;
    if (!isAdminLike(me.role) && targetUserId !== me.id) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    const fields = await getCustomerDetailFieldConfig(String(me.tenantId || "1"), targetUserId);
    json(res, 200, { ok: true, fields, defaults: DEFAULT_CUSTOMER_DETAIL_FIELDS });
    return;
  }
  if (routePath === "/local/customers/detail-fields" && req.method === "PUT") {
    const users = await userStore.listUsers();
    const me = users.find((u) => u.id === session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!isAdminLike(me.role)) { json(res, 403, { ok: false, message: "管理者のみ設定できます" }); return; }
    try {
      const body = await readJsonBody(req);
      const targetUserId = String(body.userId || me.id);
      const saved = await saveCustomerDetailFieldConfig(String(me.tenantId || "1"), targetUserId, body.fields || []);
      json(res, 200, { ok: true, fields: saved });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── POST /local/media-asset-upload（素材ギャラリー・サーバー経由S3） ──
  if (routePath === "/local/media-asset-upload" && req.method === "POST") {
    const me = await sessionMe(session.userId);
    if (!me) { json(res, 401, { ok: false }); return; }
    if (!canUsePropertyMediaRoles(me.role)) { json(res, 403, { ok: false, message: "権限がありません" }); return; }
    try {
      const body = await readJsonBody(req);
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
      const created = await createMediaAssetRecord(String(me.tenantId || "1"), me.id, {
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

  // ── PATCH /local/customers/:id ──
  if (routePath.startsWith("/local/customers/") && req.method === "PATCH") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) {
      json(res, 400, { ok: false, message: "物件が選択されていません。" });
      return;
    }
    try {
      const customerId = decodeURIComponent(
        routePath.slice("/local/customers/".length)
      );
      if (!customerId) {
        json(res, 400, { ok: false, message: "顧客IDが不正です" });
        return;
      }
      const body = await readJsonBody(req);
      const result = await patchLocalCustomer(prop.id, customerId, body);
      json(res, 200, result);
    } catch (e) {
      console.error(e);
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── GET /local/customers/:id/history ──
  if (routePath.startsWith("/local/customers/") && routePath.endsWith("/history") && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) {
      json(res, 400, { ok: false, message: "物件が選択されていません。" });
      return;
    }
    try {
      const base = routePath.slice("/local/customers/".length, -"/history".length);
      const customerId = decodeURIComponent(base);
      const limit = url.searchParams.get("limit") || "50";
      const items = await listCustomerHistory(prop.id, customerId, limit);
      json(res, 200, { ok: true, history: items });
    } catch (e) {
      console.error(e);
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  if (routePath.startsWith("/local/customers/") && routePath.endsWith("/reactions") && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const base = routePath.slice("/local/customers/".length, -"/reactions".length);
      const customerId = decodeURIComponent(base);
      const limit = url.searchParams.get("limit") || "100";
      const items = await listCustomerReactions(prop.id, customerId, limit);
      json(res, 200, { ok: true, items });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }

  // ── GET/POST /local/customers/:id/sales-history ──
  if (routePath.startsWith("/local/customers/") && routePath.endsWith("/sales-history") && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const base = routePath.slice("/local/customers/".length, -"/sales-history".length);
      const customerId = decodeURIComponent(base);
      const limit = url.searchParams.get("limit") || "100";
      const items = await listSalesHistory(prop.id, customerId, limit);
      json(res, 200, { ok: true, items });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/local/customers/") && routePath.endsWith("/sales-history") && req.method === "POST") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const base = routePath.slice("/local/customers/".length, -"/sales-history".length);
      const customerId = decodeURIComponent(base);
      const body = await readJsonBody(req);
      await addSalesHistory(prop.id, customerId, body || {});
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/local/customers/") && routePath.includes("/sales-history/") && req.method === "PUT") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const tail = routePath.slice("/local/customers/".length);
      const i = tail.indexOf("/sales-history/");
      const customerId = decodeURIComponent(tail.slice(0, i));
      const salesId = decodeURIComponent(tail.slice(i + "/sales-history/".length));
      const body = await readJsonBody(req);
      await updateSalesHistory(prop.id, customerId, salesId, body || {});
      json(res, 200, { ok: true });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/local/customers/") && routePath.includes("/sales-history/") && req.method === "DELETE") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const tail = routePath.slice("/local/customers/".length);
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

  // ── GET/POST/DELETE /local/customers/:id/files ──
  if (routePath.startsWith("/local/customers/") && routePath.endsWith("/files") && req.method === "GET") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const base = routePath.slice("/local/customers/".length, -"/files".length);
      const customerId = decodeURIComponent(base);
      const limit = url.searchParams.get("limit") || "100";
      const files = await listCustomerFiles(prop.id, customerId, limit);
      json(res, 200, { ok: true, files });
    } catch (e) {
      json(res, 500, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/local/customers/") && routePath.endsWith("/files") && req.method === "POST") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const base = routePath.slice("/local/customers/".length, -"/files".length);
      const customerId = decodeURIComponent(base);
      const body = await readJsonBody(req);
      const out = await addCustomerFile(prop.id, customerId, body || {});
      json(res, 200, { ok: true, ...out });
    } catch (e) {
      json(res, 400, { ok: false, message: e.message || String(e) });
    }
    return;
  }
  if (routePath.startsWith("/local/customers/") && routePath.includes("/files/") && req.method === "DELETE") {
    const prop = await getActivePropertyForUser(session.userId);
    if (!prop) { json(res, 400, { ok: false, message: "物件が選択されていません。" }); return; }
    try {
      const tail = routePath.slice("/local/customers/".length);
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

  // ── POST /api/v1/... → デジタライズ API ──
  if (routePath.startsWith("/api/") && req.method === "POST") {
    const prop = await getActivePropertyForUser(session.userId);
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
  } catch (e) {
    console.error("[api/handler] Unhandled error", e && (e.stack || e));
    // ここで落ちると Vercel 側が汎用 500 しか返さないので、JSONで返す
    try {
      json(res, 500, { ok: false, message: e?.message || String(e) });
    } catch (_) {
      res.statusCode = 500;
      res.end("Internal Server Error");
    }
  }
};
