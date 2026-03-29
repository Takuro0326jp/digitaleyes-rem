/** ログイン時デフォルト物件ID（サーバー応答欠落時の UI 用。解除API・ログアウトでのみ消す） */
const LS_LOGIN_DEFAULT_PROPERTY_ID = "rem_login_default_property_id";

function readLoginDefaultCacheRaw() {
  try {
    const c = localStorage.getItem(LS_LOGIN_DEFAULT_PROPERTY_ID);
    return c != null && String(c).trim() !== "" ? String(c) : null;
  } catch (_) {
    return null;
  }
}

/** 解除API・ログイン応答用。`undefined`＝キー欠落は触らない。`null`/空文字で削除 */
function writeLoginDefaultCache(loginDefaultPropertyId) {
  try {
    if (loginDefaultPropertyId === undefined) return;
    const v = loginDefaultPropertyId;
    if (v != null && String(v).trim() !== "") {
      localStorage.setItem(LS_LOGIN_DEFAULT_PROPERTY_ID, String(v));
    } else {
      localStorage.removeItem(LS_LOGIN_DEFAULT_PROPERTY_ID);
    }
  } catch (_) {}
}

/** 物件一覧のラジオ用。user に無いときはキャッシュを参照 */
function readLoginDefaultIdForUi() {
  const u = Auth.getUser();
  const fromUser = u && u.loginDefaultPropertyId;
  if (fromUser != null && String(fromUser).trim() !== "") return String(fromUser);
  return readLoginDefaultCacheRaw();
}

/** 同一オリジン（Vercel / proxy-server）では相対URL。file:// 直開きのみ localhost */
const CONFIG = {
  get API_BASE() {
    return typeof window !== "undefined" && window.location.protocol === "file:"
      ? "http://localhost:3001"
      : "";
  },
};

// ── トークン管理 ───────────────────────────────
const Auth = {
  getToken: () => localStorage.getItem("token"),
  setToken: (t) => localStorage.setItem("token", t),
  clear: () => {
    localStorage.removeItem("token");
    localStorage.removeItem("user");
    localStorage.removeItem("properties");
    localStorage.removeItem("activeProperty");
    try {
      localStorage.removeItem(LS_LOGIN_DEFAULT_PROPERTY_ID);
    } catch (_) {}
  },
  getUser: () => { try { return JSON.parse(localStorage.getItem("user")); } catch { return null; } },
  setUser: (u) => localStorage.setItem("user", JSON.stringify(u)),
  getProperties: () => { try { return JSON.parse(localStorage.getItem("properties")) || []; } catch { return []; } },
  setProperties: (p) => localStorage.setItem("properties", JSON.stringify(p)),
  getActiveProperty: () => { try { return JSON.parse(localStorage.getItem("activeProperty")); } catch { return null; } },
  setActiveProperty: (p) => localStorage.setItem("activeProperty", JSON.stringify(p)),
};

/** マスター(1)・クライアント管理者(2): 旧「管理者」と同等のナビ・設定系 */
function remIsAdminLikeRole(role) {
  const n = Number(role);
  return n === 1 || n === 2;
}

/** ヘッダー用: 権限ラベル（ツールチップ・aria 用） */
function remRoleTitle(role) {
  const n = Number(role);
  if (n === 1) return "マスター";
  if (n === 2) return "クライアント管理者";
  if (n === 3) return "物件管理者";
  if (n === 4) return "ユーザー";
  return "ユーザー";
}

/**
 * ログイン者名の左に表示する権限アイコン（SVG・ブランド色 currentColor）
 */
function remRoleIconHtml(role) {
  const title = remRoleTitle(role);
  const n = Number(role);
  const svgOpen =
    '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">';
  let inner = "";
  if (n === 1) {
    inner =
      '<polygon fill="currentColor" stroke="none" points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>';
  } else if (n === 2) {
    inner =
      '<rect width="20" height="14" x="2" y="7" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>';
  } else if (n === 3) {
    inner =
      '<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>';
  } else {
    inner =
      '<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>';
  }
  const titleEsc = String(title).replace(/"/g, "&quot;");
  return `<span class="header-role-badge" role="img" aria-label="${titleEsc}" title="${titleEsc}">${svgOpen}${inner}</svg></span>`;
}

/** サーバー応答でローカル user を上書きするが、キー欠落時に消えないようマージ（loginDefaultPropertyId 等） */
function mergeStoredUser(serverUser) {
  const prev = Auth.getUser();
  const p = prev && typeof prev === "object" ? prev : {};
  const s = serverUser && typeof serverUser === "object" ? serverUser : {};
  return { ...p, ...s };
}

// ── 認証チェック（各ページで呼ぶ） ──────────────
async function requireAuth() {
  const token = Auth.getToken();
  if (!token) { window.location.href = "login.html"; return false; }
  try {
    const res = await fetch(`${CONFIG.API_BASE}/auth/me`, {
      headers: { "Authorization": `Bearer ${token}` }
    });
    const data = await res.json();
    if (!data.ok) { Auth.clear(); window.location.href = "login.html"; return false; }
    let user = mergeStoredUser(data.user);
    const cachedLd = readLoginDefaultCacheRaw();
    if (cachedLd && (user.loginDefaultPropertyId == null || String(user.loginDefaultPropertyId).trim() === "")) {
      user = { ...user, loginDefaultPropertyId: cachedLd };
    }
    Auth.setUser(user);
    if (user.loginDefaultPropertyId != null && String(user.loginDefaultPropertyId).trim() !== "") {
      writeLoginDefaultCache(user.loginDefaultPropertyId);
    }
    Auth.setProperties(data.properties);
    // アクティブ物件を同期
    const active = data.properties.find(
      (p) => String(p.id) === String(user.activePropertyId)
    );
    if (active) Auth.setActiveProperty(active);
    else if (data.properties.length) {
      Auth.setActiveProperty(data.properties[0]);
      await selectProperty(data.properties[0].id);
    } else Auth.setActiveProperty(null);
    const pageFile =
      typeof window !== "undefined" && window.location && window.location.pathname
        ? String(window.location.pathname).split("/").pop() || ""
        : "";
    if (user.mustChangePassword && pageFile !== "settings.html") {
      window.location.href = "settings.html?requirePw=1";
      return false;
    }
    return true;
  } catch { return true; /* プロキシが起動していない場合は続行 */ }
}

async function selectProperty(propertyId) {
  await fetch(`${CONFIG.API_BASE}/user/select-property`, {
    method: "POST",
    headers: { "Authorization": `Bearer ${Auth.getToken()}`, "Content-Type": "application/json" },
    body: JSON.stringify({ propertyId })
  });
  const props = Auth.getProperties();
  const p = props.find(p => p.id === propertyId);
  if (p) Auth.setActiveProperty(p);
}

/** ログイン時に最初に表示する物件。propertyId が null/空で解除（アクティブ物件は変えない） */
async function setLoginDefaultProperty(propertyId) {
  const token = Auth.getToken();
  if (!token) {
    throw new Error("ログイン情報がありません。再度ログインしてください。");
  }
  const res = await fetch(`${CONFIG.API_BASE}/user/login-default-property`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ propertyId: propertyId == null || propertyId === "" ? null : propertyId }),
  });
  const text = await res.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_) {
    data = {};
  }
  if (!res.ok || data.ok === false) {
    const msg =
      data.message ||
      (res.status === 401
        ? "認証に失敗しました。再度ログインしてください。"
        : `設定に失敗しました（HTTP ${res.status}）`);
    throw new Error(msg);
  }
  const clearing = propertyId == null || propertyId === "";
  const prev = Auth.getUser() || {};
  const server = data.user && typeof data.user === "object" ? data.user : {};
  let user = { ...prev, ...server };
  if (clearing) {
    user = { ...user, loginDefaultPropertyId: null };
    writeLoginDefaultCache(null);
  } else {
    const fromSrv = server.loginDefaultPropertyId;
    const ld =
      fromSrv != null && String(fromSrv).trim() !== ""
        ? String(fromSrv)
        : String(propertyId);
    user = { ...user, loginDefaultPropertyId: ld, activePropertyId: ld };
    writeLoginDefaultCache(ld);
  }
  Auth.setUser(user);
  const props = Auth.getProperties();
  const ap = props.find((p) => String(p.id) === String(user.activePropertyId));
  if (ap) Auth.setActiveProperty(ap);
  return data;
}

async function logout() {
  await fetch(`${CONFIG.API_BASE}/auth/logout`, {
    method: "POST",
    headers: { "Authorization": `Bearer ${Auth.getToken()}` }
  }).catch(() => {});
  Auth.clear();
  window.location.href = "login.html";
}

// ── ヘッダー描画 ───────────────────────────────
function renderHeader(currentPage) {
  const user = Auth.getUser();
  const props = Auth.getProperties();
  const active = Auth.getActiveProperty();
  const isAdmin = remIsAdminLikeRole(user?.role);
  const labelOfProp = (p) => {
    const raw = p?.name ?? p?.propertyName ?? "";
    const s = String(raw).trim();
    return s && s !== "undefined" ? s : `物件(${p?.id || "-"})`;
  };
  const escAttr = (s) =>
    String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const displayName = escAttr(user?.name || "");

  const propertyOptions = props.length
    ? props.map(p => {
      const lbl = String(labelOfProp(p)).replace(/[&<>"']/g, (c) => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c]));
      const cls = p.id === active?.id ? "current" : "";
      return `<button type="button" class="prop-picker-item ${cls}" onclick="chooseProperty('${p.id}')">${lbl}</button>`;
    }).join("")
    : `<button type="button" class="prop-picker-item" onclick="location.href='property.html'">物件未登録 → 物件管理へ</button>`;

  const navItems = [
    { label: "ダッシュボード", href: "dashboard.html" },
    { label: "顧客一覧", href: "customer.html" },
    { label: "顧客マッピング", href: "customer-mapping.html" },
    { label: "分析レポート", href: "analysis.html" },
    { label: "ポータル管理", href: "ad.html" },
    { label: "接客スケジュール", href: "schedule.html" },
    { label: "物件管理", href: "property.html" },
    { label: "素材ギャラリー", href: "gallery.html" },
    { label: "設定", href: "settings.html", adminOnly: true },
    { label: "クライアント管理", href: "client.html", adminOnly: true },
    { label: "アカウント管理", href: "account.html", adminOnly: true },
  ].filter((n) => !n.adminOnly || isAdmin);

  return `
    <aside class="sidebar">
      <div class="sidebar-brand">
        <span class="brand-main">Real Estate Manager</span>
        <span class="brand-sub">by DIGITALEYES</span>
      </div>
      <div class="sidebar-group">
        ${navItems.map(n => `<a href="${n.href}" class="${n.href === currentPage ? "current" : ""}">${n.label}</a>`).join("")}
      </div>
      <div class="sidebar-footer">
        <button class="btn-logout" onclick="logout()">ログアウト</button>
      </div>
    </aside>

    <div class="header-welcome">
      <span class="header-welcome-line">
        ${remRoleIconHtml(user?.role)}
        <span class="user-name">${displayName}</span>
        <span>さま</span>
      </span>
    </div>
    <div class="d-flex align-items-center gap-3">
      <div class="header-select">
        <div class="prop-picker" id="propPicker">
          <button type="button" class="prop-picker-btn" onclick="togglePropertyPicker()">${labelOfProp(active || props[0] || { id: "-" })}</button>
          <div class="prop-picker-menu">${propertyOptions}</div>
        </div>
      </div>
    </div>
  `;
}

function togglePropertyPicker() {
  const el = document.getElementById("propPicker");
  if (!el) return;
  el.classList.toggle("open");
}
function closePropertyPicker() {
  const el = document.getElementById("propPicker");
  if (!el) return;
  el.classList.remove("open");
}
async function chooseProperty(propertyId) {
  closePropertyPicker();
  await onPropertyChange(propertyId);
}
async function onPropertyChange(propertyId) {
  await selectProperty(propertyId);
  const pageFile = (location.pathname.split("/").pop() || "").toLowerCase();
  if (pageFile === "property-detail.html") {
    location.href = `property-detail.html?id=${encodeURIComponent(propertyId)}`;
    return;
  }
  location.reload();
}
if (typeof document !== "undefined") {
  document.addEventListener("click", (ev) => {
    const el = document.getElementById("propPicker");
    if (!el) return;
    if (!el.contains(ev.target)) el.classList.remove("open");
  });
}

// ── APIコール ──────────────────────────────────
async function apiCall(params) {
  const active = Auth.getActiveProperty();
  if (!active) throw new Error("物件が選択されていません。設定ページから物件を追加してください。");
  const postData = new URLSearchParams();
  postData.append("table_name", active.tableName);
  Object.entries(params).forEach(([k, v]) => {
    postData.append(k, typeof v === "object" ? JSON.stringify(v) : String(v));
  });
  const res = await fetch(`${CONFIG.API_BASE}/api/v1/database/acquisition.json`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      "Authorization": `Bearer ${Auth.getToken()}`
    },
    body: postData.toString()
  });
  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_) {
    if (!res.ok) {
      throw new Error(
        `HTTPエラー: ${res.status} — 応答がJSONではありません（${text.slice(0, 120)}${text.length > 120 ? "…" : ""}）`
      );
    }
    throw new Error("API応答の解析に失敗しました");
  }
  if (!res.ok) {
    const msg =
      data.message ||
      data.error ||
      data.Message ||
      (typeof data.status === "string" ? data.status : "") ||
      "";
    throw new Error(msg ? `HTTPエラー: ${res.status} — ${msg}` : `HTTPエラー: ${res.status}`);
  }
  if (!data.status) throw new Error(data.message || "APIエラー");
  return data.data;
}

function parseTotalCount(data) {
  const c = data.count || data.total_count || "0";
  return typeof c === "string" ? parseInt(c.split("/").pop()) : c;
}

/** 一覧列キー: 全角ドット等を正規化（保存済み `c．status` 等でセル描画が一致しないのを防ぐ） */
function normalizeCustomerListColKey(key) {
  return String(key || "")
    .trim()
    .replace(/\uFF0E/g, ".")
    .replace(/\u3002/g, ".")
    .normalize("NFKC");
}

function baitaiBadge(v) {
  if (!v) return "";
  const map = { "公式HP": "badge-hp", "SUUMO": "badge-suumo", "HOMES": "badge-homes", "Yahoo!不動産": "badge-yahoo" };
  return `<span class="badge-tag ${map[v] || 'badge-hp'}">${v}</span>`;
}
function statusBadge(v) {
  const s = v == null ? "" : String(v).trim();
  if (!s) return "";
  const map = { "資料請求・エントリー": "badge-entry", "新規来場": "badge-visit", "再来場": "badge-visit", "契約": "badge-contract", "引渡": "badge-contract", "検討中止": "badge-cancel" };
  return `<span class="badge-status ${map[s] || 'badge-entry'}">${s}</span>`;
}
function exportCSV(rows, headers, keys, filename, formatCell) {
  const fmt =
    typeof formatCell === "function"
      ? formatCell
      : (_k, v, _r) => v;
  const csv = [
    headers.join(","),
    ...rows.map((r) =>
      keys
        .map((k) => {
          const v = fmt(k, r[k], r);
          const s = v == null ? "" : String(v);
          return `"${s.replace(/"/g, '""')}"`;
        })
        .join(",")
    ),
  ].join("\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
  const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = filename; a.click();
}
