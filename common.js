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
  clear: () => { localStorage.removeItem("token"); localStorage.removeItem("user"); localStorage.removeItem("properties"); localStorage.removeItem("activeProperty"); },
  getUser: () => { try { return JSON.parse(localStorage.getItem("user")); } catch { return null; } },
  setUser: (u) => localStorage.setItem("user", JSON.stringify(u)),
  getProperties: () => { try { return JSON.parse(localStorage.getItem("properties")) || []; } catch { return []; } },
  setProperties: (p) => localStorage.setItem("properties", JSON.stringify(p)),
  getActiveProperty: () => { try { return JSON.parse(localStorage.getItem("activeProperty")); } catch { return null; } },
  setActiveProperty: (p) => localStorage.setItem("activeProperty", JSON.stringify(p)),
};

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
    Auth.setUser(data.user);
    Auth.setProperties(data.properties);
    // アクティブ物件を同期
    const active = data.properties.find(p => p.id === data.user.activePropertyId);
    if (active) Auth.setActiveProperty(active);
    else if (data.properties.length) {
      Auth.setActiveProperty(data.properties[0]);
      await selectProperty(data.properties[0].id);
    } else Auth.setActiveProperty(null);
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
  const isAdmin = Number(user?.role) === 2;
  const labelOfProp = (p) => {
    const raw = p?.name ?? p?.propertyName ?? "";
    const s = String(raw).trim();
    return s && s !== "undefined" ? s : `物件(${p?.id || "-"})`;
  };
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
      <span class="user-name">${user?.name || ""}</span>
      <span>さま</span>
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

function baitaiBadge(v) {
  if (!v) return "";
  const map = { "公式HP": "badge-hp", "SUUMO": "badge-suumo", "HOMES": "badge-homes", "Yahoo!不動産": "badge-yahoo" };
  return `<span class="badge-tag ${map[v] || 'badge-hp'}">${v}</span>`;
}
function statusBadge(v) {
  if (!v) return "";
  const map = { "資料請求・エントリー": "badge-entry", "新規来場": "badge-visit", "再来場": "badge-visit", "契約": "badge-contract", "引渡": "badge-contract", "検討中止": "badge-cancel" };
  return `<span class="badge-status ${map[v] || 'badge-entry'}">${v}</span>`;
}
function exportCSV(rows, headers, keys, filename) {
  const csv = [headers.join(","), ...rows.map(r => keys.map(k => `"${(r[k]||"").toString().replace(/"/g,'""')}"`).join(","))].join("\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
  const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = filename; a.click();
}
