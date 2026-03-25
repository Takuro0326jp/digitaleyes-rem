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
  const nav = [
    { label: "ダッシュボード", href: "dashboard.html" },
    { label: "顧客一覧", href: "customer.html" },
    { label: "接客スケジュール", href: "schedule.html" },
    { label: "物件一覧", href: "property.html" },
    { label: "クライアント一覧", href: "client.html" },
    { label: "広告管理", href: "ad.html" },
    { label: "設定", href: "settings.html" },
  ];
  const propertyOptions = props.length
    ? props.map(p => `<option value="${p.id}" ${p.id === active?.id ? "selected" : ""}>${p.name}</option>`).join("")
    : `<option value="">物件未登録 → 設定ページへ</option>`;

  return `
    <div class="header-welcome">
      <span class="user-name">${user?.name || ""} さま</span>
    </div>
    <div class="d-flex align-items-center">
      <div class="header-select">
        <select onchange="onPropertyChange(this.value)">${propertyOptions}</select>
      </div>
      <nav>
        <ul class="header-navigation__list">
          ${nav.map(n => `<li><a href="${n.href}" class="${n.href === currentPage ? 'current' : ''}">${n.label}</a></li>`).join('')}
          <li><button class="btn-logout" onclick="logout()">ログアウト</button></li>
        </ul>
      </nav>
    </div>
  `;
}

async function onPropertyChange(propertyId) {
  await selectProperty(propertyId);
  location.reload();
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
  if (!res.ok) throw new Error(`HTTPエラー: ${res.status}`);
  const data = await res.json();
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
