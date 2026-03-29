const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");

const LABEL_PRINT_SECRET_HEADER = "x-rem-label-secret";

function vercelDeploymentBaseUrl() {
  const u = String(process.env.VERCEL_URL || "").trim();
  if (!u) return "";
  return /^https?:\/\//i.test(u) ? u.replace(/\/+$/, "") : `https://${u.replace(/\/+$/, "")}`;
}

/**
 * Vercel の Node ランタイムには python3 が無いため、同デプロイ内の Python サーバーレス（api/label_print.py）へ委譲する。
 */
async function buildLabelWorkbookViaVercelPython(customers) {
  const secret = String(process.env.REM_LABEL_PRINT_INTERNAL_SECRET || "").trim();
  if (!secret) {
    throw new Error(
      "本番（Vercel）でラベル印刷するには、環境変数 REM_LABEL_PRINT_INTERNAL_SECRET を設定してください（32文字以上のランダム文字列）。"
    );
  }
  const base = vercelDeploymentBaseUrl();
  if (!base) {
    throw new Error("VERCEL_URL が取得できません。ラベル印刷をスキップできません。");
  }
  const url = `${base}/api/label_print`;
  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        [LABEL_PRINT_SECRET_HEADER]: secret,
      },
      body: JSON.stringify({ customers }),
    });
  } catch (e) {
    throw new Error(`ラベル印刷サービスへの接続に失敗しました: ${e.message || String(e)}`);
  }
  const buf = Buffer.from(await res.arrayBuffer());
  if (!res.ok) {
    let msg = buf.toString("utf8").slice(0, 800);
    try {
      const j = JSON.parse(msg);
      if (j && j.message) msg = j.message;
    } catch (_) {}
    throw new Error(msg || `ラベル印刷に失敗しました（HTTP ${res.status}）`);
  }
  return buf;
}

function runPython(scriptPath, args) {
  return new Promise((resolve, reject) => {
    const cmd = process.env.PYTHON_BIN || "python3";
    const cp = spawn(cmd, [scriptPath, ...args], { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    cp.stderr.on("data", (d) => { stderr += String(d || ""); });
    cp.on("error", (e) => {
      if (e && e.code === "ENOENT") {
        reject(new Error(
          `${cmd} が見つかりません。Python 3 と openpyxl をインストールするか、本番では Vercel 用の REM_LABEL_PRINT_INTERNAL_SECRET を設定してください。`
        ));
        return;
      }
      reject(e);
    });
    cp.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(stderr.trim() || `Python exited with code ${code}`));
    });
  });
}

async function buildLabelWorkbookWithOpenpyxl(templatePath, customers) {
  if (!Array.isArray(customers) || !customers.length) {
    throw new Error("customers が空です");
  }
  if (String(process.env.VERCEL || "").trim() === "1") {
    return buildLabelWorkbookViaVercelPython(customers);
  }
  const pyPath = path.join(__dirname, "label-print-openpyxl.py");
  if (!fs.existsSync(pyPath)) {
    throw new Error("label-print-openpyxl.py が見つかりません");
  }
  if (!fs.existsSync(templatePath)) {
    throw new Error("テンプレートファイルが見つかりません");
  }
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "rem-label-"));
  const inJson = path.join(tmpDir, `in-${crypto.randomUUID()}.json`);
  const outXlsx = path.join(tmpDir, `out-${crypto.randomUUID()}.xlsx`);
  try {
    fs.writeFileSync(inJson, JSON.stringify({ customers }), "utf-8");
    await runPython(pyPath, [templatePath, inJson, outXlsx]);
    const buf = fs.readFileSync(outXlsx);
    return buf;
  } catch (e) {
    const msg = String(e && e.message ? e.message : e);
    if (/No module named ['"]openpyxl['"]/.test(msg)) {
      throw new Error("openpyxl が未インストールです。`python3 -m pip install openpyxl` を実行してください。");
    }
    throw e;
  } finally {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
  }
}

module.exports = { buildLabelWorkbookWithOpenpyxl };

