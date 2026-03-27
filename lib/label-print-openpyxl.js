const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");

function runPython(scriptPath, args) {
  return new Promise((resolve, reject) => {
    const cmd = process.env.PYTHON_BIN || "python3";
    const cp = spawn(cmd, [scriptPath, ...args], { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    cp.stderr.on("data", (d) => { stderr += String(d || ""); });
    cp.on("error", (e) => reject(e));
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

