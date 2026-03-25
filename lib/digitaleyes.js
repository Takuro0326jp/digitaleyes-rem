const https = require("https");

const API_HOST = "api.digital-eyes.jp";
const ACQUISITION_PATH = "/api/v1/database/acquisition.json";

/**
 * デジタライズ acquisition.json を POST
 * @param {object} prop 物件 { databaseId, databasePassword, tableName }
 * @param {object} secrets { SECRET_ID, SECRET_PASSWORD }
 * @param {object} paramsObj limit, page, order, fields, conditions など（table_name は内部で付与）
 */
function postAcquisition(prop, secrets, paramsObj) {
  const body = new URLSearchParams();
  Object.entries(paramsObj).forEach(([k, v]) => {
    if (v === undefined || v === null) return;
    body.append(k, typeof v === "object" ? JSON.stringify(v) : String(v));
  });
  body.set("X_secret_id", secrets.SECRET_ID || "");
  body.set("X_secret_password", secrets.SECRET_PASSWORD || "");
  body.set("X_database_id", prop.databaseId);
  body.set("X_database_password", prop.databasePassword || "");
  body.set("table_name", prop.tableName);

  const payload = body.toString();

  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: API_HOST,
        path: ACQUISITION_PATH,
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
          "Content-Length": Buffer.byteLength(payload),
        },
      },
      (pres) => {
        let raw = "";
        pres.on("data", (c) => (raw += c));
        pres.on("end", () => {
          try {
            resolve(JSON.parse(raw));
          } catch (e) {
            reject(new Error("デジタライズAPIの応答がJSONではありません"));
          }
        });
      }
    );
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

module.exports = { postAcquisition, API_HOST, ACQUISITION_PATH };
