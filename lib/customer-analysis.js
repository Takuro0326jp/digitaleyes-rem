/**
 * customer_snapshot から分析レポート用の集計を生成する。
 * payload は JSON（主に c.* キー）。仕様書の日本語キーはフォールバックとして参照する。
 */
const { getDb } = require("./sync-db");
const { getVisitStatuses, stateLabelFromRaw } = require("./sync-service");
const { DEFAULT_VISIT_STATUSES } = require("./visit-status-defaults");

function toYmd(v) {
  if (!v) return "";
  const s = String(v).trim();
  const m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (m) return `${m[1]}-${String(m[2]).padStart(2, "0")}-${String(m[3]).padStart(2, "0")}`;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return "";
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function propStartYmd(property) {
  const raw = property?.createdAt || property?.created_at || "";
  const y = toYmd(raw);
  return y || "1970-01-01";
}

function buildStatusSets(statusList) {
  const list = Array.isArray(statusList) && statusList.length ? statusList : DEFAULT_VISIT_STATUSES;
  const keiyaku = new Set();
  const raijo = new Set();
  for (const s of list) {
    const x = String(s || "");
    if (!x) continue;
    if (/契約|引渡/.test(x)) keiyaku.add(x);
    if (/検討中止|エントリー|来場予約/.test(x)) continue;
    if (/来場|申込|契約|引渡|要望|登録/.test(x)) raijo.add(x);
  }
  return { keiyaku, raijo };
}

function parsePayload(raw) {
  if (raw && typeof raw === "object") return raw;
  try {
    const o = JSON.parse(String(raw || "{}"));
    return o && typeof o === "object" ? o : {};
  } catch {
    return {};
  }
}

function pickField(p, denorm, japaneseKeys, cKeys) {
  for (const k of japaneseKeys) {
    const v = p[k];
    if (v != null && String(v).trim() !== "") return String(v).trim();
  }
  for (const k of cKeys) {
    const v = p[k];
    if (v != null && String(v).trim() !== "") return String(v).trim();
  }
  for (const k of cKeys) {
    const col = k === "c.state" ? "state" : k === "c.city" ? "city" : null;
    if (col && denorm[col] != null && String(denorm[col]).trim() !== "") return String(denorm[col]).trim();
  }
  return "";
}

function labelOrMissing(s) {
  const t = String(s || "").trim();
  return t ? t : "未記入";
}

function normPayloadKey(k) {
  return String(k ?? "")
    .normalize("NFKC")
    .replace(/\uFF0E/g, ".")
    .replace(/\u3002/g, ".")
    .trim();
}

/** 全角ドットの c．city 等に対応 */
function pickLooseFromObject(obj, candidateKeys) {
  if (!obj || typeof obj !== "object") return "";
  for (const want of candidateKeys) {
    const wn = normPayloadKey(want);
    for (const [k, v] of Object.entries(obj)) {
      if (normPayloadKey(k) === wn && v != null && String(v).trim() !== "") {
        return String(v).trim();
      }
    }
  }
  return "";
}

/**
 * 分析「市区町村別」用: マージ済み payload と API 生の remote_payload の両方から市区→町村の順で解決。
 * ラベルは市区町村名のみ（都道府県は付けない）。
 */
function municipalityLabelOnly(pMain, pRemote, denormCity, rowCity) {
  const cityKeys = ["市区郡", "市区町村", "c.city"];
  const townKeys = ["町村", "c.town"];
  const fromCity =
    pickLooseFromObject(pMain, cityKeys) ||
    pickLooseFromObject(pRemote, cityKeys) ||
    String(denormCity || "").trim() ||
    String(rowCity || "").trim();
  if (fromCity) return fromCity;
  const fromTown =
    pickLooseFromObject(pMain, townKeys) || pickLooseFromObject(pRemote, townKeys);
  return fromTown ? String(fromTown).trim() : "";
}

function splitMedia(s) {
  if (!s) return [];
  return String(s)
    .split(/[,、\n|;/]+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseBirthToAge(birthRaw, refYmd) {
  if (!birthRaw) return null;
  const s = String(birthRaw).trim();
  let y;
  let m = 1;
  let d = 1;
  const iso = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (iso) {
    y = Number(iso[1]);
    m = Number(iso[2]);
    d = Number(iso[3]);
  } else if (/^\d{8}$/.test(s)) {
    y = Number(s.slice(0, 4));
    m = Number(s.slice(4, 6));
    d = Number(s.slice(6, 8));
  } else {
    const dt = new Date(s.replace(/\//g, "-"));
    if (Number.isNaN(dt.getTime())) return null;
    y = dt.getFullYear();
    m = dt.getMonth() + 1;
    d = dt.getDate();
  }
  const ref = new Date(`${refYmd}T12:00:00`);
  const bd = new Date(y, m - 1, d);
  if (Number.isNaN(ref.getTime()) || Number.isNaN(bd.getTime())) return null;
  let age = ref.getFullYear() - bd.getFullYear();
  const md = ref.getMonth() - bd.getMonth();
  if (md < 0 || (md === 0 && ref.getDate() < bd.getDate())) age -= 1;
  return age;
}

function ageBand(age) {
  if (age == null || !Number.isFinite(age)) return "未記入";
  if (age < 25) return "25歳未満";
  if (age < 30) return "25〜29歳";
  if (age < 35) return "30〜34歳";
  if (age < 40) return "35〜39歳";
  if (age < 45) return "40〜44歳";
  if (age < 50) return "45〜49歳";
  if (age < 55) return "50〜54歳";
  return "55歳以上";
}

function incomeSortKey(label) {
  if (label === "未記入") return 999999;
  const s = String(label);
  const nums = s.match(/\d+/g);
  if (!nums || !nums.length) return 888888;
  return Number(nums[0]) * 1000 + Number(nums[1] || 0);
}

function bucket3() {
  return { han_total: 0, han_week: 0, rai_total: 0, rai_week: 0, kei_total: 0, kei_week: 0 };
}

function addHan(b, inWeek, inTotal) {
  if (inTotal) {
    b.han_total += 1;
    if (inWeek) b.han_week += 1;
  }
}
function addRai(b, inWeek, inTotal, ok) {
  if (!ok) return;
  if (inTotal) {
    b.rai_total += 1;
    if (inWeek) b.rai_week += 1;
  }
}
function addKei(b, inWeek, inTotal, ok) {
  if (!ok) return;
  if (inTotal) {
    b.kei_total += 1;
    if (inWeek) b.kei_week += 1;
  }
}

function mapToMediaArray(m) {
  return [...m.entries()]
    .map(([label, b]) => ({
      label,
      han_total: b.han_total,
      han_week: b.han_week,
      rai_total: b.rai_total,
      rai_week: b.rai_week,
      kei_total: b.kei_total,
    }))
    .sort((a, b) => b.han_total - a.han_total || String(a.label).localeCompare(String(b.label), "ja"));
}

function mapToDimArray(m, sortFn) {
  const arr = [...m.entries()].map(([label, b]) => ({
    label,
    han_total: b.han_total,
    han_week: b.han_week,
    rai_total: b.rai_total,
    rai_week: b.rai_week,
    kei_total: b.kei_total,
  }));
  arr.sort(sortFn);
  return arr;
}

function mapToSimpleBars(m, sortFn) {
  const arr = [...m.entries()].map(([label, o]) => ({
    label,
    han: o.han,
    rai: o.rai,
    kei: o.kei,
  }));
  arr.sort(sortFn);
  return arr;
}

function mapToParking(m) {
  return [...m.entries()]
    .map(([label, o]) => ({ label, han: o.han }))
    .sort((a, b) => b.han - a.han || String(a.label).localeCompare(String(b.label), "ja"));
}

function isHyogoRow(state) {
  return String(state || "").includes("兵庫");
}

/**
 * libsql / @libsql/client は行を「連想配列」または「列順の配列」で返すことがある。
 * queryLocalCustomers と同様に正規化する。
 */
function coerceSnapshotRow(row) {
  if (row == null) return null;
  /** タプル行: 配列、または Array 風オブジェクト（列名プロパティが無い） */
  const tupleLike =
    Array.isArray(row) ||
    (typeof row === "object" &&
      row.customer_id === undefined &&
      row.date_entry === undefined &&
      row.payload === undefined &&
      row[0] != null);
  if (tupleLike) {
    const r = row;
    return {
      customer_id: r[0],
      date_entry: r[1],
      upd_date: r[2],
      status: r[3],
      state: r[4],
      city: r[5],
      baitai: r[6],
      yosan: r[7],
      ninzu: r[8],
      jikosikin: r[9],
      questionnaire23: r[10],
      questionnaire24: r[11],
      payload: r[12],
      remote_payload: r[13],
    };
  }
  return {
    customer_id: row.customer_id,
    date_entry: row.date_entry,
    upd_date: row.upd_date,
    status: row.status,
    state: row.state,
    city: row.city,
    baitai: row.baitai,
    yosan: row.yosan,
    ninzu: row.ninzu,
    jikosikin: row.jikosikin,
    questionnaire23: row.questionnaire23,
    questionnaire24: row.questionnaire24,
    payload: row.payload,
    remote_payload: row.remote_payload,
  };
}

/** 反響日: 列 → payload の c.date_entry → upd（列 / c.upd_date）の順で解決 */
function effectiveReactionYmd(norm, p) {
  return (
    toYmd(norm?.date_entry) ||
    toYmd(p?.["c.date_entry"]) ||
    toYmd(norm?.upd_date) ||
    toYmd(p?.["c.upd_date"])
  );
}

/**
 * @param {object} opts
 * @param {string} opts.propertyId
 * @param {object} opts.property — getPropertyByIdForUser の戻り
 * @param {string} opts.weekStart YYYY-MM-DD
 * @param {string} opts.weekEnd YYYY-MM-DD
 */
async function buildCustomerAnalysisReport({ propertyId, property, weekStart, weekEnd }) {
  const db = await getDb();
  const visitStatuses = await getVisitStatuses(propertyId);
  const { keiyaku, raijo } = buildStatusSets(visitStatuses);

  const totalStart = propStartYmd(property);
  const r = await db.execute({
    sql: `SELECT customer_id, date_entry, upd_date, status, state, city, baitai, yosan, ninzu, jikosikin,
                 questionnaire23, questionnaire24, payload, remote_payload
          FROM customer_snapshot WHERE property_id = ?`,
    args: [propertyId],
  });

  const rows = r.rows || [];
  const refYmd = weekEnd;

  const mediaMap = new Map();
  const prefMap = new Map();
  const areaMap = new Map();
  const hyogoCityMap = new Map();
  const budgetMap = new Map();
  const rentMap = new Map();
  const madoriMap = new Map();
  const mensekiMap = new Map();
  const parkingMap = new Map();
  const ageMap = new Map();
  const incomeMap = new Map();

  let summary = {
    week: { han: 0, raijo: 0, keiyaku: 0 },
    total: { han: 0, raijo: 0, keiyaku: 0 },
    raijo_rate: 0,
    keiyaku_rate: 0,
  };

  for (const raw of rows) {
    const row = coerceSnapshotRow(raw);
    if (!row) continue;
    const p = parsePayload(row.payload);
    const pRemote = parsePayload(row.remote_payload);
    const denorm = {
      state: String(row.state ?? p["c.state"] ?? "").trim(),
      city: String(row.city ?? p["c.city"] ?? "").trim(),
      baitai: String(row.baitai ?? p["c.baitai"] ?? "").trim(),
      status: String(row.status ?? p["c.status"] ?? "").trim(),
    };
    const de = effectiveReactionYmd(row, p);
    const st = denorm.status;
    const inWeek = de && de >= weekStart && de <= weekEnd;
    const inTotal = de && de >= totalStart && de <= weekEnd;
    const isRai = raijo.has(st);
    const isKei = keiyaku.has(st);

    if (inWeek) {
      summary.week.han += 1;
      if (isRai) summary.week.raijo += 1;
      if (isKei) summary.week.keiyaku += 1;
    }
    if (inTotal) {
      summary.total.han += 1;
      if (isRai) summary.total.raijo += 1;
      if (isKei) summary.total.keiyaku += 1;
    }

    const mediaRaw = pickField(p, denorm, ["この物件を知ったキッカケ（複数選択可）"], ["c.baitai"]);
    const tokens = splitMedia(mediaRaw || denorm.baitai);
    if (!tokens.length) tokens.push(labelOrMissing(""));

    for (const token of tokens) {
      const lab = labelOrMissing(token);
      if (!mediaMap.has(lab)) mediaMap.set(lab, bucket3());
      const b = mediaMap.get(lab);
      addHan(b, inWeek, inTotal);
      addRai(b, inWeek, inTotal, isRai);
      addKei(b, inWeek, inTotal, isKei);
    }

    const rawFromField = String(pickField(p, denorm, ["都道府県"], ["c.state"]) || "").trim();
    const rawState = rawFromField || String(denorm.state || "").trim();
    const stateVal = rawState ? stateLabelFromRaw(rawState) || rawState : "";
    const pref = labelOrMissing(stateVal);
    const cityValRaw = String(pickField(p, denorm, [], ["c.city"]) || denorm.city || "").trim();

    if (!prefMap.has(pref)) prefMap.set(pref, bucket3());
    const pb = prefMap.get(pref);
    addHan(pb, inWeek, inTotal);
    addRai(pb, inWeek, inTotal, isRai);
    addKei(pb, inWeek, inTotal, isKei);

    const muniLabel = municipalityLabelOnly(p, pRemote, denorm.city, row.city);
    if (muniLabel) {
      if (!areaMap.has(muniLabel)) areaMap.set(muniLabel, bucket3());
      const ab = areaMap.get(muniLabel);
      addHan(ab, inWeek, inTotal);
      addRai(ab, inWeek, inTotal, isRai);
      addKei(ab, inWeek, inTotal, isKei);
    }

    if (isHyogoRow(stateVal) || isHyogoRow(denorm.state)) {
      const cLab = labelOrMissing(pickField(p, denorm, ["市区郡"], ["c.city"]) || cityValRaw);
      if (!hyogoCityMap.has(cLab)) hyogoCityMap.set(cLab, bucket3());
      const hb = hyogoCityMap.get(cLab);
      addHan(hb, inWeek, inTotal);
      addRai(hb, inWeek, inTotal, isRai);
      addKei(hb, inWeek, inTotal, isKei);
    }

    function simpleBucket(map, labelRaw) {
      const lab = labelOrMissing(labelRaw);
      if (!map.has(lab)) map.set(lab, { han: 0, rai: 0, kei: 0 });
      const x = map.get(lab);
      if (inTotal) {
        x.han += 1;
        if (isRai) x.rai += 1;
        if (isKei) x.kei += 1;
      }
    }

    simpleBucket(
      budgetMap,
      pickField(p, denorm, ["ご予算"], ["c.yosan"]) || row.yosan
    );
    simpleBucket(
      rentMap,
      pickField(p, denorm, ["現在のお住まいの月額家賃（駐車場代含）"], [])
    );
    simpleBucket(
      madoriMap,
      pickField(p, denorm, ["ご希望間取り"], ["c.questionnaire23"]) || row.questionnaire23
    );
    simpleBucket(
      mensekiMap,
      pickField(p, denorm, ["ご希望面積"], ["c.questionnaire24"]) || row.questionnaire24
    );

    const parkLab = labelOrMissing(pickField(p, denorm, ["駐車場"], []));
    if (!parkingMap.has(parkLab)) parkingMap.set(parkLab, { han: 0 });
    if (inTotal) parkingMap.get(parkLab).han += 1;

    const birthRaw = pickField(p, denorm, ["生年月日"], ["c.birth"]);
    const age = parseBirthToAge(birthRaw, refYmd);
    const agL = ageBand(age);
    if (!ageMap.has(agL)) ageMap.set(agL, bucket3());
    const agb = ageMap.get(agL);
    addHan(agb, inWeek, inTotal);
    addRai(agb, inWeek, inTotal, isRai);
    addKei(agb, inWeek, inTotal, isKei);

    const incLab = labelOrMissing(
      pickField(p, denorm, ["年収（税込）"], ["c.questionnaire30"])
    );
    if (!incomeMap.has(incLab)) incomeMap.set(incLab, bucket3());
    const ib = incomeMap.get(incLab);
    addHan(ib, inWeek, inTotal);
    addRai(ib, inWeek, inTotal, isRai);
    addKei(ib, inWeek, inTotal, isKei);
  }

  const ageOrder = [
    "25歳未満",
    "25〜29歳",
    "30〜34歳",
    "35〜39歳",
    "40〜44歳",
    "45〜49歳",
    "50〜54歳",
    "55歳以上",
    "未記入",
  ];
  const ageArr = mapToDimArray(ageMap, (a, b) => {
    const ia = ageOrder.indexOf(a.label);
    const ibb = ageOrder.indexOf(b.label);
    const va = ia < 0 ? 100 : ia;
    const vb = ibb < 0 ? 100 : ibb;
    return va - vb || String(a.label).localeCompare(String(b.label), "ja");
  });

  const incomeArr = mapToDimArray(incomeMap, (a, b) => {
    const da = incomeSortKey(a.label);
    const db = incomeSortKey(b.label);
    if (da !== db) return da - db;
    return String(a.label).localeCompare(String(b.label), "ja");
  });

  const missLast = (a, b) => {
    const ae = a.label === "未記入" ? 1 : 0;
    const be = b.label === "未記入" ? 1 : 0;
    if (ae !== be) return ae - be;
    return b.han_total - a.han_total;
  };
  const missLastSimple = (a, b) => {
    const ae = a.label === "未記入" ? 1 : 0;
    const be = b.label === "未記入" ? 1 : 0;
    if (ae !== be) return ae - be;
    return b.han - a.han;
  };

  summary.raijo_rate = summary.total.han ? Math.round((summary.total.raijo / summary.total.han) * 1000) / 10 : 0;
  summary.keiyaku_rate = summary.total.raijo
    ? Math.round((summary.total.keiyaku / summary.total.raijo) * 1000) / 10
    : 0;

  return {
    meta: {
      propertyId,
      weekStart,
      weekEnd,
      totalRangeStart: totalStart,
      rowCount: rows.length,
    },
    summary,
    media: mapToMediaArray(mediaMap),
    prefecture: mapToDimArray(prefMap, missLast),
    area: mapToDimArray(areaMap, missLast),
    hyogoCities: mapToDimArray(hyogoCityMap, missLast),
    budget: mapToSimpleBars(budgetMap, missLastSimple),
    rent: mapToSimpleBars(rentMap, missLastSimple),
    madori: mapToSimpleBars(madoriMap, missLastSimple),
    menseki: mapToSimpleBars(mensekiMap, missLastSimple),
    parking: mapToParking(parkingMap),
    age: ageArr,
    income: incomeArr,
  };
}

module.exports = { buildCustomerAnalysisReport, toYmd };
