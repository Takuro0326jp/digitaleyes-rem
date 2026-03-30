const ExcelJS = require("exceljs");
const { getDb } = require("./sync-db");
const { getVisitStatuses } = require("./sync-service");
const { loadWeekCustomersForReport } = require("./weekly-report-details");

function toYmd(v) {
  if (!v) return "";
  const s = String(v).trim();
  const m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (m) return `${m[1]}-${String(m[2]).padStart(2, "0")}-${String(m[3]).padStart(2, "0")}`;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return "";
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function addDays(ymd, days) {
  const d = new Date(`${ymd}T00:00:00`);
  d.setDate(d.getDate() + days);
  return toYmd(d);
}

function fmtJa(ymd) {
  const d = new Date(`${ymd}T00:00:00`);
  const w = ["日", "月", "火", "水", "木", "金", "土"][d.getDay()];
  return `${d.getFullYear()}/${d.getMonth() + 1}/${d.getDate()}(${w})`;
}

function weekDayJa(ymd) {
  const d = new Date(`${ymd}T00:00:00`);
  return ["日", "月", "火", "水", "木", "金", "土"][d.getDay()];
}

function generateWeeks(startDate, endDate, weekStartDay) {
  const weeks = [];
  const s = new Date(`${startDate}T00:00:00`);
  const e = new Date(`${endDate}T00:00:00`);
  const weekEndDay = weekStartDay === "monday" ? 0 : 6;

  let firstWeekEnd = new Date(s);
  while (firstWeekEnd.getDay() !== weekEndDay) firstWeekEnd.setDate(firstWeekEnd.getDate() + 1);
  if (firstWeekEnd > e) firstWeekEnd = new Date(e);
  weeks.push({ weekNum: 1, start: toYmd(s), end: toYmd(firstWeekEnd) });

  let currentStart = new Date(firstWeekEnd);
  currentStart.setDate(currentStart.getDate() + 1);
  let weekNum = 2;
  while (currentStart <= e) {
    let currentEnd = new Date(currentStart);
    currentEnd.setDate(currentEnd.getDate() + 6);
    if (currentEnd > e) currentEnd = new Date(e);
    weeks.push({ weekNum, start: toYmd(currentStart), end: toYmd(currentEnd) });
    currentStart = new Date(currentEnd);
    currentStart.setDate(currentStart.getDate() + 1);
    weekNum += 1;
  }
  return weeks;
}

function listYmd(startYmd, endYmd) {
  const out = [];
  let cur = startYmd;
  while (cur <= endYmd) {
    out.push(cur);
    cur = addDays(cur, 1);
  }
  return out;
}

/** YYYY-MM-DD の暦日の曜日（0=日 … 6=土）。サーバー/ブラウザのローカルTZに依存しない */
function ymdUtcWeekday(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd || "").trim());
  if (!m) return null;
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]))).getUTCDay();
}

/** LibSQL の Row は列名アクセスが効かない環境があるため r[0], r[1]… にフォールバック */
function toMapByDate(rows) {
  const m = new Map();
  (rows || []).forEach((r) => {
    const d = toYmd(r.date ?? r[0]);
    if (!d) return;
    m.set(d, Number(r.count ?? r[1] ?? 0));
  });
  return m;
}

function mergeActionRows(rows) {
  const map = new Map();
  (rows || []).forEach((r) => {
    const d = toYmd(r.date ?? r[0]);
    if (!d) return;
    const at = String(r.action_type ?? r[1] ?? "");
    const key = `${d}__${at}`;
    map.set(key, Number(r.count ?? r[2] ?? 0));
  });
  return map;
}

function mergeStatusRows(rows) {
  const out = new Map();
  (rows || []).forEach((r) => {
    const d = toYmd(r.date ?? r[0]);
    if (!d) return;
    const toV = r.to_value ?? r[1];
    const fromV = r.from_value ?? r[2];
    const k = `${d}__${String(toV ?? "")}__${String(fromV ?? "")}`;
    out.set(k, Number(r.count ?? r[3] ?? 0));
  });
  return out;
}

function pickByKeywords(list, keywords) {
  return (list || []).filter((v) => {
    const s = String(v || "").toLowerCase();
    return keywords.some((k) => s.includes(k));
  });
}

function sumMapByDates(map, dates) {
  return dates.reduce((acc, d) => acc + Number(map.get(d) || 0), 0);
}

function calcMonthlyCount(map, startYmd, endYmd, targetMonth) {
  let total = 0;
  let cur = startYmd;
  while (cur <= endYmd) {
    const d = new Date(`${cur}T00:00:00`);
    if (d.getMonth() === targetMonth) total += Number(map.get(cur) || 0);
    cur = addDays(cur, 1);
  }
  return total;
}

function propStartYmd(property) {
  const raw = property?.createdAt || property?.created_at || "";
  const y = toYmd(raw);
  return y || "1970-01-01";
}

/**
 * 累計・月別の開始日。createdAt が未設定・不正で weekEnd より後ろになると
 * listYmd が空になり週計だけ残るため、必ず weekEnd 以前に丸める。
 */
function effectiveTotalStartYmd(property, weekEndYmd) {
  let s = propStartYmd(property);
  if (!s || s > weekEndYmd) s = "1970-01-01";
  return s;
}

/** statusChangeMap のキーは YYYY-MM-DD__to_value__from_value（to に __ が含まれる場合あり） */
function parseStatusChangeMapKey(k) {
  if (typeof k !== "string" || k.length < 13) return null;
  const d = k.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null;
  if (k.slice(10, 12) !== "__") return null;
  const rest = k.slice(12);
  const li = rest.lastIndexOf("__");
  if (li < 0) return null;
  return { d, toV: rest.slice(0, li), fromV: rest.slice(li + 2) };
}

/** 1行分の日次マップ（集計テーブル行と同一ロジック） */
function buildDailyMapForRowDef(def, dateList, allAgg) {
  const daily = new Map();
  for (const d of dateList) {
    let val = 0;
    if (def.type === "reaction") {
      val = Number(allAgg.reactionMap.get(d) || 0);
    } else if (def.type === "sales") {
      const sk = def.salesKey || def.key;
      const keys = [...allAgg.salesActionMap.keys()].filter((k) => k.startsWith(`${d}__`) && k.includes(sk));
      val = keys.reduce((acc, k) => acc + Number(allAgg.salesActionMap.get(k) || 0), 0);
    } else if (def.type === "status_to") {
      const keys = [...allAgg.statusChangeMap.keys()].filter((k) => {
        const p = parseStatusChangeMapKey(k);
        return p && p.d === d && (def.keys || []).includes(p.toV);
      });
      val = keys.reduce((acc, k) => acc + Number(allAgg.statusChangeMap.get(k) || 0), 0);
    } else if (def.type === "status_cancel") {
      const keys = [...allAgg.statusChangeMap.keys()].filter((k) => {
        const p = parseStatusChangeMapKey(k);
        return (
          p &&
          p.d === d &&
          (def.cancelKeys || []).includes(p.toV) &&
          (def.keys || []).includes(p.fromV)
        );
      });
      val = keys.reduce((acc, k) => acc + Number(allAgg.statusChangeMap.get(k) || 0), 0);
    } else if (def.type === "fixed") {
      val = Number(def.fixed || 0);
    }
    daily.set(d, val);
  }
  return daily;
}

/**
 * 週末日（指定 end）が属する暦月を右端とし、それより前の暦月を合わせて n ヶ月分（古い順・左→右で増える）。
 * 各要素はその月の初日・末日（YYYY-MM-DD）。ラベルは年をまたぐときだけ年を付与。
 */
function lastNCalendarMonthsEndingAt(weekEndYmd, n) {
  const end = new Date(`${weekEndYmd}T12:00:00`);
  const endYear = end.getFullYear();
  const out = [];
  for (let i = n - 1; i >= 0; i--) {
    const ref = new Date(end.getFullYear(), end.getMonth() - i, 1);
    const y = ref.getFullYear();
    const m = ref.getMonth();
    const first = `${y}-${String(m + 1).padStart(2, "0")}-01`;
    const lastD = new Date(y, m + 1, 0).getDate();
    const last = `${y}-${String(m + 1).padStart(2, "0")}-${String(lastD).padStart(2, "0")}`;
    const label = y === endYear ? `${m + 1}月` : `${y}年${m + 1}月`;
    out.push({ label, first, last });
  }
  return out;
}

function sumDefInYmdRange(def, fromYmd, toYmd, allAgg) {
  if (!fromYmd || !toYmd || fromYmd > toYmd) return 0;
  const dates = listYmd(fromYmd, toYmd);
  if (!dates.length) return 0;
  const map = buildDailyMapForRowDef(def, dates, allAgg);
  return sumMapByDates(map, dates);
}

const DASHBOARD_MONTH_COLS = 5;

function metricRowDefinitions(applyStatuses, contractStatuses, cancelStatuses, remainContracts) {
  return [
    { id: "newVisit", row: 7, label: "新規来場", type: "sales", salesKey: "新規来場" },
    { id: "revisit", row: 8, label: "再来場", type: "sales", salesKey: "再来場" },
    { id: "reRevisit", row: 9, label: "再々来場", type: "sales", salesKey: "再々来場" },
    { id: "application", row: 10, label: "申込", type: "status_to", keys: applyStatuses },
    { id: "appCancel", row: 11, label: "申込キャンセル", type: "status_cancel", keys: applyStatuses, cancelKeys: cancelStatuses },
    { id: "contract", row: 12, label: "契約", type: "status_to", keys: contractStatuses },
    { id: "cancellation", row: 13, label: "解約", type: "status_to", keys: cancelStatuses },
    { id: "inquiry", row: 14, label: "反響", type: "reaction" },
    { id: "remainContracts", row: 15, label: "残契約数", type: "fixed", fixed: remainContracts },
  ];
}

/**
 * 週報UI・テンプレ書き込み用: 指定期間1週の集計（日別・週計・月別列相当・累計）
 */
async function computeDashboardWeekStats(property, weekStart, weekEnd) {
  const propertyId = property.id;
  const statuses = await getVisitStatuses(propertyId);
  const applyStatuses = pickByKeywords(statuses, ["申込"]);
  const contractStatuses = pickByKeywords(statuses, ["契約"]);
  const cancelStatuses = pickByKeywords(statuses, ["解約", "キャンセル"]);

  const totalUnits = Number(property.total_units || property.totalUnits || 0) || 0;
  const db = await getDb();
  const contractedRows = await db.execute({
    sql: `SELECT COUNT(*) AS n FROM customer_snapshot WHERE property_id = ? AND status IN (${contractStatuses.map(() => "?").join(",") || "''"})`,
    args: [propertyId, ...contractStatuses],
  });
  const cr0 = contractedRows.rows?.[0];
  const contractedCount = Number(cr0?.n ?? cr0?.[0] ?? 0);
  const remainContracts = Math.max(0, totalUnits - contractedCount);

  const dates = listYmd(weekStart, weekEnd);
  const endMonth = new Date(`${weekEnd}T00:00:00`).getMonth();
  const allAggWeek = await loadAggregates(propertyId, weekStart, weekEnd);

  const totalStart = effectiveTotalStartYmd(property, weekEnd);
  const allAggTotal = await loadAggregates(propertyId, totalStart, weekEnd);
  const fullDatesTotal = listYmd(totalStart, weekEnd);

  const defs = metricRowDefinitions(applyStatuses, contractStatuses, cancelStatuses, remainContracts);
  const monthBuckets = lastNCalendarMonthsEndingAt(weekEnd, DASHBOARD_MONTH_COLS);
  const monthLabels = monthBuckets.map((b) => b.label);

  const daily = {};
  const weekSum = {};
  const monthSum = {};
  const monthly = {};
  const totalSum = {};

  for (const def of defs) {
    if (def.id === "remainContracts") {
      daily[def.id] = dates.map(() => remainContracts);
      weekSum[def.id] = remainContracts;
      monthSum[def.id] = remainContracts;
      monthly[def.id] = monthBuckets.map(() => "");
      totalSum[def.id] = remainContracts;
      continue;
    }
    const mapWeek = buildDailyMapForRowDef(def, dates, allAggWeek);
    const mapTotal = buildDailyMapForRowDef(def, fullDatesTotal, allAggTotal);
    daily[def.id] = dates.map((d) => Number(mapWeek.get(d) || 0));
    weekSum[def.id] = sumMapByDates(mapWeek, dates);
    monthSum[def.id] = calcMonthlyCount(mapWeek, weekStart, weekEnd, endMonth);
    totalSum[def.id] = sumMapByDates(mapTotal, fullDatesTotal);

    // 月別5列: 各暦月の合計（物件計上日以降のみ）。右端列は当月初日〜週末日（当月中の実績）
    monthly[def.id] = monthBuckets.map((b, idx) => {
      const isRightmost = idx === monthBuckets.length - 1;
      const rangeEnd = isRightmost ? weekEnd : b.last;
      const rangeStart = totalStart > b.first ? totalStart : b.first;
      if (!rangeEnd || !rangeStart || rangeStart > rangeEnd) return 0;
      return Number(sumDefInYmdRange(def, rangeStart, rangeEnd, allAggTotal)) || 0;
    });
  }

  const customers = await loadWeekCustomersForReport(
    propertyId,
    weekStart,
    weekEnd,
    applyStatuses,
    contractStatuses,
    cancelStatuses
  );

  return {
    ok: true,
    weekStart,
    weekEnd,
    propertyId,
    totalUnits,
    remainContracts,
    daily,
    weekSum,
    monthSum,
    monthLabels,
    monthly,
    totalSum,
    labels: defs.map((d) => ({ id: d.id, label: d.label })),
    customers,
  };
}

async function loadAggregates(propertyId, startDate, endDate) {
  const db = await getDb();
  const endExclusive = addDays(endDate, 1);

  const [reactions, statusChanges, salesActions] = await Promise.all([
    db.execute({
      sql: `SELECT substr(date_entry, 1, 10) AS date, COUNT(*) AS count
            FROM customer_snapshot
            WHERE property_id = ? AND date_entry >= ? AND date_entry < ?
            GROUP BY substr(date_entry, 1, 10)`,
      args: [propertyId, startDate, endExclusive],
    }),
    db.execute({
      sql: `SELECT substr(created_at, 1, 10) AS date, to_value, from_value, COUNT(*) AS count
            FROM customer_history
            WHERE property_id = ? AND kind = 'status_changed' AND created_at >= ? AND created_at < ?
            GROUP BY substr(created_at, 1, 10), to_value, from_value`,
      args: [propertyId, startDate, endExclusive],
    }),
    db.execute({
      sql: `SELECT substr(action_date, 1, 10) AS date, action_type, COUNT(*) AS count
            FROM customer_sales_history
            WHERE property_id = ? AND action_date >= ? AND action_date < ?
            GROUP BY substr(action_date, 1, 10), action_type`,
      args: [propertyId, startDate, endExclusive],
    }),
  ]);

  return {
    reactionMap: toMapByDate(reactions.rows || []),
    salesActionMap: mergeActionRows(salesActions.rows || []),
    statusChangeMap: mergeStatusRows(statusChanges.rows || []),
  };
}

async function buildWeeklyReportBuffer({ property, startDate, endDate }) {
  const weekStartDay = String(property.week_start_day || "monday") === "sunday" ? "sunday" : "monday";
  const weeks = generateWeeks(startDate, endDate, weekStartDay);
  const workbook = new ExcelJS.Workbook();

  const statuses = await getVisitStatuses(property.id);
  const applyStatuses = pickByKeywords(statuses, ["申込"]);
  const contractStatuses = pickByKeywords(statuses, ["契約"]);
  const cancelStatuses = pickByKeywords(statuses, ["解約", "キャンセル"]);

  const totalUnits = Number(property.total_units || property.totalUnits || 0) || 0;
  const db = await getDb();
  const contractedRows = await db.execute({
    sql: `SELECT COUNT(*) AS n FROM customer_snapshot WHERE property_id = ? AND status IN (${contractStatuses.map(() => "?").join(",") || "''"})`,
    args: [property.id, ...contractStatuses],
  });
  const contractedCount = Number(contractedRows.rows?.[0]?.n || 0);
  const remainContracts = Math.max(0, totalUnits - contractedCount);

  const allAgg = await loadAggregates(property.id, startDate, endDate);
  const fullDates = listYmd(startDate, endDate);

  const created = toYmd(new Date());
  for (const wk of weeks) {
    const ws = workbook.addWorksheet(`${wk.weekNum}週目`);
    const dates = listYmd(wk.start, wk.end);
    const endMonth = new Date(`${wk.end}T00:00:00`).getMonth();
    const dataStartCol = 2;
    const summaryWeekCol = dataStartCol + dates.length;
    const summaryMonthCol = summaryWeekCol + 1;
    const summaryTotalCol = summaryMonthCol + 1;

    ws.getCell(1, 1).value = `${property.name || "物件"}　週　間　報　告　書`;
    ws.mergeCells(1, 1, 1, summaryTotalCol);
    ws.getCell(1, summaryTotalCol).value = `作成日: ${created}`;
    ws.getCell(2, 1).value = `総戸数 ${totalUnits || "-"}戸`;
    ws.getCell(3, 1).value = "今後の契約予定";
    ws.getCell(4, 1).value = `${fmtJa(wk.start)} 〜 ${fmtJa(wk.end)}`;

    ws.getCell(5, 1).value = "日付";
    dates.forEach((d, i) => { ws.getCell(5, dataStartCol + i).value = `${new Date(`${d}T00:00:00`).getDate()}`; });
    ws.getCell(5, summaryWeekCol).value = "週計";
    ws.getCell(5, summaryMonthCol).value = "月別";
    ws.getCell(5, summaryTotalCol).value = "累計";

    ws.getCell(6, 1).value = "曜日";
    dates.forEach((d, i) => { ws.getCell(6, dataStartCol + i).value = weekDayJa(d); });

    const rowDefs = metricRowDefinitions(applyStatuses, contractStatuses, cancelStatuses, remainContracts);

    for (const def of rowDefs) {
      ws.getCell(def.row, 1).value = def.label;
      const daily = buildDailyMapForRowDef(def, fullDates, allAgg);

      dates.forEach((d, i) => { ws.getCell(def.row, dataStartCol + i).value = Number(daily.get(d) || 0); });
      ws.getCell(def.row, summaryWeekCol).value = sumMapByDates(daily, dates);
      ws.getCell(def.row, summaryMonthCol).value = calcMonthlyCount(daily, wk.start, wk.end, endMonth);
      ws.getCell(def.row, summaryTotalCol).value = sumMapByDates(daily, fullDates);
    }

    const sectionStart = 17;
    ws.getCell(sectionStart, 1).value = "申込件数";
    ws.getCell(sectionStart + 3, 1).value = "申込キャンセル件数";
    ws.getCell(sectionStart + 6, 1).value = "契約件数";
    ws.getCell(sectionStart + 9, 1).value = "解約件数";

    ws.columns = Array.from({ length: summaryTotalCol }, (_, i) => ({
      width: i === 0 ? 20 : 10,
    }));
  }

  return workbook.xlsx.writeBuffer();
}

module.exports = {
  buildWeeklyReportBuffer,
  generateWeeks,
  computeDashboardWeekStats,
  listYmd,
  ymdUtcWeekday,
};
