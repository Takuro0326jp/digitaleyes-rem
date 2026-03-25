const ExcelJS = require("exceljs");
const { getDb } = require("./sync-db");
const { getVisitStatuses } = require("./sync-service");

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

function toMapByDate(rows) {
  const m = new Map();
  (rows || []).forEach((r) => {
    const d = toYmd(r.date);
    if (!d) return;
    m.set(d, Number(r.count || 0));
  });
  return m;
}

function mergeActionRows(rows) {
  const map = new Map();
  (rows || []).forEach((r) => {
    const key = `${toYmd(r.date)}__${String(r.action_type || "")}`;
    map.set(key, Number(r.count || 0));
  });
  return map;
}

function mergeStatusRows(rows) {
  const out = new Map();
  (rows || []).forEach((r) => {
    const d = toYmd(r.date);
    if (!d) return;
    const k = `${d}__${String(r.to_value || "")}__${String(r.from_value || "")}`;
    out.set(k, Number(r.count || 0));
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

    const rowDefs = [
      { row: 7, label: "新規来場", type: "sales", key: "新規来場" },
      { row: 8, label: "再来場", type: "sales", key: "再来場" },
      { row: 9, label: "再々来場", type: "sales", key: "再々来場" },
      { row: 10, label: "申込", type: "status_to", keys: applyStatuses },
      { row: 11, label: "申込キャンセル", type: "status_cancel", keys: applyStatuses, cancelKeys: cancelStatuses },
      { row: 12, label: "契約", type: "status_to", keys: contractStatuses },
      { row: 13, label: "解約", type: "status_to", keys: cancelStatuses },
      { row: 14, label: "反響", type: "reaction" },
      { row: 15, label: "残契約数", type: "fixed", fixed: remainContracts },
    ];

    for (const def of rowDefs) {
      ws.getCell(def.row, 1).value = def.label;
      const daily = new Map();
      for (const d of fullDates) {
        let val = 0;
        if (def.type === "reaction") {
          val = Number(allAgg.reactionMap.get(d) || 0);
        } else if (def.type === "sales") {
          const keys = [...allAgg.salesActionMap.keys()].filter((k) => k.startsWith(`${d}__`) && k.includes(def.key));
          val = keys.reduce((acc, k) => acc + Number(allAgg.salesActionMap.get(k) || 0), 0);
        } else if (def.type === "status_to") {
          const keys = [...allAgg.statusChangeMap.keys()].filter((k) => {
            const [dk, toV] = k.split("__");
            return dk === d && (def.keys || []).includes(toV);
          });
          val = keys.reduce((acc, k) => acc + Number(allAgg.statusChangeMap.get(k) || 0), 0);
        } else if (def.type === "status_cancel") {
          const keys = [...allAgg.statusChangeMap.keys()].filter((k) => {
            const [dk, toV, fromV] = k.split("__");
            return dk === d && (def.cancelKeys || []).includes(toV) && (def.keys || []).includes(fromV);
          });
          val = keys.reduce((acc, k) => acc + Number(allAgg.statusChangeMap.get(k) || 0), 0);
        } else if (def.type === "fixed") {
          val = Number(def.fixed || 0);
        }
        daily.set(d, val);
      }

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
};
