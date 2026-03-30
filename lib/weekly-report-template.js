const path = require("path");
const fs = require("fs");
const ExcelJS = require("exceljs");
const { getVisitStatuses } = require("./sync-service");
const { loadWeekCustomersForReport } = require("./weekly-report-details");

const TEMPLATE_FILENAME = "weekly_report_template.xlsx";

const DEFAULT_EXCEL_DETAIL = ["申込", "申込キャンセル", "契約", "解約"];

function templatePath() {
  return path.join(__dirname, "..", "assets", "templates", TEMPLATE_FILENAME);
}

function pickByKeywords(list, keywords) {
  return (list || []).filter((v) => {
    const s = String(v || "").toLowerCase();
    return keywords.some((k) => s.includes(k));
  });
}

function parseYmd(v) {
  const s = String(v || "").trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  return new Date(`${s}T12:00:00`);
}

/**
 * @param {object} opts
 * @param {object} opts.property — getPropertyByIdForUser の戻り
 * @param {string} opts.weekStart YYYY-MM-DD
 * @param {string} opts.weekEnd YYYY-MM-DD
 * @param {string[]|null} [opts.detailExcelStatuses] Excel に書き込む明細ステータス（既定: 申込・申込キャンセル・契約・解約）
 * @param {object} [opts.texts]
 */
async function buildWeeklyReportTemplateBuffer(opts) {
  const property = opts.property;
  const weekStart = String(opts.weekStart || "").trim();
  const weekEnd = String(opts.weekEnd || "").trim();
  const texts = opts.texts || {};

  const tp = templatePath();
  if (!fs.existsSync(tp)) {
    throw new Error(
      "週報テンプレートがありません。assets/templates/weekly_report_template.xlsx を配置してください（README 参照）。"
    );
  }

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.readFile(tp);
  const sheet = wb.getWorksheet("1週目") || wb.worksheets[0];
  if (!sheet) throw new Error("テンプレートにシートがありません。");

  sheet.getCell("A4").value = new Date(`${weekStart}T12:00:00`);
  sheet.getCell("I4").value = new Date(`${weekEnd}T12:00:00`);

  const setText = (addr, v) => {
    if (v == null || v === "") return;
    sheet.getCell(addr).value = v;
  };

  setText("Y18", "〈先週の広告活動〉");
  setText("Y19", texts.adLastWeek);
  setText("AK18", "〈先週の営業活動〉");
  setText("AK19", texts.salesLastWeek);
  setText("Y28", "〈今週の広告活動〉");
  setText("Y29", texts.adThisWeek);
  setText("AK28", "〈今週の営業活動〉");
  setText("AK29", texts.salesThisWeek);
  setText("Y38", "〈依頼事項〉");
  setText("Y39", texts.request);
  setText("AK38", "〈備考〉");
  setText("AK39", texts.notes);

  const statuses = await getVisitStatuses(property.id);
  const applyStatuses = pickByKeywords(statuses, ["申込"]);
  const contractStatuses = pickByKeywords(statuses, ["契約"]);
  const cancelStatuses = pickByKeywords(statuses, ["解約", "キャンセル"]);

  const customersAll = await loadWeekCustomersForReport(
    property.id,
    weekStart,
    weekEnd,
    applyStatuses,
    contractStatuses,
    cancelStatuses
  );

  const excelStatuses =
    opts.detailExcelStatuses != null
      ? opts.detailExcelStatuses.map((x) => String(x))
      : [...DEFAULT_EXCEL_DETAIL];

  const byStatus = (st) => customersAll.filter((c) => c.status === st);

  const setDateCell = (addr, ymd) => {
    const d = parseYmd(ymd);
    if (d) sheet.getCell(addr).value = d;
    else if (ymd) sheet.getCell(addr).value = String(ymd);
  };

  if (excelStatuses.includes("申込")) {
    byStatus("申込").slice(0, 9).forEach((c, i) => {
      const r = 19 + i;
      sheet.getCell(`C${r}`).value = c.roomNumber || "";
      sheet.getCell(`E${r}`).value = c.price || "";
      sheet.getCell(`G${r}`).value = c.name || "";
      setDateCell(`I${r}`, c.applicationDate);
      sheet.getCell(`K${r}`).value = c.deposit || "";
      setDateCell(`M${r}`, c.contractDate);
      sheet.getCell(`O${r}`).value = c.staff || "";
      sheet.getCell(`Q${r}`).value = c.withholding || "";
    });
  }

  if (excelStatuses.includes("申込キャンセル")) {
    byStatus("申込キャンセル").slice(0, 2).forEach((c, i) => {
      const r = 30 + i;
      sheet.getCell(`C${r}`).value = c.roomNumber || "";
      sheet.getCell(`E${r}`).value = c.price || "";
      sheet.getCell(`G${r}`).value = c.name || "";
      setDateCell(`I${r}`, c.applicationDate);
      sheet.getCell(`K${r}`).value = c.deposit || "";
      setDateCell(`M${r}`, c.cancelDate);
      sheet.getCell(`O${r}`).value = c.staff || "";
      sheet.getCell(`Q${r}`).value = c.notes || "";
    });
  }

  if (excelStatuses.includes("契約")) {
    byStatus("契約").slice(0, 8).forEach((c, i) => {
      const r = 34 + i;
      sheet.getCell(`C${r}`).value = c.roomNumber || "";
      sheet.getCell(`E${r}`).value = c.price || "";
      sheet.getCell(`G${r}`).value = c.name || "";
      setDateCell(`I${r}`, c.applicationDate);
      sheet.getCell(`K${r}`).value = c.deposit || "";
      setDateCell(`M${r}`, c.contractDate);
      sheet.getCell(`O${r}`).value = c.staff || "";
      sheet.getCell(`Q${r}`).value = c.withholding || "";
    });
  }

  if (excelStatuses.includes("解約")) {
    byStatus("解約").slice(0, 2).forEach((c, i) => {
      const r = 44 + i;
      sheet.getCell(`C${r}`).value = c.roomNumber || "";
      sheet.getCell(`E${r}`).value = c.price || "";
      sheet.getCell(`G${r}`).value = c.name || "";
      setDateCell(`I${r}`, c.contractDate);
      sheet.getCell(`K${r}`).value = c.deposit || "";
      setDateCell(`M${r}`, c.cancellationDate);
      sheet.getCell(`O${r}`).value = c.staff || "";
      sheet.getCell(`Q${r}`).value = c.notes || "";
    });
  }

  return wb.xlsx.writeBuffer();
}

module.exports = {
  buildWeeklyReportTemplateBuffer,
  templatePath,
  TEMPLATE_FILENAME,
};
