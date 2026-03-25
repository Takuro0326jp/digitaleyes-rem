const ExcelJS = require("exceljs");

const THIN = { style: "thin", color: { argb: "FFB8C0CC" } };
const borderAll = { top: THIN, left: THIN, bottom: THIN, right: THIN };

/** 日本語環境で一般的なゴシック系（環境に無い場合は Excel 側で置換） */
const FONT = "Yu Gothic";

/**
 * 顧客カルテ用のスタイル付き xlsx を生成する。
 * @param {{ title?: string, jaDate?: string, rows: { label: string, value: string }[] }} opts
 */
async function buildCustomerKarteExcelBuffer(opts) {
  const title = String(opts.title || "顧客カルテ").trim() || "顧客カルテ";
  const jaDate = String(opts.jaDate || "").trim();
  const rows = Array.isArray(opts.rows) ? opts.rows : [];

  const wb = new ExcelJS.Workbook();
  wb.creator = "DIGITALEYES REM";
  const ws = wb.addWorksheet("顧客カルテ", {
    views: [{ state: "frozen", ySplit: 2 }],
  });

  ws.columns = [
    { width: 30 },
    { width: 9 },
    { width: 9 },
    { width: 9 },
    { width: 9 },
    { width: 9 },
    { width: 11 },
    { width: 11 },
  ];

  ws.mergeCells(1, 1, 1, 8);
  const titleCell = ws.getCell(1, 1);
  titleCell.value = title;
  titleCell.font = { bold: true, size: 16, name: FONT, color: { argb: "FF1F2D3D" } };
  titleCell.alignment = { vertical: "middle", horizontal: "center" };
  titleCell.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFE8EEF5" },
  };
  titleCell.border = borderAll;
  ws.getRow(1).height = 38;

  ws.mergeCells(2, 1, 2, 5);
  const left2 = ws.getCell(2, 1);
  left2.border = borderAll;
  left2.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFFDFDFE" },
  };

  const f2 = ws.getCell(2, 6);
  f2.value = "ご記入日";
  f2.font = { bold: true, name: FONT, size: 11, color: { argb: "FF465A6B" } };
  f2.alignment = { vertical: "middle", horizontal: "right" };
  f2.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFF2F5F8" },
  };
  f2.border = borderAll;

  ws.mergeCells(2, 7, 2, 8);
  const d2 = ws.getCell(2, 7);
  d2.value = jaDate;
  d2.font = { name: FONT, size: 11 };
  d2.alignment = { vertical: "middle", horizontal: "left" };
  d2.border = borderAll;
  ws.getRow(2).height = 26;

  let r = 3;
  rows.forEach((item, i) => {
    const label = item.label != null ? String(item.label) : "";
    const value = item.value != null ? String(item.value) : "";

    ws.mergeCells(r, 2, r, 8);

    const cLabel = ws.getCell(r, 1);
    cLabel.value = label;
    cLabel.font = { bold: true, name: FONT, size: 11, color: { argb: "FF465A6B" } };
    cLabel.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFF2F5F8" },
    };
    cLabel.alignment = { vertical: "top", horizontal: "left", wrapText: true };
    cLabel.border = borderAll;

    const cVal = ws.getCell(r, 2);
    cVal.value = value;
    cVal.font = { name: FONT, size: 11, color: { argb: "FF2C3E50" } };
    cVal.alignment = { vertical: "top", horizontal: "left", wrapText: true };
    cVal.border = borderAll;
    if (i % 2 === 1) {
      cVal.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFFAFBFC" },
      };
    }

    const nl = (value.match(/\n/g) || []).length;
    const flatLen = value.replace(/\n/g, "").length;
    const approxLines = Math.max(1, nl + 1 + Math.ceil(flatLen / 46));
    ws.getRow(r).height = Math.min(180, Math.max(22, 10 + approxLines * 13));
    r += 1;
  });

  return wb.xlsx.writeBuffer();
}

module.exports = { buildCustomerKarteExcelBuffer };
