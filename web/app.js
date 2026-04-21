const INPUT_TYPES = ["xlsx", "csv", "tsv", "txt", "json", "xml"];
const OUTPUT_TYPES = ["csv", "tsv", "txt", "xlsx", "json", "xml"];
const TRANSFORMS = ["None", "Text (force)", "UK Postcode (extract)", "Address first line (before comma)", "UK mobile -> 44", "Digits only", "Digits: keep last N", "Extract by regex", "Split + take part", "Prefix if missing", "Suffix", "Regex replace", "Date: format", "Name: extract first", "Name: extract title", "Name: extract surname"];
const CALLER_AI_DEFAULT_COLUMNS = ["Name", "PhoneNumber", "CardNumber", "DateOfBirth", "PostalCode", "Title", "Surname"];
const DUMMY_CARD_OUTPUT_NAMES = new Set(["CardNumber"]);
const DATE_EXPORT_COLUMNS = new Set(["StageDate", "FundedDate", "LastPaymentDate"]);
function dummyCardNumber() {
  return String(Math.floor(Math.random() * 10000)).padStart(4, "0");
}
const state = {
  mode: "simple",
  simple: { file: null, rows: [], columns: [], fileType: null, parsedName: "output" },
  callerAi: { file: null, rows: [], columns: [], exportRows: [], previewRows: [], previewColumns: [], parsedName: "caller_ai" },
  merge: {
    files: [],
    mergedRows: [],
    mergedColumns: [],
    exportRows: [],
    diagnostics: [],
    notes: [],
    template: null,
    unmatchedReports: [],
    previewRows: [],
    previewColumns: [],
    combineMethod: "merge",
    schemaMode: "strict",
    addSourceFile: true,
    sourceColumnName: "SourceFile"
  }
};

function safeFilename(name, maxLen = 80) {
  return (name || "sheet").trim().replace(/[^\w\-. ]+/g, "_").replace(/\s+/g, " ").replace(/^[ ._]+|[ ._]+$/g, "").slice(0, maxLen) || "sheet";
}
function detectFileType(name) {
  const ext = (name || "").toLowerCase().split(".").pop();
  if (!INPUT_TYPES.includes(ext)) throw new Error(`Unsupported file type: .${ext}`);
  return ext;
}
function parseColumns(raw) { return String(raw || "").split(",").map((part) => part.trim()).filter(Boolean); }
function normaliseTransformName(name) {
  return name === "UK mobile → 44" ? "UK mobile -> 44" : (name || "None");
}
function normaliseLabel(value) { return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, ""); }
function firstMatchingColumn(columns, aliases) {
  const normalised = new Map(columns.map((column) => [normaliseLabel(column), column]));
  for (const alias of aliases) {
    const match = normalised.get(normaliseLabel(alias));
    if (match) return match;
  }
  return "";
}
function matchingColumns(columns, aliases) {
  const normalisedAliases = aliases.map((alias) => normaliseLabel(alias));
  return Array.from(new Set(columns.filter((column) => {
    const normalisedColumn = normaliseLabel(column);
    return normalisedAliases.some((alias) => alias && (normalisedColumn.includes(alias) || alias.includes(normalisedColumn)));
  })));
}
function buildCallerAiOutputSpec(columns) {
  const nameSource = firstMatchingColumn(columns, ["name", "full name", "customer name", "client name", "contact name"]);
  const phoneSource = firstMatchingColumn(columns, ["phone", "phone number", "mobile", "mobile number", "telephone", "tel", "contact number"]);
  const cardSource = firstMatchingColumn(columns, ["card number", "card", "cardnumber", "account number", "account"]);
  const dobSource = firstMatchingColumn(columns, ["date of birth", "dob", "birth date", "dateofbirth"]);
  const postcodeCandidates = matchingColumns(columns, ["postcode", "post code", "postal code", "postalcode", "zip", "zip code", "zipcode", "address", "full address", "address line 1", "address1", "street"]);
  const postcodeSource = postcodeCandidates[0] || "";
  const titleSource = firstMatchingColumn(columns, ["title", "salutation", "prefix", "customer title"]) || nameSource;
  const surnameSource = firstMatchingColumn(columns, ["surname", "last name", "family name"]) || nameSource;
  return [
    { source: nameSource || "(blank)", transform: "Name: extract first", params: {}, output_name: "Name" },
    { source: phoneSource || "(blank)", transform: "UK mobile -> 44", params: {}, output_name: "PhoneNumber" },
    { source: cardSource || "(blank)", transform: "Digits: keep last N", params: { n: 4 }, output_name: "CardNumber" },
    { source: dobSource || "(blank)", transform: "Date: format", params: { format: "%Y-%m-%d" }, output_name: "DateOfBirth" },
    { source: postcodeSource || "(blank)", transform: "UK Postcode (extract)", params: { fallback_sources: postcodeCandidates.slice(1) }, output_name: "PostalCode" },
    { source: titleSource || "(blank)", transform: "Name: extract title", params: {}, output_name: "Title" },
    { source: surnameSource || "(blank)", transform: "Name: extract surname", params: {}, output_name: "Surname" }
  ];
}
function buildCallerAiDefaultRow(outputName, columns = []) {
  const defaultRows = buildCallerAiOutputSpec(columns);
  const matched = defaultRows.find((row) => row.output_name === outputName);
  if (matched) return { ...matched, params: { ...(matched.params || {}) } };
  return { source: "(blank)", transform: "None", params: {}, output_name: outputName || "" };
}
function resetCallerAiOutputRows(columns) {
  state.callerAi.exportRows = CALLER_AI_DEFAULT_COLUMNS.map((outputName) => buildCallerAiDefaultRow(outputName, columns));
}
function looksLikeXmlText(text) { const trimmed = text.trimStart(); return trimmed.startsWith("<?xml") || trimmed.startsWith("<"); }
function autoDelimiter(text) {
  const sample = text.split(/\r?\n/).slice(0, 5).join("\n");
  const candidates = [",", ";", "\t", "|"];
  let best = ","; let bestCount = -1;
  candidates.forEach((candidate) => {
    const count = (sample.match(new RegExp(candidate === "|" ? "\\|" : candidate, "g")) || []).length;
    if (count > bestCount) { best = candidate; bestCount = count; }
  });
  return best;
}
function parseDelimited(text, delimiter, headerRow) {
  const lines = text.split(/\r?\n/).filter((line) => line.length);
  if (!lines.length) return [];
  const sep = delimiter === "auto" ? autoDelimiter(text) : (delimiter === "\\t" ? "\t" : delimiter);
  const split = (line) => {
    const out = []; let current = ""; let inQuotes = false;
    for (let i = 0; i < line.length; i += 1) {
      const char = line[i];
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') { current += '"'; i += 1; }
        else { inQuotes = !inQuotes; }
      } else if (char === sep && !inQuotes) { out.push(current); current = ""; }
      else { current += char; }
    }
    out.push(current);
    return out.map((value) => value.trim());
  };
  const rows = lines.map(split);
  const headers = rows[Math.max(0, headerRow - 1)] || [];
  return rows.slice(headerRow).map((row) => Object.fromEntries(headers.map((header, index) => [header || `Column_${index + 1}`, row[index] ?? ""])));
}
function parsePlainText(text) {
  return text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean).map((value) => ({ value }));
}
function shouldTreatTxtAsPlainText(text, delimiter, headerRow) {
  if (delimiter !== "auto" || Number(headerRow) !== 1) return false;
  const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (lines.length <= 1) return true;
  const candidates = [",", ";", "\t", "|"];
  const hasStructuredDelimiter = candidates.some((candidate) => lines.some((line) => line.includes(candidate)));
  if (!hasStructuredDelimiter) return true;
  const detected = autoDelimiter(text);
  const splitCounts = lines.slice(0, 5).map((line) => line.split(detected).length);
  return Math.max(...splitCounts, 1) <= 1;
}
function rowsFromColumnObject(data) {
  const rowIds = Array.from(new Set(Object.values(data).flatMap((value) => Object.keys(value || {}))));
  return rowIds.map((rowId) => Object.fromEntries(Object.entries(data).map(([column, values]) => [column, values?.[rowId] ?? ""])));
}
function parseJson(text) {
  const data = JSON.parse(text);
  if (Array.isArray(data)) return data.map((item) => (typeof item === "object" && item !== null ? item : { value: item }));
  if (data && typeof data === "object") {
    if (Array.isArray(data.data) && Array.isArray(data.columns)) {
      return data.data.map((row) => Object.fromEntries(data.columns.map((column, index) => [column, row[index] ?? ""])));
    }
    const values = Object.values(data);
    if (values.length && values.every((value) => Array.isArray(value))) {
      const rowCount = Math.max(...values.map((value) => value.length), 0);
      return Array.from({ length: rowCount }, (_, index) => Object.fromEntries(Object.entries(data).map(([column, columnValues]) => [column, columnValues[index] ?? ""])));
    }
    if (values.length && values.every((value) => value && typeof value === "object" && !Array.isArray(value))) {
      const numericOuterKeys = Object.keys(data).every((key) => /^\d+$/.test(key));
      return numericOuterKeys ? Object.values(data) : rowsFromColumnObject(data);
    }
    const listValues = values.filter((value) => Array.isArray(value));
    if (listValues.length === 1) {
      return listValues[0].map((item) => (typeof item === "object" && item !== null ? item : { value: item }));
    }
    return [data];
  }
  return [{ value: data }];
}
function parseXml(text) {
  const xml = new DOMParser().parseFromString(text, "application/xml");
  const root = xml.documentElement;
  const rows = [];
  Array.from(root.children).forEach((child) => {
    const fieldNodes = Array.from(child.children).filter((node) => node.tagName === "field" && node.getAttribute("name"));
    const record = {};
    if (fieldNodes.length) fieldNodes.forEach((node) => { record[node.getAttribute("name")] = node.textContent || ""; });
    else Array.from(child.children).forEach((node) => { record[node.tagName] = node.textContent || ""; });
    if (Object.keys(record).length) rows.push(record);
  });
  return rows;
}
function toXml(rows, rootName = "rows", rowName = "row") {
  const escape = (value) => String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\"/g, "&quot;");
  return `<?xml version="1.0" encoding="utf-8"?>\n<${rootName}>${rows.map((row) => `<${rowName}>${Object.entries(row).map(([key, value]) => `<field name="${escape(key)}">${escape(value ?? "")}</field>`).join("")}</${rowName}>`).join("")}</${rootName}>`;
}
function rowsToCsv(rows, delimiter, columnsOverride = []) {
  const columns = columnsOverride.length ? columnsOverride : uniqueColumns(rows);
  if (!columns.length) return "";
  const esc = (value) => {
    const text = String(value ?? "");
    return /["\n,;|\t]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  };
  return [columns.join(delimiter), ...rows.map((row) => columns.map((column) => esc(formatValueForDownload(column, row[column], "text"))).join(delimiter))].join("\n");
}
function uniqueColumns(rows) {
  return Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
}
function excelSerialToDate(serial) {
  const wholeDays = Math.floor(Number(serial) || 0);
  const fractionalDay = Number(serial) - wholeDays;
  const date = new Date(Date.UTC(1899, 11, 30 + wholeDays));
  const totalMs = Math.round(fractionalDay * 86400000);
  return new Date(date.getTime() + totalMs);
}
function isWorksheetDateCell(cell) {
  if (!cell) return false;
  if (cell.t === "d" && cell.v instanceof Date) return true;
  return cell.t === "n" && Boolean(cell.z) && typeof XLSX?.SSF?.is_date === "function" && XLSX.SSF.is_date(cell.z);
}
function isTargetDateColumn(columnName) { return DATE_EXPORT_COLUMNS.has(String(columnName || "").trim()); }
function parseExportDateValue(value) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const text = String(value ?? "").trim();
  if (!text) return null;
  if (/^\d+(?:\.0+)?$/.test(text)) {
    const serial = Number(text);
    if (serial >= 1 && serial <= 60000) return new Date(Date.UTC(1899, 11, 30) + serial * 86400000);
  }
  const slashMatch = text.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (slashMatch) {
    const year = slashMatch[3].length === 2 ? Number(`20${slashMatch[3]}`) : Number(slashMatch[3]);
    return new Date(Date.UTC(
      year,
      Number(slashMatch[2]) - 1,
      Number(slashMatch[1]),
      Number(slashMatch[4] || 0),
      Number(slashMatch[5] || 0),
      Number(slashMatch[6] || 0)
    ));
  }
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}
function formatDateForDownload(value) {
  const parsed = parseExportDateValue(value);
  if (!parsed) return value ?? "";
  const day = String(parsed.getUTCDate()).padStart(2, "0");
  const month = String(parsed.getUTCMonth() + 1).padStart(2, "0");
  const year = String(parsed.getUTCFullYear());
  return `${day}/${month}/${year}`;
}
function formatValueForDownload(column, value, mode = "text") {
  if (!isTargetDateColumn(column)) return value;
  if (mode === "xlsx") return parseExportDateValue(value) || value;
  return formatDateForDownload(value);
}
function prepareRowsForXlsx(rows, columns) {
  const dateColumns = columns.filter((column) => isTargetDateColumn(column));
  const preparedRows = rows.map((row) => {
    const out = {};
    columns.forEach((column) => {
      const value = row[column];
      if (dateColumns.includes(column)) {
        out[column] = formatValueForDownload(column, value, "xlsx");
      } else {
        out[column] = value;
      }
    });
    return out;
  });
  return { preparedRows, dateColumns };
}
function parseWorkbookSheet(workbook, sheetName) {
  const worksheet = workbook.Sheets[sheetName];
  const range = XLSX.utils.decode_range(worksheet["!ref"] || "A1:A1");
  const headerRowIndex = range.s.r;
  const headers = [];
  const dateColumns = new Set(headers.filter((header) => isTargetDateColumn(header)));

  for (let col = range.s.c; col <= range.e.c; col += 1) {
    const headerCell = worksheet[XLSX.utils.encode_cell({ r: headerRowIndex, c: col })];
    const headerValue = headerCell?.v ?? `Column_${col + 1}`;
    headers.push(String(headerValue || `Column_${col + 1}`));
  }
  headers.forEach((header) => {
    if (isTargetDateColumn(header)) dateColumns.add(header);
  });

  const rows = [];
  for (let row = headerRowIndex + 1; row <= range.e.r; row += 1) {
    const record = {};
    let hasValue = false;
    headers.forEach((header, index) => {
      const col = range.s.c + index;
      const cell = worksheet[XLSX.utils.encode_cell({ r: row, c: col })];
      let value = "";
      if (cell) {
        if (dateColumns.has(header)) {
          value = parseExportDateValue(cell.v) || parseExportDateValue(cell.w) || "";
        } else if (cell.v !== undefined && cell.v !== null) {
          value = cell.v;
        }
      }
      if (value !== "") hasValue = true;
      record[header] = value;
    });
    if (hasValue) rows.push(record);
  }

  return { rows, dateColumns: Array.from(dateColumns) };
}
async function parseFile(file, options = {}) {
  const fileType = detectFileType(file.name);
  const headerRow = Number(options.headerRow || 1);
  const delimiter = options.delimiter || "auto";
  if (fileType === "xlsx") {
    const workbook = XLSX.read(await file.arrayBuffer(), { type: "array", cellDates: false, cellNF: true });
    const parsedSheet = parseWorkbookSheet(workbook, workbook.SheetNames[0]);
    return { fileType, rows: parsedSheet.rows, fileName: file.name, dateColumns: parsedSheet.dateColumns };
  }
  const text = await file.text();
  if (fileType === "json") return { fileType, rows: parseJson(text), fileName: file.name };
  if (fileType === "xml" || (fileType === "txt" && looksLikeXmlText(text))) return { fileType: fileType === "txt" ? "xml" : fileType, rows: parseXml(text), fileName: file.name };
  const usedDelimiter = fileType === "csv" ? "," : fileType === "tsv" ? "\t" : delimiter;
  if (fileType === "txt" && shouldTreatTxtAsPlainText(text, usedDelimiter, headerRow)) {
    return { fileType, rows: parsePlainText(text), fileName: file.name };
  }
  return { fileType, rows: parseDelimited(text, usedDelimiter, headerRow), fileName: file.name };
}
async function exportRows(rows, fileName, type, columnsOverride = []) {
  const name = `${safeFilename(fileName)}.${type}`;
  const textDelimiter = type === "tsv" ? "\t" : ",";
  const columns = columnsOverride.length ? columnsOverride : uniqueColumns(rows);
  if (type === "xlsx") {
    const { preparedRows, dateColumns } = prepareRowsForXlsx(rows, columns);
    const worksheet = XLSX.utils.json_to_sheet(preparedRows, { header: columns, cellDates: true });
    dateColumns.forEach((column) => {
      const colIndex = columns.indexOf(column);
      if (colIndex < 0) return;
      for (let rowIndex = 0; rowIndex < preparedRows.length; rowIndex += 1) {
        const cellRef = XLSX.utils.encode_cell({ r: rowIndex + 1, c: colIndex });
        const cell = worksheet[cellRef];
        if (cell && (cell.t === "d" || cell.v instanceof Date)) cell.z = "dd/mm/yyyy";
      }
    });
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Sheet1");
    XLSX.writeFile(workbook, name);
    return;
  }
  const serializableRows = rows.map((row) => Object.fromEntries(columns.map((column) => [column, formatValueForDownload(column, row[column], "text")])));
  const content = type === "json" ? JSON.stringify(serializableRows, null, 2) : type === "xml" ? toXml(serializableRows) : rowsToCsv(serializableRows, textDelimiter, columns);
  downloadBlob(name, content, type === "json" ? "application/json" : type === "xml" ? "application/xml" : "text/plain;charset=utf-8");
}
function downloadBlob(name, content, mime) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url; link.download = name; link.click();
  URL.revokeObjectURL(url);
}
function setStatus(id, text, tone = "info") { const el = document.getElementById(id); el.className = `alert alert-${tone}`; el.textContent = text; }
function renderTable(tableId, rows, limit = 25, columnsOverride = []) {
  const table = document.getElementById(tableId); const thead = table.querySelector("thead"); const tbody = table.querySelector("tbody");
  thead.innerHTML = ""; tbody.innerHTML = "";
  const columns = columnsOverride.length ? columnsOverride : uniqueColumns(rows);
  if (!columns.length) return;
  const headerRow = document.createElement("tr"); columns.forEach((column) => { const th = document.createElement("th"); th.textContent = column; headerRow.appendChild(th); }); thead.appendChild(headerRow);
  rows.slice(0, limit).forEach((row) => { const tr = document.createElement("tr"); columns.forEach((column) => { const td = document.createElement("td"); td.textContent = formatValueForDownload(column, row[column], "text") ?? ""; tr.appendChild(td); }); tbody.appendChild(tr); });
}
function normaliseKey(value) { return String(value ?? "").trim().replace(/\.0$/, ""); }
function buildCompositeKey(row, keyCols) { return keyCols.map((col) => normaliseKey(row[col])).join("||"); }
function diagnosticsForFile(entry) {
  const keys = entry.rows.map((row) => buildCompositeKey(row, entry.keyCols));
  const duplicates = keys.filter((key, index) => key && keys.indexOf(key) !== index).length;
  return { Role: entry.role, Rows: entry.rows.length, Columns: uniqueColumns(entry.rows).length, "Blank keys": keys.filter((key) => !key).length, "Duplicate keys": duplicates, "Distinct keys": new Set(keys).size };
}
function aggregateRows(rows, keyCols) {
  const map = new Map();
  rows.forEach((row) => {
    const key = buildCompositeKey(row, keyCols);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(row);
  });
  return Array.from(map.entries()).map(([key, group]) => {
    const out = {};
    uniqueColumns(group).forEach((column) => {
      const values = Array.from(new Set(group.map((row) => String(row[column] ?? "")).filter(Boolean)));
      out[column] = values.length <= 1 ? (values[0] || "") : values.join(" | ");
    });
    keyCols.forEach((col, index) => { out[col] = key.split("||")[index] || ""; });
    return out;
  });
}
function prepareRows(entry) {
  const rows = entry.rows.map((row) => ({ ...row, __merge_key__: buildCompositeKey(row, entry.keyCols) }));
  const dupes = rows.filter((row, index) => rows.findIndex((candidate) => candidate.__merge_key__ === row.__merge_key__) !== index && row.__merge_key__).length;
  if (entry.duplicateStrategy === "Error" && dupes) throw new Error(`${entry.role} has duplicate merge keys.`);
  if (entry.duplicateStrategy === "Keep first") return rows.filter((row, index) => rows.findIndex((candidate) => candidate.__merge_key__ === row.__merge_key__) === index);
  if (entry.duplicateStrategy === "Keep last") return rows.filter((row, index) => rows.findLastIndex((candidate) => candidate.__merge_key__ === row.__merge_key__) === index);
  if (entry.duplicateStrategy === "Aggregate values") return aggregateRows(rows, entry.keyCols).map((row) => ({ ...row, __merge_key__: buildCompositeKey(row, entry.keyCols) }));
  return rows;
}
function mergePrepared(baseRows, rightRows, how, suffix) {
  const rightMap = new Map(); rightRows.forEach((row) => { if (!rightMap.has(row.__merge_key__)) rightMap.set(row.__merge_key__, []); rightMap.get(row.__merge_key__).push(row); });
  const matchedRightKeys = new Set(); const result = []; const leftOnly = []; const rightOnly = [];
  baseRows.forEach((leftRow) => {
    const matches = rightMap.get(leftRow.__merge_key__) || [];
    if (!matches.length) {
      if (how === "left" || how === "outer") result.push({ ...leftRow, __indicator__: "left_only" });
      leftOnly.push(leftRow);
      return;
    }
    matches.forEach((rightRow) => {
      matchedRightKeys.add(rightRow.__merge_key__);
      const merged = { ...leftRow };
      Object.entries(rightRow).forEach(([key, value]) => {
        if (key === "__merge_key__") return;
        if (merged[key] !== undefined && key !== "__merge_key__") merged[`${key}__${suffix}`] = value;
        else merged[key] = value;
      });
      merged.__indicator__ = "both";
      result.push(merged);
    });
  });
  rightRows.forEach((rightRow) => {
    if (!matchedRightKeys.has(rightRow.__merge_key__)) {
      rightOnly.push(rightRow);
      if (how === "outer") result.push({ ...rightRow, __indicator__: "right_only" });
    }
  });
  return { result, leftOnly, rightOnly, bothCount: result.filter((row) => row.__indicator__ === "both").length };
}
function appendDiagnosticsForFile(entry) {
  return {
    File: entry.fileName,
    Rows: entry.rows.length,
    Columns: uniqueColumns(entry.rows).length
  };
}
function nextAvailableColumnName(preferred, existingColumns) {
  const base = String(preferred || "").trim() || "SourceFile";
  if (!existingColumns.includes(base)) return base;
  let suffix = 1;
  while (existingColumns.includes(`${base}_${suffix}`)) suffix += 1;
  return `${base}_${suffix}`;
}
function appendRows(entries, schemaMode, addSourceFile, sourceColumnName) {
  if (!entries.length) throw new Error("Add files before combining them.");
  const orderedColumns = [];
  entries.forEach((entry) => {
    uniqueColumns(entry.rows).forEach((column) => {
      if (!orderedColumns.includes(column)) orderedColumns.push(column);
    });
  });
  const firstColumns = uniqueColumns(entries[0].rows);
  let combineColumns = schemaMode === "union" ? [...orderedColumns] : [...firstColumns];
  if (schemaMode === "strict") {
    const firstSet = new Set(firstColumns);
    entries.slice(1).forEach((entry) => {
      const entryColumns = uniqueColumns(entry.rows);
      const entrySet = new Set(entryColumns);
      const missing = firstColumns.filter((column) => !entrySet.has(column));
      const extras = entryColumns.filter((column) => !firstSet.has(column));
      if (missing.length || extras.length) {
        const detail = [
          missing.length ? `missing columns: ${missing.join(", ")}` : "",
          extras.length ? `extra columns: ${extras.join(", ")}` : ""
        ].filter(Boolean).join("; ");
        throw new Error(`${entry.fileName} does not match the first file schema (${detail || "columns differ"}).`);
      }
    });
  }
  let finalSourceColumnName = String(sourceColumnName || "").trim() || "SourceFile";
  const notes = [`Schema mode: ${schemaMode === "union" ? "union columns" : "strict same columns"}`];
  if (addSourceFile) {
    finalSourceColumnName = nextAvailableColumnName(finalSourceColumnName, combineColumns);
    if (finalSourceColumnName !== (String(sourceColumnName || "").trim() || "SourceFile")) {
      notes.push(`Source column renamed to ${finalSourceColumnName} to avoid a name collision`);
    }
  }
  const combinedRows = entries.flatMap((entry) => entry.rows.map((row) => {
    const out = Object.fromEntries(combineColumns.map((column) => [column, row[column] ?? ""]));
    if (addSourceFile) out[finalSourceColumnName] = entry.fileName;
    return out;
  }));
  if (addSourceFile) combineColumns = [...combineColumns, finalSourceColumnName];
  notes.push(`Combined ${entries.length} files into ${combinedRows.length} rows`);
  return { combinedRows, combineColumns, notes, sourceColumnName: addSourceFile ? finalSourceColumnName : null };
}
function getTransformParams(name) {
  if (name === "Digits: keep last N") return { n: 4 };
  if (name === "Extract by regex") return { pattern: "(\\w+)", group: 1, ignore_case: true };
  if (name === "Split + take part") return { delim: ",", index: 0 };
  if (name === "Prefix if missing") return { prefix: "" };
  if (name === "Suffix") return { suffix: "" };
  if (name === "Regex replace") return { pattern: "\\s+", repl: "", ignore_case: true };
  if (name === "Date: format") return { format: "%Y-%m-%d" };
  return {};
}
function pad2(value) { return String(value).padStart(2, "0"); }
function formatDateParts(date, outputFormat) {
  const yyyy = String(date.getUTCFullYear());
  const mm = pad2(date.getUTCMonth() + 1);
  const dd = pad2(date.getUTCDate());
  return String(outputFormat || "%Y-%m-%d").replace(/%Y/g, yyyy).replace(/%m/g, mm).replace(/%d/g, dd);
}
function formatDateValue(value, outputFormat = "%Y-%m-%d") {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (/^\d+(?:\.0+)?$/.test(text)) {
    const serial = Number(text);
    if (serial >= 1 && serial <= 60000) {
      return formatDateParts(new Date(Date.UTC(1899, 11, 30) + serial * 86400000), outputFormat);
    }
  }
  const slashMatch = text.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$/);
  if (slashMatch) {
    const year = slashMatch[3].length === 2 ? Number(`20${slashMatch[3]}`) : Number(slashMatch[3]);
    return formatDateParts(new Date(Date.UTC(year, Number(slashMatch[2]) - 1, Number(slashMatch[1]))), outputFormat);
  }
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? text : formatDateParts(parsed, outputFormat);
}
function applyTransformValue(value, name, params) {
  const text = String(value ?? "");
  if (name === "None" || name === "Text (force)") return text;
  if (name === "UK Postcode (extract)") { const match = text.toUpperCase().match(/\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/); return match ? match[1].trim() : ""; }
  if (name === "Address first line (before comma)") return text.split(",").map((part) => part.trim()).find(Boolean) || text.trim();
  if (name === "UK mobile -> 44") { let digits = text.replace(/\s+/g, "").replace(/\.0$/, "").replace(/\D/g, ""); if (!digits) return ""; if (digits.startsWith("44")) return digits; if (digits.startsWith("0")) return `44${digits.slice(1)}`; return `44${digits}`; }
  if (name === "Digits only") return (text.match(/\d/g) || []).join("");
  if (name === "Digits: keep last N") { const digits = (text.match(/\d/g) || []).join(""); const n = Number(params.n || 4); return digits.length >= n ? digits.slice(-n) : ""; }
  if (name === "Extract by regex") { try { const match = text.match(new RegExp(params.pattern || "", params.ignore_case ? "i" : "")); return match ? (match[Number(params.group || 1)] || "") : ""; } catch { return ""; } }
  if (name === "Split + take part") { const parts = text.split(params.delim || ",").map((part) => part.trim()); let index = Number(params.index || 0); if (index < 0) index = parts.length + index; return parts[index] ?? ""; }
  if (name === "Prefix if missing") { const prefix = String(params.prefix || ""); return prefix && !text.startsWith(prefix) ? `${prefix}${text}` : text; }
  if (name === "Suffix") return `${text}${String(params.suffix || "")}`;
  if (name === "Regex replace") { try { return text.replace(new RegExp(params.pattern || "", params.ignore_case ? "ig" : "g"), params.repl || ""); } catch { return text; } }
  if (name === "Date: format") return formatDateValue(text, params.format || "%Y-%m-%d");
  if (name === "Name: extract first") {
    const withoutTitle = text.trim().replace(/^(mr|mrs|ms|miss|mx|dr|prof|sir|lady|lord|rev)\.?\s+/i, "");
    const parts = withoutTitle.split(/\s+/).filter(Boolean);
    return parts.length ? parts[0].replace(/,$/, "") : "";
  }
  if (name === "Name: extract title") {
    const match = text.trim().match(/^(mr|mrs|ms|miss|mx|dr|prof|sir|lady|lord|rev)\.?\s+/i);
    if (match) return match[0].trim().replace(/\.$/, "");
    const simple = text.trim().replace(/\.$/, "");
    return /^(mr|mrs|ms|miss|mx|dr|prof|sir|lady|lord|rev)$/i.test(simple) ? simple : "";
  }
  if (name === "Name: extract surname") {
    const withoutTitle = text.trim().replace(/^(mr|mrs|ms|miss|mx|dr|prof|sir|lady|lord|rev)\.?\s+/i, "");
    const parts = withoutTitle.split(/\s+/).filter(Boolean);
    while (parts.length && /^(jr|sr|ii|iii|iv|v)\.?$/i.test(parts[parts.length - 1])) parts.pop();
    return parts.length ? parts[parts.length - 1].replace(/,$/, "") : "";
  }
  return text;
}
function outputTemplatePayload() {
  return {
    version: 2,
    join_type: document.getElementById("join-type").value,
    base_role: document.getElementById("base-role").value,
    merge_keys_by_role: Object.fromEntries(state.merge.files.map((entry) => [entry.role, entry.keyCols])),
    duplicate_strategy_by_role: Object.fromEntries(state.merge.files.map((entry) => [entry.role, entry.duplicateStrategy])),
    output_spec: state.merge.exportRows
  };
}
async function downloadUnmatchedZip() {
  if (!state.merge.unmatchedReports.length) return;
  const zip = new JSZip();
  state.merge.unmatchedReports.forEach((report) => { zip.file(report.name, rowsToCsv(report.rows, ",")); });
  const content = await zip.generateAsync({ type: "blob" });
  const url = URL.createObjectURL(content); const link = document.createElement("a"); link.href = url; link.download = "unmatched_reports.zip"; link.click(); URL.revokeObjectURL(url);
}
function renderMergeDiagnostics() {
  const container = document.getElementById("merge-diagnostics"); container.innerHTML = "";
  state.merge.diagnostics.forEach((item) => {
    const card = document.createElement("article"); card.className = "summary-card";
    if (Object.prototype.hasOwnProperty.call(item, "Role")) {
      card.innerHTML = `<span class="summary-label">${item.Role}</span><strong class="summary-value summary-value--primary">${item.Rows}</strong><div>Blank: ${item["Blank keys"]} | Duplicates: ${item["Duplicate keys"]}</div>`;
    } else {
      card.innerHTML = `<span class="summary-label">${item.File}</span><strong class="summary-value summary-value--primary">${item.Rows}</strong><div>Columns: ${item.Columns}</div>`;
    }
    container.appendChild(card);
  });
}
function renderMergeNotes() {
  const el = document.getElementById("merge-notes");
  el.innerHTML = state.merge.notes.length ? state.merge.notes.map((note) => `<div>${note}</div>`).join("") : "No notes yet.";
}
function renderMappingRows() {
  const list = document.getElementById("mapping-list"); list.innerHTML = "";
  const columns = state.merge.mergedColumns;
  state.merge.exportRows.forEach((row, index) => {
    const item = document.createElement("div"); item.className = "mapping-row";
    const sourceOptions = ["(blank)", ...columns].map((column) => `<option value="${escapeHtml(column)}" ${row.source === column ? "selected" : ""}>${escapeHtml(column)}</option>`).join("");
    const transformOptions = TRANSFORMS.map((name) => `<option value="${escapeHtml(name)}" ${row.transform === name ? "selected" : ""}>${escapeHtml(name)}</option>`).join("");
    item.innerHTML = `<div class="form-group"><label>Source</label><select data-field="source" data-index="${index}">${sourceOptions}</select></div><div class="form-group"><label>Transform</label><select data-field="transform" data-index="${index}">${transformOptions}</select></div><div class="form-group"><label>Output name</label><input data-field="output_name" data-index="${index}" value="${escapeHtml(row.output_name || "")}"></div><button class="btn btn-ghost" data-remove-index="${index}" type="button">Remove</button>`;
    list.appendChild(item);
    if (["Digits: keep last N", "Extract by regex", "Split + take part", "Prefix if missing", "Suffix", "Regex replace"].includes(row.transform)) {
      const params = document.createElement("div"); params.className = "form-grid form-grid--three";
      params.innerHTML = renderParamInputs(row, index);
      list.appendChild(params);
    }
  });
  const add = document.createElement("button"); add.className = "btn btn-secondary"; add.type = "button"; add.textContent = "Add Output Column"; add.onclick = () => { state.merge.exportRows.push({ source: "(blank)", transform: "None", params: {}, output_name: "" }); renderMappingRows(); updateExportRows(); }; list.appendChild(add);
}
function renderCallerAiMappingRows() {
  const list = document.getElementById("caller-ai-mapping-list"); list.innerHTML = "";
  const columns = state.callerAi.columns || [];
  state.callerAi.exportRows.forEach((row, index) => {
    const item = document.createElement("div"); item.className = "mapping-row";
    const sourceOptions = ["(blank)", ...columns].map((column) => `<option value="${escapeHtml(column)}" ${row.source === column ? "selected" : ""}>${escapeHtml(column)}</option>`).join("");
    const transformOptions = TRANSFORMS.map((name) => `<option value="${escapeHtml(name)}" ${row.transform === name ? "selected" : ""}>${escapeHtml(name)}</option>`).join("");
    item.innerHTML = `<div class="form-group"><label>Source</label><select data-caller-ai-field="source" data-index="${index}">${sourceOptions}</select></div><div class="form-group"><label>Transform</label><select data-caller-ai-field="transform" data-index="${index}">${transformOptions}</select></div><div class="form-group"><label>Output name</label><input data-caller-ai-field="output_name" data-index="${index}" value="${escapeHtml(row.output_name || "")}" placeholder="e.g. PhoneNumber"></div><button class="btn btn-ghost" data-caller-ai-remove-index="${index}" type="button">Remove</button>`;
    list.appendChild(item);
    if (["Digits: keep last N", "Extract by regex", "Split + take part", "Prefix if missing", "Suffix", "Regex replace"].includes(row.transform)) {
      const params = document.createElement("div"); params.className = "form-grid form-grid--three";
      params.innerHTML = renderCallerAiParamInputs(row, index);
      list.appendChild(params);
    }
  });
  const add = document.createElement("button");
  add.className = "btn btn-secondary";
  add.type = "button";
  add.textContent = "Add Output Column";
  add.onclick = () => {
    state.callerAi.exportRows.push({ source: "(blank)", transform: "None", params: {}, output_name: "" });
    renderCallerAiMappingRows();
    updateCallerAiExportRows();
  };
  list.appendChild(add);
}
function renderParamInputs(row, index) {
  const p = row.params || {};
  if (row.transform === "Digits: keep last N") return `<div class="form-group"><label>N</label><input data-param="n" data-index="${index}" type="number" value="${Number(p.n || 4)}"></div>`;
  if (row.transform === "Extract by regex") return `<div class="form-group"><label>Pattern</label><input data-param="pattern" data-index="${index}" value="${escapeHtml(p.pattern || "")}"></div><div class="form-group"><label>Group</label><input data-param="group" data-index="${index}" type="number" value="${Number(p.group || 1)}"></div><label class="checkbox inline-checkbox"><input data-param="ignore_case" data-index="${index}" type="checkbox" ${p.ignore_case !== false ? "checked" : ""}><span>Ignore case</span></label>`;
  if (row.transform === "Split + take part") return `<div class="form-group"><label>Delimiter</label><input data-param="delim" data-index="${index}" value="${escapeHtml(p.delim || ",")}"></div><div class="form-group"><label>Index</label><input data-param="index" data-index="${index}" type="number" value="${Number(p.index || 0)}"></div>`;
  if (row.transform === "Prefix if missing") return `<div class="form-group"><label>Prefix</label><input data-param="prefix" data-index="${index}" value="${escapeHtml(p.prefix || "")}"></div>`;
  if (row.transform === "Suffix") return `<div class="form-group"><label>Suffix</label><input data-param="suffix" data-index="${index}" value="${escapeHtml(p.suffix || "")}"></div>`;
  if (row.transform === "Regex replace") return `<div class="form-group"><label>Pattern</label><input data-param="pattern" data-index="${index}" value="${escapeHtml(p.pattern || "")}"></div><div class="form-group"><label>Replace with</label><input data-param="repl" data-index="${index}" value="${escapeHtml(p.repl || "")}"></div><label class="checkbox inline-checkbox"><input data-param="ignore_case" data-index="${index}" type="checkbox" ${p.ignore_case !== false ? "checked" : ""}><span>Ignore case</span></label>`;
  return "";
}
function renderCallerAiParamInputs(row, index) {
  const p = row.params || {};
  if (row.transform === "Digits: keep last N") return `<div class="form-group"><label>N</label><input data-caller-ai-param="n" data-index="${index}" type="number" value="${Number(p.n || 4)}"></div>`;
  if (row.transform === "Extract by regex") return `<div class="form-group"><label>Pattern</label><input data-caller-ai-param="pattern" data-index="${index}" value="${escapeHtml(p.pattern || "")}"></div><div class="form-group"><label>Group</label><input data-caller-ai-param="group" data-index="${index}" type="number" value="${Number(p.group || 1)}"></div><label class="checkbox inline-checkbox"><input data-caller-ai-param="ignore_case" data-index="${index}" type="checkbox" ${p.ignore_case !== false ? "checked" : ""}><span>Ignore case</span></label>`;
  if (row.transform === "Split + take part") return `<div class="form-group"><label>Delimiter</label><input data-caller-ai-param="delim" data-index="${index}" value="${escapeHtml(p.delim || ",")}"></div><div class="form-group"><label>Index</label><input data-caller-ai-param="index" data-index="${index}" type="number" value="${Number(p.index || 0)}"></div>`;
  if (row.transform === "Prefix if missing") return `<div class="form-group"><label>Prefix</label><input data-caller-ai-param="prefix" data-index="${index}" value="${escapeHtml(p.prefix || "")}"></div>`;
  if (row.transform === "Suffix") return `<div class="form-group"><label>Suffix</label><input data-caller-ai-param="suffix" data-index="${index}" value="${escapeHtml(p.suffix || "")}"></div>`;
  if (row.transform === "Regex replace") return `<div class="form-group"><label>Pattern</label><input data-caller-ai-param="pattern" data-index="${index}" value="${escapeHtml(p.pattern || "")}"></div><div class="form-group"><label>Replace with</label><input data-caller-ai-param="repl" data-index="${index}" value="${escapeHtml(p.repl || "")}"></div><label class="checkbox inline-checkbox"><input data-caller-ai-param="ignore_case" data-index="${index}" type="checkbox" ${p.ignore_case !== false ? "checked" : ""}><span>Ignore case</span></label>`;
  return "";
}
function updateExportRows() {
  if (state.merge.combineMethod === "append") {
    state.merge.previewRows = state.merge.mergedRows || [];
    state.merge.previewColumns = state.merge.mergedColumns || [];
    renderTable("merge-table", state.merge.previewRows, 25, state.merge.previewColumns);
    if (!state.merge.previewColumns.length) {
      setStatus("export-status", "Combine your files to preview and download the result.", "info");
    } else {
      setStatus("export-status", `Your combined file is ready: ${state.merge.previewRows.length} row(s), ${state.merge.previewColumns.length} column(s). Choose a format and download it.`, "info");
    }
    return;
  }
  state.merge.exportRows = state.merge.exportRows.filter((row) => row.output_name || row.source !== "(blank)");
  const outputNames = state.merge.exportRows.map((row) => String(row.output_name || "").trim()).filter(Boolean);
  const duplicateNames = outputNames.filter((name, index) => outputNames.indexOf(name) !== index);
  const mergedRows = state.merge.mergedRows || [];
  const previewColumns = [];
  const missingSources = [];
  state.merge.exportRows.forEach((row) => {
    const outputName = String(row.output_name || "").trim();
    if (!outputName) return;
    previewColumns.push(outputName);
    if (row.source !== "(blank)" && !state.merge.mergedColumns.includes(row.source)) missingSources.push(`${outputName} <- ${row.source}`);
  });
  const exportData = mergedRows.map((mergedRow) => {
    const out = {};
    state.merge.exportRows.forEach((row) => {
      const outputName = String(row.output_name || "").trim();
      if (!outputName) return;
      if (row.source === "(blank)") {
        out[outputName] = DUMMY_CARD_OUTPUT_NAMES.has(outputName) ? dummyCardNumber() : "";
        return;
      }
      if (!state.merge.mergedColumns.includes(row.source)) return;
      out[outputName] = applyTransformValue(mergedRow[row.source], row.transform, row.params || {});
    });
    return out;
  });
  state.merge.previewRows = exportData;
  state.merge.previewColumns = previewColumns;
  renderTable("merge-table", exportData, 25, previewColumns);
  if (duplicateNames.length) {
    setStatus("export-status", `Output column names must be unique: ${Array.from(new Set(duplicateNames)).join(", ")}`, "danger");
  } else if (!previewColumns.length) {
    setStatus("export-status", "No export columns are configured yet. Add at least one output column name, or use '(blank)' to create an empty required column.", "info");
  } else if (missingSources.length) {
    setStatus("export-status", `Some mapped source columns were not found after the combined dataset was built: ${missingSources.join(", ")}`, "danger");
  } else {
    setStatus("export-status", `Export preview ready: ${exportData.length} row(s), ${previewColumns.length} column(s).`, "info");
  }
}
function updateCallerAiExportRows() {
  state.callerAi.exportRows = state.callerAi.exportRows.filter((row) => row.output_name || row.source !== "(blank)");
  const activeRows = state.callerAi.exportRows
    .map((row) => ({ ...row, output_name: String(row.output_name || "").trim() }))
    .filter((row) => row.output_name);
  const previewColumns = activeRows.map((row) => row.output_name);
  const duplicateNames = previewColumns.filter((name, index) => previewColumns.indexOf(name) !== index);
  const missingSources = [];
  const exportData = (state.callerAi.rows || []).map((sourceRow) => {
    const out = {};
    activeRows.forEach((row) => {
      if (row.source === "(blank)") {
        out[row.output_name] = DUMMY_CARD_OUTPUT_NAMES.has(row.output_name) ? dummyCardNumber() : "";
        return;
      }
      const fallbackSources = Array.isArray(row.params?.fallback_sources) ? row.params.fallback_sources : [];
      const candidateSources = [row.source, ...fallbackSources.filter((candidate) => candidate !== row.source)];
      const availableSources = candidateSources.filter((candidate) => state.callerAi.columns.includes(candidate));
      if (!availableSources.length) {
        missingSources.push(`${row.output_name} <- ${row.source}`);
        out[row.output_name] = "";
        return;
      }
      let value = "";
      for (const candidate of availableSources) {
        const transformed = applyTransformValue(sourceRow[candidate], row.transform, row.params || {});
        if (String(transformed || "").trim()) {
          value = transformed;
          break;
        }
      }
      out[row.output_name] = value;
    });
    return out;
  });
  state.callerAi.previewRows = exportData;
  state.callerAi.previewColumns = previewColumns;
  renderTable("caller-ai-table", exportData, 25, previewColumns);
  const dummyCardUsed = activeRows.some((row) => row.output_name === "CardNumber" && row.source === "(blank)");
  if (!state.callerAi.rows.length) {
    setStatus("caller-ai-export-status", "Preview will appear after the file is parsed.", "info");
  } else if (duplicateNames.length) {
    setStatus("caller-ai-export-status", `Output column names must be unique: ${Array.from(new Set(duplicateNames)).join(", ")}`, "danger");
  } else if (!previewColumns.length) {
    setStatus("caller-ai-export-status", "Add at least one output column to build the download preview.", "info");
  } else if (missingSources.length) {
    setStatus("caller-ai-export-status", `Some Caller AI source columns were not found: ${Array.from(new Set(missingSources)).join(", ")}`, "danger");
  } else if (dummyCardUsed) {
    setStatus("caller-ai-export-status", `Caller AI CSV ready: ${exportData.length} row(s), ${previewColumns.length} column(s). No card number column was detected, so CardNumber has been filled with random 4-digit dummy values.`, "info");
  } else {
    setStatus("caller-ai-export-status", `Caller AI CSV ready: ${exportData.length} row(s), ${previewColumns.length} column(s).`, "info");
  }
}
function escapeHtml(value) {
  return String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\"/g, "&quot;");
}
function renderFileCards() {
  const list = document.getElementById("merge-file-list"); list.innerHTML = "";
  state.merge.files.forEach((entry, index) => {
    const card = document.createElement("div"); card.className = "file-card";
    const mergeControls = state.merge.combineMethod === "merge"
      ? `<div class="form-grid form-grid--three"><div class="form-group"><label>Role</label><input data-file-field="role" data-index="${index}" value="${escapeHtml(entry.role)}"></div><div class="form-group"><label>Merge key columns</label><input data-file-field="keyCols" data-index="${index}" value="${escapeHtml(entry.keyCols.join(", "))}"></div><div class="form-group"><label>Duplicate strategy</label><select data-file-field="duplicateStrategy" data-index="${index}"><option ${entry.duplicateStrategy === "Keep first" ? "selected" : ""}>Keep first</option><option ${entry.duplicateStrategy === "Keep last" ? "selected" : ""}>Keep last</option><option ${entry.duplicateStrategy === "Aggregate values" ? "selected" : ""}>Aggregate values</option><option ${entry.duplicateStrategy === "Error" ? "selected" : ""}>Error</option></select></div></div>`
      : `<div class="surface-panel">Rows from this file will be appended into the combined dataset in upload order.</div>`;
    card.innerHTML = `<div class="file-card-head"><div><h3>${escapeHtml(entry.fileName)}</h3><div>${entry.rows.length} rows | ${uniqueColumns(entry.rows).length} columns</div></div><button class="btn btn-ghost" data-delete-file="${index}" type="button">Remove</button></div>${mergeControls}`;
    list.appendChild(card);
  });
  syncBaseRoleOptions();
}
function updateMergeModeUI() {
  const isAppendMode = state.merge.combineMethod === "append";
  document.getElementById("merge-key-options").classList.toggle("hidden", isAppendMode);
  document.getElementById("append-options").classList.toggle("hidden", !isAppendMode);
  document.getElementById("merge-mapping-card").classList.toggle("hidden", isAppendMode);
  document.getElementById("download-unmatched").disabled = isAppendMode || !state.merge.unmatchedReports.length;
  renderFileCards();
  updateExportRows();
}
function syncBaseRoleOptions() {
  const select = document.getElementById("base-role");
  const current = select.value;
  select.innerHTML = state.merge.files.map((entry) => `<option value="${escapeHtml(entry.role)}">${escapeHtml(entry.role)}</option>`).join("");
  if (state.merge.files.some((entry) => entry.role === current)) select.value = current;
}
function buildDefaultOutputRows(count) {
  state.merge.exportRows = Array.from({ length: Number(count || 10) }, () => ({ source: "(blank)", transform: "None", params: {}, output_name: "" }));
  renderMappingRows();
}
async function handleSimpleFile() {
  const file = document.getElementById("simple-file").files[0];
  if (!file) return;
  try {
    const parsed = await parseFile(file, { delimiter: document.getElementById("simple-delimiter").value, headerRow: document.getElementById("simple-header-row").value });
    state.simple = { file, rows: parsed.rows, columns: uniqueColumns(parsed.rows), fileType: parsed.fileType, parsedName: safeFilename(file.name.replace(/\.[^.]+$/, "")) };
    renderTable("simple-table", parsed.rows);
    setStatus("simple-status", `${file.name}: ${parsed.rows.length} rows loaded.`, "info");
  } catch (error) { setStatus("simple-status", error.message, "danger"); }
}
async function handleCallerAiFile() {
  const file = document.getElementById("caller-ai-file").files[0];
  if (!file) return;
  try {
    const parsed = await parseFile(file, { delimiter: document.getElementById("caller-ai-delimiter").value, headerRow: document.getElementById("caller-ai-header-row").value });
    const columns = uniqueColumns(parsed.rows);
    const parsedName = `${safeFilename(file.name.replace(/\.[^.]+$/, ""))}_caller_ai`;
    state.callerAi = {
      file,
      rows: parsed.rows,
      columns,
      exportRows: [],
      previewRows: [],
      previewColumns: [],
      parsedName
    };
    resetCallerAiOutputRows(columns);
    renderCallerAiMappingRows();
    renderTable("caller-ai-source-table", parsed.rows);
    updateCallerAiExportRows();
    setStatus("caller-ai-status", `${file.name}: ${parsed.rows.length} rows loaded.`, "info");
  } catch (error) {
    setStatus("caller-ai-status", error.message, "danger");
  }
}
function switchMode(mode) {
  state.mode = mode;
  document.getElementById("simple-mode").classList.toggle("hidden", mode !== "simple");
  document.getElementById("caller-ai-mode").classList.toggle("hidden", mode !== "caller-ai");
  document.getElementById("merge-mode").classList.toggle("hidden", mode !== "merge");
  document.querySelectorAll(".mode-tab").forEach((button) => {
    const active = button.dataset.mode === mode;
    button.classList.toggle("active", active);
    button.classList.toggle("btn-secondary", !active);
  });
}
async function handleMergeFiles() {
  const files = Array.from(document.getElementById("merge-files").files || []);
  if (!files.length) return;
  for (let index = 0; index < files.length; index += 1) {
    const file = files[index];
    try {
      const parsed = await parseFile(file, { delimiter: "auto", headerRow: 1 });
      state.merge.files.push({ fileName: file.name, role: `File${state.merge.files.length + 1}`, rows: parsed.rows, keyCols: [uniqueColumns(parsed.rows)[0] || ""].filter(Boolean), duplicateStrategy: "Keep first" });
    } catch (error) { setStatus("merge-status", `${file.name}: ${error.message}`, "danger"); }
  }
  renderFileCards();
  setStatus("merge-status", `${state.merge.files.length} file(s) ready.`, "info");
}
function runMerge() {
  try {
    if (!state.merge.files.length) throw new Error("Add files before running a merge.");
    if (state.merge.combineMethod === "append") {
      state.merge.diagnostics = state.merge.files.map(appendDiagnosticsForFile);
      const schemaMode = document.getElementById("append-schema-mode").value;
      const addSourceFile = document.getElementById("append-source-file").checked;
      const sourceColumnName = document.getElementById("append-source-column").value || "SourceFile";
      const { combinedRows, combineColumns, notes } = appendRows(state.merge.files, schemaMode, addSourceFile, sourceColumnName);
      state.merge.mergedRows = combinedRows;
      state.merge.mergedColumns = combineColumns;
      state.merge.notes = notes;
      state.merge.unmatchedReports = [];
      renderMergeDiagnostics(); renderMergeNotes();
      updateExportRows();
      updateMergeModeUI();
      setStatus("merge-status", `Combined ${state.merge.mergedRows.length} row(s).`, "info");
      return;
    }
    state.merge.diagnostics = state.merge.files.map(diagnosticsForFile);
    const processed = Object.fromEntries(state.merge.files.map((entry) => [entry.role, { ...entry, rows: prepareRows(entry) }]));
    const baseRole = document.getElementById("base-role").value || state.merge.files[0].role;
    let mergedRows = processed[baseRole].rows.map((row) => ({ ...row }));
    const joinType = document.getElementById("join-type").value;
    const excludeUnmatched = document.getElementById("exclude-unmatched").checked;
    const notes = []; const unmatchedReports = [];
    state.merge.files.filter((entry) => entry.role !== baseRole).forEach((entry) => {
      const { result, leftOnly, rightOnly, bothCount } = mergePrepared(mergedRows, processed[entry.role].rows, joinType, safeFilename(entry.role, 20));
      if (leftOnly.length) unmatchedReports.push({ name: `${safeFilename(baseRole)}_unmatched_against_${safeFilename(entry.role)}.csv`, rows: leftOnly });
      if (rightOnly.length) unmatchedReports.push({ name: `${safeFilename(entry.role)}_orphans_vs_${safeFilename(baseRole)}.csv`, rows: rightOnly });
      notes.push(`${entry.role}: matched ${bothCount} rows`);
      if (leftOnly.length) notes.push(`${entry.role}: ${leftOnly.length} base rows had no match`);
      if (rightOnly.length) notes.push(`${entry.role}: ${rightOnly.length} rows were present only in this file`);
      mergedRows = excludeUnmatched ? result.filter((row) => row.__indicator__ === "both") : result;
      mergedRows = mergedRows.map((row) => { const out = { ...row }; delete out.__indicator__; return out; });
    });
    state.merge.mergedRows = mergedRows.map((row) => { const out = { ...row }; delete out.__merge_key__; return out; });
    state.merge.mergedColumns = uniqueColumns(state.merge.mergedRows);
    state.merge.notes = notes;
    state.merge.unmatchedReports = unmatchedReports;
    renderMergeDiagnostics(); renderMergeNotes();
    if (!state.merge.exportRows.length) buildDefaultOutputRows(document.getElementById("output-columns-count").value);
    updateExportRows();
    updateMergeModeUI();
    setStatus("merge-status", `Merged ${state.merge.mergedRows.length} row(s).`, "info");
  } catch (error) { setStatus("merge-status", error.message, "danger"); }
}
function bindEvents() {
  document.querySelectorAll(".mode-tab").forEach((button) => button.addEventListener("click", () => switchMode(button.dataset.mode)));
  document.getElementById("simple-file").addEventListener("change", handleSimpleFile);
  document.getElementById("simple-download").addEventListener("click", () => {
    if (!state.simple.rows.length) return;
    exportRows(state.simple.rows, state.simple.parsedName, document.getElementById("simple-output-type").value);
  });
  document.getElementById("caller-ai-file").addEventListener("change", handleCallerAiFile);
  document.getElementById("caller-ai-download").addEventListener("click", () => {
    if (!state.callerAi.previewColumns.length) {
      setStatus("caller-ai-export-status", "There is no Caller AI CSV to download yet.", "danger");
      return;
    }
    exportRows(state.callerAi.previewRows || [], state.callerAi.parsedName || "caller_ai", "csv", state.callerAi.previewColumns || []);
  });
  document.getElementById("caller-ai-add-output").addEventListener("click", () => {
    state.callerAi.exportRows.push({ source: "(blank)", transform: "None", params: {}, output_name: "" });
    renderCallerAiMappingRows();
    updateCallerAiExportRows();
  });
  document.getElementById("caller-ai-reset-defaults").addEventListener("click", () => {
    resetCallerAiOutputRows(state.callerAi.columns || []);
    renderCallerAiMappingRows();
    updateCallerAiExportRows();
  });
  document.getElementById("merge-combine-method").addEventListener("change", (event) => {
    state.merge.combineMethod = event.target.value;
    state.merge.unmatchedReports = [];
    updateMergeModeUI();
  });
  document.getElementById("append-source-file").addEventListener("change", (event) => {
    state.merge.addSourceFile = event.target.checked;
    document.getElementById("append-source-column").disabled = !event.target.checked;
  });
  document.getElementById("merge-files").addEventListener("change", handleMergeFiles);
  document.getElementById("run-merge").addEventListener("click", runMerge);
  document.getElementById("build-output-rows").addEventListener("click", () => buildDefaultOutputRows(document.getElementById("output-columns-count").value));
  document.getElementById("download-merged").addEventListener("click", () => {
    const outputNames = state.merge.previewColumns || [];
    if (!outputNames.length) {
      setStatus("export-status", "There are no export columns to download yet.", "danger");
      return;
    }
    exportRows(state.merge.previewRows || [], document.getElementById("merge-export-name").value || "merged_output", document.getElementById("merge-output-type").value, outputNames);
  });
  document.getElementById("download-template").addEventListener("click", () => {
    if (state.merge.combineMethod !== "merge") {
      setStatus("merge-status", "Mapping templates are only available for key-based merge mode right now.", "info");
      return;
    }
    downloadBlob("mapping_template.json", JSON.stringify(outputTemplatePayload(), null, 2), "application/json");
  });
  document.getElementById("download-unmatched").addEventListener("click", downloadUnmatchedZip);
  document.getElementById("template-file").addEventListener("change", async (event) => {
    const file = event.target.files[0]; if (!file) return;
    if (state.merge.combineMethod !== "merge") {
      setStatus("merge-status", "Templates can only be loaded in key-based merge mode right now.", "info");
      event.target.value = "";
      return;
    }
    try {
      const template = JSON.parse(await file.text());
      state.merge.template = template;
      document.getElementById("join-type").value = template.join_type || "left";
      state.merge.exportRows = (template.output_spec || []).map((row) => ({ source: row.source || "(blank)", transform: normaliseTransformName(row.transform), params: row.params || {}, output_name: row.output_name || "" }));
      state.merge.files.forEach((entry) => {
        entry.keyCols = template.merge_keys_by_role?.[entry.role] || entry.keyCols;
        entry.duplicateStrategy = template.duplicate_strategy_by_role?.[entry.role] || entry.duplicateStrategy;
      });
      renderFileCards(); renderMappingRows(); setStatus("merge-status", "Template loaded.", "info");
    } catch (error) { setStatus("merge-status", error.message, "danger"); }
  });
  document.addEventListener("input", (event) => {
    const target = event.target;
    if (target.matches("[data-file-field]")) {
      const entry = state.merge.files[Number(target.dataset.index)];
      if (target.dataset.fileField === "role") entry.role = target.value.trim() || `File${Number(target.dataset.index) + 1}`;
      if (target.dataset.fileField === "keyCols") entry.keyCols = parseColumns(target.value);
      renderFileCards();
    }
    if (target.matches("[data-field]")) {
      const row = state.merge.exportRows[Number(target.dataset.index)];
      row[target.dataset.field] = target.dataset.field === "transform" ? normaliseTransformName(target.value) : target.value;
      if (target.dataset.field === "transform") row.params = getTransformParams(target.value);
      renderMappingRows(); updateExportRows();
    }
    if (target.matches("[data-caller-ai-field]")) {
      const row = state.callerAi.exportRows[Number(target.dataset.index)];
      row[target.dataset.callerAiField] = target.dataset.callerAiField === "transform" ? normaliseTransformName(target.value) : target.value;
      if (target.dataset.callerAiField === "transform") row.params = getTransformParams(target.value);
      renderCallerAiMappingRows(); updateCallerAiExportRows();
    }
    if (target.matches("[data-param]")) {
      const row = state.merge.exportRows[Number(target.dataset.index)];
      row.params ||= {};
      row.params[target.dataset.param] = target.type === "checkbox" ? target.checked : target.value;
      updateExportRows();
    }
    if (target.matches("[data-caller-ai-param]")) {
      const row = state.callerAi.exportRows[Number(target.dataset.index)];
      row.params ||= {};
      row.params[target.dataset.callerAiParam] = target.type === "checkbox" ? target.checked : target.value;
      updateCallerAiExportRows();
    }
  });
  document.addEventListener("change", (event) => {
    const target = event.target;
    if (target.matches("select[data-file-field='duplicateStrategy']")) state.merge.files[Number(target.dataset.index)].duplicateStrategy = target.value;
    if (target.matches("[data-param]") && target.type === "checkbox") {
      const row = state.merge.exportRows[Number(target.dataset.index)]; row.params ||= {}; row.params[target.dataset.param] = target.checked; updateExportRows();
    }
    if (target.matches("[data-caller-ai-param]") && target.type === "checkbox") {
      const row = state.callerAi.exportRows[Number(target.dataset.index)]; row.params ||= {}; row.params[target.dataset.callerAiParam] = target.checked; updateCallerAiExportRows();
    }
  });
  document.addEventListener("click", (event) => {
    const target = event.target;
    if (target.matches("[data-delete-file]")) {
      state.merge.files.splice(Number(target.dataset.deleteFile), 1); renderFileCards(); setStatus("merge-status", `${state.merge.files.length} file(s) ready.`, "info");
    }
    if (target.matches("[data-remove-index]")) {
      state.merge.exportRows.splice(Number(target.dataset.removeIndex), 1); renderMappingRows(); updateExportRows();
    }
    if (target.matches("[data-caller-ai-remove-index]")) {
      state.callerAi.exportRows.splice(Number(target.dataset.callerAiRemoveIndex), 1); renderCallerAiMappingRows(); updateCallerAiExportRows();
    }
  });
}

bindEvents();
updateMergeModeUI();
document.getElementById("append-source-column").disabled = !document.getElementById("append-source-file").checked;


