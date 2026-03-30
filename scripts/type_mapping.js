/**
 * type_mapping.js
 *
 * Maps ClickHouse query results to Syntheta-compatible MySQL column types.
 * Paste this into an n8n Code node between the ClickHouse SELECT and MySQL INSERT nodes.
 *
 * This replicates the type inference logic from Syntheta4's file_upload.py:
 *   Integer  → INT
 *   Float    → DECIMAL(20,6)
 *   DateTime → DATETIME
 *   Boolean  → TINYINT(1)
 *   String   → VARCHAR(500) or TEXT (if > 1000 chars)
 *
 * Input:  $input.all() — rows from ClickHouse
 * Output: cleaned rows + a _syntheta_meta item with DDL info
 */

// ── Configuration ──────────────────────────────────────────────
const TABLE_PREFIX = 'syn_';
const TARGET_TABLE = TABLE_PREFIX + ($input.first().json._target_table || 'pipeline_data');
const DATETIME_THRESHOLD = 0.8; // 80% of non-null values must parse as dates
// ───────────────────────────────────────────────────────────────

/**
 * Infer Syntheta-compatible MySQL type from an array of sample values.
 * Mirrors: infer_sqlalchemy_dtype() in file_upload.py
 */
function inferMySQLType(values) {
  const nonNull = values.filter(v => v !== null && v !== undefined && v !== '');
  if (nonNull.length === 0) return 'VARCHAR(500)';

  // Check boolean (must come before integer check)
  const boolCount = nonNull.filter(v =>
    v === true || v === false || v === 0 || v === 1 || v === '0' || v === '1'
      || String(v).toLowerCase() === 'true' || String(v).toLowerCase() === 'false'
  ).length;
  if (boolCount === nonNull.length) return 'TINYINT(1)';

  // Check integer
  const intCount = nonNull.filter(v => Number.isInteger(Number(v)) && !isNaN(Number(v))).length;
  if (intCount === nonNull.length) return 'INT';

  // Check float / decimal
  const floatCount = nonNull.filter(v => !isNaN(parseFloat(v))).length;
  if (floatCount === nonNull.length) return 'DECIMAL(20,6)';

  // Check datetime
  const dateCount = nonNull.filter(v => !isNaN(Date.parse(v))).length;
  if (dateCount / nonNull.length >= DATETIME_THRESHOLD) return 'DATETIME';

  // String — check max length
  const maxLen = Math.max(...nonNull.map(v => String(v).length));
  return maxLen > 1000 ? 'TEXT' : 'VARCHAR(500)';
}

/**
 * Coerce a value to match the inferred MySQL type.
 */
function coerceValue(value, mysqlType) {
  if (value === null || value === undefined) return null;

  switch (mysqlType) {
    case 'INT':
      return parseInt(value, 10) || 0;
    case 'DECIMAL(20,6)':
      return parseFloat(value) || 0.0;
    case 'TINYINT(1)':
      if (typeof value === 'boolean') return value ? 1 : 0;
      return ['true', '1'].includes(String(value).toLowerCase()) ? 1 : 0;
    case 'DATETIME': {
      const d = new Date(value);
      if (isNaN(d.getTime())) return null;
      return d.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
    }
    default:
      return String(value);
  }
}

// ── Main ───────────────────────────────────────────────────────
const items = $input.all();
if (items.length === 0) return [];

// Remove internal meta fields
const cleanItems = items
  .filter(item => !item.json._target_table)
  .map(item => {
    const cleaned = { ...item.json };
    delete cleaned._target_table;
    return cleaned;
  });

// Collect column samples (first 200 rows)
const sampleSize = Math.min(cleanItems.length, 200);
const columns = Object.keys(cleanItems[0]);
const columnSamples = {};
columns.forEach(col => {
  columnSamples[col] = cleanItems.slice(0, sampleSize).map(row => row[col]);
});

// Infer types
const typeMap = {};
columns.forEach(col => {
  typeMap[col] = inferMySQLType(columnSamples[col]);
});

// Coerce all values
const output = cleanItems.map(row => {
  const coerced = {};
  columns.forEach(col => {
    coerced[col] = coerceValue(row[col], typeMap[col]);
  });
  return { json: coerced };
});

// Append metadata item (consumed by downstream nodes for DDL if needed)
output.push({
  json: {
    _syntheta_meta: true,
    table_name: TARGET_TABLE,
    column_types: typeMap,
    row_count: cleanItems.length
  }
});

return output;
