/**
 * transform_mongo_docs.js
 * 
 * Flatten nested MongoDB documents into flat objects suitable for ClickHouse insertion.
 * Paste this into an n8n Code node after the MongoDB read node.
 *
 * Input:  $input.all() — array of MongoDB documents (may contain nested objects/arrays)
 * Output: array of flat key-value objects ready for ClickHouse INSERT
 */

// ── Configuration ──────────────────────────────────────────────
const ARRAY_STRATEGY = 'json_stringify'; // 'json_stringify' | 'first_element' | 'join_comma'
const MAX_DEPTH = 5;
const SEPARATOR = '_';
// Fields to exclude from output (e.g. internal Mongo fields)
const EXCLUDE_FIELDS = new Set(['__v']);
// ───────────────────────────────────────────────────────────────

function flattenObject(obj, prefix = '', depth = 0) {
  const result = {};

  if (depth > MAX_DEPTH) return result;

  for (const [key, value] of Object.entries(obj)) {
    if (EXCLUDE_FIELDS.has(key)) continue;

    const flatKey = prefix ? `${prefix}${SEPARATOR}${key}` : key;

    // Rename _id to id for ClickHouse compatibility
    const outputKey = flatKey === '_id' ? 'id' : flatKey.replace(/^_/, '');

    if (value === null || value === undefined) {
      result[outputKey] = null;
    } else if (Array.isArray(value)) {
      switch (ARRAY_STRATEGY) {
        case 'json_stringify':
          result[outputKey] = JSON.stringify(value);
          break;
        case 'first_element':
          result[outputKey] = value.length > 0 ? String(value[0]) : null;
          break;
        case 'join_comma':
          result[outputKey] = value.map(String).join(', ');
          break;
      }
    } else if (value instanceof Date) {
      // Format as ClickHouse-compatible DateTime string
      result[outputKey] = value.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
    } else if (typeof value === 'object') {
      // Check for MongoDB ObjectId pattern { $oid: "..." }
      if (value.$oid) {
        result[outputKey] = value.$oid;
      } else if (value.$date) {
        const d = new Date(value.$date);
        result[outputKey] = d.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
      } else if (value.$numberLong) {
        result[outputKey] = parseInt(value.$numberLong, 10);
      } else if (value.$numberDecimal) {
        result[outputKey] = parseFloat(value.$numberDecimal);
      } else {
        // Recurse into nested object
        Object.assign(result, flattenObject(value, outputKey, depth + 1));
      }
    } else {
      result[outputKey] = value;
    }
  }

  return result;
}

// ── Main ───────────────────────────────────────────────────────
const items = $input.all();
const output = [];

for (const item of items) {
  const doc = item.json;
  const flat = flattenObject(doc);
  output.push({ json: flat });
}

return output;
