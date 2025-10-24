// Super tiny TS export-default object stripper.
// Assumes the i18n files are `export default { ... }` with plain objects.
// We do NOT evaluate code. We remove trailing commas where safe.

import fs from 'fs';

function stripExportDefault(tsSource) {
  // Remove leading "export default" and possible semicolon at end.
  let s = tsSource
    .replace(/^\s*export\s+default\s+/m, '')
    .trim();

  // Remove trailing semicolon if present
  if (s.endsWith(';')) s = s.slice(0, -1);

  // LSSM objects are JSON-like but can contain single-quoted strings and trailing commas.
  // Convert single quotes to double quotes when safe.
  // This is naive but works because keys are usually unquoted or single-quoted.
  // Strategy: rely on JSON5-ish shape; but we can't import JSON5. We'll massage lightly.

  // Replace single quotes around simple tokens: 'text' -> "text"
  s = s.replace(/'([^'\\]*?)'/g, (_, g1) => `"${g1.replace(/"/g, '\\"')}"`);

  // Remove trailing commas before } or ]
  s = s.replace(/,\s*([}\]])/g, '$1');

  // Ensure keys are quoted: foo: -> "foo":
  s = s.replace(/([,{]\s*)([A-Za-z0-9_]+)\s*:/g, '$1"$2":');

  return s;
}

if (process.argv.length < 3) {
  console.error('Usage: node ts_to_json.mjs <input.ts>');
  process.exit(1);
}

const inputPath = process.argv[2];
const raw = fs.readFileSync(inputPath, 'utf8');
const stripped = stripExportDefault(raw);

try {
  const obj = JSON.parse(stripped);
  process.stdout.write(JSON.stringify(obj));
} catch (e) {
  // On failure, print the massaged text to help debug
  console.error('JSON parse error:', e.message);
  console.error('----- BEGIN MASSAGED TEXT -----');
  console.error(stripped.slice(0, 2000));
  console.error('----- END MASSAGED TEXT -----');
  process.exit(2);
}
