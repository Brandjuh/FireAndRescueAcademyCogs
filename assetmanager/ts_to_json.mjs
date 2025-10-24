// ts_to_json.mjs
// Minimal TS -> JS evaluator for LSSM-style i18n files (no deps).
import fs from "fs";
import vm from "vm";
import path from "path";

if (process.argv.length < 3) {
  console.error("Usage: node ts_to_json.mjs <file.ts>");
  process.exit(2);
}

const file = process.argv[2];
let src = fs.readFileSync(file, "utf8");

// 1) Normalize newlines, strip BOM
src = src.replace(/^\uFEFF/, "");

// 2) Strip comments but keep strings
function stripComments(s) {
  let out = "";
  let i = 0, n = s.length;
  let inStr = null, esc = false, inLine = false, inBlock = false;
  while (i < n) {
    const ch = s[i];
    const nx = i + 1 < n ? s[i + 1] : "";
    if (inLine) {
      if (ch === "\n") { inLine = false; out += ch; }
      i++; continue;
    }
    if (inBlock) {
      if (ch === "*" && nx === "/") { inBlock = false; i += 2; continue; }
      i++; continue;
    }
    if (inStr) {
      out += ch;
      if (esc) esc = false;
      else if (ch === "\\") esc = true;
      else if (ch === inStr) inStr = null;
      i++; continue;
    }
    if (ch === "/" && nx === "/") { inLine = true; i += 2; continue; }
    if (ch === "/" && nx === "*") { inBlock = true; i += 2; continue; }
    if (ch === "'" || ch === '"' || ch === "`") { inStr = ch; out += ch; i++; continue; }
    out += ch; i++;
  }
  return out;
}
src = stripComments(src);

// 3) Drop type-only imports/exports, interfaces, type aliases
src = src
  .replace(/^\s*import\s+type\s+.*?;?\s*$/gm, "")
  .replace(/^\s*export\s+type\s+.*?;?\s*$/gm, "")
  .replace(/^\s*type\s+[A-Za-z0-9_]+\s*=\s*[\s\S]*?;\s*$/gm, "")
  .replace(/^\s*interface\s+[A-Za-z0-9_]+\s*{[\s\S]*?}\s*$/gm, "");

// 4) Convert 'export const' to 'const'
src = src.replace(/\bexport\s+const\b/g, "const");

// 5) Remove simple generic params on functions/constants: foo<T extends U>(...) -> foo(...)
src = src.replace(/function\s+([A-Za-z0-9_]+)\s*<[^>]*>\s*\(/g, "function $1(");
src = src.replace(/const\s+([A-Za-z0-9_]+)\s*<[^>]*>\s*=\s*/g, "const $1 = ");

// 6) Remove obvious type annotations in function params and returns
//    This is heuristic but works fine for these i18n files.
src = src.replace(/\(([^\)]*)\)\s*:\s*[A-Za-z0-9_<>\[\]\|\&\s\.]+(\s*=>|\s*{)/g, "($1)$2");
src = src.replace(/:\s*[A-Za-z0-9_<>\[\]\|\&\s\.]+(?=\s*(=|;|,|\)))/g, "");

// 7) Remove 'as const' / 'as Type'
src = src.replace(/\s+as\s+const\b/g, "");
src = src.replace(/\s+as\s+[A-Za-z0-9_<>\[\]\.\|&\s]+/g, "");

// 8) Turn `export default X` into assignment to __DEFAULT__
let defaultAssigned = false;
src = src.replace(/\bexport\s+default\s+/g, () => {
  defaultAssigned = true;
  return "const __DEFAULT__ = ";
});

if (!defaultAssigned) {
  console.error("No `export default` found.");
  process.exit(3);
}

// 9) Execute in a sandbox and capture default
const sandbox = {
  console,
  exports: {},
  module: { exports: {} },
  require,  // usually not used here
  __dirname: path.dirname(path.resolve(file)),
  __filename: path.resolve(file),
};
vm.createContext(sandbox);

const wrapped = `${src}\n;globalThis.__RESULT__ = (typeof __DEFAULT__ !== 'undefined' ? __DEFAULT__ : undefined);`;
try {
  vm.runInContext(wrapped, sandbox, { filename: file, timeout: 5000 });
} catch (e) {
  console.error("Execution error:", e && e.stack ? e.stack : e);
  process.exit(4);
}

const result = sandbox.__RESULT__;
if (typeof result === "undefined") {
  console.error("No default export value evaluated.");
  process.exit(5);
}

// 10) Print JSON
try {
  process.stdout.write(JSON.stringify(result));
} catch (e) {
  console.error("JSON stringify failed:", e && e.message ? e.message : e);
  process.exit(6);
}
