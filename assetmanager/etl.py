#!/usr/bin/env node
// assetmanager/ts_to_json.mjs
// ESM wrapper that uses ts-node programmatically via createRequire.
// Works on Node 18, no --loader flags needed.

import { createRequire } from "module";
import { pathToFileURL } from "url";
import path from "path";
import fs from "fs";

const require = createRequire(import.meta.url);

// Ensure args
const file = process.argv[2];
if (!file) {
  console.error("Usage: ts_to_json.mjs <file.ts>");
  process.exit(2);
}

const abs = path.resolve(file);
if (!fs.existsSync(abs)) {
  console.error(`File not found: ${abs}`);
  process.exit(2);
}

// Register ts-node (transpile-only) for CommonJS require()
try {
  const tsnode = require("ts-node");
  tsnode.register({
    transpileOnly: true,
    compilerOptions: {
      module: "commonjs",
      moduleResolution: "node",
      target: "es2020",
      esModuleInterop: true,
      resolveJsonModule: true,
      skipLibCheck: true,
    },
  });
} catch (e) {
  console.error(
    "ts-node is not installed. Install it with: npm i -g ts-node typescript"
  );
  console.error(String(e && e.message ? e.message : e));
  process.exit(1);
}

// Now require the TS file. It should export a default big object.
let mod;
try {
  mod = require(abs);
} catch (e) {
  console.error(`Failed to require TS module: ${abs}`);
  console.error(String(e && e.message ? e.message : e));
  process.exit(1);
}

const data =
  (mod && (mod.default ?? mod.exports ?? mod)) !== undefined
    ? mod.default ?? mod.exports ?? mod
    : undefined;

if (data === undefined) {
  console.error("Module has no default export or usable value.");
  process.exit(1);
}

try {
  process.stdout.write(JSON.stringify(data));
} catch (e) {
  console.error("Failed to serialize module default export to JSON:");
  console.error(String(e && e.message ? e.message : e));
  process.exit(1);
}
