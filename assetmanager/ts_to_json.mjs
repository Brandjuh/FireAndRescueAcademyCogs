#!/usr/bin/env node
// assetmanager/ts_to_json.mjs
// ESM wrapper that registers ts-node (transpile-only).
// It works even when ts-node is only installed *globally*.
// Node 18 compatible, no --loader flags needed.

import { createRequire } from "module";
import path from "path";
import fs from "fs";
import { execSync } from "child_process";

const require = createRequire(import.meta.url);

function registerTsNodeOrDie() {
  // 1) Try normal resolution (local node_modules)
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
        skipLibCheck: true
      }
    });
    return;
  } catch (_) {
    // keep going
  }

  // 2) Try global npm root: `npm root -g`
  let globalRoot = process.env.NPM_GLOBAL_ROOT;
  if (!globalRoot) {
    try {
      globalRoot = execSync("npm root -g", { stdio: ["ignore", "pipe", "ignore"] })
        .toString()
        .trim();
    } catch {
      // ignore
    }
  }

  if (globalRoot) {
    try {
      // Point a require at the global root. From there, normal "ts-node/..." resolution works.
      const globalReq = createRequire(path.join(globalRoot, "node_modules", "._global_stub.js"));
      const tsnode = globalReq("ts-node");
      tsnode.register({
        transpileOnly: true,
        compilerOptions: {
          module: "commonjs",
          moduleResolution: "node",
          target: "es2020",
          esModuleInterop: true,
          resolveJsonModule: true,
          skipLibCheck: true
        }
      });
      return;
    } catch (_) {
      // fallthrough to error
    }
  }

  // 3) Give a crystal clear error with a fix
  console.error(
    "ts-node cannot be resolved in this environment.\n" +
      "Fix options:\n" +
      "  a) Install locally in your cog folder: npm i ts-node typescript\n" +
      "  b) Or keep global and expose it by setting NODE_PATH to your global npm root, e.g.\n" +
      "       export NODE_PATH=$(npm root -g)\n" +
      "     and ensure the Red service inherits that env.\n" +
      "  c) Or set NPM_GLOBAL_ROOT env var to the output of `npm root -g` for this process."
  );
  process.exit(1);
}

function requireTsModule(absTsPath) {
  // After ts-node registration, require() can load TS files as CJS.
  try {
    return require(absTsPath);
  } catch (e) {
    console.error(`Failed to require TS module: ${absTsPath}`);
    console.error(String(e && e.message ? e.message : e));
    process.exit(1);
  }
}

// -------- Main --------
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

registerTsNodeOrDie();

const mod = requireTsModule(abs);
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
