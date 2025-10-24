#!/usr/bin/env node
// assetmanager/ts_to_json.mjs
// ESM wrapper that registers ts-node (transpile-only) and then require()s a TS file.
// Works even if ts-node is only installed globally.

import { createRequire } from "module";
import path from "path";
import fs from "fs";
import { execSync } from "child_process";

const require = createRequire(import.meta.url);

function registerTsNodeOrDie() {
  // 1) Try local resolution
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

  // 2) Try global npm root
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

  console.error(
    "ts-node cannot be resolved in this environment.\n" +
      "Fix options:\n" +
      "  a) Install locally in the cog folder: npm i ts-node typescript\n" +
      "  b) Or expose global modules: export NODE_PATH=$(npm root -g) for the Red service\n" +
      "  c) Or set NPM_GLOBAL_ROOT env var to the output of `npm root -g`."
  );
  process.exit(1);
}

function requireTsModule(absTsPath) {
  try {
    return require(absTsPath);
  } catch (e) {
    console.error(`Failed to require TS module: ${absTsPath}`);
    console.error(String(e && e.message ? e.message : e));
    process.exit(1);
  }
}

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
