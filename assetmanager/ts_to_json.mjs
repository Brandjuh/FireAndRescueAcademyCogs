#!/usr/bin/env node
// assetmanager/ts_to_json.mjs
// ESM wrapper: registreert ts-node (transpile-only), patcht CJS loader met stubs
// voor ontbrekende LSSM helpers (zoals ../../registerEquipment), en require()'t het TS-bestand.

import { createRequire } from "module";
import Module from "module";
import path from "path";
import fs from "fs";
import { execSync } from "child_process";

const require = createRequire(import.meta.url);

// --- loader patches ---------------------------------------------------------

// Identity shim die teruggeeft wat er in gaat, met default export.
function identityShim(x) {
  return x;
}
identityShim.default = identityShim;

// Patronen van bekende LSSM helper-paden die we niet hebben,
// maar die conceptueel "register" helpers zijn en dus identity mogen zijn.
const REGISTER_RE = /(^|\/)register[A-Za-z]+$/; // bv ../../registerEquipment
// Eventueel nog extra utility shims kun je hier toevoegen als het ooit nodig is.
const EXTRA_STUBS = new Set([
  // voorbeelden:
  // "utils/mergeDeep",
  // "utils/withDefaults",
]);

// Patch CJS loader: als een require faalt en het lijkt een register-helper,
// lever dan de identityShim ipv error.
const _origLoad = Module._load;
Module._load = function (request, parent, isMain) {
  try {
    return _origLoad.apply(this, arguments);
  } catch (err) {
    // Alleen stubs voor bekende patronen, anders gooi de error gewoon door.
    if (REGISTER_RE.test(request) || EXTRA_STUBS.has(request)) {
      return identityShim;
    }
    // Sommige TS files importeren relatieve paden alsof ze bestaan.
    // Als het een relatieve import is en eindigt op /registerXyz uit parent dir, alsnog shim.
    try {
      const maybe = path.resolve(parent?.path ? path.dirname(parent.path) : process.cwd(), request);
      if (REGISTER_RE.test(maybe)) return identityShim;
    } catch (_) {
      // ignore
    }
    throw err;
  }
};

// --- ts-node registratie ----------------------------------------------------

function registerTsNodeOrDie() {
  // 1) Probeer lokale node_modules
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
    return;
  } catch (_) {
    // keep going
  }

  // 2) Probeer globale npm root
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
          skipLibCheck: true,
        },
      });
      return;
    } catch (_) {
      // fallthrough to error
    }
  }

  console.error(
    "ts-node cannot be resolved.\n" +
      "Fix options:\n" +
      "  a) Install locally in the cog folder: npm i ts-node typescript\n" +
      "  b) Or expose global modules: export NODE_PATH=$(npm root -g) for the Red service\n" +
      "  c) Or set NPM_GLOBAL_ROOT env var to the output of `npm root -g`."
  );
  process.exit(1);
}

// --- main -------------------------------------------------------------------

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

// Zorg dat relative imports binnen het TS-bestand zich gedragen.
process.chdir(path.dirname(abs));

registerTsNodeOrDie();

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
