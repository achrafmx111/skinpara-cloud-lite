#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const serverPath = path.join(__dirname, "server.js");
const source = fs.readFileSync(serverPath, "utf8");

const checks = [
  ["reasoning leak guard", /reasoningLeak[\s\S]{0,900}let me \(\?:unpack/],
  ["encoding corruption guard", /encoding_corruption/],
  ["language validator", /function validateAdvisorLanguage/],
  ["catalog grounding prompt", /CATALOG GROUNDING/],
  ["purchase intent rules", /CONTEXT & PURCHASE INTENT/],
  ["medical safety rules", /MEDICAL SAFETY/],
  ["history window", /\.slice\(-10\)/],
  ["current-message consultation context", /customerContext = \[\.\.\.priorUserMessages, value\]/],
  ["test order mode exists", /SKINPARA_ORDER_MODE/],
  ["real-order production guard exists", /ORDER_MODE|orderMode/]
];

let failed = 0;
for (const [name, pattern] of checks) {
  const ok = pattern.test(source);
  process.stdout.write(`${ok ? "PASS" : "FAIL"} - ${name}\n`);
  if (!ok) failed++;
}

const forbiddenCustomerArtifacts = [
  /return\s+["'`][^"'\`]*(?:Ø|Ù|Ã|Â|â€|ï¸|ðŸ)/,
  /return\s+["'`][^"'\`]*let me unpack/i,
  /return\s+["'`][^"'\`]*checking history/i
];
for (const pattern of forbiddenCustomerArtifacts) {
  const ok = !pattern.test(source);
  process.stdout.write(`${ok ? "PASS" : "FAIL"} - no customer-facing corruption/reasoning literal\n`);
  if (!ok) failed++;
}

if (failed) {
  process.stderr.write(`\nQUALITY GATE FAILED: ${failed} check(s)\n`);
  process.exit(1);
}
process.stdout.write("\nSTATIC QUALITY GATE PASSED\n");
