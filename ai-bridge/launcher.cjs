const { spawn } = require("node:child_process");

const children = [
  spawn(process.execPath, ["catalog-service.mjs"], { stdio: "inherit", env: process.env }),
  spawn(process.execPath, ["server.js"], { stdio: "inherit", env: process.env })
];

let stopping = false;
function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  for (const child of children) {
    if (!child.killed) child.kill("SIGTERM");
  }
  setTimeout(() => process.exit(code), 250).unref();
}

for (const child of children) {
  child.on("exit", code => stop(code ?? 1));
  child.on("error", () => stop(1));
}

process.on("SIGTERM", () => stop(0));
process.on("SIGINT", () => stop(0));
