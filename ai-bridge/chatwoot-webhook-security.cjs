const crypto = require("crypto");

const DEFAULT_MAX_AGE_SEC = 300;
const MAX_BODY_BYTES = 5_000_000;

function parsePositiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0
    ? parsed
    : fallback;
}

function loadWebhookSecurityConfig(env = process.env) {
  return {
    enabled:
      String(
        env.SKINPARA_CHATWOOT_WEBHOOK_AUTH_ENABLED ||
        "false"
      ).toLowerCase() === "true",
    secret:
      env.SKINPARA_CHATWOOT_WEBHOOK_SECRET || "",
    maxAgeSec:
      parsePositiveInteger(
        env.SKINPARA_CHATWOOT_WEBHOOK_MAX_AGE_SEC,
        DEFAULT_MAX_AGE_SEC
      ),
    maxBodyBytes:
      parsePositiveInteger(
        env.SKINPARA_CHATWOOT_WEBHOOK_MAX_BODY_BYTES,
        MAX_BODY_BYTES
      )
  };
}

function safeEqualHex(left, right) {
  if (
    !/^[a-f0-9]{64}$/i.test(left) ||
    !/^[a-f0-9]{64}$/i.test(right)
  ) {
    return false;
  }

  const leftBuffer = Buffer.from(left, "hex");
  const rightBuffer = Buffer.from(right, "hex");

  return (
    leftBuffer.length === rightBuffer.length &&
    crypto.timingSafeEqual(leftBuffer, rightBuffer)
  );
}

function correlationId(timestamp, rawBody) {
  return crypto
    .createHash("sha256")
    .update(String(timestamp))
    .update(".")
    .update(rawBody)
    .digest("hex")
    .slice(0, 16);
}

function verifyChatwootWebhook({
  rawBody,
  headers,
  config,
  nowMs = Date.now()
}) {
  if (!config.enabled) {
    return {
      ok: true,
      authDisabled: true,
      correlationId: correlationId("disabled", rawBody)
    };
  }

  if (!config.secret) {
    return {
      ok: false,
      status: 503,
      code: "auth_not_configured"
    };
  }

  const timestampHeader =
    headers["x-chatwoot-timestamp"];
  const signatureHeader =
    headers["x-chatwoot-signature"];

  if (!timestampHeader || !signatureHeader) {
    return {
      ok: false,
      status: 401,
      code: "missing_auth"
    };
  }

  if (!/^\d+$/.test(String(timestampHeader))) {
    return {
      ok: false,
      status: 401,
      code: "invalid_timestamp"
    };
  }

  const timestamp = Number(timestampHeader);
  if (!Number.isSafeInteger(timestamp)) {
    return {
      ok: false,
      status: 401,
      code: "invalid_timestamp"
    };
  }

  const nowSec = Math.floor(nowMs / 1000);
  if (timestamp < nowSec - config.maxAgeSec) {
    return {
      ok: false,
      status: 401,
      code: "expired"
    };
  }

  if (timestamp > nowSec + config.maxAgeSec) {
    return {
      ok: false,
      status: 401,
      code: "future_timestamp"
    };
  }

  const match =
    /^sha256=([a-f0-9]{64})$/i.exec(
      String(signatureHeader)
    );

  if (!match) {
    return {
      ok: false,
      status: 401,
      code: "invalid_signature"
    };
  }

  const expected = crypto
    .createHmac("sha256", config.secret)
    .update(String(timestamp))
    .update(".")
    .update(rawBody)
    .digest("hex");

  if (!safeEqualHex(expected, match[1])) {
    return {
      ok: false,
      status: 401,
      code: "invalid_signature"
    };
  }

  return {
    ok: true,
    timestamp,
    correlationId: correlationId(timestamp, rawBody),
    replayDigest: crypto
      .createHash("sha256")
      .update(String(timestamp))
      .update(".")
      .update(rawBody)
      .update(".")
      .update(match[1])
      .digest("hex")
  };
}

async function claimChatwootWebhookReplay({
  redisCommand,
  replayDigest,
  maxAgeSec
}) {
  const ttlSec = Math.max(
    maxAgeSec + 60,
    maxAgeSec * 2
  );

  const result = await redisCommand([
    "SET",
    `skinpara:webhook:chatwoot:replay:${replayDigest}`,
    "1",
    "NX",
    "EX",
    String(ttlSec)
  ]);

  return result === "OK";
}

function readRawBody(req, maxBodyBytes = MAX_BODY_BYTES) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let totalBytes = 0;
    let settled = false;

    req.on("data", chunk => {
      if (settled) return;

      const buffer = Buffer.isBuffer(chunk)
        ? chunk
        : Buffer.from(chunk);
      totalBytes += buffer.length;

      if (totalBytes > maxBodyBytes) {
        settled = true;
        reject(new Error("payload_too_large"));
        return;
      }

      chunks.push(buffer);
    });

    req.on("end", () => {
      if (!settled) {
        settled = true;
        resolve(Buffer.concat(chunks, totalBytes));
      }
    });

    req.on("error", error => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    });
  });
}

module.exports = {
  claimChatwootWebhookReplay,
  loadWebhookSecurityConfig,
  readRawBody,
  safeEqualHex,
  verifyChatwootWebhook
};
