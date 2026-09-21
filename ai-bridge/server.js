const http = require("http");
const { safeFetch } = require("./safe-fetch.cjs");
const fs = require("fs");
const path = require("path");
const net = require("net");
const tls = require("tls");
const catalogRag = require("./catalog-rag.cjs");
const { createDirectProcessor } = require("./direct-channel.cjs");
const {
  claimChatwootWebhookReplay,
  loadWebhookSecurityConfig,
  readRawBody,
  verifyChatwootWebhook
} = require("./chatwoot-webhook-security.cjs");

const PORT = Number(process.env.PORT || 8787);

const CHATWOOT_BASE_URL =
  process.env.CHATWOOT_BASE_URL || "http://rails:3000";

const CHATWOOT_ACCOUNT_ID =
  process.env.CHATWOOT_ACCOUNT_ID || "1";

const CHATWOOT_API_TOKEN =
  process.env.CHATWOOT_API_TOKEN || "";

const OPENROUTER_API_KEY =
  process.env.OPENROUTER_API_KEY || "";

const OPENROUTER_MODEL =
  process.env.OPENROUTER_MODEL ||
  "google/gemini-2.5-flash-lite";

const SHOPIFY_SHOP =
  process.env.SHOPIFY_SHOP || "";

const SHOPIFY_CLIENT_ID =
  process.env.SHOPIFY_CLIENT_ID || "";

const SHOPIFY_CLIENT_SECRET =
  process.env.SHOPIFY_CLIENT_SECRET || "";

const SHOPIFY_API_VERSION =
  process.env.SHOPIFY_API_VERSION || "2026-07";

const SUPABASE_URL =
  process.env.SUPABASE_URL || "";

const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const AI_ENABLED =
  process.env.SKINPARA_AI_ENABLED === "true";

const SHOPIFY_ENABLED =
  process.env.SKINPARA_SHOPIFY_ENABLED === "true";

const SUPABASE_ENABLED =
  process.env.SKINPARA_SUPABASE_ENABLED === "true";

const REDIS_URL = process.env.REDIS_URL || "";

let REDIS_HOST =
  process.env.SKINPARA_REDIS_HOST || "redis";

let REDIS_PORT =
  Number(process.env.SKINPARA_REDIS_PORT || 6379);

let REDIS_USERNAME = "";
let REDIS_PASSWORD = "";
let REDIS_TLS_ENABLED =
  process.env.REDIS_TLS_ENABLED === "true";

if (REDIS_URL) {
  try {
    const parsedRedisUrl = new URL(REDIS_URL);

    REDIS_HOST = parsedRedisUrl.hostname || REDIS_HOST;
    REDIS_PORT = Number(parsedRedisUrl.port || REDIS_PORT);
    REDIS_USERNAME = decodeURIComponent(parsedRedisUrl.username || "");
    REDIS_PASSWORD = decodeURIComponent(parsedRedisUrl.password || "");
    REDIS_TLS_ENABLED =
      REDIS_TLS_ENABLED ||
      parsedRedisUrl.protocol === "rediss:";
  } catch {
    throw new Error("invalid_REDIS_URL");
  }
}

const QUEUE_NAME =
  process.env.SKINPARA_QUEUE_NAME ||
  "skinpara:ai:queue:v1";

const CHANNEL_MODE = process.env.SKINPARA_CHANNEL_MODE || "chatwoot";
const DIRECT_QUEUE_NAME = process.env.SKINPARA_DIRECT_INBOUND_QUEUE || "skinpara:cloud-lite:kapso-direct:inbound";
const DIRECT_PROCESSING_QUEUE = `${DIRECT_QUEUE_NAME}:processing`;
const DIRECT_OUTBOUND_QUEUE = process.env.SKINPARA_DIRECT_OUTBOUND_QUEUE || "skinpara:cloud-lite:kapso-direct:outbound";
const DIRECT_INTERNAL_TOKEN = process.env.SKINPARA_DIRECT_INTERNAL_TOKEN || "";

const WORKER_COUNT =
  Math.max(
    1,
    Number(process.env.SKINPARA_WORKERS || 10)
  );

const QUEUE_MAX_RETRIES =
  Math.max(
    0,
    Number(process.env.SKINPARA_QUEUE_RETRIES || 3)
  );

const CONVERSATION_LOCK_SECONDS =
  Math.max(
    10,
    Number(process.env.SKINPARA_CONVERSATION_LOCK_SECONDS || 60)
  );

const CHATWOOT_WEBHOOK_SECURITY =
  loadWebhookSecurityConfig();

const LOG_DIR = path.join(__dirname, "logs");

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

const processedMessages = new Map();

const queueMetrics = {
  activeJobs: 0,
  completed: 0,
  failed: 0,
  retries: 0,
  totalLatencyMs: 0,
  maxLatencyMs: 0
};

let shopifyTokenCache = {
  token: null,
  expiresAt: 0
};



// =========================================================
// REDIS QUEUE V1
// =========================================================

function redisEncode(args) {
  return (
    `*${args.length}\r\n` +
    args.map(value => {
      const s = String(value);
      return `$${Buffer.byteLength(s)}\r\n${s}\r\n`;
    }).join("")
  );
}

function parseRedisValue(buffer, offset = 0) {
  if (offset >= buffer.length) {
    return null;
  }

  const type = String.fromCharCode(buffer[offset]);

  const lineEnd = buffer.indexOf("\r\n", offset, "utf8");

  if (lineEnd < 0) {
    return null;
  }

  const line =
    buffer.toString("utf8", offset + 1, lineEnd);

  if (type === "+" || type === ":" || type === "-") {
    return {
      value:
        type === ":"
          ? Number(line)
          : line,
      next: lineEnd + 2,
      error: type === "-"
    };
  }

  if (type === "$") {
    const length = Number(line);

    if (length === -1) {
      return {
        value: null,
        next: lineEnd + 2
      };
    }

    const start = lineEnd + 2;
    const end = start + length;

    if (buffer.length < end + 2) {
      return null;
    }

    return {
      value: buffer.toString("utf8", start, end),
      next: end + 2
    };
  }

  if (type === "*") {
    const count = Number(line);

    if (count === -1) {
      return {
        value: null,
        next: lineEnd + 2
      };
    }

    let cursor = lineEnd + 2;
    const values = [];

    for (let i = 0; i < count; i++) {
      const parsed =
        parseRedisValue(buffer, cursor);

      if (!parsed) {
        return null;
      }

      if (parsed.error) {
        return parsed;
      }

      values.push(parsed.value);
      cursor = parsed.next;
    }

    return {
      value: values,
      next: cursor
    };
  }

  throw new Error(
    `redis_unknown_response:${type}`
  );
}

function redisCommand(args) {
  return new Promise((resolve, reject) => {
    const socketOptions = {
      host: REDIS_HOST,
      port: REDIS_PORT
    };

    const socket = REDIS_TLS_ENABLED
      ? tls.connect({
          ...socketOptions,
          servername: REDIS_HOST
        })
      : net.createConnection(socketOptions);

    let buffer = Buffer.alloc(0);

    const cleanup = () => {
      if (!socket.destroyed) {
        socket.destroy();
      }
    };

    socket.setTimeout(10000);

    const connectionEvent =
      REDIS_TLS_ENABLED ? "secureConnect" : "connect";

    socket.on(connectionEvent, () => {
      if (REDIS_PASSWORD) {
        const authArgs =
          REDIS_USERNAME
            ? ["AUTH", REDIS_USERNAME, REDIS_PASSWORD]
            : ["AUTH", REDIS_PASSWORD];

        socket.write(redisEncode(authArgs));
        return;
      }

      socket.write(redisEncode(args));
    });

    socket.on("data", chunk => {
      buffer = Buffer.concat([buffer, chunk]);

      let parsed;

      try {
        parsed =
          parseRedisValue(buffer, 0);
      } catch (e) {
        cleanup();
        reject(e);
        return;
      }

      if (!parsed) {
        return;
      }

      if (
        REDIS_PASSWORD &&
        !socket.__skinparaRedisAuthenticated
      ) {
        if (parsed.error) {
          cleanup();
          reject(
            new Error(`redis_auth_error:${parsed.value}`)
          );
          return;
        }

        socket.__skinparaRedisAuthenticated = true;
        buffer = buffer.subarray(parsed.next);
        socket.write(redisEncode(args));
        return;
      }

      cleanup();

      if (parsed.error) {
        reject(
          new Error(
            `redis_error:${parsed.value}`
          )
        );
        return;
      }

      resolve(parsed.value);
    });

    socket.on("timeout", () => {
      cleanup();
      reject(
        new Error("redis_timeout")
      );
    });

    socket.on("error", error => {
      cleanup();
      reject(error);
    });
  });
}

async function enqueueAIJob(
  payload,
  attempt = 0
) {

  // Foundation V2.1:
  // Record latest incoming message immediately when webhook arrives.
  // This prevents worker scheduling order from changing debounce order.
  try {

    const debounceConversationId =
      Number(
        payload?.conversation?.id ||
        payload?.conversation_id
      );

    const debounceMessageId =
      Number(
        payload?.id
      );

    if (
      attempt === 0 &&
      debounceConversationId &&
      debounceMessageId
    ) {
      await setLatestDebounceMessage(
        debounceConversationId,
        debounceMessageId
      );
    }

  } catch (error) {

    log(
      "debounce_enqueue_marker_error",
      {
        error:
          error.message
      }
    );
  }


  const job = {
    id:
      `${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}`,

    attempt,
    createdAt:
      new Date().toISOString(),

    payload
  };

  await redisCommand([
    "RPUSH",
    QUEUE_NAME,
    JSON.stringify(job)
  ]);

  log("queue_enqueued", {
    jobId: job.id,
    attempt,
    messageId: payload?.id || null,
    conversationId:
      payload?.conversation?.id ||
      payload?.conversation_id ||
      null
  });

  return job;
}

function shouldQueueWebhook(payload) {
  if (
    payload?.event !==
    "message_created"
  ) {
    return false;
  }

  if (
    payload?.message_type !==
      "incoming" &&
    payload?.message_type !== 0
  ) {
    return false;
  }

  if (payload?.private === true) {
    return false;
  }

  return true;
}

function queueEventKey(payload) {
  const conversationId =
    Number(
      payload?.conversation?.id ||
      payload?.conversation_id
    );

  const messageId =
    Number(payload?.id);

  return (
    conversationId &&
    messageId
  )
    ? `${conversationId}:${messageId}`
    : null;
}

async function acquireConversationLock(
  conversationId,
  workerId
) {
  if (!conversationId) {
    return true;
  }

  const result =
    await redisCommand([
      "SET",
      `skinpara:lock:conversation:${conversationId}`,
      workerId,
      "NX",
      "EX",
      String(
        CONVERSATION_LOCK_SECONDS
      )
    ]);

  return result === "OK";
}

async function releaseConversationLock(
  conversationId
) {
  if (!conversationId) {
    return;
  }

  try {
    await redisCommand([
      "DEL",
      `skinpara:lock:conversation:${conversationId}`
    ]);
  } catch (e) {
    log("queue_lock_release_error", {
      conversationId,
      error: e.message
    });
  }
}


// =========================================================
// SKINPARA FOUNDATION V2
// Reliable Queue + Durable Dedup + Debounce + Recovery
// =========================================================

function processingQueueName() {
  return `${QUEUE_NAME}:processing`;
}

function messageDoneKey(messageId) {
  return `skinpara:ai:done:${messageId}`;
}

function messageLockKey(messageId) {
  return `skinpara:ai:message-lock:${messageId}`;
}

function debounceKey(conversationId) {
  return `skinpara:ai:debounce:${conversationId}`;
}

function conversationV2LockKey(
  conversationId
) {
  return `skinpara:ai:conversation-v2:${conversationId}`;
}

function queueErrorIsPermanent(error) {
  const text =
    String(
      error?.message ||
      error ||
      ""
    );

  return (
    text.includes('"code":400') ||
    text.includes('code":400') ||
    text.includes("models' array") ||
    text.includes("Reasoning is mandatory") ||
    text.includes("401") ||
    text.includes("403") ||
    text.includes(
      "free-models-per-day"
    ) ||
    text.includes(
      "openrouter_free_tier_daily"
    ) ||
    text.includes(
      'X-RateLimit-Remaining'
    ) &&
    text.includes('"0"')
  );
}

async function ackReliableJob(rawJob) {
  if (!rawJob) return;

  await redisCommand([
    "LREM",
    processingQueueName(),
    "1",
    rawJob
  ]);
}

async function isMessageDone(
  messageId
) {
  if (!messageId) return false;

  const result =
    await redisCommand([
      "GET",
      messageDoneKey(messageId)
    ]);

  return result === "1";
}

async function markMessageDone(
  messageId
) {
  if (!messageId) return;

  await redisCommand([
    "SET",
    messageDoneKey(messageId),
    "1",
    "EX",
    "604800"
  ]);
}

async function acquireMessageLock(
  messageId,
  workerId
) {
  if (!messageId) return false;

  const result =
    await redisCommand([
      "SET",
      messageLockKey(messageId),
      workerId,
      "NX",
      "EX",
      "180"
    ]);

  return result === "OK";
}

async function releaseMessageLock(
  messageId
) {
  if (!messageId) return;

  await redisCommand([
    "DEL",
    messageLockKey(messageId)
  ]);
}


async function setLatestDebounceMessage(
  conversationId,
  messageId
) {

  const key =
    debounceKey(
      conversationId
    );

  const ttlSeconds =
    120;

  // Foundation V2.2
  //
  // Chatwoot webhooks can arrive out of order.
  //
  // Example:
  // message 197 webhook may arrive before 196.
  //
  // Therefore we NEVER store "last arrival".
  // We store the HIGHEST Chatwoot message ID.
  //
  // Redis Lua makes compare + set atomic.

  const lua = `
    local current =
      tonumber(
        redis.call(
          "GET",
          KEYS[1]
        ) or "0"
      )

    local incoming =
      tonumber(ARGV[1])

    local ttl =
      tonumber(ARGV[2])

    if incoming > current then

      redis.call(
        "SET",
        KEYS[1],
        ARGV[1],
        "EX",
        ttl
      )

      return incoming

    else

      if current > 0 then
        redis.call(
          "EXPIRE",
          KEYS[1],
          ttl
        )
      end

      return current
    end
  `;

  return await redisCommand([
    "EVAL",
    lua,
    "1",
    key,
    String(messageId),
    String(ttlSeconds)
  ]);
}


async function isLatestDebounceMessage(
  conversationId,
  messageId
) {
  const value =
    await redisCommand([
      "GET",
      debounceKey(conversationId)
    ]);

  return (
    String(value) ===
    String(messageId)
  );
}

async function acquireV2ConversationLock(
  conversationId,
  workerId
) {
  const lockKey =
    conversationV2LockKey(
      conversationId
    );

  const deadline =
    Date.now() +
    (
      Number(
        process.env
          .SKINPARA_CONVERSATION_LOCK_SECONDS ||
        60
      ) *
      1000
    );

  while (
    Date.now() <
    deadline
  ) {
    const result =
      await redisCommand([
        "SET",
        lockKey,
        workerId,
        "NX",
        "EX",
        String(
          Number(
            process.env
              .SKINPARA_CONVERSATION_LOCK_SECONDS ||
            60
          )
        )
      ]);

    if (result === "OK") {
      return true;
    }

    await sleep(250);
  }

  return false;
}

async function releaseV2ConversationLock(
  conversationId
) {
  await redisCommand([
    "DEL",
    conversationV2LockKey(
      conversationId
    )
  ]);
}

async function recoverProcessingQueue() {
  const processing =
    processingQueueName();

  let recovered = 0;

  while (true) {
    const raw =
      await redisCommand([
        "RPOPLPUSH",
        processing,
        QUEUE_NAME
      ]);

    if (!raw) break;

    recovered++;
  }

  if (recovered > 0) {
    log(
      "queue_recovery_complete",
      {
        recovered
      }
    );
  }

  return recovered;
}

async function processQueueJob(
  job,
  workerId,
  rawJob
) {
  const payload =
    job?.payload || {};

  const conversationId =
    Number(
      payload.conversation?.id ||
      payload.conversation_id
    );

  const messageId =
    Number(payload?.id);

  const attempt =
    Number(
      job?.attempt || 0
    );

  let conversationLocked =
    false;

  let messageLocked =
    false;

  let activeMetricAdded =
    false;

  let startedAt =
    Date.now();

  try {

    // -----------------------------------------------------
    // Durable dedupe
    // -----------------------------------------------------

    if (
      messageId &&
      await isMessageDone(messageId)
    ) {
      await ackReliableJob(rawJob);

      log(
        "queue_duplicate_durable",
        {
          workerId,
          conversationId,
          messageId
        }
      );

      return;
    }


    // -----------------------------------------------------
    // Debounce
    // Customer may send:
    // salam
    // bghit cleanser
    // bachra oily
    //
    // We only answer the latest message.
    // -----------------------------------------------------

    if (
      attempt === 0 &&
      conversationId &&
      messageId
    ) {

      const debounceMs =
        Math.max(
          500,
          Number(
            process.env
              .SKINPARA_DEBOUNCE_MS ||
            1600
          )
        );

      await sleep(
        debounceMs
      );

      const latest =
        await isLatestDebounceMessage(
          conversationId,
          messageId
        );

      if (!latest) {

        await markMessageDone(
          messageId
        );

        await ackReliableJob(
          rawJob
        );

        log(
          "queue_debounced",
          {
            workerId,
            conversationId,
            messageId
          }
        );

        return;
      }
    }


    // -----------------------------------------------------
    // Message lock
    // Prevent same Chatwoot webhook from replying twice.
    // -----------------------------------------------------

    if (messageId) {

      messageLocked =
        await acquireMessageLock(
          messageId,
          workerId
        );

      if (!messageLocked) {

        if (
          await isMessageDone(
            messageId
          )
        ) {
          await ackReliableJob(
            rawJob
          );

          return;
        }

        log(
          "queue_duplicate_inflight",
          {
            workerId,
            conversationId,
            messageId
          }
        );

        await ackReliableJob(
          rawJob
        );

        return;
      }
    }


    // -----------------------------------------------------
    // One AI response at a time per conversation
    // -----------------------------------------------------

    conversationLocked =
      await acquireV2ConversationLock(
        conversationId,
        workerId
      );

    if (!conversationLocked) {
      throw new Error(
        "conversation_lock_timeout"
      );
    }


    if (
      typeof queueMetrics !==
      "undefined"
    ) {
      queueMetrics.activeJobs++;
      activeMetricAdded = true;
    }


    log(
      "queue_processing",
      {
        workerId,
        jobId: job.id,
        attempt,
        conversationId,
        messageId
      }
    );


    // -----------------------------------------------------
    // Existing SkinPara business processor
    // -----------------------------------------------------

    const result =
      await processIncomingMessage(
        payload
      );


    // -----------------------------------------------------
    // SUCCESS
    // -----------------------------------------------------

    if (messageId) {
      await markMessageDone(
        messageId
      );
    }

    await ackReliableJob(
      rawJob
    );

    const latency =
      Date.now() -
      startedAt;

    if (
      typeof queueMetrics !==
      "undefined"
    ) {
      queueMetrics.completed++;
      queueMetrics.totalLatencyMs +=
        latency;

      queueMetrics.maxLatencyMs =
        Math.max(
          queueMetrics.maxLatencyMs,
          latency
        );
    }

    log(
      "queue_processed",
      {
        workerId,
        jobId: job.id,
        latencyMs: latency,
        result
      }
    );

  } catch (error) {

    log(
      "queue_job_error",
      {
        workerId,
        jobId: job?.id,
        attempt,
        conversationId,
        messageId,
        error:
          error.message
      }
    );


    // Remove old in-memory dedupe marker before retry.
    // Current processIncomingMessage uses this Map.
    try {
      if (
        conversationId &&
        messageId &&
        typeof processedMessages !==
          "undefined"
      ) {
        processedMessages.delete(
          `${conversationId}:${messageId}`
        );
      }
    } catch (_) {}


    const permanent =
      queueErrorIsPermanent(
        error
      );


    // -----------------------------------------------------
    // RETRY
    // -----------------------------------------------------

    if (
      !permanent &&
      attempt <
      QUEUE_MAX_RETRIES
    ) {

      const delay =
        Math.min(
          8000,
          1000 *
          Math.pow(
            2,
            attempt
          )
        );

      await sleep(delay);

      await enqueueAIJob(
        payload,
        attempt + 1
      );

      await ackReliableJob(
        rawJob
      );

      if (
        typeof queueMetrics !==
        "undefined"
      ) {
        queueMetrics.retries++;
      }

      log(
        "queue_retry_scheduled",
        {
          workerId,
          jobId: job.id,
          nextAttempt:
            attempt + 1,
          delay
        }
      );

    } else {

      // ---------------------------------------------------
      // FINAL FAILURE
      // ---------------------------------------------------

      await ackReliableJob(
        rawJob
      );

      if (messageId) {
        await markMessageDone(
          messageId
        );
      }

      if (
        typeof queueMetrics !==
        "undefined"
      ) {
        queueMetrics.failed++;
      }

      log(
        "queue_job_failed",
        {
          workerId,
          jobId: job.id,
          attempts:
            attempt + 1,
          conversationId,
          messageId,
          permanent
        }
      );


      // ---------------------------------------------------
      // Human fallback
      // ---------------------------------------------------

      try {

        if (
          conversationId &&
          typeof handleHumanHandoff ===
            "function"
        ) {

          await handleHumanHandoff(
            conversationId
          );

          log(
            "queue_human_fallback_sent",
            {
              conversationId,
              messageId
            }
          );
        }

      } catch (
        fallbackError
      ) {

        log(
          "queue_human_fallback_failed",
          {
            conversationId,
            error:
              fallbackError.message
          }
        );
      }
    }

  } finally {

    if (
      activeMetricAdded &&
      typeof queueMetrics !==
        "undefined" &&
      queueMetrics.activeJobs > 0
    ) {
      queueMetrics.activeJobs--;
    }

    if (conversationLocked) {
      try {
        await releaseV2ConversationLock(
          conversationId
        );
      } catch (_) {}
    }

    if (messageLocked) {
      try {
        await releaseMessageLock(
          messageId
        );
      } catch (_) {}
    }
  }
}


async function queueWorker(
  workerNumber
) {

  const workerId =
    `worker-${workerNumber}`;

  const processing =
    processingQueueName();

  log(
    "queue_worker_started",
    {
      workerId
    }
  );

  while (true) {

    let rawJob = null;

    try {

      // Reliable queue:
      //
      // QUEUE_NAME
      //     ? atomic move
      // processing queue
      //
      // Job stays in processing until ACK.

      rawJob =
        await redisCommand([
          "BRPOPLPUSH",
          QUEUE_NAME,
          processing,
          "5"
        ]);

      if (!rawJob) {
        continue;
      }

      let job;

      try {

        job =
          JSON.parse(
            rawJob
          );

      } catch (error) {

        log(
          "queue_invalid_job",
          {
            workerId,
            error:
              error.message
          }
        );

        await ackReliableJob(
          rawJob
        );

        continue;
      }

      await processQueueJob(
        job,
        workerId,
        rawJob
      );

    } catch (error) {

      // IMPORTANT:
      // If Node/container crashes,
      // rawJob remains inside processing queue.
      // Startup recovery returns it to QUEUE_NAME.

      log(
        "queue_worker_error",
        {
          workerId,
          error:
            error.message
        }
      );

      await sleep(
        1500
      );
    }
  }
}


async function startQueueWorkers() {

  try {

    const recovered =
      await recoverProcessingQueue();

    log(
      "queue_recovery_checked",
      {
        recovered
      }
    );

  } catch (error) {

    log(
      "queue_recovery_error",
      {
        error:
          error.message
      }
    );
  }


  for (
    let i = 1;
    i <= WORKER_COUNT;
    i++
  ) {

    queueWorker(i).catch(
      error => {

        log(
          "queue_worker_fatal",
          {
            workerId:
              `worker-${i}`,

            error:
              error.message
          }
        );
      }
    );
  }


  log(
    "queue_started",
    {
      queue:
        QUEUE_NAME,

      processing_queue:
        processingQueueName(),

      reliable:
        true,

      durable_dedup:
        true,

      debounce_ms:
        Number(
          process.env
            .SKINPARA_DEBOUNCE_MS ||
          1600
        ),

      workers:
        WORKER_COUNT,

      retries:
        QUEUE_MAX_RETRIES,

      redis:
        `${REDIS_HOST}:${REDIS_PORT}`
    }
  );
}




// =========================================================
// QUEUE STATUS - FOUNDATION V2
// =========================================================

async function getQueueStatus() {

  let redisOk = false;
  let queueLength = null;
  let processingLength = null;

  try {

    const ping =
      await redisCommand([
        "PING"
      ]);

    redisOk =
      ping === "PONG";

    queueLength =
      Number(
        await redisCommand([
          "LLEN",
          QUEUE_NAME
        ])
      );

    processingLength =
      Number(
        await redisCommand([
          "LLEN",
          processingQueueName()
        ])
      );

  } catch (error) {

    log(
      "queue_status_redis_error",
      {
        error:
          error.message
      }
    );
  }


  const completed =
    Number(
      queueMetrics?.completed || 0
    );

  const totalLatency =
    Number(
      queueMetrics?.totalLatencyMs || 0
    );


  return {

    ok:
      redisOk,

    redis: {
      ok:
        redisOk,

      host:
        REDIS_HOST,

      port:
        REDIS_PORT
    },


    queue: {
      name:
        QUEUE_NAME,

      length:
        queueLength,

      processing_name:
        processingQueueName(),

      processing_length:
        processingLength,

      total_pending:
        (
          Number(queueLength || 0) +
          Number(processingLength || 0)
        )
    },


    workers: {
      configured:
        WORKER_COUNT,

      active:
        Number(
          queueMetrics?.activeJobs || 0
        )
    },


    metrics: {
      completed:
        completed,

      failed:
        Number(
          queueMetrics?.failed || 0
        ),

      retries:
        Number(
          queueMetrics?.retries || 0
        ),

      average_latency_ms:
        completed > 0
          ? Math.round(
              totalLatency /
              completed
            )
          : 0,

      max_latency_ms:
        Number(
          queueMetrics?.maxLatencyMs || 0
        )
    },


    retries: {
      max:
        QUEUE_MAX_RETRIES
    },


    foundation_v2: {
      reliable_queue:
        true,

      durable_dedup:
        true,

      debounce:
        true,

      debounce_ms:
        Number(
          process.env
            .SKINPARA_DEBOUNCE_MS ||
          1600
        ),

      crash_recovery:
        true,

      processing_queue:
        processingQueueName()
    },


    conversation_lock_seconds:
      Number(
        process.env
          .SKINPARA_CONVERSATION_LOCK_SECONDS ||
        60
      ),


    ai: {
      enabled:
        AI_ENABLED,

      routing:
        "single_model",

      configured_model:
        OPENROUTER_MODEL,

      models: [
        OPENROUTER_MODEL
      ]
    }

  };
}


// =========================================================
// UTILITIES
// =========================================================

function log(message, data = null) {
  const line =
    `[${new Date().toISOString()}] ${message}` +
    (data ? ` ${JSON.stringify(data)}` : "");

  console.log(line);

  fs.appendFileSync(
    path.join(LOG_DIR, "events.log"),
    line + "\n",
    "utf8"
  );
}

function sendJson(res, status, payload) {
  res.writeHead(status, {
    "Content-Type": "application/json"
  });

  res.end(JSON.stringify(payload, null, 2));
}

function readBody(req) {
  return readRawBody(req, 5_000_000)
    .then(body => body.toString("utf8"));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function cleanText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function configured(value) {
  return Boolean(
    value &&
    !value.includes("CHANGE_ME") &&
    !value.includes("PUT_") &&
    !value.includes("YOUR_")
  );
}


function isHumanRequest(text) {

  const t =
    String(text || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();

  const directTerms = [
    "human",
    "agent",
    "person",
    "personne",
    "conseiller",
    "responsable",

    "bghit nhder m3a chi wahed",
    "bghit nhdar m3a chi wahed",
    "bghit ???? m3a chi wahed",
    "bghit nhedr m3a chi wahed",
    "bghit nhder m3a wahed",
    "bghit nhdar m3a wahed",

    "bghit chi wahed",
    "baghi nhder m3a chi wahed",
    "bagha nhder m3a chi wahed",
    "wach kayn chi wahed",
    "wach kayn chi wahd",
    "3ayet lia chi wahed",
    "3ayet lia responsable",

    "?????",
    "????",
    "?????",
    "?????",
    "???? ???? ?? ?? ????",
    "???? ???? ?? ?? ????",
    "???? ????? ?? ?? ????",
    "???? ?? ????",
    "???? ???????"
  ];

  if (
    directTerms.some(
      term =>
        t.includes(
          term.toLowerCase()
        )
    )
  ) {
    return true;
  }


  // Flexible Darija fallback
  const wantsTalk =
    t.includes("bghit") ||
    t.includes("baghi") ||
    t.includes("bagha");

  const talkWords =
    t.includes("nhder") ||
    t.includes("nhdar") ||
    t.includes("nhedr") ||
    t.includes("????") ||
    t.includes("????");

  const humanWords =
    t.includes("chi wahed") ||
    t.includes("chi wahd") ||
    t.includes("responsable") ||
    t.includes("agent");

  return (
    wantsTalk &&
    talkWords &&
    humanWords
  );
}

function meaningfulSearchTerms(text) {
  const stop = new Set([
    "ana","bghit","baghi","bagha","wach","hada","hadi",
    "chi","lia","liya","dial","dyal","mn","min","fi","f",
    "pour","avec","une","des","les","le","la","un",
    "the","and","for","with","this","that",
    "???","????","???","????","??","??","???","??"
  ]);

  return cleanText(text)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s-]/gu, " ")
    .split(/\s+/)
    .filter(x => x.length >= 3 && !stop.has(x))
    .slice(0, 6);
}


// =========================================================
// CHATWOOT
// =========================================================

async function chatwootFetch(endpoint, options = {}) {
  const url =
    `${CHATWOOT_BASE_URL}/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}${endpoint}`;

  const response = await safeFetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "api_access_token": CHATWOOT_API_TOKEN,
      ...(options.headers || {})
    }
  });

  const text = await response.text();

  let data;

  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!response.ok) {
    throw new Error(
      `chatwoot_${response.status}: ${JSON.stringify(data)}`
    );
  }

  return data;
}

async function getConversationMessages(conversationId) {
  const data = await chatwootFetch(
    `/conversations/${conversationId}/messages`
  );

  const payload =
    Array.isArray(data.payload)
      ? data.payload
      : [];

  return payload
    .filter(m => !m.private)
    .slice(-12)
    .map(m => ({
      role:
        m.message_type === 0 ||
        m.message_type === "incoming"
          ? "user"
          : "assistant",

      content: cleanText(m.content)
    }))
    .filter(m => m.content);
}

async function sendChatwootMessage(
  conversationId,
  content
) {
  const jsonBody = JSON.stringify({
    content,
    message_type: "outgoing",
    private: false,
    content_type: "text",
    content_attributes: {
      skinpara_ai: true
    }
  });

  return chatwootFetch(
    `/conversations/${conversationId}/messages`,
    {
      method: "POST",
      body: Buffer.from(jsonBody, "utf-8")
    }
  );
}

async function getConversationLabels(conversationId) {
  const data = await chatwootFetch(
    `/conversations/${conversationId}/labels`
  );

  return Array.isArray(data.payload)
    ? data.payload
    : [];
}

async function setConversationLabels(
  conversationId,
  labels
) {
  return chatwootFetch(
    `/conversations/${conversationId}/labels`,
    {
      method: "POST",
      body: JSON.stringify({
        labels
      })
    }
  );
}

async function addConversationLabel(
  conversationId,
  label
) {
  try {
    const existing =
      await getConversationLabels(conversationId);

    if (!existing.includes(label)) {
      await setConversationLabels(
        conversationId,
        [...existing, label]
      );
    }
  } catch (e) {
    log("label_update_failed", {
      conversationId,
      label,
      error: e.message
    });
  }
}


// =========================================================
// SHOPIFY AUTH
// =========================================================

async function getShopifyAccessToken() {
  const now = Date.now();

  if (
    shopifyTokenCache.token &&
    shopifyTokenCache.expiresAt > now + 60_000
  ) {
    return shopifyTokenCache.token;
  }

  if (
    !configured(SHOPIFY_SHOP) ||
    !configured(SHOPIFY_CLIENT_ID) ||
    !configured(SHOPIFY_CLIENT_SECRET)
  ) {
    throw new Error("shopify_credentials_missing");
  }

  const body = new URLSearchParams();

  body.set("grant_type", "client_credentials");
  body.set("client_id", SHOPIFY_CLIENT_ID);
  body.set("client_secret", SHOPIFY_CLIENT_SECRET);

  const response = await safeFetch(
    `https://${SHOPIFY_SHOP}.myshopify.com/admin/oauth/access_token`,
    {
      method: "POST",
      headers: {
        "Content-Type":
          "application/x-www-form-urlencoded"
      },
      body
    }
  );

  const data = await response.json();

  if (!response.ok || !data.access_token) {
    throw new Error(
      `shopify_token_failed: ${JSON.stringify(data)}`
    );
  }

  const expiresIn =
    Number(data.expires_in || 86399);

  shopifyTokenCache = {
    token: data.access_token,
    expiresAt:
      Date.now() + expiresIn * 1000
  };

  return data.access_token;
}

async function shopifyGraphQL(query, variables = {}) {
  const token =
    await getShopifyAccessToken();

  const response = await safeFetch(
    `https://${SHOPIFY_SHOP}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token
      },
      body: JSON.stringify({
        query,
        variables
      })
    }
  );

  const data = await response.json();

  if (
    !response.ok ||
    data.errors
  ) {
    throw new Error(
      `shopify_graphql_failed: ${JSON.stringify(data.errors || data)}`
    );
  }

  return data.data;
}

async function searchProducts(text) {
  if (!SHOPIFY_ENABLED) return [];

  const terms =
    meaningfulSearchTerms(text);

  if (!terms.length) return [];

  const queryString =
    terms
      .map(term =>
        `(title:*${term}* OR vendor:*${term}* OR product_type:*${term}*)`
      )
      .join(" OR ");

  const query = `
    query SearchProducts($query: String!) {
      products(
        first: 3,
        query: $query,
        sortKey: RELEVANCE
      ) {
        nodes {
          id
          title
          handle
          vendor
          productType
          status
          description
          featuredMedia {
            preview {
              image {
                url
              }
            }
          }
          variants(first: 1) {
            nodes {
              id
              title
              sku
              price
              inventoryQuantity
            }
          }
        }
      }
    }
  `;

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const data =
        await shopifyGraphQL(
          query,
          { query: queryString }
        );

      return (
        data.products?.nodes || []
      ).map(product => ({
        id: product.id,
        title: product.title,
        handle: product.handle,
        vendor: product.vendor,
        productType: product.productType,
        description:
          cleanText(product.description)
            .slice(0, 280),

        image:
          product.featuredMedia
            ?.preview
            ?.image
            ?.url || null,

        variants:
          (product.variants?.nodes || [])
            .map(v => ({
              id: v.id,
              title: v.title,
              sku: v.sku,
              price: v.price,
              inventoryQuantity:
                v.inventoryQuantity
            }))
      }));

    } catch (e) {

      log("shopify_read_error", {
        attempt,
        error: e.message
      });

      if (attempt < 2) {
        const delay =
          attempt === 1
            ? 500
            : attempt === 2
              ? 1000
              : 2000;

        log("shopify_retry", {
          attempt,
          nextAttempt: attempt + 1,
          delay
        });

        await sleep(delay);
        continue;
      }

      log("product_search_failed", {
        error: e.message,
        queryString
      });

      return [];
    }
  }

  return [];
}


// =========================================================
// SUPABASE
// =========================================================

async function supabaseFetch(
  endpoint,
  options = {}
) {
  if (!SUPABASE_ENABLED) {
    throw new Error("supabase_disabled");
  }

  const response = await safeFetch(
    `${SUPABASE_URL}/rest/v1${endpoint}`,
    {
      ...options,
      headers: {
        "Content-Type": "application/json",
        "apikey":
          SUPABASE_SERVICE_ROLE_KEY,

        "Authorization":
          `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,

        ...(options.headers || {})
      }
    }
  );

  const text =
    await response.text();

  let data;

  try {
    data =
      text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!response.ok) {
    throw new Error(
      `supabase_${response.status}: ${text}`
    );
  }

  return data;
}

async function recordLifetimeOrderEvent(
  eventType,
  state,
  conversation = null
) {
  if (!SUPABASE_ENABLED || !state) {
    return null;
  }

  const conversationId =
    Number(state.conversation_id || conversation?.id || 0);

  try {
    const currentConversation =
      conversation ||
      (conversationId
        ? await getConversationForOrder(conversationId)
        : null);

    const contact =
      currentConversation?.meta?.sender ||
      currentConversation?.contact ||
      {};

    const contactId =
      Number(contact.id || currentConversation?.contact_id || 0);

    if (!contactId || !conversationId) {
      throw new Error("lifetime_event_identity_missing");
    }

    const externalOrderId =
      cleanText(state.shopify_order_id || "");

    const orderKey =
      externalOrderId
        ? `order:${externalOrderId}`
        : `conversation:${conversationId}`;

    const normalizedEventType =
      String(eventType).replaceAll("-", "_");

    const result = await supabaseFetch(
      "/rpc/skinpara_record_order_event",
      {
        method: "POST",
        headers: {
          "Prefer": "return=representation"
        },
        body: JSON.stringify({
          p_event_key:
            `${orderKey}:${normalizedEventType}`,
          p_order_key:
            orderKey,
          p_event_type:
            normalizedEventType,
          p_contact_id:
            contactId,
          p_account_id:
            Number(CHATWOOT_ACCOUNT_ID),
          p_conversation_id:
            conversationId,
          p_customer_name:
            state.customer_name || contact.name || null,
          p_phone_number:
            state.phone || contact.phone_number || null,
          p_external_order_id:
            externalOrderId || null,
          p_order_name:
            state.shopify_order_name || null,
          p_product_name:
            state.product_title || state.last_product || null,
          p_order_value:
            Number(state.order_total || 0),
          p_test_mode:
            state.order_test_mode !== false,
          p_event_at:
            new Date().toISOString(),
          p_metadata: {
            source: "skinpara_bridge",
            order_mode: ORDER_MODE
          }
        })
      }
    );

    log("customer_lifetime_event", {
      conversation_id: conversationId,
      contact_id: contactId,
      event_type: normalizedEventType,
      duplicate: Boolean(result?.duplicate)
    });

    await recordAutomationEvent({
      eventType: normalizedEventType,
      eventKey:
        `lifecycle:${orderKey}:${normalizedEventType}`,
      sourceEventId:
        `${orderKey}:${normalizedEventType}`,
      contactId,
      conversationId,
      orderId: externalOrderId || orderKey,
      productName:
        state.product_title || state.last_product || null,
      payload: {
        order_mode: ORDER_MODE,
        test_mode: state.order_test_mode !== false
      }
    });

    // Referral rewards are evaluated only from the durable delivered event.
    // The database RPC is transactional, test-mode gated and idempotent.
    if (normalizedEventType === "delivered") {
      try {
        const loyalty = await supabaseFetch(
          "/rpc/skinpara_process_referral_delivery",
          {
            method: "POST",
            headers: { "Prefer": "return=representation" },
            body: JSON.stringify({
              p_event_key: `${orderKey}:${normalizedEventType}`,
              p_order_key: orderKey
            })
          }
        );
        log("loyalty_delivery_evaluated", {
          contact_id: contactId,
          order_key: orderKey,
          qualified: Boolean(loyalty?.qualified),
          duplicate: Boolean(loyalty?.duplicate),
          reason: loyalty?.reason || null
        });
      } catch (loyaltyError) {
        log("loyalty_delivery_evaluation_error", {
          contact_id: contactId,
          order_key: orderKey,
          error: loyaltyError.message
        });
      }
    }

    return result;
  } catch (error) {
    log("customer_lifetime_event_error", {
      conversation_id: conversationId || null,
      event_type: eventType,
      error: error.message
    });

    return null;
  }
}

async function recordAutomationEvent({
  eventType,
  eventKey,
  sourceEventId = null,
  contactId = null,
  conversationId = null,
  orderId = null,
  productName = null,
  payload = {}
}) {
  if (!SUPABASE_ENABLED) return null;
  try {
    const rows = await supabaseFetch(
      "/skinpara_automation_events?on_conflict=event_key",
      {
        method: "POST",
        headers: {
          "Prefer":
            "resolution=ignore-duplicates,return=representation"
        },
        body: JSON.stringify({
          event_key: String(eventKey),
          event_type:
            String(eventType).replaceAll("-", "_"),
          source_event_id:
            sourceEventId ? String(sourceEventId) : null,
          contact_id:
            contactId ? Number(contactId) : null,
          conversation_id:
            conversationId ? Number(conversationId) : null,
          order_id:
            orderId ? String(orderId) : null,
          product_name:
            productName || null,
          payload,
          event_at:
            new Date().toISOString()
        })
      }
    );
    return Array.isArray(rows) ? rows[0] || null : null;
  } catch (error) {
    log("automation_event_write_skipped", {
      event_type: eventType,
      conversation_id: conversationId || null,
      error: error.message
    });
    return null;
  }
}

async function getCustomerMemory(contactId) {
  if (
    !SUPABASE_ENABLED ||
    !contactId
  ) {
    return null;
  }

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const data = await supabaseFetch(
        `/skinpara_customer_memory?contact_id=eq.${contactId}&limit=1`
      );

      return Array.isArray(data)
        ? data[0] || null
        : null;

    } catch (e) {

      log("supabase_read_error", {
        attempt,
        contactId,
        error: e.message
      });

      if (attempt < 3) {
        const delay =
          attempt === 1
            ? 500
            : attempt === 2
              ? 1000
              : 2000;

        log("supabase_retry", {
          attempt,
          nextAttempt: attempt + 1,
          delay
        });

        await sleep(delay);
        continue;
      }

      log("supabase_memory_read_skipped", {
        error: e.message
      });

      return null;
    }
  }

  return null;
}

async function upsertCustomerMemory(
  contact,
  summary,
  products
) {
  if (
    !SUPABASE_ENABLED ||
    !contact?.id
  ) {
    return;
  }

  try {
    await supabaseFetch(
      "/skinpara_customer_memory?on_conflict=contact_id",
      {
        method: "POST",

        headers: {
          "Prefer":
            "resolution=merge-duplicates,return=minimal"
        },

        body: JSON.stringify({
          contact_id: contact.id,
          account_id:
            Number(CHATWOOT_ACCOUNT_ID),

          customer_name:
            contact.name || null,

          phone_number:
            contact.phone_number || null,

          last_summary:
            summary || null,

          last_products:
            products || [],

          last_seen_at:
            new Date().toISOString(),

          updated_at:
            new Date().toISOString()
        })
      }
    );

  } catch (e) {
    log("supabase_memory_write_skipped", {
      error: e.message
    });
  }
}

async function saveAIEvent({
  eventKey,
  conversationId,
  messageId,
  contactId,
  userMessage,
  assistantMessage,
  products
}) {
  if (!SUPABASE_ENABLED) return;

  try {
    await supabaseFetch(
      "/skinpara_ai_events",
      {
        method: "POST",

        headers: {
          "Prefer":
            "return=minimal"
        },

        body: JSON.stringify({
          event_key: eventKey,
          account_id:
            Number(CHATWOOT_ACCOUNT_ID),

          conversation_id:
            conversationId,

          message_id:
            messageId,

          contact_id:
            contactId,

          user_message:
            userMessage,

          assistant_message:
            assistantMessage,

          products:
            products || [],

          model:
            OPENROUTER_MODEL
        })
      }
    );

  } catch (e) {
    log("supabase_event_write_skipped", {
      error: e.message
    });
  }
}


// =========================================================
// OPENROUTER
// =========================================================

function productContext(products) {
  if (!products.length) {
    return "No matching Shopify products were found.";
  }

  return products.map((p, index) => {
    const variants =
      p.variants.map(v =>
        [
          `variant=${v.title}`,
          `price=${v.price} MAD`,
          v.sku ? `sku=${v.sku}` : ""
        ]
        .filter(Boolean)
        .join(", ")
      ).join(" | ");

    return [
      `${index + 1}. ${p.title}`,
      `Brand: ${p.vendor || "Unknown"}`,
      `Type: ${p.productType || "Unknown"}`,
      `Description: ${p.description || "No description"}`,
      `Variants: ${variants || "No variants"}`
    ].join("\n");
  }).join("\n\n");
}

// =========================================================
// STOCK OUTPUT SAFETY GUARD
// =========================================================

/**
 * validateCustomerAIResponse
 * Rejects AI output that contains:
 *  1. Reasoning/chain-of-thought leakage
 *  2. Raw inventory field values (inventory=N, inventoryQuantity:N, stock_count:N)
 *  3. Definitive out-of-stock / in-stock claims not from confirmed business state
 *  4. Responses > 1200 chars
 */
function validateCustomerAIResponse(value) {
  const text = cleanText(value);
  if (!text) return { ok: false, reason: "empty" };
  // The model may occasionally over-explain. Length is a presentation issue,
  // not a trust/safety failure: trim later instead of discarding an otherwise
  // valid answer and falling back to a generic message.
  if (text.length > 4000) return { ok: false, reason: "too_long" };

  // 1. Reasoning leakage
  const reasoningLeak =
    /(?:here(?:'s| is) (?:a |the )?thinking process|analy[sz]e user input|check rules\/constraints|determine response strategy|step[- ]by[- ]step reasoning|system prompt|<think>|<\/think>|let me (?:unpack|analy[sz]e|reason|think)|checking history|critical realization|important context shift|need to (?:confirm|clarify|determine)|the customer is|based on their messages|hmm\.{0,3})/i;
  if (reasoningLeak.test(text)) {
    return { ok: false, reason: "reasoning_leak" };
  }

  // Reject explicit titles/kinship terms that assume gender. Do not reject ordinary
  // Arabic verb forms: many valid Darija/Arabic replies use them naturally.
  if (/\b(?:madame|monsieur|mademoiselle|sir|ma'am|sister|brother)\b|(?:سيدتي|سيدي|أختي|أخي)/i.test(text)) {
    return { ok: false, reason: "unsupported_gender_assumption" };
  }

  // 2. Raw inventory value leakage
  // Catches: "inventory=0", "inventoryQuantity: 5", "stock_count:0"
  if (/\b(?:inventory(?:Quantity)?|stock[_\-]count|stock[_\-]quantity)\s*[=:]\s*\d+/i.test(text)) {
    return { ok: false, reason: "inventory_leak" };
  }
  if (/\binventory\s*=\s*\d+/i.test(text)) {
    return { ok: false, reason: "inventory_leak" };
  }

  // 3. Definitive stock conclusions without business confirmation
  // Allowed: hedged language ("availability will be confirmed", "ghadi nwakked", "nkhedm nshuf")
  // Rejected: definitive assertions like "out of stock", "rupture de stock", "not available"
  const definitiveStockClaim =
    /(?:\bout[\s\-]of[\s\-]stock\b|\brupture\s+de\s+stock\b|\bhors[\s\-]stock\b|\bnot\s+(?:in\s+)?stock\b|\bnot\s+available\b|\bnon\s+disponible\b|\bhors\s+de\s+stock\b|\bin\s+stock\b|\ben\s+stock\b|\bdisponible(?:s)?\b|\bavailable\b|\bkayn(?:a)?\b)/i;
  if (definitiveStockClaim.test(text)) {
    return { ok: false, reason: "unsupported_stock_claim" };
  }

  if (/(?:same|identical|exactly the same)\s+formula|(?:m[eê]me|identique|exactement la m[eê]me)\s+formule|نفس التركيبة/i.test(text)) {
    return { ok: false, reason: "unsupported_formula_claim" };
  }

  return { ok: true, text };
}

const directStore = {
  async insertMessage(row) {
    const result = await supabaseFetch("/skinpara_channel_messages?on_conflict=event_key", {
      method: "POST",
      headers: { "Prefer": "resolution=ignore-duplicates,return=representation" },
      body: JSON.stringify(row)
    });
    return Array.isArray(result) && result.length > 0;
  },
  async getHistory(conversationKey) {
    const rows = await supabaseFetch(`/skinpara_channel_messages?conversation_key=eq.${encodeURIComponent(conversationKey)}&select=role,content,created_at,id&order=created_at.desc,id.desc&limit=20`);
    // PostgREST applies LIMIT before ordering the returned window. Fetch newest
    // messages first so long-lived WhatsApp conversations do not get stuck on
    // their oldest 20 rows, then restore chronological order for the LLM.
    return Array.isArray(rows) ? rows.reverse() : [];
  },
  async hasEvent(eventKey) {
    const rows = await supabaseFetch(`/skinpara_channel_messages?event_key=eq.${encodeURIComponent(eventKey)}&select=event_key&limit=1`);
    return Array.isArray(rows) && rows.length > 0;
  },
  async markHandoff(eventKey, reason) {
    await supabaseFetch(`/skinpara_channel_messages?event_key=eq.${encodeURIComponent(eventKey)}`, {
      method: "PATCH",
      headers: { "Prefer": "return=minimal" },
      body: JSON.stringify({ handoff_required: true, handoff_reason: reason, updated_at: new Date().toISOString() })
    });
  }
};

const processDirectMessage = createDirectProcessor({
  store: directStore,
  enqueueOutbound: async event => redisCommand(["RPUSH", DIRECT_OUTBOUND_QUEUE, JSON.stringify(event)]),
  callAdvisor: callAI,
  searchCatalog: async text => {
    const result = await catalogRag.searchCatalog(text).catch(error => ({ ok: false, fallback: true, products: [], reason: error?.message || "catalog_search_error" }));
    return { ...result, context: catalogRag.buildCatalogContext(result) };
  },
  searchProducts
});

async function enqueueDirectJob(payload) {
  const job = { id: `direct-${Date.now()}-${Math.random().toString(36).slice(2)}`, attempt: 0, createdAt: new Date().toISOString(), payload };
  await redisCommand(["RPUSH", DIRECT_QUEUE_NAME, JSON.stringify(job)]);
  return job;
}

async function directQueueWorker() {
  log("direct_queue_worker_started", { queue: DIRECT_QUEUE_NAME });
  while (true) {
    let rawJob = null;
    try {
      rawJob = await redisCommand(["LMOVE", DIRECT_QUEUE_NAME, DIRECT_PROCESSING_QUEUE, "LEFT", "RIGHT"]);
      if (!rawJob) { await sleep(500); continue; }
      const job = JSON.parse(rawJob);
      const result = await processDirectMessage(job.payload);
      await redisCommand(["LREM", DIRECT_PROCESSING_QUEUE, "1", rawJob]);
      log("direct_queue_processed", { jobId: job.id, result });
    } catch (error) {
      log("direct_queue_error", { error: error.message });
      if (rawJob) {
        try {
          const job = JSON.parse(rawJob);
          await redisCommand(["LREM", DIRECT_PROCESSING_QUEUE, "1", rawJob]);
          if (Number(job.attempt || 0) < QUEUE_MAX_RETRIES) {
            job.attempt = Number(job.attempt || 0) + 1;
            await redisCommand(["LPUSH", DIRECT_QUEUE_NAME, JSON.stringify(job)]);
          }
        } catch (_) {}
      }
      await sleep(1000);
    }
  }
}

/**
 * sanitizeCustomerResponse — last-mile deterministic max-3-products enforcer.
 * productContext() already slices to 3 products in the prompt, but this is a
 * hard safety net — if the model enumerates a 4th product from training data,
 * we truncate before sending to the customer.
 */
function sanitizeCustomerResponse(text, contextStr = "") {
  let sanitized = text;
  const fourthBullet = /(?:^|\n)(?:4|5|6|7|8|9|10)\.[ \t]+/m;
  const match = fourthBullet.exec(sanitized);
  if (match) {
    sanitized = sanitized.slice(0, match.index).trimEnd();
    log("stock_guard_truncated_to_3", { original_length: text.length, truncated_length: sanitized.length });
  }

  const validText = contextStr.toLowerCase();
  
  const lines = sanitized.split('\n');
  const verifiedLines = lines.map(line => {
    const lowerLine = line.toLowerCase();
    if (lowerLine.includes('http')) {
      const urlMatch = line.match(/https?:\/\/[^\s]+/);
      if (urlMatch && !validText.includes(urlMatch[0].toLowerCase())) return null;
    }
    if (lowerLine.includes('mad') || lowerLine.includes('درهم')) {
      const priceMatch = line.match(/\d+(\.\d+)?/);
      if (priceMatch && !validText.includes(priceMatch[0])) return null;
    }
    if (lowerLine.includes('ml') || lowerLine.includes('g ') || lowerLine.includes('standard')) {
      const sizeMatch = line.match(/\d+(ml|g)|standard/i);
      if (sizeMatch && !validText.includes(sizeMatch[0].toLowerCase())) return null;
    }
    return line;
  }).filter(l => l !== null);

  return verifiedLines.join('\n');
}

function deterministicAdvisorFallback(userMessage, kind = "unavailable") {
  const value = cleanText(userMessage);
  const lower = value.toLowerCase();
  const isArabic = /[\u0600-\u06ff]/.test(value);
  const isFrench = /\b(bonjour|salut|merci|peau|cheveux|produit|routine|conseille|voudrais|cherche|explique|deuxième|commander|compare)\b/i.test(lower);
  const isEnglish = /\b(hello|hi|thanks|skin|hair|product|routine|recommend|looking|want)\b/i.test(lower);

  if (kind === "unverified") {
    if (isArabic) return "سمح ليا، ما قدرتش نأكد ليك جواب موثوق دابا. نقدر نكمل معاك بسؤال واحد باش نفهم الحالة مزيان، أو نحولك لمستشار من SkinPara.";
    if (isFrench) return "Je ne peux pas confirmer une réponse fiable pour le moment. Je peux continuer avec une question ciblée ou transmettre votre demande à un conseiller SkinPara.";
    if (isEnglish) return "I can’t verify a reliable answer right now. I can continue with one focused question or hand this over to a SkinPara adviser.";
    return "Sme7 lia, ma 9dertch n2ekked jawab mawthou9 daba. N9der nkemmel m3ak b so2al wa7ed wade7, ola n7awlek lmostachar SkinPara.";
  }

  if (isArabic) return "المستشار الآلي ما متاحش مؤقتاً. يقدر واحد من فريق SkinPara يكمل معاك.";
  if (isFrench) return "Le conseiller automatique est momentanément indisponible. Un membre de l’équipe SkinPara pourra reprendre votre demande.";
  if (isEnglish) return "The automated adviser is temporarily unavailable. A SkinPara team member can continue helping you.";
  return "Lmostachar l2ali ma khddamch mowa9atan. Y9der chi wa7ed mn team SkinPara ykemmel m3ak.";
}

function deterministicConsultationReply(userMessage, history = []) {
  const value = cleanText(userMessage);
  if (!/[\u0600-\u06ff]/.test(value)) return null;

  // Build known facts from the complete recent customer conversation, including
  // the latest message. This prevents the deterministic guard from asking a
  // question the customer has just answered.
  const priorUserMessages = (history || [])
    .filter(x => x.role === "user")
    .map(x => cleanText(x.content))
    .filter(Boolean);
  const customerContext = [...priorUserMessages, value].join(" ");
  const current = value.toLowerCase();

  const skinConcern = /(بشر|وجه|حبوب|دهني|دهنية|جاف|جافة|مختلط|مختلطة|حساس|حساسة|روتين|غسول|منظف|مرطب|سيروم|واقي|spf|cerave)/i.test(customerContext);
  if (!skinConcern) return null;

  const hasSkinType = /(دهني|دهنية|جاف|جافة|مختلط|مختلطة|عادي|عادية|حساس|حساسة)/i.test(customerContext);
  const hasSensitivity = /(حساس|حساسة|كيحمر|تحمر|حكة|تهيج)/i.test(customerContext);
  const hasCurrentRoutine = /(كنستعمل|كنستعمل غير|استعمل|روتين|غسول|منظف|مرطب|سيروم|واقي|spf|cerave|cera ve)/i.test(customerContext);

  if (!hasSkinType) return "أكيد نقدر نعاونك. بشرتك دهنية، جافة، مختلطة ولا حساسة؟";
  if (!hasSensitivity) return "فهمتك. واش بشرتك كتكون حساسة أو كتحمر وكتتهيج بسهولة؟";

  // If the latest message itself supplies routine/product information, never
  // repeat the routine question. Let the grounded AI/catalog path advance.
  const currentSuppliesRoutine = /(كنستعمل|استعمل|غسول|منظف|مرطب|سيروم|واقي|spf|cerave|cera ve)/i.test(current);
  if (!hasCurrentRoutine && !currentSuppliesRoutine) {
    return "مزيان. شنو كتستعمل دابا فالعناية اليومية ديال بشرتك؟";
  }
  return null;
}

function validateAdvisorLanguage(userMessage, responseText) {
  const input = cleanText(userMessage);
  const output = cleanText(responseText);
  const inputHasArabic = /[\u0600-\u06ff]/.test(input);
  const outputHasArabic = /[\u0600-\u06ff]/.test(output);

  // Reject classic UTF-8/Latin-1 mojibake before anything reaches WhatsApp.
  if (/(?:Ã|Â|Ø|Ù|â€|ï¸|ðŸ)/.test(output)) {
    return { ok: false, reason: "encoding_corruption" };
  }

  // Arabic-script Darija must remain Arabic-script Darija.
  if (inputHasArabic && !outputHasArabic) {
    return { ok: false, reason: "arabic_script_required" };
  }

  // Latin/French/English/Arabizi input must not unexpectedly switch to Arabic script.
  if (!inputHasArabic && outputHasArabic) {
    return { ok: false, reason: "script_mismatch" };
  }

  return { ok: true };
}

function deterministicOrdinalFollowup(userMessage, catalogContext) {
  const input = cleanText(userMessage);
  const ordinal = /(?:second|second one|deuxi[eè]me|الثاني|التاني|هاد الثاني|2[eè]me)/i.test(input) ? 1 : /(?:first|first one|premier|premi[eè]re|الأول|الاول|هاد الأول)/i.test(input) ? 0 : -1;
  if (ordinal < 0) return null;
  const titles = [...String(catalogContext || "").matchAll(/^Title:\s*(.+)$/gm)].map(m => m[1].trim());
  const title = titles[ordinal];
  if (!title) return null;
  const purchase = /\b(?:want|buy|order|commander|commande|veux|bghit|baghi|nakhd|nakhdo)\b|بغيت|نطلب|ناخد/i.test(input);
  const fr = /\b(?:deuxi[eè]me|commander|commande|veux|explique|celui)\b/i.test(input);
  const en = /\b(?:second|first|want|buy|order|explain|that one)\b/i.test(input);
  const ar = /[\u0600-\u06ff]/.test(input);
  if (purchase) {
    if (fr) return "D’accord. Vous parlez bien de " + title + ". Quelle quantité souhaitez-vous ?";
    if (en) return "Sure. You mean " + title + ". What quantity would you like?";
    if (ar) return "أكيد. كتقصد " + title + ". شحال من وحدة بغيتي؟";
    return "Wakha. Kat9sed " + title + ". Ch7al mn wa7da bghiti?";
  }
  if (fr) return "Le produit sélectionné est " + title + ".";
  if (en) return "The selected product is " + title + ".";
  if (ar) return "المنتج اللي قصدتي هو " + title + ".";
  return "Lproduit li 9sedti howa " + title + ".";
}

function deterministicCatalogComparison(userMessage, catalogContext) {
  const input = cleanText(userMessage);
  if (!/\b(compare|comparer|comparaison|difference|diff[eé]rence|versus|vs)\b|قارن|الفرق/i.test(input)) return null;
  const titles = [...String(catalogContext || "").matchAll(/^Title:\s*(.+)$/gm)].map(m => m[1].trim());
  const mentioned = titles.filter(title => input.toLowerCase().includes(title.toLowerCase()));
  const selected = (mentioned.length >= 2 ? mentioned : titles).slice(0, 2);
  if (selected.length < 2) return null;
  const fr = /\b(compare|comparer|comparaison|diff[eé]rence)\b/i.test(input);
  const en = /\b(compare|difference|versus|vs)\b/i.test(input) && !fr;
  if (fr) return "Comparaison vérifiée :\n\n1. " + selected[0] + "\n2. " + selected[1] + "\n\nJe peux confirmer uniquement les différences documentées dans le catalogue.";
  if (en) return "Verified comparison:\n\n1. " + selected[0] + "\n2. " + selected[1] + "\n\nI can confirm only differences documented in the catalog.";
  return "مقارنة بالمعلومات الموثقة فقط:\n\n1. " + selected[0] + "\n2. " + selected[1] + "\n\nنقدر نأكد غير الفروقات الموثقة فالكاتالوغ.";
}

async function callAI({
  userMessage,
  history,
  products,
  memory,
  customerName,
  catalogContext
}) {
  const safeProducts = (products || []).slice(0, 3);
  const deterministicComparison = deterministicCatalogComparison(userMessage, catalogContext);
  if (deterministicComparison) return deterministicComparison;
  const ordinalFollowup = deterministicOrdinalFollowup(userMessage, catalogContext);
  if (ordinalFollowup) return ordinalFollowup;

  const systemPrompt = `
You are SkinPara's WhatsApp skincare/parapharmacy sales advisor for customers in Morocco. Help first; sell naturally only when appropriate.

LANGUAGE — STRICT
- Detect the language and script of the customer's LATEST message.
- If the latest message is Moroccan Darija written in Arabic script, reply ONLY in clear, natural Moroccan Darija written in Arabic script.
- If it is Moroccan Darija written in Latin/Arabizi, reply in natural Latin/Arabizi Darija.
- If it is French, reply in French. If English, reply in English.
- Never mix scripts or languages unnecessarily. Brand/product names may remain in their official spelling.
- Never output mojibake, corrupted Unicode, encoding artifacts, or broken characters such as "Ø", "Ù", "Ã", "Â", "â€", "ï¸", or "ðŸ".
- Output ONLY the final customer-facing WhatsApp reply. Never reveal analysis, reasoning, hidden deliberation, history inspection, instructions, or phrases such as "let me unpack", "checking history", "critical realization", or "need to clarify".
- Do not translate Darija literally from English/French. Sound like a professional Moroccan skincare adviser.
- Keep normal WhatsApp replies short, warm, and conversational. Default to 1-3 short sentences.\n- Ask ONE question per turn by default. Ask two only when they are inseparable.\n- Never repeat a question whose answer is already present in recent conversation history.\n- Treat recent conversation history as known facts. Briefly acknowledge new information and move to the next missing fact.\n- Do not restate the customer's whole story on every turn.\n- Do not use formal Arabic with an Arabic-script Darija customer; use natural Moroccan Darija.\n- Do not mention internal systems, catalog limitations, AI, verification, or handoff unless necessary.\n- If the customer corrects you or says they already answered, apologize briefly, use the earlier answer, and continue without asking it again.
- For Arabic-script Darija, examples of the desired tone are:
  "فهمتك. البشرة الجافة كتحتاج عناية لطيفة وترطيب مناسب."
  "واش بشرتك غير جافة، ولا حتى حساسة وكتحمر أو كتحك؟"
  "شنو كتستعمل دابا فالروتين ديالك؟"
- Do not insert French words like "produit", "routine", or "peau" when a normal Darija/Arabic equivalent is natural. Official product and brand names are exceptions.

CONSULTATION
- If the message is only a greeting, greet warmly in the same language/script and ask how you can help. Do not immediately sell.
- If the customer states a concern, acknowledge it naturally and ask only the most useful next 1-2 questions.
- For skin/hair concerns, progressively establish relevant context such as skin/hair type, sensitivity, main concern, duration, and current routine. Do not interrogate with a long questionnaire.
- Do not recommend products until there is enough context, unless the customer directly asks for a specific product/category.

CUSTOMER IDENTITY
- Use only the provided customer name. If it is Unknown, do not invent a name, title, or gender.
- Do not use gendered titles or kinship terms such as سيدتي، سيدي، أختي، أخي unless the customer explicitly supplied that identity.
- Prefer gender-neutral Moroccan Darija phrasing when practical.

CATALOG GROUNDING
- Every product-specific claim must be grounded in CATALOG INTELLIGENCE or SHOPIFY PRODUCTS below.
- Shopify is authoritative for price, variant and stock-related facts.
- Never invent a product, price, size, stock state, benefit, ingredient, URL, variant, or availability.
- Never claim a product is in stock or unavailable unless verified business state explicitly says so.
- If no verified matching product exists, say that briefly and ask a useful clarifying question instead of guessing.
- Recommend at most 3 products.

PRODUCT FORMAT
When recommending products, put each field on its own line and leave a blank line between products. Use natural labels in the customer's language. Example for Arabic-script Darija:
1️⃣ [اسم المنتج الحقيقي]
[وصف قصير مبني فقط على المعلومات الموثقة]
💰 الثمن: [الثمن الحقيقي بالدرهم]
📦 الحجم: [الحجم الحقيقي، إذا كان متوفرا]
🔗 الرابط: [الرابط الحقيقي، إذا كان متوفرا]

ROUTINES
- Before building a routine, obtain missing relevant context such as skin type, concern, sensitivity and current actives.
- Use only verified catalog products and keep the routine minimal.
- Do not invent compatibility, ingredients, or medical effects.

CONTEXT & PURCHASE INTENT
- Resolve references such as "الثاني", "هاد الثاني", "the second one", or "celui-là" from the immediately preceding verified recommendations. If ambiguous, ask.
- A question, comparison, routine request, or explanation is NOT purchase intent.
- Start purchase confirmation only after explicit intent to buy/order.
- Before any order step, confirm exact verified product, variant/size when needed, and quantity.
- Never claim an order was created or confirmed unless the order engine confirms it.
- Mention delivery timing only when supported by supplied business context.

MEDICAL SAFETY
- Never diagnose or prescribe.
- For severe reaction, breathing difficulty, significant swelling, infection warning signs, intense pain, or other medical-risk situations: do not recommend skincare products; advise prompt pharmacist/doctor/dermatologist guidance and urgent help for serious warning signs; require human handoff.

CUSTOMER CONTEXT
Name: ${customerName || "Unknown"}
Memory: ${cleanText(memory?.last_summary || "").slice(0, 220) || "None"}

CATALOG INTELLIGENCE
${catalogContext || "No catalog intelligence is available."}

SHOPIFY PRODUCTS
${productContext(safeProducts)}
`.trim();
  const recentHistory = history
    .filter(x =>
      x.role === "user" ||
      x.role === "assistant"
    )
    .slice(-10)
    .filter((x, index, arr) => {
      const isLast = index === arr.length - 1;

      return !(
        isLast &&
        x.role === "user" &&
        cleanText(x.content) === cleanText(userMessage)
      );
    });

  const messages = [
    {
      role: "system",
      content: systemPrompt
    },

    ...recentHistory,

    {
      role: "user",
      content: userMessage
    }
  ];
  let lastAIError = null;

  for (let attempt = 1; attempt <= 3; attempt++) {

    const controller =
      new AbortController();

    const timeout =
      setTimeout(
        () => controller.abort(),
        25000
      );

    try {

      const response = await safeFetch(
        "https://openrouter.ai/api/v1/chat/completions",
        {
          method: "POST",

          signal:
            controller.signal,

          headers: {
            "Authorization":
              `Bearer ${OPENROUTER_API_KEY}`,

            "Content-Type":
              "application/json",

            "X-Title":
              "SkinPara AI Sales Assistant"
          },

          body: JSON.stringify({
            model: OPENROUTER_MODEL,

            messages,

            temperature: 0.35,

            max_tokens: 320
          })
        }
      );

      const text =
        await response.text();

      let data;

      try {
        data =
          text
            ? JSON.parse(text)
            : {};
      } catch {
        data = {
          raw: text
        };
      }

      const content =
        data.choices?.[0]
          ?.message?.content;

      const validatedContent =
        validateCustomerAIResponse(content);

      const languageValidation = validatedContent.ok
        ? validateAdvisorLanguage(userMessage, validatedContent.text)
        : { ok: false, reason: validatedContent.reason };

      if (
        response.ok &&
        validatedContent.ok &&
        languageValidation.ok
      ) {
        clearTimeout(timeout);

        if (attempt > 1) {
          log("openrouter_recovered", {
            attempt,
            model:
              data.model || null
          });
        }

        // Last-mile: enforce catalog safety, then keep WhatsApp output concise.
        const contextStr = (catalogContext || "") + " " + (products ? JSON.stringify(products) : "");
        let finalText = sanitizeCustomerResponse(validatedContent.text, contextStr);
        if (finalText.length > 1200) {
          const cut = finalText.slice(0, 1200);
          const boundary = Math.max(cut.lastIndexOf("\n"), cut.lastIndexOf(". "), cut.lastIndexOf("؟"), cut.lastIndexOf("! "));
          finalText = (boundary >= 500 ? cut.slice(0, boundary + 1) : cut).trim();
          log("advisor_response_trimmed", { original_length: validatedContent.text.length, final_length: finalText.length });
        }
        return finalText;
      }

      if (response.ok && (!validatedContent.ok || !languageValidation.ok)) {
        const rejectionReason = validatedContent.ok
          ? languageValidation.reason
          : validatedContent.reason;
        lastAIError = new Error(
          `openrouter_invalid_customer_response:${rejectionReason}`
        );
        log("openrouter_response_rejected", {
          attempt,
          reason: rejectionReason,
          model: data.model || null
        });
        continue;
      }

      const permanent =
        response.status >= 400 &&
        response.status < 500 &&
        response.status !== 408 &&
        response.status !== 409 &&
        response.status !== 429;

      lastAIError =
        new Error(
          `openrouter_failed:${response.status}: ${JSON.stringify(data)}`
        );

      log("openrouter_attempt_failed", {
        attempt,
        status:
          response.status,
        model:
          data.model || null,
        finishReason:
          data.choices?.[0]
            ?.finish_reason || null,
        hasContent:
          Boolean(
            cleanText(content)
          )
      });

      if (permanent) {
        throw lastAIError;
      }

    } catch (e) {

      lastAIError = e;

      const permanentMessage =
        e.message.includes('"code":400') ||
        e.message.includes("models' array") ||
        e.message.includes("401") ||
        e.message.includes("403");

      if (permanentMessage) {
        clearTimeout(timeout);
        throw e;
      }

      log("openrouter_network_error", {
        attempt,
        error:
          e.name === "AbortError"
            ? "timeout"
            : e.message
      });

    } finally {
      clearTimeout(timeout);
    }

    if (attempt < 3) {

      const delay =
        attempt === 1
          ? 750
          : 1500;

      log("openrouter_retry", {
        attempt,
        nextAttempt:
          attempt + 1,
        delay
      });

      await sleep(delay);
    }
  }

  if (
    lastAIError?.message?.startsWith(
      "openrouter_invalid_customer_response:"
    )
  ) {
    log("openrouter_safe_fallback", {
      reason: "invalid_customer_response"
    });
    return deterministicAdvisorFallback(userMessage, "unverified");
  }

  log("openrouter_safe_fallback", { reason: lastAIError ? lastAIError.message : "unknown_failure" });
  return deterministicAdvisorFallback(userMessage, "unavailable");
}




// =========================================================
// SKINPARA STOCK ORDER ENGINE V1
// =========================================================
//
// FLOW:
//
// pending-stock
//      ?
// confirmed / rejected
//      ?
// confirmed -> create Shopify pending order
//      ?
// order-created
//
// Default mode = TEST.
// No real Shopify order until:
// SKINPARA_ORDER_MODE=live
//
// =========================================================


const ORDER_MODE =
  String(
    process.env
      .SKINPARA_ORDER_MODE ||
    "test"
  ).toLowerCase();


function stockStateKey(
  conversationId
) {
  return `skinpara:stock-order:${conversationId}`;
}


function orderLockKey(
  conversationId
) {
  return `skinpara:order-lock:${conversationId}`;
}


function cleanOrderString(
  value,
  max = 250
) {
  return String(
    value || ""
  )
    .replace(/\\s+/g, " ")
    .trim()
    .slice(0, max);
}


function normalizeVariantId(
  value
) {

  const v =
    cleanOrderString(
      value,
      200
    );

  if (
    !v.startsWith(
      "gid://shopify/ProductVariant/"
    )
  ) {
    throw new Error(
      "invalid_shopify_variant_id"
    );
  }

  return v;
}


function normalizeQuantity(
  value
) {

  const q =
    Number(value || 1);

  if (
    !Number.isInteger(q) ||
    q < 1 ||
    q > 20
  ) {
    throw new Error(
      "invalid_quantity"
    );
  }

  return q;
}


async function saveStockState(
  conversationId,
  state
) {

  await redisCommand([
    "SET",
    stockStateKey(
      conversationId
    ),
    JSON.stringify(
      state
    ),
    "EX",
    "604800"
  ]);

  return state;
}


async function getStockState(
  conversationId
) {

  const raw =
    await redisCommand([
      "GET",
      stockStateKey(
        conversationId
      )
    ]);

  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(
      raw
    );
  } catch {
    return null;
  }
}


async function acquireOrderLock(
  conversationId
) {

  const result =
    await redisCommand([
      "SET",
      orderLockKey(
        conversationId
      ),
      String(
        Date.now()
      ),
      "NX",
      "EX",
      "120"
    ]);

  return (
    result === "OK"
  );
}


async function releaseOrderLock(
  conversationId
) {

  try {

    await redisCommand([
      "DEL",
      orderLockKey(
        conversationId
      )
    ]);

  } catch (_) {}
}


// =========================================================
// CHATWOOT HELPERS
// =========================================================


async function getConversationForOrder(
  conversationId
) {

  return await chatwootFetch(
    `/conversations/${conversationId}`,
    {
      method:
        "GET"
    }
  );
}


async function getConversationLabelsV1(
  conversationId
) {

  const data =
    await chatwootFetch(
      `/conversations/${conversationId}/labels`,
      {
        method:
          "GET"
      }
    );

  return Array.isArray(
    data?.payload
  )
    ? data.payload
    : [];
}


async function setWorkflowLabels(
  conversationId,
  {
    add = [],
    remove = []
  } = {}
) {

  const current =
    await getConversationLabelsV1(
      conversationId
    );

  const removeSet =
    new Set(
      remove
    );

  const next =
    [
      ...new Set([
        ...current.filter(
          label =>
            !removeSet.has(
              label
            )
        ),

        ...add
      ])
    ];


  await chatwootFetch(
    `/conversations/${conversationId}/labels`,
    {
      method:
        "POST",

      body:
        JSON.stringify({
          labels:
            next
        })
    }
  );


  return next;
}


async function mergeConversationAttributes(
  conversationId,
  attrs
) {

  const conversation =
    await getConversationForOrder(
      conversationId
    );

  const current =
    conversation?.custom_attributes ||
    {};


  const next = {
    ...current,
    ...attrs
  };


  const result =
    await chatwootFetch(
      `/conversations/${conversationId}/custom_attributes`,
      {
        method:
          "POST",

        body:
          JSON.stringify({
            custom_attributes:
              next
          })
      }
    );


  return (
    result?.custom_attributes ||
    next
  );
}


// =========================================================
// SHOPIFY ORDER
// =========================================================


function splitCustomerName(
  name
) {

  const value =
    cleanOrderString(
      name,
      100
    );

  if (!value) {

    return {
      firstName:
        "",
      lastName:
        "SkinPara Customer"
    };
  }


  const parts =
    value
      .split(" ")
      .filter(Boolean);


  if (
    parts.length === 1
  ) {

    return {
      firstName:
        "",
      lastName:
        parts[0]
    };
  }


  return {
    firstName:
      parts
        .slice(
          0,
          -1
        )
        .join(" "),

    lastName:
      parts[
        parts.length - 1
      ]
  };
}


async function createShopifyPendingOrder(
  state
) {

  const variantId =
    normalizeVariantId(
      state.variant_id
    );

  const quantity =
    normalizeQuantity(
      state.quantity
    );


  // -----------------------------------------------------
  // TEST MODE
  // -----------------------------------------------------

  if (
    ORDER_MODE !==
    "live"
  ) {

    return {
      test_mode:
        true,

      id:
        `TEST-ORDER-${state.conversation_id}`,

      name:
        `#TEST-${state.conversation_id}`,

      financial_status:
        "PENDING"
    };
  }


  // -----------------------------------------------------
  // LIVE MODE
  // Shopify orderCreate
  // financialStatus = PENDING
  // -----------------------------------------------------

  const nameParts =
    splitCustomerName(
      state.customer_name
    );


  const shippingAddress = {

    firstName:
      nameParts.firstName ||
      undefined,

    lastName:
      nameParts.lastName,

    address1:
      cleanOrderString(
        state.address,
        200
      ),

    city:
      cleanOrderString(
        state.city,
        100
      ),

    countryCode:
      "MA"
  };


  Object.keys(
    shippingAddress
  ).forEach(
    key => {

      if (
        !shippingAddress[key]
      ) {
        delete shippingAddress[key];
      }
    }
  );


  const orderInput = {

    lineItems: [
      {
        variantId,
        quantity
      }
    ],

    financialStatus:
      "PENDING",

    shippingAddress
  };


  const email =
    cleanOrderString(
      state.email,
      200
    );

  if (email) {
    orderInput.email =
      email;
  }


  const query = `
    mutation SkinParaOrderCreate(
      $order: OrderCreateOrderInput!,
      $options: OrderCreateOptionsInput
    ) {
      orderCreate(
        order: $order,
        options: $options
      ) {
        userErrors {
          field
          message
        }

        order {
          id
          name
          displayFinancialStatus

          lineItems(first: 10) {
            nodes {
              id
              title
              quantity

              variant {
                id
              }
            }
          }
        }
      }
    }
  `;


  const data =
    await shopifyGraphQL(
      query,
      {
        order:
          orderInput,

        options: {
          sendReceipt:
            false,

          sendFulfillmentReceipt:
            false,

          inventoryBehaviour:
            "DECREMENT_OBEYING_POLICY"
        }
      }
    );


  const payload =
    data?.orderCreate;


  if (!payload) {

    throw new Error(
      "shopify_order_create_empty_response"
    );
  }


  const userErrors =
    payload.userErrors ||
    [];


  if (
    userErrors.length
  ) {

    throw new Error(
      "shopify_order_create_error:" +
      JSON.stringify(
        userErrors
      )
    );
  }


  if (
    !payload.order?.id
  ) {

    throw new Error(
      "shopify_order_missing_id"
    );
  }


  return {
    test_mode:
      false,

    id:
      payload.order.id,

    name:
      payload.order.name,

    financial_status:
      payload.order
        .displayFinancialStatus
  };
}


// =========================================================
// PENDING STOCK
// =========================================================


async function markPendingStock(
  input
) {

  const conversationId =
    Number(
      input.conversation_id
    );


  if (!conversationId) {
    throw new Error(
      "conversation_id_required"
    );
  }


  const state = {

    version:
      1,

    conversation_id:
      conversationId,

    variant_id:
      normalizeVariantId(
        input.variant_id
      ),

    product_title:
      cleanOrderString(
        input.product_title,
        200
      ),

    quantity:
      normalizeQuantity(
        input.quantity
      ),

    customer_name:
      cleanOrderString(
        input.customer_name,
        100
      ),

    phone:
      cleanOrderString(
        input.phone,
        50
      ),

    email:
      cleanOrderString(
        input.email,
        200
      ),

    address:
      cleanOrderString(
        input.address,
        250
      ),

    city:
      cleanOrderString(
        input.city,
        100
      ),

    order_total:
      input.order_total != null
        ? Number(
            input.order_total
          )
        : null,

    stock_status:
      "pending",

    order_status:
      "not_created",

    shopify_order_id:
      null,

    shopify_order_name:
      null,

    created_at:
      new Date()
        .toISOString(),

    updated_at:
      new Date()
        .toISOString()
  };


  await saveStockState(
    conversationId,
    state
  );


  await setWorkflowLabels(
    conversationId,
    {
      add: [
        "pending-stock"
      ],

      remove: [
        "stock-confirmed",
        "out-of-stock",
        "order-created"
      ]
    }
  );


  await mergeConversationAttributes(
    conversationId,
    {
      stock_status:
        "pending",

      last_product:
        state.product_title ||
        state.variant_id,

      order_total:
        state.order_total,

      shopify_order_id:
        ""
    }
  );

  try {
    const automationConversation =
      await getConversationForOrder(conversationId);
    const automationContactId = Number(
      automationConversation?.meta?.sender?.id ||
      automationConversation?.contact_id || 0
    );
    await recordAutomationEvent({
      eventType: "pending_stock",
      eventKey: `pending_stock:conversation:${conversationId}`,
      sourceEventId: `conversation:${conversationId}:pending_stock`,
      contactId: automationContactId,
      conversationId,
      productName: state.product_title || null,
      payload: { order_mode: ORDER_MODE }
    });
  } catch (_) {}


  log(
    "stock_pending",
    {
      conversationId,
      variantId:
        state.variant_id,
      quantity:
        state.quantity
    }
  );


  return {
    ok:
      true,

    state
  };
}


// =========================================================
// CONFIRM STOCK + CREATE ORDER
// =========================================================


async function confirmStockAndCreateOrder(
  conversationId
) {

  conversationId =
    Number(
      conversationId
    );


  if (!conversationId) {
    throw new Error(
      "conversation_id_required"
    );
  }


  const locked =
    await acquireOrderLock(
      conversationId
    );


  if (!locked) {

    throw new Error(
      "order_creation_locked"
    );
  }


  try {

    let state =
      await getStockState(
        conversationId
      );


    if (!state) {

      throw new Error(
        "pending_stock_state_not_found"
      );
    }


    // -----------------------------------------------------
    // IDEMPOTENCY
    // -----------------------------------------------------

    if (
      state.order_status ===
        "created" &&
      state.shopify_order_id
    ) {

      return {
        ok:
          true,

        duplicate:
          true,

        state
      };
    }


    state.stock_status =
      "confirmed";

    state.updated_at =
      new Date()
        .toISOString();


    await saveStockState(
      conversationId,
      state
    );


    await setWorkflowLabels(
      conversationId,
      {
        add: [
          "stock-confirmed"
        ],

        remove: [
          "pending-stock",
          "out-of-stock"
        ]
      }
    );


    await mergeConversationAttributes(
      conversationId,
      {
        stock_status:
          "confirmed"
      }
    );


    log(
      "stock_confirmed",
      {
        conversationId
      }
    );

    try {
      const automationConversation =
        await getConversationForOrder(conversationId);
      const automationContactId = Number(
        automationConversation?.meta?.sender?.id ||
        automationConversation?.contact_id || 0
      );
      await recordAutomationEvent({
        eventType: "stock_confirmed",
        eventKey: `stock_confirmed:conversation:${conversationId}`,
        sourceEventId: `conversation:${conversationId}:stock_confirmed`,
        contactId: automationContactId,
        conversationId,
        productName: state.product_title || null,
        payload: { order_mode: ORDER_MODE }
      });
    } catch (_) {}


    // -----------------------------------------------------
    // CREATE SHOPIFY ORDER
    // -----------------------------------------------------

    const order =
      await createShopifyPendingOrder(
        state
      );


    state.order_status =
      "created";

    state.shopify_order_id =
      order.id;

    state.shopify_order_name =
      order.name;

    state.order_test_mode =
      Boolean(
        order.test_mode
      );

    state.updated_at =
      new Date()
        .toISOString();


    await saveStockState(
      conversationId,
      state
    );


    await setWorkflowLabels(
      conversationId,
      {
        add: [
          "order-created",
          "stock-confirmed"
        ],

        remove: [
          "pending-stock",
          "out-of-stock"
        ]
      }
    );


    await mergeConversationAttributes(
      conversationId,
      {
        stock_status:
          "confirmed",

        shopify_order_id:
          order.id,

        order_total:
          state.order_total
      }
    );


    try {

      const message =
        order.test_mode
          ?
          "? Test order flow confirmed. Stock confirmed and order simulation created successfully."
          :
          `? ?? ????? ????? ${order.name || ""}. ????? ????? ???? ????? ????? ???? ????? ?????.`;


      await sendChatwootMessage(
        conversationId,
        message
      );

    } catch (
      messageError
    ) {

      log(
        "order_confirmation_message_error",
        {
          conversationId,
          error:
            messageError.message
        }
      );
    }

    await recordLifetimeOrderEvent(
      "order_created",
      state
    );


    log(
      "order_created",
      {
        conversationId,

        orderId:
          order.id,

        orderName:
          order.name,

        testMode:
          order.test_mode
      }
    );


    return {
      ok:
        true,

      duplicate:
        false,

      order,

      state
    };

  } finally {

    await releaseOrderLock(
      conversationId
    );
  }
}


// =========================================================
// REJECT STOCK
// =========================================================


async function rejectStock(
  conversationId,
  reason = null
) {

  conversationId =
    Number(
      conversationId
    );


  if (!conversationId) {

    throw new Error(
      "conversation_id_required"
    );
  }


  let state =
    await getStockState(
      conversationId
    );


  if (!state) {

    throw new Error(
      "pending_stock_state_not_found"
    );
  }


  state.stock_status =
    "out_of_stock";

  state.stock_rejection_reason =
    cleanOrderString(
      reason,
      300
    ) || null;

  state.updated_at =
    new Date()
      .toISOString();


  await saveStockState(
    conversationId,
    state
  );


  await setWorkflowLabels(
    conversationId,
    {
      add: [
        "out-of-stock"
      ],

      remove: [
        "pending-stock",
        "stock-confirmed"
      ]
    }
  );


  await mergeConversationAttributes(
    conversationId,
    {
      stock_status:
        "out_of_stock"
    }
  );


  try {

    await sendChatwootMessage(
      conversationId,
      "??? ?????? ????? ?? ?????? ????. ???? ????? ???? ???? ????? ??"
    );

  } catch (
    messageError
  ) {

    log(
      "stock_reject_message_error",
      {
        conversationId,

        error:
          messageError.message
      }
    );
  }


  log(
    "stock_rejected",
    {
      conversationId,

      reason:
        state
          .stock_rejection_reason
    }
  );

  await recordLifetimeOrderEvent(
    "rejected",
    state
  );


  return {
    ok:
      true,

    state
  };
}


// =========================================================
// STATUS
// =========================================================


async function getStockOrderStatus(
  conversationId
) {

  const state =
    await getStockState(
      Number(
        conversationId
      )
    );


  return {

    ok:
      true,

    order_mode:
      ORDER_MODE,

    state:
      state || null
  };
}






// =========================================================
// SKINPARA MASTER COMPLETION V1
// =========================================================


const SKINPARA_VIP_DELIVERED_ORDERS =
  Math.max(
    1,
    Number(
      process.env
        .SKINPARA_VIP_DELIVERED_ORDERS ||
      5
    )
  );


const SKINPARA_REWARDS_ENABLED =
  String(
    process.env
      .SKINPARA_REWARDS_ENABLED ||
    "false"
  ).toLowerCase() ===
  "true";


const SKINPARA_DELIVERY_PROVIDER =
  String(
    process.env
      .SKINPARA_DELIVERY_PROVIDER ||
    "manual"
  );


function lifecycleKey(
  conversationId
) {

  return `skinpara:lifecycle:${conversationId}`;
}


function customerStatsKey(
  contactId
) {

  return `skinpara:customer-stats:${contactId}`;
}


async function saveLifecycle(
  conversationId,
  data
) {

  await redisCommand([
    "SET",
    lifecycleKey(
      conversationId
    ),
    JSON.stringify(
      data
    ),
    "EX",
    "15552000"
  ]);

  return data;
}


async function getLifecycle(
  conversationId
) {

  const raw =
    await redisCommand([
      "GET",
      lifecycleKey(
        conversationId
      )
    ]);

  if (!raw) {
    return null;
  }

  try {

    return JSON.parse(
      raw
    );

  } catch {

    return null;
  }
}


function normalizePhone(
  value
) {

  let phone =
    String(
      value || ""
    )
      .replace(
        /[^0-9+]/g,
        ""
      )
      .trim();


  if (
    phone.startsWith(
      "06"
    ) ||
    phone.startsWith(
      "07"
    )
  ) {

    phone =
      "+212" +
      phone.slice(1);
  }


  if (
    phone.startsWith(
      "212"
    )
  ) {

    phone =
      "+" + phone;
  }


  return phone;
}


function validateCODData(
  state
) {

  const errors = [];


  if (
    !String(
      state.customer_name ||
      ""
    ).trim()
  ) {

    errors.push(
      "customer_name"
    );
  }


  const phone =
    normalizePhone(
      state.phone
    );


  if (
    !phone ||
    phone.length < 10
  ) {

    errors.push(
      "phone"
    );
  }


  if (
    !String(
      state.city ||
      ""
    ).trim()
  ) {

    errors.push(
      "city"
    );
  }


  if (
    !String(
      state.address ||
      ""
    ).trim()
  ) {

    errors.push(
      "address"
    );
  }


  if (
    !state.variant_id
  ) {

    errors.push(
      "variant_id"
    );
  }


  if (
    !Number(
      state.quantity
    )
  ) {

    errors.push(
      "quantity"
    );
  }


  return {
    valid:
      errors.length === 0,

    missing:
      errors,

    normalized_phone:
      phone
  };
}


// =========================================================
// CUSTOMER / CONVERSATION DATA
// =========================================================


async function updateOrderCustomerData(
  input
) {

  const conversationId =
    Number(
      input.conversation_id
    );


  if (!conversationId) {

    throw new Error(
      "conversation_id_required"
    );
  }


  const state =
    await getStockState(
      conversationId
    );


  if (!state) {

    throw new Error(
      "order_state_not_found"
    );
  }


  if (
    input.customer_name != null
  ) {

    state.customer_name =
      cleanOrderString(
        input.customer_name,
        100
      );
  }


  if (
    input.phone != null
  ) {

    state.phone =
      normalizePhone(
        input.phone
      );
  }


  if (
    input.city != null
  ) {

    state.city =
      cleanOrderString(
        input.city,
        100
      );
  }


  if (
    input.address != null
  ) {

    state.address =
      cleanOrderString(
        input.address,
        250
      );
  }


  if (
    input.email != null
  ) {

    state.email =
      cleanOrderString(
        input.email,
        200
      );
  }


  state.updated_at =
    new Date()
      .toISOString();


  await saveStockState(
    conversationId,
    state
  );


  const validation =
    validateCODData(
      state
    );


  log(
    "order_customer_data_updated",
    {
      conversationId,

      valid:
        validation.valid,

      missing:
        validation.missing
    }
  );


  return {
    ok:
      true,

    validation,

    state
  };
}


// =========================================================
// SAFE CONFIRM
// =========================================================


async function safeConfirmStockAndOrder(
  conversationId
) {

  const state =
    await getStockState(
      Number(
        conversationId
      )
    );


  if (!state) {

    throw new Error(
      "order_state_not_found"
    );
  }


  const validation =
    validateCODData(
      state
    );


  if (
    !validation.valid
  ) {

    return {
      ok:
        false,

      ready:
        false,

      error:
        "missing_cod_data",

      missing:
        validation.missing
    };
  }


  state.phone =
    validation
      .normalized_phone;


  await saveStockState(
    conversationId,
    state
  );


  return await confirmStockAndCreateOrder(
    conversationId
  );
}


// =========================================================
// READY TO SHIP
// =========================================================


async function markReadyToShip(
  conversationId
) {

  conversationId =
    Number(
      conversationId
    );


  const state =
    await getStockState(
      conversationId
    );


  if (
    !state ||
    state.order_status !==
      "created"
  ) {

    throw new Error(
      "order_not_created"
    );
  }


  await setWorkflowLabels(
    conversationId,
    {
      add: [
        "ready-to-ship"
      ],

      remove: [
        "pending-stock"
      ]
    }
  );


  await mergeConversationAttributes(
    conversationId,
    {
      stock_status:
        "confirmed"
    }
  );


  const lifecycle =
    (
      await getLifecycle(
        conversationId
      )
    ) || {};


  lifecycle.status =
    "ready_to_ship";

  lifecycle.ready_to_ship_at =
    new Date()
      .toISOString();


  await saveLifecycle(
    conversationId,
    lifecycle
  );

  await recordLifetimeOrderEvent(
    "ready_to_ship",
    state
  );


  log(
    "order_ready_to_ship",
    {
      conversationId
    }
  );


  return {
    ok:
      true,

    lifecycle
  };
}


// =========================================================
// SHIPPED
// =========================================================


async function markOrderShipped(
  input
) {

  const conversationId =
    Number(
      input.conversation_id
    );


  if (!conversationId) {

    throw new Error(
      "conversation_id_required"
    );
  }


  const state =
    await getStockState(
      conversationId
    );


  if (
    !state ||
    state.order_status !==
      "created"
  ) {

    throw new Error(
      "order_not_created"
    );
  }


  const trackingNumber =
    cleanOrderString(
      input.tracking_number,
      150
    );


  const company =
    cleanOrderString(
      input.delivery_company ||
      SKINPARA_DELIVERY_PROVIDER,
      100
    );


  await setWorkflowLabels(
    conversationId,
    {
      add: [
        "shipped"
      ],

      remove: [
        "ready-to-ship"
      ]
    }
  );


  const lifecycle =
    (
      await getLifecycle(
        conversationId
      )
    ) || {};


  lifecycle.status =
    "shipped";

  lifecycle.delivery_company =
    company;

  lifecycle.tracking_number =
    trackingNumber ||
    null;

  lifecycle.shipped_at =
    new Date()
      .toISOString();


  await saveLifecycle(
    conversationId,
    lifecycle
  );

  await recordLifetimeOrderEvent(
    "shipped",
    state
  );


  try {

    let message =
      "?? ??????? ????? ????? ???????.";

    if (
      trackingNumber
    ) {

      message +=
        ` ??? ??????: ${trackingNumber}`;
    }


    await sendChatwootMessage(
      conversationId,
      message
    );

  } catch (_) {}


  log(
    "order_shipped",
    {
      conversationId,
      company,
      trackingNumber:
        trackingNumber ||
        null
    }
  );


  return {
    ok:
      true,

    lifecycle
  };
}


// =========================================================
// CUSTOMER STATS
// =========================================================


async function incrementDeliveredStats(
  contactId
) {

  if (!contactId) {

    return null;
  }


  const key =
    customerStatsKey(
      contactId
    );


  const raw =
    await redisCommand([
      "GET",
      key
    ]);


  let stats = {
    delivered_orders:
      0,

    lifetime_delivered_total:
      0
  };


  if (raw) {

    try {

      stats = {
        ...stats,
        ...JSON.parse(raw)
      };

    } catch (_) {}
  }


  stats.delivered_orders =
    Number(
      stats.delivered_orders ||
      0
    ) + 1;


  stats.updated_at =
    new Date()
      .toISOString();


  await redisCommand([
    "SET",
    key,
    JSON.stringify(
      stats
    ),
    "EX",
    "31536000"
  ]);


  return stats;
}


// =========================================================
// DELIVERED
// =========================================================


async function markOrderDelivered(
  conversationId
) {

  conversationId =
    Number(
      conversationId
    );


  if (!conversationId) {

    throw new Error(
      "conversation_id_required"
    );
  }


  const state =
    await getStockState(
      conversationId
    );


  if (
    !state ||
    state.order_status !==
      "created"
  ) {

    throw new Error(
      "order_not_created"
    );
  }


  const conversation =
    await getConversationForOrder(
      conversationId
    );


  const contactId =
    Number(
      conversation?.meta
        ?.sender?.id ||
      conversation?.contact_id ||
      0
    );


  await setWorkflowLabels(
    conversationId,
    {
      add: [
        "delivered"
      ],

      remove: [
        "shipped",
        "ready-to-ship"
      ]
    }
  );


  const lifecycle =
    (
      await getLifecycle(
        conversationId
      )
    ) || {};


  if (
    lifecycle.status ===
    "delivered"
  ) {

    return {
      ok:
        true,

      duplicate:
        true,

      lifecycle
    };
  }


  lifecycle.status =
    "delivered";

  lifecycle.delivered_at =
    new Date()
      .toISOString();


  await saveLifecycle(
    conversationId,
    lifecycle
  );

  await recordLifetimeOrderEvent(
    "delivered",
    state,
    conversation
  );


  const stats =
    await incrementDeliveredStats(
      contactId
    );


  if (
    stats &&
    stats.delivered_orders >=
      2
  ) {

    await setWorkflowLabels(
      conversationId,
      {
        add: [
          "repeat-customer"
        ]
      }
    );
  }


  if (
    stats &&
    stats.delivered_orders >=
      SKINPARA_VIP_DELIVERED_ORDERS
  ) {

    await setWorkflowLabels(
      conversationId,
      {
        add: [
          "vip"
        ]
      }
    );
  }


  try {

    await sendChatwootMessage(
      conversationId,
      "????? ??? ?? ??????? ?????. ??? ?????? ?? ????? ??? ????? ????????? ??? ??? ????."
    );

  } catch (_) {}


  log(
    "order_delivered",
    {
      conversationId,
      contactId,
      deliveredOrders:
        stats
          ?.delivered_orders ||
        null
    }
  );


  return {
    ok:
      true,

    duplicate:
      false,

    lifecycle,

    customer_stats:
      stats
  };
}


// =========================================================
// LIVE READINESS
// =========================================================


async function getLiveReadiness(
  conversationId
) {

  const state =
    conversationId
      ? await getStockState(
          Number(
            conversationId
          )
        )
      : null;


  const cod =
    state
      ? validateCODData(
          state
        )
      : null;


  return {

    ok:
      true,

    order_mode:
      ORDER_MODE,

    live_order_enabled:
      ORDER_MODE ===
        "live",

    shopify_enabled:
      SHOPIFY_ENABLED,

    ai_enabled:
      AI_ENABLED,

    supabase_enabled:
      SUPABASE_ENABLED,

    delivery_provider:
      SKINPARA_DELIVERY_PROVIDER,

    rewards_enabled:
      SKINPARA_REWARDS_ENABLED,

    vip_after_delivered_orders:
      SKINPARA_VIP_DELIVERED_ORDERS,

    conversation_id:
      conversationId ||
      null,

    cod_validation:
      cod,

    blockers: [
      ...(ORDER_MODE !== "live"
        ? [
            "order_mode_is_test"
          ]
        : []),

      ...(SKINPARA_DELIVERY_PROVIDER ===
        "manual"
        ? [
            "delivery_provider_manual"
          ]
        : []),

      ...(!SKINPARA_REWARDS_ENABLED
        ? [
            "rewards_disabled"
          ]
        : [])
    ]
  };
}


// =========================================================
// OPS SUMMARY
// =========================================================


async function getSkinParaOpsSummary() {

  let queue = null;
  let processing = null;


  try {

    queue =
      Number(
        await redisCommand([
          "LLEN",
          QUEUE_NAME
        ])
      );


    processing =
      Number(
        await redisCommand([
          "LLEN",
          processingQueueName()
        ])
      );

  } catch (_) {}


  return {

    ok:
      true,

    timestamp:
      new Date()
        .toISOString(),

    order_mode:
      ORDER_MODE,

    workers:
      WORKER_COUNT,

    queue_length:
      queue,

    processing_length:
      processing,

    delivery_provider:
      SKINPARA_DELIVERY_PROVIDER,

    rewards_enabled:
      SKINPARA_REWARDS_ENABLED,

    foundation: {
      reliable_queue:
        true,

      durable_dedup:
        true,

      debounce:
        true,

      crash_recovery:
        true
    }
  };
}




// =========================================================
// HUMAN HANDOFF
// =========================================================

async function handleHumanHandoff(
  conversationId
) {
  await addConversationLabel(
    conversationId,
    "human-handoff"
  );

  await sendChatwootMessage(
    conversationId,
    "غادي نحول طلبك لواحد من مستشاري SkinPara باش يكمل معاك بأمان."
  );
}


// =========================================================
// MAIN MESSAGE PROCESSOR
// =========================================================

async function processIncomingMessage(
  payload
) {
  if (
    payload.event !==
    "message_created"
  ) {
    return {
      ignored: true,
      reason: "not_message_created"
    };
  }

  if (
    payload.message_type !== "incoming" &&
    payload.message_type !== 0
  ) {
    return {
      ignored: true,
      reason: "not_incoming"
    };
  }

  if (payload.private === true) {
    return {
      ignored: true,
      reason: "private_message"
    };
  }

  const messageId =
    Number(payload.id);

  const conversationId =
    Number(
      payload.conversation?.id ||
      payload.conversation_id
    );

  const contact =
    payload.sender?.type === "contact"
      ? payload.sender
      : payload.contact ||
        payload.sender ||
        {};

  const contactId =
    Number(
      contact.id ||
      payload.contact?.id
    );

  const userMessage =
    cleanText(payload.content);

  // MEDICAL PRE-GUARD
  if (userMessage && /(حروق|حمرا|يضرني|ألم|حساسية مفرطة|تهيج|ضيق تنفس|تنفس|تورم|burn|pain|swelling|breathing|reaction)/i.test(userMessage)) {
    log("medical_pre_guard_triggered", { conversationId });
    const safeMsg = "فهمتك، والسلامة هي الأهم. ما نقدرش نشخص الحالة ولا نعطي علاج هنا. خاص التواصل بسرعة مع صيدلي أو طبيب جلدية، وإذا كانت صعوبة فالتنفس أو تورم قوي فالوجه أو العينين خاص مساعدة طبية مستعجلة فوراً. غادي نحول المحادثة لمستشار من SkinPara.";
    await sendChatwootMessage(conversationId, safeMsg);
    await addConversationLabel(conversationId, "human-handoff").catch(() => {});
    return { ok: true, reason: "medical_pre_guard_triggered" };
  }

  if (
    !messageId ||
    !conversationId ||
    !userMessage
  ) {
    return {
      ignored: true,
      reason: "missing_required_data"
    };
  }

  const eventKey =
    `${conversationId}:${messageId}`;

  if (
    processedMessages.has(eventKey)
  ) {
    return {
      ignored: true,
      reason: "duplicate"
    };
  }

  processedMessages.set(
    eventKey,
    Date.now()
  );

  setTimeout(() => {
    processedMessages.delete(eventKey);
  }, 1000 * 60 * 60);

  log("incoming_message", {
    messageId,
    conversationId,
    contactId,
    content: userMessage.slice(0, 300)
  });

  await Promise.allSettled([
    recordAutomationEvent({
      eventType: "new_lead",
      eventKey: `new_lead:contact:${contactId}`,
      sourceEventId: `contact:${contactId}`,
      contactId,
      conversationId,
      payload: { source: "chatwoot" }
    }),
    recordAutomationEvent({
      eventType: "customer_replied",
      eventKey: `customer_replied:message:${messageId}`,
      sourceEventId: `message:${messageId}`,
      contactId,
      conversationId,
      payload: { source: "chatwoot" }
    })
  ]);

  if (isHumanRequest(userMessage)) {
    await handleHumanHandoff(
      conversationId
    );

    return {
      handled: true,
      mode: "human_handoff"
    };
  }

  if (!AI_ENABLED) {
    return {
      handled: false,
      reason: "ai_disabled"
    };
  }

  await addConversationLabel(
    conversationId,
    "ai-conversation"
  );

  const [
    history,
    products,
    memory,
    catalogResult
  ] = await Promise.all([
    getConversationMessages(
      conversationId
    ).catch(() => []),

    searchProducts(
      userMessage
    ),

    getCustomerMemory(
      contactId
    ),

    catalogRag.searchCatalog(
      userMessage
    ).catch(error => ({
      ok: false,
      fallback: true,
      products: [],
      reason: error?.message || "catalog_search_error"
    }))
  ]);

  log("catalog_rag_result", {
    conversation_id: conversationId,
    ok: Boolean(catalogResult?.ok),
    fallback: Boolean(catalogResult?.fallback),
    intent:
      catalogResult?.intelligence?.intent ||
      null,
    product_count:
      Array.isArray(catalogResult?.products)
        ? catalogResult.products.length
        : 0,
    direct_request:
      Boolean(catalogResult?.direct_request),
    reason:
      catalogResult?.reason ||
      null
  });

  const catalogContext =
    catalogRag.buildCatalogContext(
      catalogResult
    );

  const assistantMessage =
    await callAI({
      userMessage,
      history,
      products,
      memory,
      catalogContext,
      customerName:
        contact.name ||
        payload.contact?.name ||
        null
    });

  await sendChatwootMessage(
    conversationId,
    assistantMessage
  );

  const summary =
    `Customer said: ${userMessage.slice(0, 500)} | AI replied: ${assistantMessage.slice(0, 500)}`;

  await Promise.allSettled([
    upsertCustomerMemory(
      {
        id: contactId,
        name:
          contact.name ||
          payload.contact?.name,
        phone_number:
          contact.phone_number ||
          payload.contact?.phone_number
      },
      summary,
      products.slice(0, 3)
    ),

    saveAIEvent({
      eventKey,
      conversationId,
      messageId,
      contactId,
      userMessage,
      assistantMessage,
      products:
        products.slice(0, 3)
    })
  ]);

  return {
    handled: true,
    conversationId,
    productsFound:
      products.length
  };
}


// =========================================================
// TESTS
// =========================================================

async function testShopify() {
  try {
    const products =
      await searchProducts(
        "cleanser serum moisturizer"
      );

    return {
      ok: true,
      products_found:
        products.length,
      products:
        products.slice(0, 3)
          .map(x => ({
            title: x.title,
            vendor: x.vendor,
            variants:
              x.variants.length
          }))
    };

  } catch (e) {
    return {
      ok: false,
      error: e.message
    };
  }
}

async function testChatwoot() {
  try {
    const data =
      await chatwootFetch(
        "/contacts"
      );

    return {
      ok: true,
      contacts:
        data?.meta?.count || 0
    };

  } catch (e) {
    return {
      ok: false,
      error: e.message
    };
  }
}

async function testSupabase() {
  if (!SUPABASE_ENABLED) {
    return {
      ok: false,
      reason:
        "supabase_disabled"
    };
  }

  try {
    await supabaseFetch(
      "/skinpara_customer_memory?limit=1"
    );

    await supabaseFetch(
      "/skinpara_ai_events?limit=1"
    );

    return {
      ok: true,
      schema_ready: true
    };

  } catch (e) {
    return {
      ok: false,
      schema_ready: false,
      error: e.message,
      next:
        "Run skinpara-bridge/supabase-schema.sql in Supabase SQL Editor"
    };
  }
}

async function testOpenRouter() {
  try {
    const response = await safeFetch(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        method: "POST",

        headers: {
          "Authorization":
            `Bearer ${OPENROUTER_API_KEY}`,

          "Content-Type":
            "application/json"
        },

        body: JSON.stringify({
          model:
            OPENROUTER_MODEL,

          messages: [
            {
              role: "user",
              content:
                "Reply only with: SKINPARA_AI_OK"
            }
          ],

          max_tokens: 300
        })
      }
    );

    const data =
      await response.json();

    return {
      ok:
        response.ok &&
        Boolean(
          data.choices?.[0]
            ?.message?.content
        ),

      model:
        OPENROUTER_MODEL,

      reply:
        data.choices?.[0]
          ?.message?.content || null
    };

  } catch (e) {
    return {
      ok: false,
      error: e.message
    };
  }
}


// =========================================================
// HTTP SERVER
// =========================================================

const server =
  http.createServer(
    async (req, res) => {
      try {

        if (
          req.method === "GET" &&
          req.url === "/health"
        ) {
          return sendJson(
            res,
            200,
            {
              status: "ok",
              service:
                "skinpara-ai-bridge",

              ai_enabled:
                AI_ENABLED,

              shopify_enabled:
                SHOPIFY_ENABLED,

              supabase_enabled:
                SUPABASE_ENABLED,

              model:
                OPENROUTER_MODEL,

              channel_mode:
                CHANNEL_MODE
            }
          );
        }

        if (
          req.method === "POST" &&
          req.url === "/internal/kapso-direct/inbound"
        ) {
          if (CHANNEL_MODE !== "kapso_direct") return sendJson(res, 404, { ok: false, error: "direct_mode_disabled" });
          if (!DIRECT_INTERNAL_TOKEN || req.headers["x-skinpara-internal-token"] !== DIRECT_INTERNAL_TOKEN) {
            return sendJson(res, 401, { ok: false, error: "unauthorized" });
          }
          let rawBody;
          try { rawBody = await readRawBody(req, 64 * 1024); }
          catch { return sendJson(res, 400, { ok: false, error: "invalid_request" }); }
          let payload;
          try { payload = JSON.parse(rawBody.toString("utf8")); }
          catch { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }
          const job = await enqueueDirectJob(payload);
          return sendJson(res, 202, { ok: true, queued: true, job_id: job.id });
        }

        if (
          req.method === "GET" &&
          req.url === "/queue/status"
        ) {
          return sendJson(
            res,
            200,
            await getQueueStatus()
          );
        }



        // ==================================================
        // STOCK + ORDER ENGINE V1 ROUTES
        // ==================================================


        if (
          req.method === "GET" &&
          req.url.startsWith(
            "/stock/status"
          )
        ) {

          const url =
            new URL(
              req.url,
              "http://localhost"
            );

          const conversationId =
            Number(
              url.searchParams.get(
                "conversation_id"
              )
            );


          if (!conversationId) {

            return sendJson(
              res,
              400,
              {
                ok:
                  false,

                error:
                  "conversation_id_required"
              }
            );
          }


          return sendJson(
            res,
            200,
            await getStockOrderStatus(
              conversationId
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/stock/pending"
        ) {

          const raw =
            await readBody(
              req
            );

          const input =
            JSON.parse(
              raw || "{}"
            );


          return sendJson(
            res,
            200,
            await markPendingStock(
              input
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/stock/confirm"
        ) {

          const raw =
            await readBody(
              req
            );

          const input =
            JSON.parse(
              raw || "{}"
            );


          return sendJson(
            res,
            200,
            await confirmStockAndCreateOrder(
              input.conversation_id
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/stock/reject"
        ) {

          const raw =
            await readBody(
              req
            );

          const input =
            JSON.parse(
              raw || "{}"
            );


          return sendJson(
            res,
            200,
            await rejectStock(
              input.conversation_id,
              input.reason
            )
          );
        }




        // ==================================================
        // SKINPARA MASTER COMPLETION V1 ROUTES
        // ==================================================


        if (
          req.method === "GET" &&
          req.url.startsWith(
            "/ops/summary"
          )
        ) {

          return sendJson(
            res,
            200,
            await getSkinParaOpsSummary()
          );
        }


        if (
          req.method === "GET" &&
          req.url.startsWith(
            "/order/live-readiness"
          )
        ) {

          const url =
            new URL(
              req.url,
              "http://localhost"
            );

          const conversationId =
            Number(
              url.searchParams.get(
                "conversation_id"
              ) ||
              0
            );


          return sendJson(
            res,
            200,
            await getLiveReadiness(
              conversationId ||
              null
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/order/customer-data"
        ) {

          const raw =
            await readBody(
              req
            );

          return sendJson(
            res,
            200,
            await updateOrderCustomerData(
              JSON.parse(
                raw || "{}"
              )
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/supplier/confirm"
        ) {

          const raw =
            await readBody(
              req
            );

          const input =
            JSON.parse(
              raw || "{}"
            );


          return sendJson(
            res,
            200,
            await safeConfirmStockAndOrder(
              input.conversation_id
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/supplier/reject"
        ) {

          const raw =
            await readBody(
              req
            );

          const input =
            JSON.parse(
              raw || "{}"
            );


          return sendJson(
            res,
            200,
            await rejectStock(
              input.conversation_id,
              input.reason
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/order/ready-to-ship"
        ) {

          const raw =
            await readBody(
              req
            );

          const input =
            JSON.parse(
              raw || "{}"
            );


          return sendJson(
            res,
            200,
            await markReadyToShip(
              input.conversation_id
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/order/shipped"
        ) {

          const raw =
            await readBody(
              req
            );


          return sendJson(
            res,
            200,
            await markOrderShipped(
              JSON.parse(
                raw || "{}"
              )
            )
          );
        }


        if (
          req.method === "POST" &&
          req.url ===
            "/order/delivered"
        ) {

          const raw =
            await readBody(
              req
            );

          const input =
            JSON.parse(
              raw || "{}"
            );


          return sendJson(
            res,
            200,
            await markOrderDelivered(
              input.conversation_id
            )
          );
        }


        if (
          req.method === "GET" &&
          req.url === "/test/chatwoot"
        ) {
          return sendJson(
            res,
            200,
            await testChatwoot()
          );
        }

        if (
          req.method === "GET" &&
          req.url === "/test/shopify"
        ) {
          return sendJson(
            res,
            200,
            await testShopify()
          );
        }

        if (
          req.method === "GET" &&
          req.url === "/test/supabase"
        ) {
          return sendJson(
            res,
            200,
            await testSupabase()
          );
        }

        if (
          req.method === "GET" &&
          req.url === "/test/openrouter"
        ) {
          return sendJson(
            res,
            200,
            await testOpenRouter()
          );
        }

        if (
          req.method === "POST" &&
          req.url === "/webhooks/chatwoot"
        ) {
          let rawBody;

          try {
            rawBody = await readRawBody(
              req,
              CHATWOOT_WEBHOOK_SECURITY.maxBodyBytes
            );
          } catch (error) {
            const tooLarge =
              error.message === "payload_too_large";

            log("chatwoot_webhook_malformed", {
              reason: tooLarge
                ? "payload_too_large"
                : "body_read_failed"
            });

            return sendJson(
              res,
              tooLarge ? 413 : 400,
              {
                ok: false,
                error: tooLarge
                  ? "payload_too_large"
                  : "invalid_request"
              }
            );
          }

          const verification =
            verifyChatwootWebhook({
              rawBody,
              headers: req.headers,
              config: CHATWOOT_WEBHOOK_SECURITY
            });

          if (!verification.ok) {
            const expired =
              verification.code === "expired" ||
              verification.code === "future_timestamp";

            log(
              expired
                ? "chatwoot_webhook_expired"
                : "chatwoot_webhook_auth_failed",
              { reason: verification.code }
            );

            return sendJson(
              res,
              verification.status,
              {
                ok: false,
                error: "webhook_auth_failed"
              }
            );
          }

          if (!verification.authDisabled) {
            let claimed;

            try {
              claimed =
                await claimChatwootWebhookReplay({
                  redisCommand,
                  replayDigest:
                    verification.replayDigest,
                  maxAgeSec:
                    CHATWOOT_WEBHOOK_SECURITY.maxAgeSec
                });
            } catch {
              log("chatwoot_webhook_auth_failed", {
                correlationId:
                  verification.correlationId,
                reason: "replay_store_unavailable"
              });

              return sendJson(res, 503, {
                ok: false,
                error: "webhook_security_unavailable"
              });
            }

            if (!claimed) {
              log("chatwoot_webhook_replay", {
                correlationId:
                  verification.correlationId,
                reason: "duplicate_request"
              });

              return sendJson(res, 200, {
                ok: true,
                queued: false,
                duplicate: true
              });
            }

            log("chatwoot_webhook_auth_success", {
              correlationId:
                verification.correlationId,
              timestamp:
                verification.timestamp
            });
          }

          const raw = rawBody.toString("utf8");

          let payload;

          try {
            payload =
              JSON.parse(raw || "{}");
          } catch {
            log("chatwoot_webhook_malformed", {
              correlationId:
                verification.correlationId,
              reason: "invalid_json"
            });

            return sendJson(
              res,
              400,
              {
                ok: false,
                error:
                  "invalid_json"
              }
            );
          }

          if (
            !shouldQueueWebhook(
              payload
            )
          ) {
            sendJson(
              res,
              200,
              {
                ok: true,
                queued: false,
                ignored: true
              }
            );

            return;
          }

          try {
            const job =
              await enqueueAIJob(
                payload,
                0
              );

            sendJson(
              res,
              200,
              {
                ok: true,
                queued: true,
                job_id: job.id
              }
            );

          } catch (error) {
            log(
              "queue_enqueue_error",
              {
                message:
                  error.message
              }
            );

            return sendJson(
              res,
              503,
              {
                ok: false,
                queued: false,
                error:
                  "queue_unavailable"
              }
            );
          }

          return;
        }

        return sendJson(
          res,
          404,
          {
            error:
              "not_found"
          }
        );

      } catch (error) {
        log(
          "server_error",
          {
            message:
              error.message,

            stack:
              error.stack
          }
        );

        return sendJson(
          res,
          500,
          {
            ok: false,
            error:
              error.message
          }
        );
      }
    }
  );

startQueueWorkers();
if (CHANNEL_MODE === "kapso_direct") {
  directQueueWorker().catch(error => log("direct_queue_worker_fatal", { error: error.message }));
}

if (!CHATWOOT_WEBHOOK_SECURITY.enabled) {
  log("chatwoot_webhook_auth_disabled", {
    reason: "explicit_compatibility_mode"
  });
} else if (!CHATWOOT_WEBHOOK_SECURITY.secret) {
  log("chatwoot_webhook_auth_misconfigured", {
    reason: "missing_secret_fail_closed"
  });
}

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    log(
      `SkinPara AI Bridge listening on ${PORT}`
    );
  }
);

















