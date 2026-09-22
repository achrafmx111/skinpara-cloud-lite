import http from "node:http";
import pkg from './safe-fetch.cjs';
const { safeFetch: fetch } = pkg;
import fs from "node:fs";
import path from "node:path";
import { createClient } from "redis";
import {
  isSandboxGated,
  validateAllowedTestNumber,
  verifyKapsoSignature,
  parseKapsoInbound,
  deduplicateKapsoMessage,
  translateKapsoToChatwoot,
  translateChatwootToKapso,
  buildKapsoOutboundRequest
} from "./kapso-adapter.js";
import { createAutomationEngine } from "./automation-engine.js";
import { createCampaignEngine } from "./campaign-engine.js";
import { createWhatsAppProvider, normalizeMoroccanPhone, sessionWindowDecision } from "./whatsapp-provider.js";
import { createDeliveryService } from "./delivery-provider.js";
import { createAnalyticsService } from "./analytics-service.js";
import { buildDirectInboundJob, directOutboundGate } from "./direct-channel.js";

const PORT =
  Number(process.env.PORT || 8790);

const CHATWOOT_BASE_URL =
  process.env.CHATWOOT_INTERNAL_URL ||
  process.env.CHATWOOT_BASE_URL ||
  "http://rails:3000";

const CHATWOOT_ACCOUNT_ID =
  Number(
    process.env.CHATWOOT_ACCOUNT_ID || 1
  );

const CHATWOOT_API_TOKEN =
  process.env.CHATWOOT_API_TOKEN || "";

const BRIDGE_URL =
  process.env.SKINPARA_BRIDGE_URL ||
  "http://skinpara-ai-bridge:8787";

const CHANNEL_MODE = process.env.SKINPARA_CHANNEL_MODE || "chatwoot";
const DIRECT_INTERNAL_TOKEN = process.env.SKINPARA_DIRECT_INTERNAL_TOKEN || "";
const DIRECT_OUTBOUND_QUEUE = process.env.SKINPARA_DIRECT_OUTBOUND_QUEUE || "skinpara:cloud-lite:kapso-direct:outbound";
const DIRECT_CUSTOMER_KEY_SECRET = process.env.SKINPARA_DIRECT_CUSTOMER_KEY_SECRET || "";

const REDIS_URL =
  process.env.REDIS_URL ||
  "redis://redis:6379";

const SUPABASE_URL =
  String(process.env.SUPABASE_URL || "")
    .replace(/\/+$/, "");

const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const SUPABASE_ENABLED =
  process.env.SKINPARA_SUPABASE_ENABLED === "true" &&
  Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);

const PUBLIC_DIR =
  path.resolve("./public");


const redis =
  createClient({
    url: REDIS_URL
  });


redis.on(
  "error",
  err =>
    console.error(
      "[redis]",
      err.message
    )
);


await redis.connect();

async function supabaseFetch(
  endpoint,
  options = {}
) {
  if (!SUPABASE_ENABLED) {
    throw new Error("supabase_disabled");
  }

  const response = await fetch(
    `${SUPABASE_URL}/rest/v1${endpoint}`,
    {
      ...options,
      headers: {
        "Content-Type": "application/json",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization":
          `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        ...(options.headers || {})
      }
    }
  );

  const text = await response.text();
  let data = null;

  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!response.ok) {
    throw new Error(
      `supabase_${response.status}: Redacted`
    );
  }

  return data;
}

async function getDurableCustomer360(
  contactId
) {
  if (!SUPABASE_ENABLED) {
    return null;
  }

  const id = Number(contactId);
  const [customers, stats, orders, wallets, ledger, referralsGiven, referredBy] =
    await Promise.all([
      supabaseFetch(
        `/skinpara_customers?contact_id=eq.${id}&limit=1`
      ),
      supabaseFetch(
        `/skinpara_customer_stats?contact_id=eq.${id}&limit=1`
      ),
      supabaseFetch(
        `/skinpara_orders?contact_id=eq.${id}&order=updated_at.desc`
      ),
      supabaseFetch(
        `/skinpara_wallet_accounts?contact_id=eq.${id}&limit=1`
      ),
      supabaseFetch(
        `/skinpara_wallet_ledger?contact_id=eq.${id}&order=created_at.desc&limit=100`
      ),
      supabaseFetch(
        `/skinpara_referrals?referrer_contact_id=eq.${id}&order=created_at.desc`
      ),
      supabaseFetch(
        `/skinpara_referrals?referred_contact_id=eq.${id}&order=created_at.desc`
      )
    ]);

  return {
    customer:
      Array.isArray(customers)
        ? customers[0] || null
        : null,
    stats:
      Array.isArray(stats)
        ? stats[0] || null
        : null,
    orders:
      Array.isArray(orders)
        ? orders
        : [],
    loyalty: {
      wallet: Array.isArray(wallets) ? wallets[0] || null : null,
      ledger: Array.isArray(ledger) ? ledger : [],
      referrals_given: Array.isArray(referralsGiven) ? referralsGiven : [],
      referred_by: Array.isArray(referredBy) ? referredBy : []
    }
  };
}

async function upsertDurableCustomer(
  contactId,
  fields
) {
  if (!SUPABASE_ENABLED) {
    return null;
  }

  const now = new Date().toISOString();
  const existing = await supabaseFetch(
    `/skinpara_customers?contact_id=eq.${Number(contactId)}&select=contact_id&limit=1`
  );

  if (Array.isArray(existing) && existing.length > 0) {
    return await supabaseFetch(
      `/skinpara_customers?contact_id=eq.${Number(contactId)}`,
      {
        method: "PATCH",
        headers: {
          "Prefer": "return=representation"
        },
        body: JSON.stringify({
          last_seen_at: now,
          updated_at: now,
          ...fields
        })
      }
    );
  }

  return await supabaseFetch(
    "/skinpara_customers",
    {
      method: "POST",
      headers: {
        "Prefer": "return=representation"
      },
      body: JSON.stringify({
        contact_id: Number(contactId),
        account_id: CHATWOOT_ACCOUNT_ID,
        first_seen_at: now,
        last_seen_at: now,
        updated_at: now,
        ...fields
      })
    }
  );
}


function log(
  event,
  data = {}
) {

  console.log(
    `[${new Date().toISOString()}] ${event}`,
    JSON.stringify(data)
  );
}


function sendJson(
  res,
  status,
  data
) {

  const body =
    JSON.stringify(
      data,
      null,
      2
    );

  res.writeHead(
    status,
    {
      "Content-Type":
        "application/json; charset=utf-8",

      "Cache-Control":
        "no-store"
    }
  );

  res.end(body);
}


function sendHtml(
  res,
  html
) {

  res.writeHead(
    200,
    {
      "Content-Type":
        "text/html; charset=utf-8",

      "Cache-Control":
        "no-store"
    }
  );

  res.end(html);
}



async function readRaw(req) {
  return await new Promise((resolve, reject) => {
    let chunks = [];
    req.on('data', chunk => {
      chunks.push(chunk);
      if (Buffer.concat(chunks).length > 2_000_000) reject(new Error('body_too_large'));
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

async function readJson(req) {

  return await new Promise(
    (resolve, reject) => {

      let body = "";

      req.on(
        "data",
        chunk => {

          body +=
            chunk.toString();

          if (
            body.length >
            2_000_000
          ) {

            reject(
              new Error(
                "body_too_large"
              )
            );
          }
        }
      );


      req.on(
        "end",
        () => {

          try {

            resolve(
              body
                ? JSON.parse(body)
                : {}
            );

          } catch {

            reject(
              new Error(
                "invalid_json"
              )
            );
          }
        }
      );


      req.on(
        "error",
        reject
      );
    }
  );
}


async function chatwootFetch(
  endpoint,
  options = {}
) {

  if (
    !CHATWOOT_API_TOKEN
  ) {

    throw new Error(
      "CHATWOOT_API_TOKEN_missing"
    );
  }


  const response =
    await fetch(
      `${CHATWOOT_BASE_URL}/api/v1/accounts/${CHATWOOT_ACCOUNT_ID}${endpoint}`,
      {
        ...options,

        headers: {
          "Content-Type":
            "application/json",

          "api_access_token":
            CHATWOOT_API_TOKEN,

          ...(options.headers || {})
        }
      }
    );


  const text =
    await response.text();


  let data = null;


  try {

    data =
      text
        ? JSON.parse(text)
        : {};

  } catch {

    data = {
      raw:
        text
    };
  }


  if (
    !response.ok
  ) {

    throw new Error(
      `chatwoot_${response.status}: Redacted`
    );
  }


  return data;
}


async function bridgeFetch(
  endpoint,
  options = {}
) {

  const response =
    await fetch(
      `${BRIDGE_URL}${endpoint}`,
      {
        ...options,

        headers: {
          "Content-Type":
            "application/json",

          ...(options.headers || {})
        }
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
      raw:
        text
    };
  }


  if (
    !response.ok
  ) {

    throw new Error(
      `bridge_${response.status}: Redacted`
    );
  }


  return data;
}


function normalizeLabels(
  conversation
) {

  if (
    Array.isArray(
      conversation?.labels
    )
  ) {

    return conversation.labels;
  }


  return [];
}


function contactIdFromConversation(
  conversation
) {

  return Number(
    conversation?.meta
      ?.sender?.id ||
    conversation?.contact_id ||
    0
  );
}


async function getAllConversations() {

  const output = [];


  for (
    let page = 1;
    page <= 10;
    page++
  ) {

    const data =
      await chatwootFetch(
        `/conversations?status=all&page=${page}`,
        {
          method:
            "GET"
        }
      );


    const rows =
      Array.isArray(
        data?.data?.payload
      )
        ? data.data.payload

        : Array.isArray(
            data?.payload
          )
          ? data.payload
          : [];


    output.push(
      ...rows
    );


    if (
      rows.length < 25
    ) {

      break;
    }
  }


  return output;
}


async function getConversation(
  id
) {

  return await chatwootFetch(
    `/conversations/${id}`,
    {
      method:
        "GET"
    }
  );
}


async function getConversationLabels(
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


async function mergeConversationLabels(
  conversationId,
  {
    add = [],
    remove = []
  } = {}
) {

  const current =
    await getConversationLabels(
      conversationId
    );


  const removeSet =
    new Set(remove);


  const next =
    [
      ...new Set([
        ...current.filter(
          item =>
            !removeSet.has(item)
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


async function sendConversationMessage(
  conversationId,
  content
) {

  return await chatwootFetch(
    `/conversations/${conversationId}/messages`,
    {
      method:
        "POST",

      body:
        JSON.stringify({
          content,
          message_type:
            "outgoing",
          private:
            false
        })
    }
  );
}


function samePhoneNumber(left, right) {
  const leftDigits = String(left || "").replace(/\D/g, "");
  const rightDigits = String(right || "").replace(/\D/g, "");
  if (!leftDigits || !rightDigits) return false;
  if (leftDigits === rightDigits) return true;
  const suffixLength = Math.min(9, leftDigits.length, rightDigits.length);
  return leftDigits.slice(-suffixLength) === rightDigits.slice(-suffixLength);
}


async function insertKapsoInboundIntoChatwoot(inboundData) {
  let contact = null;
  const configuredContactId = Number(process.env.KAPSO_SANDBOX_CHATWOOT_CONTACT_ID || 0);
  const configuredConversationId = Number(process.env.KAPSO_SANDBOX_CHATWOOT_CONVERSATION_ID || 0);

  if (configuredContactId > 0) {
    const response = await chatwootFetch(`/contacts/${configuredContactId}`, { method: "GET" });
    const configuredContact = response?.payload?.contact || response?.payload || response;
    if (!configuredContact?.id) throw new Error("kapso_chatwoot_contact_not_found");
    contact = configuredContact;
  }

  for (let page = 1; page <= 10 && !contact; page++) {
    const response = await chatwootFetch(`/contacts?page=${page}`, { method: "GET" });
    const contacts = Array.isArray(response?.payload)
      ? response.payload
      : response?.payload?.contacts || [];

    contact = contacts.find(
      item =>
        samePhoneNumber(item?.phone_number, inboundData.senderPhone) ||
        samePhoneNumber(item?.phone_number, process.env.KAPSO_SANDBOX_ALLOWED_TO)
    ) || null;
    if (contacts.length === 0) break;
  }

  if (!contact?.id) throw new Error("kapso_chatwoot_contact_not_found");

  const response = await chatwootFetch(`/contacts/${contact.id}/conversations`, { method: "GET" });
  const conversations = Array.isArray(response?.payload)
    ? response.payload
    : response?.payload?.conversations || [];

  const conversation =
    conversations.find(item => configuredConversationId > 0 && Number(item?.id) === configuredConversationId) ||
    conversations.find(item => item?.status === "open" && item?.meta?.channel === "Channel::Api") ||
    conversations.find(item => item?.status === "open" && item?.channel === "Channel::Api") ||
    conversations.find(item => item?.status === "open");

  if (!conversation?.id) throw new Error("kapso_chatwoot_conversation_not_found");

  const message = await chatwootFetch(`/conversations/${conversation.id}/messages`, {
    method: "POST",
    body: JSON.stringify(translateKapsoToChatwoot(inboundData))
  });

  return {
    contactId: contact.id,
    conversationId: conversation.id,
    messageId: message?.id || message?.payload?.id || null
  };
}

async function enqueueKapsoDirectInbound(inboundData) {
  if (!DIRECT_INTERNAL_TOKEN || !DIRECT_CUSTOMER_KEY_SECRET) throw new Error("kapso_direct_secrets_missing");
  const job = buildDirectInboundJob(inboundData, DIRECT_CUSTOMER_KEY_SECRET);
  const response = await bridgeFetch("/internal/kapso-direct/inbound", {
    method: "POST",
    headers: { "x-skinpara-internal-token": DIRECT_INTERNAL_TOKEN },
    body: JSON.stringify(job)
  });
  if (!response?.queued) throw new Error("kapso_direct_not_queued");
  return { jobId: response.job_id, customerKey: job.customerKey, conversationKey: job.conversationKey };
}

async function sendDirectKapsoEvent(event) {
  const gate = directOutboundGate(event);
  if (!gate.ok) return gate;
  if (!validateAllowedTestNumber(process.env.KAPSO_SANDBOX_ALLOWED_TO)) return { ok: false, reason: "not_allowed_test_number" };

  const isNew = await deduplicateKapsoMessage(redis, `direct-out:${event.eventKey}`);
  if (!isNew) return { ok: true, ignored: true, reason: "duplicate_outbound" };
  // Product-card transport is fail-closed: render only exact verified rows.
  const verifiedProducts = Array.isArray(event.verifiedProducts)
    ? event.verifiedProducts.filter(product => product && product.title).slice(0, 2)
    : [];
  const product = verifiedProducts[0] || null;
  const hasVerifiedImage = Boolean(product?.image_url && /^https:\/\//i.test(String(product.image_url)));
  const hasVerifiedPrice = product?.price !== null && product?.price !== undefined && String(product.price).trim();
  const priceText = hasVerifiedPrice ? `\nPrix: ${String(product.price).trim()} MAD` : "";
  const productBody = product
    ? `${product.title}${priceText}\n\n${event.content}`.trim()
    : event.content;

  // Kapso supports WhatsApp interactive button messages. Use them for a
  // verified product selection; never attach an unverified URL/price.
  // "Acheter maintenant" is an intent button only: ORDER_MODE remains TEST and
  // no Shopify order is created by this transport layer.
  const kapsoPayload = product
    ? {
        messaging_product: "whatsapp",
        to: process.env.KAPSO_SANDBOX_ALLOWED_TO,
        type: "interactive",
        interactive: {
          type: "button",
          ...(hasVerifiedImage ? {
            header: {
              type: "image",
              image: { link: String(product.image_url) }
            }
          } : {}),
          body: { text: productBody.slice(0, 1024) },
          action: {
            buttons: [
              { type: "reply", reply: { id: "skinpara_buy_now", title: "Acheter maintenant" } },
              { type: "reply", reply: { id: "skinpara_more_info", title: "Voir plus" } },
              { type: "reply", reply: { id: "skinpara_back_selection", title: "Retour" } }
            ]
          }
        }
      }
    : {
        messaging_product: "whatsapp",
        to: process.env.KAPSO_SANDBOX_ALLOWED_TO,
        text: { body: event.content },
        type: "text"
      };
  const requestDetails = buildKapsoOutboundRequest(kapsoPayload, process.env.KAPSO_SANDBOX_PHONE_NUMBER_ID, process.env.KAPSO_API_KEY);
  try {
    const response = await fetch(requestDetails.url, requestDetails);
    if (!response.ok) throw new Error(`kapso_${response.status}`);
    return { ok: true, status: "kapso_outbound_sent" };
  } catch (error) {
    await redis.del(`kapso:sandbox:dedupe:direct-out:${event.eventKey}`).catch(() => {});
    throw error;
  }
}

async function directOutboundWorker() {
  console.log("[KAPSO DIRECT] Outbound worker started with sandbox guards enabled.");
  while (true) {
    const raw = await redis.lPop(DIRECT_OUTBOUND_QUEUE).catch(() => null);
    if (!raw) { await new Promise(resolve => setTimeout(resolve, 500)); continue; }
    try {
      const event = JSON.parse(raw);
      const result = await sendDirectKapsoEvent(event);
      if (!result.ok && !result.ignored) throw new Error(result.reason);
    } catch (error) {
      await redis.lPush(DIRECT_OUTBOUND_QUEUE, raw).catch(() => {});
      console.error(`[KAPSO DIRECT] Outbound paused: ${error?.message || "unknown_error"}`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
}


// ============================================================
// PIPELINE
// ============================================================

const PIPELINE_STAGES = [
  {
    id:
      "new",
    title:
      "New Leads",
    labels: [
      "new-lead"
    ]
  },

  {
    id:
      "interested",
    title:
      "Interested",
    labels: [
      "interested"
    ]
  },

  {
    id:
      "pending_stock",
    title:
      "Pending Stock",
    labels: [
      "pending-stock"
    ]
  },

  {
    id:
      "confirmed",
    title:
      "Stock Confirmed",
    labels: [
      "stock-confirmed"
    ]
  },

  {
    id:
      "ordered",
    title:
      "Order Created",
    labels: [
      "order-created"
    ]
  },

  {
    id:
      "shipping",
    title:
      "Shipping",
    labels: [
      "ready-to-ship",
      "shipped"
    ]
  },

  {
    id:
      "delivered",
    title:
      "Delivered",
    labels: [
      "delivered"
    ]
  }
];


function detectPipelineStage(
  conversation
) {

  const labels =
    normalizeLabels(
      conversation
    );


  const priority = [
    "delivered",
    "shipping",
    "ordered",
    "confirmed",
    "pending_stock",
    "interested",
    "new"
  ];


  for (
    const stageId
    of priority
  ) {

    const stage =
      PIPELINE_STAGES.find(
        item =>
          item.id ===
          stageId
      );


    if (
      stage.labels.some(
        label =>
          labels.includes(
            label
          )
      )
    ) {

      return stageId;
    }
  }


  return "new";
}


async function getPipeline() {

  const conversations =
    await getAllConversations();


  const stages = {};


  for (
    const stage
    of PIPELINE_STAGES
  ) {

    stages[
      stage.id
    ] = {
      ...stage,
      conversations:
        []
    };
  }


  for (
    const conversation
    of conversations
  ) {

    const stage =
      detectPipelineStage(
        conversation
      );


    stages[
      stage
    ].conversations.push({
      id:
        conversation.id,

      contact_id:
        contactIdFromConversation(
          conversation
        ),

      name:
        conversation?.meta
          ?.sender?.name ||
        `Conversation ${conversation.id}`,

      labels:
        normalizeLabels(
          conversation
        ),

      status:
        conversation.status,

      last_activity_at:
        conversation.last_activity_at,

      custom_attributes:
        conversation.custom_attributes ||
        {}
    });
  }


  return {
    ok:
      true,

    total:
      conversations.length,

    stages:
      Object.values(
        stages
      )
  };
}


// ============================================================
// CUSTOMER 360
// ============================================================

async function getWallet(
  contactId
) {

  if (SUPABASE_ENABLED) {
    try {
      const rows = await supabaseFetch(
        `/skinpara_wallet_accounts?contact_id=eq.${Number(contactId)}&select=balance&limit=1`
      );

      if (Array.isArray(rows) && rows[0]) {
        return Number(rows[0].balance || 0);
      }

      return 0;
    } catch (error) {
      log("durable_wallet_read_fallback", {
        contact_id: Number(contactId),
        error: error.message
      });
    }
  }

  const raw =
    await redis.hGet(
      "skinpara:cc:wallets",
      String(contactId)
    );


  return Number(
    raw || 0
  );
}


async function getCustomer360(
  contactId
) {

  const contact =
    await chatwootFetch(
      `/contacts/${contactId}`,
      {
        method:
          "GET"
      }
    );


  let conversations = [];


  try {

    const result =
      await chatwootFetch(
        `/contacts/${contactId}/conversations`,
        {
          method:
            "GET"
        }
      );


    conversations =
      Array.isArray(
        result?.payload
      )
        ? result.payload
        : [];

  } catch {

    const all =
      await getAllConversations();


    conversations =
      all.filter(
        conv =>
          contactIdFromConversation(
            conv
          ) ===
          Number(contactId)
      );
  }


  const wallet =
    await getWallet(
      contactId
    );


  let delivered = 0;


  for (
    const conv
    of conversations
  ) {

    if (
      normalizeLabels(
        conv
      ).includes(
        "delivered"
      )
    ) {

      delivered++;
    }
  }


  return {
    ok:
      true,

    contact:
      contact?.payload ||
      contact,

    wallet_balance:
      wallet,

    delivered_orders:
      delivered,

    conversations:
      conversations.map(
        conv => ({
          id:
            conv.id,

          labels:
            normalizeLabels(
              conv
            ),

          custom_attributes:
            conv.custom_attributes ||
            {},

          last_activity_at:
            conv.last_activity_at
        })
      )
  };
}


// ============================================================
// SUPPLIER PANEL
// ============================================================

async function getPendingSupplierOrders() {

  const conversations =
    await getAllConversations();


  return {
    ok:
      true,

    orders:
      conversations
        .filter(
          conv =>
            normalizeLabels(
              conv
            ).includes(
              "pending-stock"
            )
        )
        .map(
          conv => ({
            conversation_id:
              conv.id,

            customer:
              conv?.meta
                ?.sender?.name ||
              "",

            contact_id:
              contactIdFromConversation(
                conv
              ),

            product:
              conv
                ?.custom_attributes
                ?.last_product ||
              "",

            total:
              conv
                ?.custom_attributes
                ?.order_total ||
              null,

            stock_status:
              conv
                ?.custom_attributes
                ?.stock_status ||
              "pending"
          })
        )
  };
}


// ============================================================
// AUTOMATION BUILDER
// ============================================================

const automationEngine = createAutomationEngine({
  supabaseFetch,
  log,
  getConversation,
  catalogUrl:
    process.env.SKINPARA_CATALOG_URL ||
    "http://skinpara-catalog-service:8792",
  allowLiveMessaging: false
});

const campaignEngine = createCampaignEngine({
  supabaseFetch,
  log,
  liveEnabled:
    process.env.SKINPARA_CAMPAIGN_LIVE_ENABLED === "true"
});

const whatsAppProvider = createWhatsAppProvider({
  supabaseFetch,
  log,
  mode: process.env.SKINPARA_WHATSAPP_PROVIDER || "disabled",
  liveEnabled:
    process.env.SKINPARA_WHATSAPP_LIVE_ENABLED === "true",
  verifyToken:
    process.env.SKINPARA_WHATSAPP_VERIFY_TOKEN || ""
});

const deliveryService = createDeliveryService({
  supabaseFetch,
  log,
  providerName: process.env.SKINPARA_DELIVERY_PROVIDER || "manual",
  liveEnabled: process.env.SKINPARA_DELIVERY_LIVE_ENABLED === "true"
});

async function safeOperational(name, work) {
  try { return { name, ok: true, data: await work() }; }
  catch (error) { return { name, ok: false, error: error.message }; }
}

const analyticsService = createAnalyticsService({
  supabaseFetch,
  getLiveConversations: getAllConversations,
  getOperationalHealth: async () => {
    const catalogUrl = process.env.SKINPARA_CATALOG_URL || "http://skinpara-catalog-service:8792";
    const items = await Promise.all([
      safeOperational("bridge", () => bridgeFetch("/health")),
      safeOperational("queue", () => bridgeFetch("/queue/status")),
      safeOperational("catalog", async () => { const r = await fetch(`${catalogUrl}/health`); if (!r.ok) throw new Error(`catalog_${r.status}`); return await r.json(); }),
      safeOperational("automation_scheduler", async () => ({ running: true, rules: (await automationEngine.listRules()).length })),
      safeOperational("campaign_scheduler", async () => ({ running: true, campaigns: (await campaignEngine.listCampaigns()).campaigns.length })),
      safeOperational("whatsapp", () => whatsAppProvider.readiness()),
      safeOperational("delivery", () => deliveryService.readiness())
    ]);
    return { source: "live_operational", mode: "LOCAL", services: Object.fromEntries(items.map(x => [x.name, x])), recent_error_count: null, limitations: ["Recent cross-service errors are not yet stored in one durable observability table"] };
  }
});

async function listAutomations() {
  return await automationEngine.listRules();
}

async function saveAutomation(input) {
  return await automationEngine.saveRule(input);
}

async function deleteAutomation(id) {
  const existing = (await listAutomations())
    .find(rule => rule.id === id);
  if (!existing) return { ok: true, id, missing: true };
  await saveAutomation({
    ...existing,
    enabled: false,
    mode: "disabled"
  });
  return { ok: true, id, disabled: true };
}

async function runAutomations() {
  return await automationEngine.runOnce();
}


// ============================================================
// CAMPAIGN CENTER
// ============================================================

const CAMPAIGNS_HASH =
  "skinpara:cc:campaigns";


async function listCampaignsLegacy() {

  const rows =
    await redis.hGetAll(
      CAMPAIGNS_HASH
    );


  return Object.values(
    rows
  ).map(
    raw => {

      try {
        return JSON.parse(raw);
      } catch {
        return null;
      }
    }
  ).filter(Boolean);
}


async function saveCampaignLegacy(
  input
) {

  const id =
    String(
      input.id ||
      `campaign_${Date.now()}`
    );


  const campaign = {
    id,

    name:
      String(
        input.name ||
        "Campaign"
      ),

    segment_label:
      String(
        input.segment_label ||
        ""
      ),

    message:
      String(
        input.message ||
        ""
      ),

    status:
      String(
        input.status ||
        "draft"
      ),

    created_at:
      input.created_at ||
      new Date()
        .toISOString(),

    updated_at:
      new Date()
        .toISOString()
  };


  await redis.hSet(
    CAMPAIGNS_HASH,
    id,
    JSON.stringify(
      campaign
    )
  );


  return campaign;
}


async function campaignAudienceLegacy(
  campaign
) {

  const conversations =
    await getAllConversations();


  return conversations.filter(
    conversation => {

      if (
        !campaign.segment_label
      ) {

        return true;
      }


      return normalizeLabels(
        conversation
      ).includes(
        campaign.segment_label
      );
    }
  );
}


async function previewCampaignLegacy(
  id
) {

  const raw =
    await redis.hGet(
      CAMPAIGNS_HASH,
      id
    );


  if (!raw) {

    throw new Error(
      "campaign_not_found"
    );
  }


  const campaign =
    JSON.parse(raw);


  const audience =
    await campaignAudienceLegacy(
      campaign
    );


  return {
    ok:
      true,

    campaign,

    audience_count:
      audience.length,

    sample:
      audience
        .slice(
          0,
          10
        )
        .map(
          conv => ({
            conversation_id:
              conv.id,

            customer:
              conv?.meta
                ?.sender?.name ||
              ""
          })
        )
  };
}


async function executeCampaignLegacy(
  id,
  confirm
) {

  // Permanent fail-closed guard: legacy direct Chatwoot sending is retired.
  throw new Error("legacy_campaign_send_disabled");

  if (
    confirm !==
    "SEND"
  ) {

    throw new Error(
      "campaign_requires_confirm_SEND"
    );
  }


  const raw =
    await redis.hGet(
      CAMPAIGNS_HASH,
      id
    );


  if (!raw) {

    throw new Error(
      "campaign_not_found"
    );
  }


  const campaign =
    JSON.parse(raw);


  if (
    !campaign.message
  ) {

    throw new Error(
      "campaign_message_empty"
    );
  }


  const audience =
    await campaignAudienceLegacy(
      campaign
    );


  let sent = 0;
  let failed = 0;


  for (
    const conversation
    of audience.slice(
      0,
      100
    )
  ) {

    try {

      await sendConversationMessage(
        conversation.id,
        campaign.message
      );

      sent++;

    } catch {

      failed++;
    }
  }


  campaign.status =
    "sent";

  campaign.sent =
    sent;

  campaign.failed =
    failed;

  campaign.sent_at =
    new Date()
      .toISOString();


  await redis.hSet(
    CAMPAIGNS_HASH,
    id,
    JSON.stringify(
      campaign
    )
  );


  return {
    ok:
      true,
    sent,
    failed
  };
}

// Durable campaign API. The legacy Redis/send implementation above is kept
// only for rollback reference and is no longer reachable by routes.
async function listCampaigns() {
  return (await campaignEngine.listCampaigns()).campaigns;
}

async function saveCampaign(input) {
  return await campaignEngine.saveCampaign(input);
}

async function previewCampaign(id) {
  return await campaignEngine.snapshotCampaign(id);
}

async function executeCampaign(id) {
  return await campaignEngine.executeCampaign(id);
}


// ============================================================
// REFERRAL + WALLET
// ============================================================

const REFERRALS_HASH =
  "skinpara:cc:referrals";

const WALLETS_HASH =
  "skinpara:cc:wallets";


async function creditWallet(
  input
) {
  const contactId = Number(input.contact_id);
  const amount = Number(input.amount);
  if (!contactId || !Number.isFinite(amount) || amount <= 0) {
    throw new Error("invalid_wallet_credit");
  }
  return await supabaseFetch("/rpc/skinpara_wallet_post", {
    method: "POST",
    body: JSON.stringify({
      p_contact_id: contactId,
      p_type: "credit",
      p_amount: amount,
      p_dedupe_key: String(input.dedupe_key || `manual_credit:${contactId}:${Date.now()}`),
      p_reason: String(input.reason || "manual_adjustment"),
      p_source: "control_center_admin",
      p_order_id: input.order_id || null,
      p_referral_id: input.referral_id || null,
      p_metadata: { test: true },
      p_status: "test"
    })
  });
}

async function debitWallet(input) {
  const contactId = Number(input.contact_id);
  const amount = Number(input.amount);
  if (!contactId || !Number.isFinite(amount) || amount <= 0) {
    throw new Error("invalid_wallet_debit");
  }
  return await supabaseFetch("/rpc/skinpara_wallet_post", {
    method: "POST",
    body: JSON.stringify({
      p_contact_id: contactId,
      p_type: "debit",
      p_amount: -amount,
      p_dedupe_key: String(input.dedupe_key || `manual_debit:${contactId}:${Date.now()}`),
      p_reason: String(input.reason || "coupon_redemption"),
      p_source: "control_center_admin",
      p_metadata: { test: true, shopify_discount_created: false },
      p_status: "test"
    })
  });
}

async function reverseWallet(input) {
  return await supabaseFetch("/rpc/skinpara_reverse_wallet_entry", {
    method: "POST",
    body: JSON.stringify({
      p_ledger_id: Number(input.ledger_id),
      p_dedupe_key: String(input.dedupe_key || `reversal:${input.ledger_id}`),
      p_reason: String(input.reason || "reward_reversal")
    })
  });
}

async function ensureReferralCode(contactId) {
  return await supabaseFetch("/rpc/skinpara_ensure_referral_code", {
    method: "POST",
    body: JSON.stringify({ p_contact_id: Number(contactId) })
  });
}


async function registerReferral(
  input
) {
  const referred = Number(input.referred_contact_id);
  let code = input.referral_code || input.code || null;
  if (!referred) throw new Error("invalid_referral");
  if (!code && input.referrer_contact_id) {
    const generated = await ensureReferralCode(Number(input.referrer_contact_id));
    code = generated.referral_code;
  }
  if (!code) throw new Error("referral_code_required");
  return await supabaseFetch("/rpc/skinpara_assign_referral", {
    method: "POST",
    body: JSON.stringify({
      p_referred_contact_id: referred,
      p_referral_code: String(code)
    })
  });
}

async function getLoyalty(contactId) {
  const id = Number(contactId);
  const durable = await getDurableCustomer360(id);
  const ledger = durable.loyalty.ledger || [];
  return {
    ok: true,
    contact_id: id,
    wallet_balance: Number(durable.loyalty.wallet?.balance || 0),
    referral_code: durable.customer?.referral_code || null,
    total_wallet_earned: ledger.filter(x => Number(x.amount) > 0).reduce((sum, x) => sum + Number(x.amount), 0),
    total_wallet_spent: Math.abs(ledger.filter(x => x.type === "debit").reduce((sum, x) => sum + Number(x.amount), 0)),
    ledger,
    referrals_given: durable.loyalty.referrals_given,
    referred_by: durable.loyalty.referred_by
  };
}


// ============================================================
// ANALYTICS
// ============================================================

async function getAnalytics() {

  const conversations =
    await getAllConversations();


  const countLabel =
    label =>
      conversations.filter(
        conv =>
          normalizeLabels(
            conv
          ).includes(
            label
          )
      ).length;


  let revenue = 0;


  for (
    const conversation
    of conversations
  ) {

    if (
      normalizeLabels(
        conversation
      ).includes(
        "order-created"
      )
    ) {

      revenue +=
        Number(
          conversation
            ?.custom_attributes
            ?.order_total ||
          0
        );
    }
  }


  const total =
    conversations.length;

  const orders =
    countLabel(
      "order-created"
    );

  const delivered =
    countLabel(
      "delivered"
    );


  return {
    ok:
      true,

    conversations:
      total,

    new_leads:
      countLabel(
        "new-lead"
      ),

    interested:
      countLabel(
        "interested"
      ),

    pending_stock:
      countLabel(
        "pending-stock"
      ),

    stock_confirmed:
      countLabel(
        "stock-confirmed"
      ),

    orders_created:
      orders,

    ready_to_ship:
      countLabel(
        "ready-to-ship"
      ),

    shipped:
      countLabel(
        "shipped"
      ),

    delivered,

    human_handoff:
      countLabel(
        "human-handoff"
      ),

    repeat_customers:
      countLabel(
        "repeat-customer"
      ),

    vip:
      countLabel(
        "vip"
      ),

    order_value_total:
      Math.round(
        revenue * 100
      ) /
      100,

    conversation_to_order_percent:
      total
        ? Math.round(
            (
              orders /
              total *
              100
            ) * 100
          ) /
          100
        : 0,

    order_to_delivered_percent:
      orders
        ? Math.round(
            (
              delivered /
              orders *
              100
            ) * 100
          ) /
          100
        : 0
  };
}



// ============================================================
// SKINPARA CUSTOMER 360 V2
// ============================================================


function numberValue(
  value
) {

  const n =
    Number(
      value
    );

  return Number.isFinite(n)
    ? n
    : 0;
}


function uniqueValues(
  values
) {

  return [
    ...new Set(
      values.filter(Boolean)
    )
  ];
}


async function getCustomer360V2(
  contactId
) {

  contactId =
    Number(
      contactId
    );


  if (
    !contactId
  ) {

    throw new Error(
      "contact_id_required"
    );
  }


  // ----------------------------------------------------------
  // CONTACT
  // ----------------------------------------------------------

  const contactResponse =
    await chatwootFetch(
      `/contacts/${contactId}`
    );


  const contact =
    contactResponse
      ?.payload
      ?.contact ||
    contactResponse
      ?.payload ||
    contactResponse;

  let durable = null;

  try {
    durable = await getDurableCustomer360(
      contactId
    );
  } catch (error) {
    log("customer360_durable_read_fallback", {
      contact_id: contactId,
      error: error.message
    });
  }


  // ----------------------------------------------------------
  // CONVERSATIONS
  // ----------------------------------------------------------

  const allConversations =
    await getAllConversations();


  const conversations =
    allConversations
      .filter(
        conversation =>
          Number(
            contactIdFromConversation(
              conversation
            )
          ) ===
          contactId
      )
      .sort(
        (a,b) =>
          Number(
            b.id
          ) -
          Number(
            a.id
          )
      );


  // ----------------------------------------------------------
  // AGGREGATES
  // ----------------------------------------------------------

  let totalOrderValue =
    0;


  let deliveredValue =
    0;


  let orderCount =
    0;


  let deliveredOrders =
    0;


  let pendingOrders =
    0;


  let shippedOrders =
    0;


  const products =
    [];


  const shopifyOrderIds =
    [];


  const orderHistory =
    [];


  for (
    const conversation
    of conversations
  ) {

    const labels =
      normalizeLabels(
        conversation
      );


    const attrs =
      conversation
        ?.custom_attributes ||
      {};


    const orderTotal =
      numberValue(
        attrs.order_total
      );


    const hasOrder =
      labels.includes(
        "order-created"
      ) ||
      Boolean(
        attrs.shopify_order_id
      );


    const delivered =
      labels.includes(
        "delivered"
      );


    const shipped =
      labels.includes(
        "shipped"
      ) ||
      labels.includes(
        "ready-to-ship"
      );


    const pending =
      labels.includes(
        "pending-stock"
      );


    if (
      hasOrder
    ) {

      orderCount += 1;

      totalOrderValue +=
        orderTotal;
    }


    if (
      delivered
    ) {

      deliveredOrders += 1;

      deliveredValue +=
        orderTotal;
    }


    if (
      shipped &&
      !delivered
    ) {

      shippedOrders += 1;
    }


    if (
      pending
    ) {

      pendingOrders += 1;
    }


    if (
      attrs.last_product
    ) {

      products.push(
        attrs.last_product
      );
    }


    if (
      attrs.shopify_order_id
    ) {

      shopifyOrderIds.push(
        attrs.shopify_order_id
      );
    }


    if (
      hasOrder ||
      pending ||
      shipped ||
      delivered ||
      attrs.stock_status
    ) {

      orderHistory.push({

        conversation_id:
          conversation.id,

        product:
          attrs.last_product ||
          null,

        total:
          orderTotal,

        shopify_order_id:
          attrs.shopify_order_id ||
          null,

        stock_status:
          attrs.stock_status ||
          null,

        labels,

        created_at:
          conversation.created_at ||
          null,

        updated_at:
          conversation.updated_at ||
          null
      });
    }
  }


  // ----------------------------------------------------------
  // WALLET
  // ----------------------------------------------------------

  let wallet =
    0;


  try {

    const rawWallet =
      await redis.hGet(
        "skinpara:cc:wallets",
        String(
          contactId
        )
      );


    if (
      rawWallet
    ) {

      try {

        const parsed =
          JSON.parse(
            rawWallet
          );


        wallet =
          numberValue(
            parsed.balance ??
            parsed.wallet_balance ??
            parsed.amount ??
            parsed
          );

      } catch {

        wallet =
          numberValue(
            rawWallet
          );
      }
    }

  } catch (_) {}

  if (durable?.loyalty?.wallet) {
    wallet = numberValue(
      durable.loyalty.wallet.balance
    );
  }


  // ----------------------------------------------------------
  // REFERRALS
  // ----------------------------------------------------------

  const referralsGiven =
    [];


  const referredBy =
    [];


  try {

    const allReferrals =
      await redis.hGetAll(
        "skinpara:cc:referrals"
      );


    for (
      const [
        key,
        raw
      ]
      of Object.entries(
        allReferrals
      )
    ) {

      let referral;


      try {

        referral =
          JSON.parse(
            raw
          );

      } catch {

        continue;
      }


      const referrer =
        Number(
          referral
            .referrer_contact_id ??
          referral
            .referrer ??
          0
        );


      const referred =
        Number(
          referral
            .referred_contact_id ??
          referral
            .referred ??
          0
        );


      if (
        referrer ===
        contactId
      ) {

        referralsGiven.push({
          id:
            key,

          ...referral
        });
      }


      if (
        referred ===
        contactId
      ) {

        referredBy.push({
          id:
            key,

          ...referral
        });
      }
    }

  } catch (_) {}

  if (durable?.loyalty) {
    referralsGiven.splice(0, referralsGiven.length, ...(durable.loyalty.referrals_given || []));
    referredBy.splice(0, referredBy.length, ...(durable.loyalty.referred_by || []));
  }

  const walletLedger = durable?.loyalty?.ledger || [];
  const totalWalletEarned = walletLedger
    .filter(entry => numberValue(entry.amount) > 0)
    .reduce((sum, entry) => sum + numberValue(entry.amount), 0);
  const totalWalletSpent = Math.abs(walletLedger
    .filter(entry => entry.type === "debit")
    .reduce((sum, entry) => sum + numberValue(entry.amount), 0));


  // ----------------------------------------------------------
  // CUSTOMER PROFILE
  // ----------------------------------------------------------

  const customAttributes =
    contact
      ?.custom_attributes ||
    {};


  const allLabels =
    uniqueValues(
      conversations
        .flatMap(
          conversation =>
            normalizeLabels(
              conversation
            )
        )
    );


  const durableStats =
    durable?.stats || null;

  if (durableStats) {
    orderCount = Number(
      durableStats.total_orders || 0
    );
    deliveredOrders = Number(
      durableStats.delivered_orders || 0
    );
    totalOrderValue = numberValue(
      durableStats.total_order_value
    );
    deliveredValue = numberValue(
      durableStats.delivered_order_value
    );
  }

  const repeatCustomer =
    durableStats
      ? Boolean(durableStats.repeat_customer)
      : allLabels.includes(
          "repeat-customer"
        ) ||
        Boolean(
          customAttributes
            .repeat_customer
        );


  const vip =
    durableStats
      ? Boolean(durableStats.vip)
      : allLabels.includes(
          "vip"
        );


  const averageOrderValue =
    durableStats
      ? numberValue(
          durableStats.average_order_value
        )
      : orderCount > 0
        ? Number(
            (
              totalOrderValue /
              orderCount
            ).toFixed(2)
          )
        : 0;

  let deliveryShipments = [];
  try {
    deliveryShipments = await supabaseFetch(
      `/skinpara_shipments?contact_id=eq.${contactId}&order=updated_at.desc&limit=20`
    );
  } catch (error) {
    log("customer360_delivery_read_fallback", { contact_id: contactId, error: error.message });
  }


  return {

    ok:
      true,


    contact_id:
      contactId,


    profile: {

      id:
        contactId,

      name:
        contact
          ?.name ||
        null,

      email:
        contact
          ?.email ||
        null,

      phone:
        contact
          ?.phone_number ||
        null,

      skin_type:
        customAttributes
          .skin_type ||
        null,

      skin_concern:
        customAttributes
          .skin_concern ||
        null,

      preferred_language:
        customAttributes
          .preferred_language ||
        null,

      shopify_customer_id:
        customAttributes
          .shopify_customer_id ||
        null,

      referral_code:
        durable?.customer
          ?.referral_code ||
        null,

      repeat_customer:
        repeatCustomer,

      vip
    },


    metrics: {

      conversations:
        conversations.length,

      orders:
        orderCount,

      delivered_orders:
        deliveredOrders,

      cancelled_orders:
        Number(
          durableStats
            ?.cancelled_orders ||
          0
        ),

      shipped_orders:
        shippedOrders,

      pending_orders:
        pendingOrders,

      total_order_value:
        Number(
          totalOrderValue
            .toFixed(2)
        ),

      delivered_value:
        Number(
          deliveredValue
            .toFixed(2)
        ),

      average_order_value:
        averageOrderValue,

      last_order_at:
        durableStats
          ?.last_order_at ||
        null,

      last_delivered_at:
        durableStats
          ?.last_delivered_at ||
        null,

      wallet_balance:
        wallet,

      referrals_given:
        referralsGiven.length,

      referred_by_count:
        referredBy.length,

      qualified_referrals:
        referralsGiven.filter(item => ["qualified","rewarded","reversed"].includes(item.status)).length,

      rewarded_referrals:
        referralsGiven.filter(item => item.status === "rewarded").length,

      total_wallet_earned:
        Number(totalWalletEarned.toFixed(2)),

      total_wallet_spent:
        Number(totalWalletSpent.toFixed(2))
    },


    products:
      uniqueValues(
        products
      ),


    shopify_order_ids:
      uniqueValues(
        shopifyOrderIds
      ),


    labels:
      allLabels,


    referral: {

      referred_by:
        referredBy,

      referrals_given:
        referralsGiven
    },

    wallet_ledger:
      walletLedger,

    delivery: {
      shipments: deliveryShipments,
      latest: deliveryShipments[0] || null
    },


    lifetime_source:
      durableStats
        ? "supabase"
        : "chatwoot_fallback",

    order_history:
      durable?.orders?.length
        ? durable.orders.map(
            order => ({
              conversation_id:
                order.conversation_id,
              product:
                order.product_name,
              total:
                numberValue(order.order_value),
              shopify_order_id:
                order.external_order_id,
              shopify_order_name:
                order.order_name,
              status:
                order.status,
              created_at:
                order.created_at,
              updated_at:
                order.updated_at
            })
          )
        : orderHistory,


    conversations:
      conversations.map(
        conversation => ({

          id:
            conversation.id,

          labels:
            normalizeLabels(
              conversation
            ),

          custom_attributes:
            conversation
              .custom_attributes ||
            {},

          created_at:
            conversation.created_at ||
            null,

          updated_at:
            conversation.updated_at ||
            null
        })
      )
  };
}


// ============================================================
// ROUTER
// ============================================================

async function handleApi(
  req,
  res,
  url
) {

  if (
    req.method ===
      "GET" &&
    url.pathname ===
      "/api/health"
  ) {

    const bridge =
      await bridgeFetch(
        "/health"
      );


    return sendJson(
      res,
      200,
      {
        ok:
          true,

        service:
          "skinpara-control-center",

        redis:
          redis.isReady,

        bridge
      }
    );
  }

  if (req.method === "GET" && url.pathname === "/api/whatsapp/readiness") {
    return sendJson(res, 200, { ok: true, ...(await whatsAppProvider.readiness()) });
  }

  if (req.method === "GET" && url.pathname === "/api/whatsapp/webhook") {
    const challenge = whatsAppProvider.verifyWebhook(url.searchParams);
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end(challenge);
  }

    if (req.method === "POST" && url.pathname === "/api/kapso/webhook") {
    if (!isSandboxGated()) return sendJson(res, 403, { ok: false, error: "sandbox_disabled" });
    
    console.log("[KAPSO INBOUND] Received webhook");
    
    const rawBody = await readRaw(req);
    const signature = req.headers["x-webhook-signature"];
    const secret = process.env.KAPSO_WEBHOOK_SECRET;
    
    const isValidSig = verifyKapsoSignature(rawBody, signature, secret);
    console.log("[KAPSO INBOUND] Signature valid: " + isValidSig);
    
    if (!isValidSig) {
      return sendJson(res, 401, { ok: false, error: "invalid_signature" });
    }
    
    let payload;
    try {
      payload = JSON.parse(rawBody.toString("utf8"));
    } catch(e) {
      return sendJson(res, 400, { ok: false, error: "invalid_json" });
    }

    if (!payload.type && typeof req.headers["x-webhook-event"] === "string") {
      payload.type = req.headers["x-webhook-event"];
    }
    
    const parsed = parseKapsoInbound(payload);
    console.log("[KAPSO INBOUND] Parsed OK: " + parsed.ok);
    
    if (!parsed.ok) {
      console.log("[KAPSO INBOUND] Ignored. Reason: " + parsed.reason + " (Detail: " + (parsed.detail || "none") + ")");
      return sendJson(res, 200, { ok: true, ignored: true, reason: parsed.reason });
    }
    
    const { phoneNumberId, senderPhone, messageId, textContent } = parsed.data;
    
    if (phoneNumberId !== process.env.KAPSO_SANDBOX_PHONE_NUMBER_ID) {
      console.log("[KAPSO INBOUND] Ignored. Unknown sandbox ID.");
      return sendJson(res, 200, { ok: true, ignored: true, reason: "unknown_sandbox" });
    }
    
    const isAllowed = validateAllowedTestNumber(senderPhone);
    console.log("[KAPSO INBOUND] Allowlist match: " + isAllowed);
    if (!isAllowed) {
      return sendJson(res, 200, { ok: true, ignored: true, reason: "not_allowed_test_number" });
    }
    
    console.log("[KAPSO INBOUND] Message ID: " + messageId);
    console.log("[KAPSO INBOUND] Event Type: whatsapp.message.received");
    console.log("[KAPSO INBOUND] Found exact text content field successfully.");
    
    const isNew = await deduplicateKapsoMessage(redis, messageId);
    if (!isNew) {
      console.log("[KAPSO INBOUND] Ignored. Duplicate or Redis unavailable.");
      return sendJson(res, 200, { ok: true, ignored: true, reason: "duplicate_or_redis_unavailable" });
    }

    try {
      const inserted = CHANNEL_MODE === "kapso_direct"
        ? await enqueueKapsoDirectInbound(parsed.data)
        : await insertKapsoInboundIntoChatwoot(parsed.data);
      console.log(CHANNEL_MODE === "kapso_direct"
        ? `[KAPSO INBOUND] Direct AI job queued: ${inserted.jobId}.`
        : `[KAPSO INBOUND] Chatwoot insert complete. Contact ${inserted.contactId}, conversation ${inserted.conversationId}, message ${inserted.messageId || "created"}.`
      );
      console.log("[KAPSO INBOUND] SUCCESS. Message safely processed.");
      return sendJson(res, 200, { ok: true, status: "kapso_webhook_received" });
    } catch (error) {
      await redis.del(`kapso:sandbox:dedupe:${messageId}`).catch(() => {});
      console.error(`[KAPSO INBOUND] Channel delivery failed: ${error?.message || "unknown_error"}`);
      return sendJson(res, 502, { ok: false, error: "channel_inbound_failed" });
    }
  }

  if (req.method === "POST" && url.pathname === "/api/kapso/outbound") {
    if (!isSandboxGated()) return sendJson(res, 403, { ok: false, error: "sandbox_disabled" });
    
    const outboundEnabled = process.env.KAPSO_SANDBOX_OUTBOUND_ENABLED === "true";
    
    const payload = await readJson(req);
    const configuredConversationId = Number(process.env.KAPSO_SANDBOX_CHATWOOT_CONVERSATION_ID || 0);
    const conversationId = Number(payload?.conversation?.id || payload?.conversation_id || 0);
    if (!configuredConversationId || conversationId !== configuredConversationId) {
      return sendJson(res, 200, { ok: true, ignored: true, reason: "not_sandbox_conversation" });
    }

    const payloadDestination = payload?.conversation?.meta?.sender?.phone_number || payload?.sender?.phone_number;
    const isAllowed =
      validateAllowedTestNumber(process.env.KAPSO_SANDBOX_ALLOWED_TO) &&
      (!payloadDestination || validateAllowedTestNumber(payloadDestination));
    console.log("[KAPSO OUTBOUND] Destination allowlist match: " + isAllowed);
    if (!isAllowed) {
      return sendJson(res, 200, { ok: true, ignored: true, reason: "not_allowed_test_number" });
    }
    
    const kapsoReq = translateChatwootToKapso(payload, process.env.KAPSO_SANDBOX_ALLOWED_TO);
    if (!kapsoReq) {
      return sendJson(res, 200, { ok: true, ignored: true, reason: "ignored_non_outgoing_or_private" });
    }

    if (!outboundEnabled) {
      console.log("[KAPSO OUTBOUND] BLOCKED. KAPSO_SANDBOX_OUTBOUND_ENABLED is false.");
      return sendJson(res, 200, { ok: true, ignored: true, reason: "outbound_disabled" });
    }

    const outMsgId = payload.id;
    if (outMsgId) {
      const isNew = await deduplicateKapsoMessage(redis, "out:" + outMsgId);
      if (!isNew) {
        console.log("[KAPSO OUTBOUND] Ignored duplicate outbound message ID: " + outMsgId);
        return sendJson(res, 200, { ok: true, ignored: true, reason: "duplicate_outbound" });
      }
    }

    const requestDetails = buildKapsoOutboundRequest(kapsoReq, process.env.KAPSO_SANDBOX_PHONE_NUMBER_ID, process.env.KAPSO_API_KEY);
    try {
      const response = await fetch(requestDetails.url, requestDetails);
      if (!response.ok) {
        if (outMsgId) await redis.del(`kapso:sandbox:dedupe:out:${outMsgId}`).catch(() => {});
        console.error(`[KAPSO OUTBOUND] Kapso API failed with status ${response.status}.`);
        return sendJson(res, 502, { ok: false, error: "kapso_api_send_failed" });
      }

      console.log("[KAPSO OUTBOUND] Sandbox message accepted by Kapso.");
      return sendJson(res, 200, { ok: true, status: "kapso_outbound_sent" });
    } catch (error) {
      if (outMsgId) await redis.del(`kapso:sandbox:dedupe:out:${outMsgId}`).catch(() => {});
      console.error(`[KAPSO OUTBOUND] Kapso API request failed: ${error?.message || "unknown_error"}`);
      return sendJson(res, 502, { ok: false, error: "kapso_api_send_failed" });
    }
  }


  if (req.method === "POST" && url.pathname === "/api/whatsapp/webhook") {
    return sendJson(res, 200, await whatsAppProvider.handleWebhook(await readJson(req)));
  }

  if (req.method === "POST" && url.pathname === "/api/whatsapp/send/session") {
    return sendJson(res, 200, await whatsAppProvider.sendSessionMessage(await readJson(req)));
  }

  if (req.method === "POST" && url.pathname === "/api/whatsapp/send/template") {
    return sendJson(res, 200, await whatsAppProvider.sendTemplateMessage(await readJson(req)));
  }

  if (req.method === "POST" && url.pathname === "/api/whatsapp/normalize-phone") {
    const input = await readJson(req);
    return sendJson(res, 200, { ok: true, normalized: normalizeMoroccanPhone(input.phone) });
  }

  if (req.method === "POST" && url.pathname === "/api/whatsapp/session-window") {
    const input = await readJson(req);
    return sendJson(res, 200, { ok: true, ...sessionWindowDecision(input.last_inbound_at, input.at || Date.now()) });
  }

  if (req.method === "GET" && url.pathname === "/api/delivery/readiness") {
    return sendJson(res, 200, { ok: true, ...(await deliveryService.readiness()) });
  }

  if (req.method === "GET" && url.pathname === "/api/delivery/shipments") {
    return sendJson(res, 200, await deliveryService.listShipments());
  }

  if (req.method === "GET" && url.pathname.match(/^\/api\/delivery\/shipments\/\d+$/)) {
    return sendJson(res, 200, await deliveryService.getShipment(url.pathname.split("/").pop()));
  }

  if (req.method === "POST" && url.pathname === "/api/delivery/shipments") {
    return sendJson(res, 200, await deliveryService.createShipment(await readJson(req)));
  }

  if (req.method === "POST" && url.pathname === "/api/delivery/events") {
    return sendJson(res, 200, await deliveryService.handleStatusEvent(await readJson(req)));
  }

  if (req.method === "POST" && url.pathname.match(/^\/api\/delivery\/shipments\/\d+\/cancel$/)) {
    const input = await readJson(req);
    return sendJson(res, 200, await deliveryService.cancelShipment({ ...input, shipment_id: Number(url.pathname.split("/")[4]) }));
  }


  if (
    req.method ===
      "GET" &&
    url.pathname ===
      "/api/pipeline"
  ) {

    return sendJson(
      res,
      200,
      await getPipeline()
    );
  }


  if (
    req.method ===
      "GET" &&
    url.pathname.startsWith(
      "/api/customer/"
    )
  ) {

    const id =
      Number(
        url.pathname.split(
          "/"
        ).pop()
      );


    return sendJson(
      res,
      200,
      await getCustomer360(
        id
      )
    );
  }


  if (
    req.method ===
      "GET" &&
    url.pathname ===
      "/api/supplier/pending"
  ) {

    return sendJson(
      res,
      200,
      await getPendingSupplierOrders()
    );
  }


  if (
    req.method ===
      "POST" &&
    url.pathname ===
      "/api/supplier/confirm"
  ) {

    const input =
      await readJson(req);


    return sendJson(
      res,
      200,
      await bridgeFetch(
        "/supplier/confirm",
        {
          method:
            "POST",

          body:
            JSON.stringify(
              input
            )
        }
      )
    );
  }

  if (req.method === "POST" && url.pathname === "/api/wallet/debit") {
    return sendJson(res, 200, await debitWallet(await readJson(req)));
  }

  if (req.method === "POST" && url.pathname === "/api/wallet/reverse") {
    return sendJson(res, 200, await reverseWallet(await readJson(req)));
  }

  if (req.method === "GET" && url.pathname.startsWith("/api/loyalty/") &&
      url.pathname !== "/api/loyalty/rules") {
    const id = Number(url.pathname.split("/").pop());
    return sendJson(res, 200, await getLoyalty(id));
  }

  if (req.method === "POST" && url.pathname === "/api/referrals/code") {
    const input = await readJson(req);
    return sendJson(res, 200, await ensureReferralCode(input.contact_id));
  }

  if (req.method === "POST" && url.pathname === "/api/loyalty/process-delivered") {
    const input = await readJson(req);
    return sendJson(res, 200, await supabaseFetch(
      "/rpc/skinpara_process_referral_delivery",
      {
        method: "POST",
        body: JSON.stringify({
          p_event_key: String(input.event_key),
          p_order_key: String(input.order_key)
        })
      }
    ));
  }

  if (req.method === "GET" && url.pathname === "/api/loyalty/rules") {
    return sendJson(res, 200, {
      ok: true,
      rules: await supabaseFetch("/skinpara_reward_rules?order=priority.desc")
    });
  }


  if (
    req.method ===
      "POST" &&
    url.pathname ===
      "/api/supplier/reject"
  ) {

    const input =
      await readJson(req);


    return sendJson(
      res,
      200,
      await bridgeFetch(
        "/supplier/reject",
        {
          method:
            "POST",

          body:
            JSON.stringify(
              input
            )
        }
      )
    );
  }


  if (
    req.method ===
      "GET" &&
    url.pathname ===
      "/api/automations"
  ) {

    return sendJson(
      res,
      200,
      {
        ok:
          true,
        automations:
          await listAutomations()
      }
    );
  }


  if (
    req.method ===
      "POST" &&
    url.pathname ===
      "/api/automations"
  ) {

    const input =
      await readJson(req);


    return sendJson(
      res,
      200,
      {
        ok:
          true,
        automation:
          await saveAutomation(
            input
          )
      }
    );
  }


  if (
    req.method ===
      "POST" &&
    url.pathname ===
      "/api/automations/run"
  ) {

    return sendJson(
      res,
      200,
      await runAutomations()
    );
  }

  if (
    req.method === "GET" &&
    url.pathname === "/api/automations/runs"
  ) {
    return sendJson(res, 200, {
      ok: true,
      runs: await automationEngine.listRuns(
        url.searchParams.get("limit") || 100
      )
    });
  }

  if (
    req.method === "POST" &&
    url.pathname === "/api/automations/events"
  ) {
    const input = await readJson(req);
    const event = {
      ...input,
      event_key:
        String(input.event_key ||
          `synthetic:${input.event_type}:${Date.now()}`),
      event_at:
        input.event_at || new Date().toISOString()
    };
    const stored = await supabaseFetch(
      "/skinpara_automation_events?on_conflict=event_key",
      {
        method: "POST",
        headers: {
          Prefer:
            "resolution=ignore-duplicates,return=representation"
        },
        body: JSON.stringify(event)
      }
    );
    return sendJson(res, 200, {
      ok: true,
      duplicate: !stored?.length,
      decisions:
        await automationEngine.scheduleEvent(
          stored?.[0] || event
        )
    });
  }

  if (
    req.method === "PUT" &&
    url.pathname.startsWith(
      "/api/automations/preferences/"
    )
  ) {
    const contactId = Number(
      url.pathname.split("/").pop()
    );
    const input = await readJson(req);
    const rows = await supabaseFetch(
      "/skinpara_communication_preferences?on_conflict=contact_id",
      {
        method: "POST",
        headers: {
          Prefer:
            "resolution=merge-duplicates,return=representation"
        },
        body: JSON.stringify({
          contact_id: contactId,
          automation_opt_out:
            Boolean(input.automation_opt_out),
          marketing_opt_out:
            Boolean(input.marketing_opt_out),
          whatsapp_marketing_allowed:
            Boolean(input.whatsapp_marketing_allowed),
          updated_at:
            new Date().toISOString()
        })
      }
    );
    return sendJson(res, 200, {
      ok: true,
      preferences: rows?.[0] || null
    });
  }


  if (
    req.method ===
      "DELETE" &&
    url.pathname.startsWith(
      "/api/automations/"
    )
  ) {

    const id =
      decodeURIComponent(
        url.pathname
          .split("/")
          .pop()
      );


    return sendJson(
      res,
      200,
      await deleteAutomation(
        id
      )
    );
  }


  if (
    req.method ===
      "GET" &&
    url.pathname ===
      "/api/campaigns"
  ) {

    return sendJson(
      res,
      200,
      {
        ok:
          true,
        campaigns:
          await listCampaigns()
      }
    );
  }

  if (req.method === "GET" && url.pathname === "/api/campaign-center") {
    return sendJson(res, 200, { ok: true, ...(await campaignEngine.listCampaigns()) });
  }

  if (req.method === "POST" && url.pathname === "/api/segments") {
    return sendJson(res, 200, { ok: true, segment: await campaignEngine.saveSegment(await readJson(req)) });
  }

  if (req.method === "POST" && url.pathname === "/api/templates") {
    return sendJson(res, 200, { ok: true, template: await campaignEngine.saveTemplate(await readJson(req)) });
  }

  if (req.method === "POST" && url.pathname.match(/^\/api\/campaigns\/[^/]+\/snapshot$/)) {
    const id = decodeURIComponent(url.pathname.split("/")[3]);
    return sendJson(res, 200, await campaignEngine.snapshotCampaign(id));
  }

  if (req.method === "POST" && url.pathname.match(/^\/api\/campaigns\/[^/]+\/(pause|cancel)$/)) {
    const parts = url.pathname.split("/");
    return sendJson(res, 200, await campaignEngine.setStatus(decodeURIComponent(parts[3]), parts[4] === "pause" ? "paused" : "cancelled"));
  }

  if (req.method === "GET" && url.pathname.match(/^\/api\/campaigns\/[^/]+\/recipients$/)) {
    const id = decodeURIComponent(url.pathname.split("/")[3]);
    return sendJson(res, 200, { ok: true, recipients: await supabaseFetch(`/skinpara_campaign_recipients?campaign_id=eq.${encodeURIComponent(id)}&order=id.asc`) });
  }


  if (
    req.method ===
      "POST" &&
    url.pathname ===
      "/api/campaigns"
  ) {

    const input =
      await readJson(req);


    return sendJson(
      res,
      200,
      {
        ok:
          true,
        campaign:
          await saveCampaign(
            input
          )
      }
    );
  }


  if (
    req.method ===
      "GET" &&
    url.pathname.startsWith(
      "/api/campaigns/"
    ) &&
    url.pathname.endsWith(
      "/preview"
    )
  ) {

    const parts =
      url.pathname.split(
        "/"
      );


    const id =
      decodeURIComponent(
        parts[
          parts.length - 2
        ]
      );


    return sendJson(
      res,
      200,
      await previewCampaign(
        id
      )
    );
  }


  if (
    req.method ===
      "POST" &&
    url.pathname.startsWith(
      "/api/campaigns/"
    ) &&
    url.pathname.endsWith(
      "/execute"
    )
  ) {

    const parts =
      url.pathname.split(
        "/"
      );


    const id =
      decodeURIComponent(
        parts[
          parts.length - 2
        ]
      );


    const input =
      await readJson(req);


    return sendJson(
      res,
      200,
      await executeCampaign(
        id,
        input.confirm
      )
    );
  }


  if (
    req.method ===
      "GET" &&
    url.pathname.match(/^\/api\/analytics\/(overview|funnel|sales|delivery|campaigns|loyalty|ai|products|agents|operational)$/)
  ) {
    const kind = url.pathname.split("/").pop();
    return sendJson(res, 200, await analyticsService.report(kind, url.searchParams));
  }

  if (
    req.method ===
      "GET" &&
    url.pathname ===
      "/api/analytics"
  ) {

    return sendJson(
      res,
      200,
      await getAnalytics()
    );
  }


  if (
    req.method ===
      "GET" &&
    url.pathname.startsWith(
      "/api/wallet/"
    )
  ) {

    const id =
      Number(
        url.pathname
          .split("/")
          .pop()
      );


    return sendJson(
      res,
      200,
      {
        ok:
          true,

        contact_id:
          id,

        balance:
          await getWallet(
            id
          )
      }
    );
  }


  if (
    req.method ===
      "POST" &&
    url.pathname ===
      "/api/wallet/credit"
  ) {

    return sendJson(
      res,
      200,
      await creditWallet(
        await readJson(req)
      )
    );
  }


  if (
    req.method ===
      "POST" &&
    url.pathname ===
      "/api/referrals"
  ) {

    return sendJson(
      res,
      200,
      await registerReferral(
        await readJson(req)
      )
    );
  }




  // =========================================================
  // CUSTOMER 360 V2
  // =========================================================

  if (
    req.method ===
      "GET" &&
    url.pathname.startsWith(
      "/api/customer360-v2/"
    )
  ) {

    const contactId =
      Number(
        url.pathname
          .split("/")
          .pop()
      );


    return sendJson(
      res,
      200,
      await getCustomer360V2(
        contactId
      )
    );
  }

  return sendJson(
    res,
    404,
    {
      ok:
        false,
      error:
        "not_found"
    }
  );
}


// ============================================================
// DEFAULT AUTOMATIONS
// ============================================================

async function seedDefaults() {

  const current =
    await listAutomations();


  if (
    current.length > 0
  ) {

    return;
  }


  await saveAutomation({
    id:
      "new_lead_welcome",

    name:
      "New Lead Welcome",

    enabled:
      true,

    event_type:
      "new_lead",

    action_type:
      "customer_notification",

    action_config: {
      template: "welcome",
      delay_seconds: 0
    },

    message_category:
      "transactional",

    cooldown_seconds:
      31536000,

    mode:
      "dry-run"
  });


  await saveAutomation({
    id:
      "no_reply_followup",

    name:
      "No Reply Follow-up",

    enabled:
      true,

    // Schedule after the first eligible lead; a durable customer_replied event
    // cancels the pending follow-up instead of creating a new one.
    event_type:
      "new_lead",

    action_type:
      "no_reply_followup",

    action_config: {
      template: "no_reply",
      delay_seconds: 3600,
      max_followups: 1
    },

    message_category:
      "transactional",

    cooldown_seconds:
      86400,

    mode:
      "dry-run"
  });


  await saveAutomation({
    id:
      "pending_stock_internal",

    name:
      "Pending Stock Internal Reminder",

    enabled:
      true,

    event_type:
      "pending_stock",

    action_type:
      "internal_reminder",

    action_config: {
      delay_seconds: 10800,
      note: "Pending stock requires internal review"
    },

    message_category:
      "internal",

    cooldown_seconds:
      10800,

    mode:
      "dry-run"
  });

  await saveAutomation({
    id: "stock_confirmed_notification",
    name: "Stock Confirmed Customer Notification",
    enabled: true,
    event_type: "stock_confirmed",
    action_type: "customer_notification",
    action_config: { template: "stock_confirmed", delay_seconds: 0 },
    message_category: "transactional",
    cooldown_seconds: 86400,
    mode: "dry-run"
  });

  await saveAutomation({
    id: "delivered_pipeline",
    name: "Delivered Pipeline Hooks",
    enabled: true,
    event_type: "delivered",
    action_type: "delivered_pipeline",
    action_config: {
      hooks: ["thank_you", "customer360", "loyalty_eligibility", "referral_eligibility"]
    },
    message_category: "transactional",
    cooldown_seconds: 0,
    mode: "dry-run"
  });

  await saveAutomation({
    id: "delivered_reorder_schedule",
    name: "Delivered Reorder Scheduler",
    enabled: true,
    event_type: "delivered",
    action_type: "schedule_reorder",
    action_config: { default_reorder_days: 60 },
    message_category: "marketing",
    cooldown_seconds: 0,
    mode: "dry-run"
  });

  await saveAutomation({
    id: "disabled_rule_test",
    name: "Disabled Rule Safety Check",
    enabled: false,
    event_type: "new_lead",
    action_type: "audit_only",
    action_config: {},
    message_category: "internal",
    mode: "disabled"
  });
}


if (CHANNEL_MODE !== "kapso_direct") {
  await seedDefaults();
}


// Run enabled automations every minute.
// Defaults are disabled until manually enabled.

if (CHANNEL_MODE !== "kapso_direct") {
  setInterval(
    () => {

    runAutomations()
      .catch(
        error =>
          log(
            "automation_scheduler_error",
            {
              error:
                error.message
            }
          )
      );

  },
    60000
  );

// Durable campaign scheduler. It can only simulate while the live safety flag
// is false; paused/cancelled campaigns are never selected.
  setInterval(
    () => {
      campaignEngine.runDueCampaigns().catch(error =>
        log("campaign_scheduler_error", { error: error.message })
      );
    },
    60000
  );
}


// ============================================================
// SERVER
// ============================================================

const server =
  http.createServer(
    async (
      req,
      res
    ) => {

      try {

          

		if (
          req.method ===
          "OPTIONS"
        ) {

          res.writeHead(
            204,
            {
              "Cache-Control":
                "no-store"
            }
          );

          return res.end();
        }


        const url =
          new URL(
            req.url,
            `http://${req.headers.host || "localhost"}`
          );

    if (req.method === "POST" && url.pathname === "/api/telemetry") {
      try {
        const event = await readJson(req);
        const safeEvent = {
          timestamp: new Date().toISOString(),
          service: event.service || "unknown",
          severity: event.severity || "INFO",
          type: event.type || "unknown_event",
          message: event.message || "",
          metadata: typeof event.metadata === "object" ? event.metadata : {}
        };
        delete safeEvent.metadata.password;
        delete safeEvent.metadata.token;
        fs.appendFileSync(path.join(process.cwd(), "telemetry.jsonl"), JSON.stringify(safeEvent) + "\n");
        return sendJson(res, 200, { ok: true });
      } catch(err) {
        return sendJson(res, 400, { ok: false });
      }
    }


        if (
          url.pathname.startsWith(
            "/api/"
          )
        ) {

          return await handleApi(
            req,
            res,
            url
          );
        }


        const index =
          fs.readFileSync(
            path.join(
              PUBLIC_DIR,
              "index.html"
            ),
            "utf8"
          );


        return sendHtml(
          res,
          index
        );

      } catch (
        error
      ) {

        log(
          "request_error",
          {
            url:
              req.url,

            error:
              error.message
          }
        );


        return sendJson(
          res,
          500,
          {
            ok:
              false,

            error:
              error.message
          }
        );
      }
    }
  );


if (CHANNEL_MODE === "kapso_direct" && process.env.KAPSO_SANDBOX_OUTBOUND_ENABLED === "true") {
  directOutboundWorker().catch(error => console.error(`[KAPSO DIRECT] Outbound worker fatal: ${error.message}`));
}

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    log(
      "control_center_started",
      {
        port:
          PORT
      }
    );
  }
);







